/**
 * ResDiary sync orchestration + HTTP surface.
 *
 * Flows (design: agora-app docs/resdiary-integration-design.md):
 *   BACKFILL   day-walk Booking/{createdDate} from EarliestDate → today, then
 *              page Customers — pushing every batch to the Agora app's
 *              token-gated POST /api/resdiary/ingest (idempotent upserts).
 *   RECONCILE  BookingChange/{date} + CustomerChange/{date} for the last N
 *              days — the safety net under ResDiary's webhooks, normally
 *              pinged daily by the app's /api/cron/resdiary-reconcile.
 *
 * The cursor lives on the /data volume (resdiary-cursor.json) next to the
 * bridge's processed-sales.json — same survival rules. The app keeps its own
 * lastPollAt stamp; this file is the operative resume point for the walk.
 *
 * Everything here is DORMANT until the RESDIARY_* env is set; routes answer
 * 503 so the money-path bridge is never coupled to ResDiary config.
 */

const fs = require('node:fs');
const path = require('node:path');
const rd = require('./resdiary');
const { findBlockOnBoundary, spliceBlock, verifySplice, withCustomerLock } = require('./blob');

const DATA_DIR = process.env.DATA_DIR
  ? path.resolve(process.env.DATA_DIR)
  : path.resolve(process.cwd(), 'data');
const CURSOR_FILE = path.join(DATA_DIR, 'resdiary-cursor.json');
const RUNS_FILE = path.join(DATA_DIR, 'resdiary-runs.json');
const MAX_RUNS = 50;
// Seven days, not two: a booking's spend lands days after the visit (bill
// close, late edits), and 02-09-2026 showed a 2-day window silently missing
// everything older. Seven still costs under 10 minutes of API time.
const DEFAULT_RECONCILE_DAYS = 7;

const INGEST_CHUNK = 200; // rows per POST — well inside the app route's 300s budget
const MAX_WALK_DAYS = 4000; // hard sanity cap (~11 years) against a bad floor date

// ── Small pure helpers (unit-tested) ─────────────────────────────────

/** YYYY-MM-DD in UTC. */
function toDateIso(d) {
  return new Date(d).toISOString().slice(0, 10);
}

function addDaysIso(dateIso, days) {
  const [y, m, d] = dateIso.split('-').map(Number);
  return toDateIso(Date.UTC(y, m - 1, d + days));
}

/** Inclusive list of YYYY-MM-DD dates. Throws on inverted/absurd ranges. */
function listDatesInclusive(fromIso, toIso) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(fromIso) || !/^\d{4}-\d{2}-\d{2}$/.test(toIso)) {
    throw new Error(`Bad date range ${fromIso}..${toIso}`);
  }
  if (fromIso > toIso) throw new Error(`Backfill range inverted: ${fromIso} > ${toIso}`);
  const out = [];
  for (let d = fromIso; d <= toIso; d = addDaysIso(d, 1)) {
    out.push(d);
    if (out.length > MAX_WALK_DAYS) throw new Error(`Range ${fromIso}..${toIso} exceeds ${MAX_WALK_DAYS} days`);
  }
  return out;
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/**
 * A record the app may overwrite a booking with: it must carry a party size
 * and a visit instant. Anything thinner is a stub and would only blank fields.
 */
function looksLikeFullBooking(b) {
  if (!b || typeof b !== 'object') return false;
  const covers = b.PartySize ?? b.CoversBooked ?? b.Covers;
  const visit = b.VisitDateTime ?? b.VisitDate;
  return covers !== undefined && covers !== null && !!visit;
}

/** ResDiary list responses are either bare arrays or a paging envelope. */
function unwrapList(body) {
  if (Array.isArray(body)) return body;
  if (body && Array.isArray(body.Data)) return body.Data;
  return [];
}

/**
 * A BookingChange row is a MODIFICATION record, not a booking. Forwarding it
 * as one is how the 02-09-2026 reconcile blanked covers/reference/spend on
 * every booking touched in six days (the app's upsert overwrote good fields
 * with the stub's blanks). So the only thing a change row is trusted for is
 * the booking ID — the full record is always fetched with `Booking/{id}`.
 * Rows without any id are skipped, never forwarded.
 */
function classifyChangeRow(row) {
  if (!row || typeof row !== 'object') return { kind: 'skip' };
  const nested = row.Booking && typeof row.Booking === 'object' ? row.Booking : null;
  const id = row.BookingId ?? nested?.Id ?? nested?.BookingId ?? row.Id;
  if (id !== undefined && id !== null && id !== '') return { kind: 'id', id };
  return { kind: 'skip' };
}

/** A customer record we can safely forward — a CustomerChange stub with none
 *  of these carries nothing the app can use and could only blank things. */
function looksLikeFullCustomer(row) {
  if (!row || typeof row !== 'object') return false;
  return ['Email', 'EmailAddress', 'FirstName', 'Surname', 'LastName', 'Mobile', 'MobileNumber', 'Name']
    .some((k) => row[k] !== undefined && row[k] !== null && row[k] !== '');
}

/** Anything the API probe creates carries this in its email. */
const PROBE_EMAIL_PREFIX = 'zz-api-probe-';

/** A customer WE fabricated to test the vendor API is not a guest of this
 *  hotel. Without this the reconcile's CustomerChange walk ships it to
 *  /api/resdiary/ingest and a junk guest profile appears in production — one
 *  hop downstream of a route whose whole promise was that it touches nobody
 *  real. Marking it deleted in ResDiary afterwards does not retract that. */
function isProbeCustomer(cust) {
  return typeof cust?.EmailAddress === 'string' && cust.EmailAddress.startsWith(PROBE_EMAIL_PREFIX)
      || typeof cust?.Email === 'string' && cust.Email.startsWith(PROBE_EMAIL_PREFIX);
}

/**
 * The whole customer record, echoed back with the one field we mean to change.
 *
 * `PUT .../Customer/{id}/` is a WHOLE-RECORD replace: whatever this object
 * omits, ResDiary clears. The first version of this listed ten fields by hand,
 * which meant every note we appended silently wiped anything outside that list
 * — address, date of birth, company, language, tags, custom fields — on a real
 * diner's record. An adversarial review found it; it was live from 04-09-2026.
 *
 * So the rule is now inverted: echo back EVERY scalar the GET handed us, and
 * name only the handful we must not send. Objects and arrays are dropped
 * because a form-encoded PUT cannot carry them meaningfully — if ResDiary
 * turns out to hold something structured we care about, that is a finding for
 * the probe, not something to guess at here.
 *
 * The ten known-required fields are still defaulted explicitly, so a record
 * that comes back sparse still satisfies the validator ("The email address
 * must be supplied").
 */
const FORM_NEVER_SEND = new Set([
  'Comments',        // we always set this ourselves
  'Id', 'CustomerId', // identity lives in the URL
]);

function customerFormFrom(before, changes = {}) {
  const b = before && typeof before === 'object' ? before : {};
  const form = {
    Title: b.Title ?? '',
    FirstName: b.FirstName ?? '',
    Surname: b.Surname ?? '',
    Email: b.Email ?? b.EmailAddress ?? '',
    MobileCountryCode: b.MobileCountryCode ?? '',
    Mobile: b.Mobile ?? '',
    PhoneCountryCode: b.PhoneCountryCode ?? '',
    Phone: b.Phone ?? '',
    ReceiveEmailMarketing: b.ReceiveEmailMarketing ?? false,
    ReceiveSmsMarketing: b.ReceiveSmsMarketing ?? false,
  };
  for (const [k, v] of Object.entries(b)) {
    if (FORM_NEVER_SEND.has(k)) continue;
    if (k in form) continue;
    if (v === null || v === undefined) continue;
    const t = typeof v;
    if (t === 'string' || t === 'number' || t === 'boolean') form[k] = v;
  }

  // The nested Address, in the bracket notation the API actually accepts —
  // proven live 05-09-2026: sending `Address[Town]` came back as the diner's
  // Town. Before this it was dropped on every write, and because the PUT
  // replaces the whole record, every note we appended BLANKED the customer's
  // address. Six fields, each only sent when the GET gave us one, so a diner
  // with no address is not handed six empty strings.
  const addr = b.Address && typeof b.Address === 'object' ? b.Address : null;
  if (addr) {
    for (const key of ['House', 'Street', 'Town', 'Postcode', 'CountyRegion', 'Country']) {
      const v = addr[key];
      if (typeof v === 'string' && v !== '') form[`Address[${key}]`] = v;
    }
  }

  if (changes.comments !== undefined) form.Comments = changes.comments;
  // ── The flag is ALWAYS sent, and its value is the whole question ─────────
  // Omitting it does NOT mean "replace" — ResDiary defaults to appending, so
  // an omitted key silently appends (probe, 05-09-2026, verdict APPENDS).
  // Sending it explicitly false is what replaces the field. The earlier
  // reasoning here — "omit rather than send false, a stringified false is a
  // guess" — was exactly backwards and would have made edit and delete
  // impossible while looking like a vendor limitation.
  if (changes.appendComments !== undefined) {
    form['CustomerOptions[AppendComments]'] = changes.appendComments === true;
  }
  if (changes.vip !== undefined) form.IsVip = changes.vip;
  return form;
}

/** Which fields the GET returned that our PUT could not carry. Reported, never
 *  silently dropped — this is how we find out the form is losing data. */
function unsentScalarKeys(before, form) {
  const b = before && typeof before === 'object' ? before : {};
  return Object.keys(b).filter((k) => {
    const v = b[k];
    if (v === null || v === undefined) return false;
    const t = typeof v;
    if (t === 'string' || t === 'number' || t === 'boolean') return false;
    if (k in form) return false;
    // Address is carried field by field in bracket notation, so it is not lost
    // even though the key itself never appears in the form.
    if (k === 'Address' && Object.keys(form).some((f) => f.startsWith('Address['))) return false;
    return true;
  });
}

/**
 * Fields a real diner may carry that our PUT cannot carry back.
 *
 * The Customer PUT replaces the WHOLE record, so a field the form cannot send
 * is a field every write destroys. Address was one until bracket notation was
 * found (05-09-2026). `CustomerCodes` is the one left: it is an array, no
 * spelling tried carries it — `CustomerCodes[0]`, `[0][Code]`, `[0][Name]` all
 * answered 200 and ignored it — and there is no known way to send it.
 *
 * Today that costs nothing: 150 of 150 real diners sampled hold an empty
 * array. But "nobody uses this field yet" is a fact about this month, not about
 * the API, and the moment someone tags a diner in the ResDiary portal a note
 * write would silently wipe it.
 *
 * So a write REFUSES rather than destroying data it cannot preserve. Fails
 * closed, costs nothing while the field stays empty, and turns a silent loss
 * into a message the moment it would matter.
 */
function unpreservableFields(before) {
  const out = [];
  if (Array.isArray(before?.CustomerCodes) && before.CustomerCodes.length > 0) out.push('CustomerCodes');
  return out;
}

/**
 * What the append attempt actually told us.
 *
 * Status BEFORE content. NOT_WRITABLE is reserved for the vendor refusing;
 * everything we merely could not read is UNDETERMINED, because the whole point
 * of this probe is to stop us concluding "not entitled" from a signal that
 * meant something else — which is exactly what happened with Reviews.
 */
function verdictOf(r) {
  if (!r) return { verdict: 'UNDETERMINED_TRANSPORT', note: 'The call never completed — a block or a timeout, not an answer.' };
  const s = r.status;
  if (s === 401 || s === 403) return { verdict: 'REFUSED', note: `The API refused with ${s} — this account is not entitled to write a customer.` };
  if (s === 404) return { verdict: 'PATH_OR_ID_WRONG', note: 'A 404 is the route or the id, not a refusal.' };
  if (typeof s === 'number' && s >= 400) return { verdict: `REJECTED_${s}`, note: 'The write was rejected — read the body; a 400 usually means the PUT wants the whole record.' };

  const comments = typeof r.body?.Comments === 'string' ? r.body.Comments : null;
  if (comments === null) {
    return { verdict: 'UNDETERMINED_NO_BODY', note: `The write returned ${s} but no readable Comments, so whether it appended cannot be told from here. Check the customer in the diary.` };
  }
  const keptFirst = comments.includes('first note');
  const gotSecond = comments.includes('appended note');
  if (gotSecond && keptFirst) return { verdict: 'APPEND_WORKS', comments, note: 'Both notes survived — AppendComments does what the docs say.' };
  if (gotSecond) return { verdict: 'WRITE_WORKS_BUT_REPLACES', comments, note: 'The write landed but the earlier note was lost — read-modify-write is required.' };
  return { verdict: 'WRITE_IGNORED', comments, note: `The API answered ${s} but neither note is present — Comments looks read-only, as it is on a booking.` };
}

function classifyCustomerRow(row) {
  if (!row || typeof row !== 'object') return null;
  const cust = row.Customer && typeof row.Customer === 'object' ? row.Customer : row;
  if (isProbeCustomer(cust)) return null;
  return looksLikeFullCustomer(cust) ? cust : null;
}

// ── Cursor ────────────────────────────────────────────────────────────

function readCursor() {
  try {
    if (fs.existsSync(CURSOR_FILE)) return JSON.parse(fs.readFileSync(CURSOR_FILE, 'utf-8'));
  } catch (err) {
    console.warn('[resdiary-sync] cursor read failed:', err.message);
  }
  return null;
}

function writeCursor(cursor) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(CURSOR_FILE, JSON.stringify(cursor, null, 2));
  } catch (err) {
    console.warn('[resdiary-sync] cursor write failed:', err.message);
  }
}

// ── Run history (volume-persisted, newest first) ─────────────────────
// The answer to "did it run, and what did it do?" without opening Railway
// logs — surfaced in /resdiary/status and pushed to the app's health page.

function readRuns() {
  try {
    if (fs.existsSync(RUNS_FILE)) {
      const list = JSON.parse(fs.readFileSync(RUNS_FILE, 'utf-8'));
      return Array.isArray(list) ? list : [];
    }
  } catch (err) {
    console.warn('[resdiary-sync] runs read failed:', err.message);
  }
  return [];
}

/** Pure: newest first, capped. */
function pushRun(list, run) {
  return [run, ...(Array.isArray(list) ? list : [])].slice(0, MAX_RUNS);
}

function recordRun(run) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(RUNS_FILE, JSON.stringify(pushRun(readRuns(), run), null, 2));
  } catch (err) {
    console.warn('[resdiary-sync] runs write failed:', err.message);
  }
  postRunSummary(run).catch((err) => console.warn('[resdiary-sync] run summary push failed:', err.message));
}

// ── Push to the Agora app ────────────────────────────────────────────

function appTarget() {
  const url = (process.env.AGORA_APP_URL || 'https://theagorahotel.app').replace(/\/$/, '');
  const token = process.env.RESDIARY_INGEST_TOKEN || '';
  return { url: `${url}/api/resdiary/ingest`, token };
}

function ingestConfigured() {
  return !!appTarget().token;
}

/** POST one batch to the app. Two retries; throws when the app keeps failing. */
async function postToApp({ bookings = [], customers = [] }) {
  if (!bookings.length && !customers.length) return { bookings: 0, customers: 0 };
  const { url, token } = appTarget();
  if (!token) throw new Error('RESDIARY_INGEST_TOKEN not set — cannot push to the app');

  let lastErr = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    if (attempt > 0) await new Promise((r) => setTimeout(r, 5000 * attempt));
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ bookings, customers }),
        signal: AbortSignal.timeout(120_000),
      });
      if (!res.ok) throw new Error(`app ingest HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
      const out = await res.json();
      return { bookings: out.bookings ?? 0, customers: out.customers ?? 0 };
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr;
}

/** Best-effort: tell the app how a run went (its Settings → ResDiary page). */
async function postRunSummary(run) {
  const { url, token } = appTarget();
  if (!token) return;
  const res = await fetch(url.replace(/\/ingest$/, '/runs'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify(run),
    signal: AbortSignal.timeout(20_000),
  });
  if (!res.ok) throw new Error(`app runs HTTP ${res.status}`);
}

// ── Sync state (single-flight) ───────────────────────────────────────

const state = {
  running: false,
  phase: null, // 'bookings' | 'customers' | 'reconcile'
  startedAt: null,
  finishedAt: null,
  lastError: null,
  counts: { days: 0, bookings: 0, customers: 0 },
};

async function runBackfill({ from, to, trigger = 'manual' } = {}) {
  if (state.running) throw new Error(`A ResDiary sync is already running (phase=${state.phase})`);
  if (!rd.isConfigured()) throw new Error('ResDiary not fully configured (creds + deployment/provider ids)');
  if (!ingestConfigured()) throw new Error('RESDIARY_INGEST_TOKEN not set');

  state.running = true;
  state.phase = 'bookings';
  state.startedAt = new Date().toISOString();
  state.finishedAt = null;
  state.lastError = null;
  state.counts = { days: 0, bookings: 0, customers: 0 };
  const run = { kind: 'backfill', trigger, startedAt: state.startedAt, finishedAt: null, ok: false, error: null, counts: null };

  try {
    const cursor = readCursor() || {};
    const today = toDateIso(Date.now());

    let floor = from || cursor.floor;
    if (!floor) {
      const earliest = await rd.getEarliestBookingDate();
      const raw = typeof earliest === 'string' ? earliest : earliest?.Date || earliest?.EarliestDate || earliest?.BookingDate;
      const parsed = raw ? String(raw).slice(0, 10) : null;
      if (!parsed || !/^\d{4}-\d{2}-\d{2}$/.test(parsed)) {
        throw new Error(`Could not read EarliestDate from response: ${JSON.stringify(earliest).slice(0, 200)}`);
      }
      floor = parsed;
    }

    const end = to || today;
    const resumeFrom = cursor.lastDateDone && cursor.lastDateDone >= floor ? addDaysIso(cursor.lastDateDone, 1) : floor;
    console.log(`[resdiary-sync] backfill bookings ${resumeFrom}..${end} (floor ${floor})`);

    if (resumeFrom <= end) {
      for (const day of listDatesInclusive(resumeFrom, end)) {
        const bookings = unwrapList(await rd.getBookingsForDate(day));
        for (const batch of chunk(bookings, INGEST_CHUNK)) {
          const out = await postToApp({ bookings: batch });
          state.counts.bookings += out.bookings;
        }
        state.counts.days++;
        writeCursor({ ...cursor, floor, lastDateDone: day, updatedAt: new Date().toISOString() });
        cursor.lastDateDone = day;
      }
    }

    // Customers: full page walk from the floor date. Idempotent, so a re-run
    // that overlaps is harmless; resume from the last completed page.
    state.phase = 'customers';
    let page = (cursor.customersPageDone || 0) + 1;
    console.log(`[resdiary-sync] backfill customers from page ${page}`);
    for (;;) {
      const body = await rd.getCustomersPage(floor, page, 100);
      const rows = unwrapList(body).map(classifyCustomerRow).filter(Boolean);
      if (rows.length) {
        const out = await postToApp({ customers: rows });
        state.counts.customers += out.customers;
      }
      writeCursor({ ...cursor, floor, customersPageDone: page, updatedAt: new Date().toISOString() });
      cursor.customersPageDone = page;
      const totalPages = body?.TotalPages ?? (rows.length < 100 ? page : page + 1);
      if (page >= totalPages || rows.length === 0) break;
      page++;
    }

    writeCursor({ ...cursor, floor, done: true, completedAt: new Date().toISOString() });
    console.log(`[resdiary-sync] backfill complete:`, state.counts);
    run.ok = true;
    return { ...state.counts };
  } catch (err) {
    state.lastError = err.message;
    run.error = err.message;
    throw err;
  } finally {
    state.running = false;
    state.phase = null;
    state.finishedAt = new Date().toISOString();
    run.finishedAt = state.finishedAt;
    run.counts = { ...state.counts };
    recordRun(run);
  }
}

async function runReconcile({ days = DEFAULT_RECONCILE_DAYS, trigger = 'manual' } = {}) {
  if (state.running) throw new Error(`A ResDiary sync is already running (phase=${state.phase})`);
  if (!rd.isConfigured()) throw new Error('ResDiary not fully configured (creds + deployment/provider ids)');
  if (!ingestConfigured()) throw new Error('RESDIARY_INGEST_TOKEN not set');

  const n = Math.min(Math.max(parseInt(days, 10) || DEFAULT_RECONCILE_DAYS, 1), 30);
  state.running = true;
  state.phase = 'reconcile';
  state.startedAt = new Date().toISOString();
  state.lastError = null;

  const counts = { days: n, bookings: 0, customers: 0, fetched: 0, skippedStubs: 0 };
  state.counts = counts; // live progress in /resdiary/status
  const run = { kind: 'reconcile', trigger, startedAt: state.startedAt, finishedAt: null, ok: false, error: null, counts };
  try {
    const today = toDateIso(Date.now());
    for (let i = n - 1; i >= 0; i--) {
      const day = addDaysIso(today, -i);

      // Change rows → the SET of booking ids touched that day → full records.
      const changeRows = unwrapList(await rd.getBookingChanges(day));
      const ids = new Set();
      for (const row of changeRows) {
        const c = classifyChangeRow(row);
        if (c.kind === 'id') ids.add(String(c.id));
        else counts.skippedStubs++;
      }
      const bookings = [];
      for (const id of ids) {
        try {
          const full = await rd.getBookingById(id);
          if (looksLikeFullBooking(full)) {
            bookings.push(full);
            counts.fetched++;
          } else {
            counts.skippedStubs++;
            console.warn(`[resdiary-sync] booking ${id}: fetched record has no covers/visit — not forwarded`);
          }
        } catch (err) {
          console.warn(`[resdiary-sync] booking ${id} fetch failed:`, err.message);
        }
      }
      for (const batch of chunk(bookings, INGEST_CHUNK)) {
        const out = await postToApp({ bookings: batch });
        counts.bookings += out.bookings;
      }

      const custRows = unwrapList(await rd.getCustomerChanges(day)).map(classifyCustomerRow).filter(Boolean);
      for (const batch of chunk(custRows, INGEST_CHUNK)) {
        const out = await postToApp({ customers: batch });
        counts.customers += out.customers;
      }
    }
    console.log('[resdiary-sync] reconcile done:', counts);
    run.ok = true;
    return counts;
  } catch (err) {
    state.lastError = err.message;
    run.error = err.message;
    throw err;
  } finally {
    state.running = false;
    state.phase = null;
    state.finishedAt = new Date().toISOString();
    run.finishedAt = state.finishedAt;
    recordRun(run);
  }
}

// ── HTTP surface ─────────────────────────────────────────────────────

/**
 * Auth: either the existing bridge admin header (X-Bridge-Token) or the
 * bearer the Vercel cron presents (RESDIARY_WORKER_TOKEN). Constant-time.
 */
function authorized(req) {
  const { timingSafeEqual } = require('node:crypto');
  const safeEq = (a, b) => {
    const ba = Buffer.from(String(a), 'utf8');
    const bb = Buffer.from(String(b), 'utf8');
    return ba.length === bb.length && timingSafeEqual(ba, bb);
  };
  const admin = process.env.BRIDGE_ADMIN_TOKEN || '';
  const worker = process.env.RESDIARY_WORKER_TOKEN || '';
  const xToken = req.get('X-Bridge-Token') || '';
  const bearer = (req.get('Authorization') || '').replace(/^Bearer\s+/i, '');
  if (admin && xToken && safeEq(admin, xToken)) return true;
  if (worker && bearer && safeEq(worker, bearer)) return true;
  return false;
}

/** The addresses sent to ResDiary for whitelisting on 04-08-2026, never
 *  acknowledged. Kept in code so /resdiary/egress can say whether what leaves
 *  today is still what they were asked to allow. */
const WHITELIST_SENT = ['162.220.232.250', '152.55.176.240', '162.220.232.252'];

// A plain browser-ish agent for the IP echoes — the ResDiary one names an
// integration and has no business in another service's logs.
const USER_AGENT_FOR_PROBE = 'AgoraPosToFolio/1.0 (egress-check)';

function getStatus() {
  return {
    configured: {
      creds: rd.hasCreds(),
      ids: rd.isConfigured(),
      ingestToken: ingestConfigured(),
      appUrl: appTarget().url,
    },
    micrositeName: process.env.RESDIARY_MICROSITE_NAME || null,
    tokenCached: rd.hasCreds() ? rd.tokenCached() : false,
    cursor: readCursor(),
    sync: { ...state },
    runs: readRuns().slice(0, 20),
  };
}

/** Mount /resdiary/* on the bridge's Express app. Additive only. */
function registerResdiaryRoutes(app) {
  app.get('/resdiary/status', (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    res.json(getStatus());
  });

  /**
   * What IP address ResDiary actually sees when this service calls out.
   *
   * The whitelist request named three addresses and was never acknowledged, so
   * two things are unknown at once: whether ResDiary added them, and whether
   * they are still OUR addresses. A platform can rotate egress without saying
   * so, and then a whitelist that was set up correctly points at nothing.
   *
   * This separates those. Run it, compare with what was sent, and the answer is
   * either "they never added it" or "they added the wrong thing" — which need
   * very different emails.
   */
  app.get('/resdiary/egress', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    // Two independent echoes: one of them being down must not read as an
    // answer, and two agreeing is worth more than one asserting.
    const probes = ['https://api.ipify.org?format=json', 'https://ifconfig.co/json'];
    const seen = [];
    for (const url of probes) {
      try {
        const r = await fetch(url, { headers: { 'User-Agent': USER_AGENT_FOR_PROBE }, signal: AbortSignal.timeout(10_000) });
        const j = await r.json();
        const ip = j.ip || j.address || null;
        if (ip) seen.push({ via: new URL(url).host, ip });
      } catch (err) {
        seen.push({ via: new URL(url).host, error: err.message });
      }
    }
    const ips = [...new Set(seen.map((s) => s.ip).filter(Boolean))];
    res.json({
      egressIps: ips,
      probes: seen,
      whitelistRequested: WHITELIST_SENT,
      // Only ever a hint — one request leaves through one of a pool, so a
      // mismatch on a single call is not proof the pool changed.
      matchesRequested: ips.length ? ips.every((ip) => WHITELIST_SENT.includes(ip)) : null,
      note: 'One call leaves via one address. Run it a few times to see the whole pool.',
    });
  });

  // Discovery: CurrentUser + Restaurants — run this the moment ResDiary
  // confirms the IP whitelist; the response carries providerId/deploymentId/
  // micrositeName for the env config.
  /**
   * POST /resdiary/customer-note-probe — can we write a guest note, or not?
   *
   * ResDiary's docs say `PUT .../Customer/{customerId}/` takes `Comments` and a
   * `CustomerOptions[AppendComments]` flag. Docs are not entitlement: the
   * Reviews endpoint was documented for us too and answered 404 for a month.
   * This settles it against the live API, from the whitelisted IP.
   *
   * ── The rules it obeys, each one earned ────────────────────────────────
   * 1. It creates its own throwaway customer and NEVER writes to an id it
   *    cannot prove is that customer. A create that fails can still answer with
   *    an EXISTING customer's id (a duplicate-email envelope is the usual
   *    shape), and blindly trusting it would put a junk note on a real diner
   *    and mark them for deletion. So the create must be 2xx AND echo our own
   *    marker back before anything else runs.
   * 2. A missing or unreadable body is UNDETERMINED, never "not writable". An
   *    empty 200 is the normal answer to an update; calling that a refusal
   *    would be the loudest possible lie.
   * 3. The verdict reads the STATUS before the content, so a 403, a 404, a 400
   *    and a network failure are four different answers rather than one.
   * 4. Cleanup runs in a `finally` AND its result is in the response, because a
   *    report that cannot say whether it tidied up is not a report. It also
   *    goes to the log, so the answer survives even if the caller drops it.
   * 5. The throwaway is filtered out of the reconcile by `isProbeCustomer`, so
   *    it can never reach the app's guest table.
   */
  /**
   * POST /resdiary/customer-note — write a note onto a ResDiary guest.
   *
   * Body: { customerId, note, vip?: boolean }
   *
   * Proven against the live API on 04-09-2026 (see /resdiary/customer-note-probe):
   * `PUT .../Customer/{id}/` is a WHOLE-RECORD replace — send only `Comments`
   * and it answers 400 "The email address must be supplied" — but
   * `CustomerOptions[AppendComments]=true` genuinely appends, CRLF-separated.
   *
   * So: read the record, send it back with the note appended. The read is not
   * optional and there is no fallback to "send what the caller thinks the
   * customer looks like" — a stale name or email from our side would overwrite
   * ResDiary's copy, and a note is never worth doing that for.
   *
   * `vip` is only sent when the caller passes it, because omitting a field on a
   * whole-record PUT is how you silently clear it.
   */
  app.post('/resdiary/customer-note', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.hasCreds()) return res.status(503).json({ error: 'resdiary_creds_not_configured' });

    const site = rd.micrositeName();
    if (!site) return res.status(503).json({ error: 'RESDIARY_MICROSITE_NAME not set' });

    const { customerId, note, vip } = req.body || {};
    if (!customerId || !String(note || '').trim()) {
      return res.status(400).json({ error: 'expected { customerId, note }' });
    }
    if (vip !== undefined && typeof vip !== 'boolean') {
      return res.status(400).json({ error: 'vip must be a boolean when supplied' });
    }

    try {
      // 1. Read. Whatever comes back is what gets sent back.
      let before;
      try {
        before = await rd.getCustomerById(customerId, site);
      } catch (err) {
        return res.status(502).json({
          error: `could not read customer ${customerId}: ${err.message}`,
          hint: 'Update Customer replaces the whole record, so it is not safe to write without reading first.',
        });
      }
      if (!before || typeof before !== 'object' || !before.Id) {
        return res.status(404).json({ error: `customer ${customerId} not found` });
      }

      const unpreservable = unpreservableFields(before);
      if (unpreservable.length) {
        return res.status(409).json({
          ok: false, reason: 'would_clear_fields', fields: unpreservable, customerId,
          error: `This diner carries ${unpreservable.join(', ')}, which this API gives us no way to send back — writing the note would erase it. Nothing was written. Add the note in ResDiary instead.`,
        });
      }

      // 2. Send it back, with the note appended and nothing else disturbed.
      //
      // Appending to an EMPTY box is a plain set, not an append. ResDiary's
      // append is unconditionally `existing + CRLF + new`, so appending into an
      // empty Comments yields a leading blank line — every diner's very first
      // note would start their comments box with one (observed on a real diner,
      // 05-09-2026). Sending the flag false when there is nothing to append to
      // writes the note on its own, which is what "add the first note" means.
      const existingComments = typeof before.Comments === 'string' ? before.Comments : '';
      const form = customerFormFrom(before, {
        comments: String(note).trim(),
        appendComments: existingComments.trim() !== '',
        ...(vip === undefined ? {} : { vip }),
      });

      const put = await rd.rdSend('PUT', `/api/ConsumerApi/v1/Restaurant/${encodeURIComponent(site)}/Customer/${customerId}/`, form);

      if (!put.ok) {
        return res.status(502).json({
          error: `ResDiary refused the write (HTTP ${put.status})`,
          status: put.status,
          body: put.body,
        });
      }

      // 3. Prove it from the response rather than assuming the 200 meant it.
      //
      // Boundary-matched, not `String.includes`: a note that is a substring of
      // a line already in the blob would otherwise report as landed without
      // having been written, and the reverse mistake — see verifySplice — turns
      // a good additive edit into a false failure the user re-ticks.
      const after = typeof put.body?.Comments === 'string' ? put.body.Comments : null;
      const landed = after === null ? null : findBlockOnBoundary(after, String(note).trim()).length >= 1;
      return res.json({
        ok: landed === true,
        customerId,
        // null means the write went out and the answer carried no Comments —
        // UNDETERMINED. The caller must not read that as a failure and retry.
        landed,
        commentsBefore: before.Comments ?? null,
        commentsAfter: after,
        droppedFields: unsentScalarKeys(before, form),
        ...(vip === undefined ? {} : { vipRequested: vip, vipAfter: put.body?.IsVip ?? null }),
        ...(landed ? {} : { note: 'The write returned 200 but the note is not in the response — check the diner in ResDiary before trusting it.' }),
      });
    } catch (err) {
      const blocked = err instanceof rd.CloudflareBlockedError;
      return res.status(blocked ? 502 : 500).json({
        error: err.message,
        ...(blocked ? { hint: 'This egress IP is not whitelisted by ResDiary.' } : {}),
      });
    }
  });

  /**
   * POST /resdiary/customer-note/replace — change or remove ONE line of a
   * diner's comments.
   *
   * Body: { customerId, previousText, newText? }   // newText null/absent = remove
   *
   * The append route can only add. This is what makes a note in the app
   * editable and deletable in the diary — and it is the dangerous one, because
   * the Comments field is shared with restaurant staff who type into the
   * ResDiary portal and have no idea this app exists.
   *
   * ── The one rule ──────────────────────────────────────────────────────
   * The only bytes changed are bytes proved to be there, matched WHOLE-LINE,
   * in the blob ResDiary handed back milliseconds earlier. Everything else is
   * copied verbatim. Zero matches and two matches both write NOTHING — they
   * are answers, not errors: the line has been reworded, or the same words
   * appear twice and choosing between them is not ours to do.
   *
   * ── Why it lives here and not in the app ──────────────────────────────
   * There is no ETag, RowVersion or Last-Modified on a ResDiary customer, so
   * the read-write window cannot be closed — only made small. Read and write
   * back to back in one handler is ~300ms; the app doing it across two network
   * hops is seconds, and that window is exactly when a manager's portal edit
   * gets destroyed. `withCustomerLock` then stops us racing ourselves: two
   * notes actioned from one guest profile would otherwise interleave as
   * read-A, read-B, write-A, write-B, and write-B — computed from a blob that
   * predates write-A — would silently reinstate the line write-A removed.
   *
   * ── Why it never retries ──────────────────────────────────────────────
   * `rdSend` retries 429/5xx up to four times with a precomputed body. On this
   * path each retry would re-PUT a blob derived from a read that may now be
   * minutes stale, so the PUT goes out with maxAttempts 1. A write we cannot
   * confirm comes back as landed:null, and the caller must ask a human rather
   * than repeat it — a false negative on a delete removes a line the
   * restaurant had since re-added.
   */
  /**
   * GET /resdiary/customer/:id — one diner's record, verbatim.
   *
   * Read-only. It exists because the Customer PUT replaces the WHOLE record, so
   * "what fields does a real diner actually carry, and which of them can our
   * form carry back" is a question that has to be answerable without writing
   * anything. The hand-picked ten-field form was silently clearing address,
   * date of birth and tags on every write until this could be asked.
   *
   * ?fields=1 returns only the SHAPE — key names and types, with the values
   * stripped — so the field inventory can be read without pulling a real
   * diner's personal data through logs and terminals.
   */
  app.get('/resdiary/customer/:id', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.hasCreds()) return res.status(503).json({ error: 'resdiary_creds_not_configured' });
    const site = rd.micrositeName();
    if (!site) return res.status(503).json({ error: 'RESDIARY_MICROSITE_NAME not set' });

    try {
      const cust = await rd.getCustomerById(req.params.id, site);
      if (!cust || typeof cust !== 'object' || !cust.Id) {
        return res.status(404).json({ error: `customer ${req.params.id} not found` });
      }
      if (req.query.fields) {
        const shape = {};
        for (const [k, v] of Object.entries(cust)) {
          shape[k] = v === null ? 'null'
            : Array.isArray(v) ? `array[${v.length}]${v.length ? ':' + JSON.stringify(v[0]) : ''}`
            : typeof v === 'object' ? `object{${Object.keys(v).join(',')}}`
            : typeof v;
        }
        const form = customerFormFrom(cust, { comments: '' });
        return res.json({ id: cust.Id, shape, formKeys: Object.keys(form), dropped: unsentScalarKeys(cust, form) });
      }
      return res.json(cust);
    } catch (err) {
      const blocked = err instanceof rd.CloudflareBlockedError;
      return res.status(blocked ? 502 : 500).json({
        error: err.message,
        ...(blocked ? { hint: 'This egress IP is not whitelisted by ResDiary.' } : {}),
      });
    }
  });

  app.post('/resdiary/customer-note/replace', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.hasCreds()) return res.status(503).json({ error: 'resdiary_creds_not_configured' });

    const site = rd.micrositeName();
    if (!site) return res.status(503).json({ error: 'RESDIARY_MICROSITE_NAME not set' });

    const { customerId, previousText, newText } = req.body || {};
    if (!customerId || !String(previousText || '').trim()) {
      return res.status(400).json({ error: 'expected { customerId, previousText, newText? }' });
    }
    const removing = newText === undefined || newText === null || String(newText).trim() === '';
    const target = removing ? null : String(newText).trim();
    const needle = String(previousText);

    try {
      return await withCustomerLock(customerId, async () => {
        // 1. Read. The blob we splice is the one ResDiary has right now, never
        //    our mirror — the mirror can be days old.
        let before;
        try {
          before = await rd.getCustomerById(customerId, site);
        } catch (err) {
          return res.status(502).json({
            error: `could not read customer ${customerId}: ${err.message}`,
            hint: 'Update Customer replaces the whole record, so it is not safe to write without reading first.',
          });
        }
        if (!before || typeof before !== 'object' || !before.Id) {
          return res.status(404).json({ reason: 'customer_not_found', error: `customer ${customerId} not found` });
        }

        const unpreservable = unpreservableFields(before);
        if (unpreservable.length) {
          return res.status(409).json({
            ok: false, reason: 'would_clear_fields', fields: unpreservable, customerId,
            error: `This diner carries ${unpreservable.join(', ')}, which this API gives us no way to send back — changing the note would erase it. Nothing was written. Edit it in ResDiary instead.`,
          });
        }

        const blob = typeof before.Comments === 'string' ? before.Comments : '';
        const hits = findBlockOnBoundary(blob, needle);

        // 2. Refuse rather than guess. Both of these write nothing at all.
        if (hits.length === 0) {
          return res.status(200).json({
            ok: false,
            reason: 'not_found',
            customerId,
            commentsBefore: blob,
            note: 'That line is not in the diner\'s comments as we last saw it — someone changed it in ResDiary. Nothing was written.',
          });
        }
        if (hits.length > 1) {
          return res.status(409).json({
            ok: false,
            reason: 'ambiguous',
            occurrences: hits.length,
            customerId,
            commentsBefore: blob,
            note: 'Those words appear more than once on this diner, so which one to change is not ours to guess. Nothing was written.',
          });
        }

        // 3. Surgery on the string we just read, then the whole record back.
        const spliced = spliceBlock(blob, hits[0], target);
        // appendComments FALSE — the flag is what makes this a replace. Omitted,
        // ResDiary appends and the splice would double the blob instead of
        // rewriting it.
        const form = customerFormFrom(before, { comments: spliced, appendComments: false });
        const put = await rd.rdSend(
          'PUT',
          `/api/ConsumerApi/v1/Restaurant/${encodeURIComponent(site)}/Customer/${encodeURIComponent(customerId)}/`,
          form,
          { maxAttempts: 1 },
        );

        if (!put.ok) {
          return res.status(502).json({
            ok: false, reason: 'refused',
            error: `ResDiary refused the write (HTTP ${put.status})`,
            status: put.status, body: put.body, commentsBefore: blob,
          });
        }

        // 4. Judge it with the same matcher that cut it.
        // A CLEARED Comments comes back as null rather than "" — so on a delete
        // that empties the field, null is the SUCCESS answer, not the
        // unreadable-body answer. Distinguishing them matters: treating a
        // successful clear as undetermined would tell staff to go and check a
        // diner that is already correct.
        const raw = put.body?.Comments;
        const emptied = raw === null && removing;
        const after = typeof raw === 'string' ? raw : emptied ? '' : null;
        const landed = verifySplice(after, needle, target);
        return res.json({
          ok: landed === true,
          action: removing ? 'deleted' : 'edited',
          customerId,
          landed,
          commentsBefore: blob,
          commentsAfter: after,
          droppedFields: unsentScalarKeys(before, form),
          ...(landed === null
            ? { note: 'The write returned 200 but echoed no Comments, so what happened cannot be told from here. Do NOT retry — open the diner in ResDiary and look.' }
            : {}),
        });
      });
    } catch (err) {
      const blocked = err instanceof rd.CloudflareBlockedError;
      return res.status(blocked ? 502 : 500).json({
        error: err.message,
        ...(blocked ? { hint: 'This egress IP is not whitelisted by ResDiary.' } : {}),
      });
    }
  });

  /**
   * POST /resdiary/booking-write-probe — is a booking's note writable AT ALL?
   *
   * Body: { reference, bookingId }  — a REAL booking, ideally cancelled.
   *
   * ── What is already settled, so this does not re-ask it ────────────────
   * On the EPOS product (03-09-2026, sandbox): `PUT {EPOS}/Restaurant/{id}/
   * Booking/{id}` exists and APPLIES changes — Covers 2→3 landed — but
   * `Comments` is silently ignored on that same call, and
   * `PUT …/Booking/{id}/Comments` is a 404. So EPOS cannot write the note.
   *
   * The CONSUMER API's booking-update path has never been tested, and that is
   * the whole question here. It is the product that turned out to carry the
   * CUSTOMER note write after the docs implied otherwise, so "one product
   * refuses" is not an answer about the other.
   *
   * ── Why this writes nothing ────────────────────────────────────────────
   * Every attempt sends an EMPTY body. On a whole-record endpoint that fails
   * validation before anything is applied — which is precisely how the EPOS
   * probe identified the route (400 with a ValidationErrors array naming
   * Covers and Tables). So the answers available are:
   *   404 → no such route          405 → wrong verb for a route that exists
   *   400 → ROUTE EXISTS, body rejected (the signal we want)
   *   403 → route exists, not entitled
   * A real reference is required for the same reason: a made-up one answers
   * 404 for "no such booking", which is indistinguishable from "no such route"
   * — the exact ambiguity that cost a month on Reviews.
   */
  app.post('/resdiary/booking-write-probe', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.hasCreds()) return res.status(503).json({ error: 'resdiary_creds_not_configured' });
    const site = rd.micrositeName();
    if (!site) return res.status(503).json({ error: 'RESDIARY_MICROSITE_NAME not set' });

    const { reference, bookingId } = req.body || {};
    if (!reference && !bookingId) return res.status(400).json({ error: 'expected { reference, bookingId } — a REAL booking' });

    const base = `/api/ConsumerApi/v1/Restaurant/${encodeURIComponent(site)}`;
    const keys = [
      ...(reference ? [{ kind: 'reference', value: String(reference) }] : []),
      ...(bookingId ? [{ kind: 'bookingId', value: String(bookingId) }] : []),
    ];

    const attempts = [];
    for (const key of keys) {
      const k = encodeURIComponent(key.value);
      const candidates = [
        { method: 'GET',   path: `${base}/Booking/${k}` },
        { method: 'PUT',   path: `${base}/Booking/${k}` },
        { method: 'PATCH', path: `${base}/Booking/${k}` },
        { method: 'POST',  path: `${base}/Booking/${k}` },
        { method: 'PUT',   path: `${base}/Booking/${k}/Comments` },
        { method: 'POST',  path: `${base}/Booking/${k}/Comment` },
        { method: 'PUT',   path: `${base}/BookingReference/${k}` },
      ];
      for (const c of candidates) {
        try {
          // Empty body throughout — nothing here can apply a change.
          const r = c.method === 'GET'
            ? await rd.rdGet(c.path).then((body) => ({ status: 200, body }), (err) => ({ status: err?.status ?? 0, body: err?.message }))
            : await rd.rdSend(c.method, c.path, {}, { maxAttempts: 1 });
          const body = typeof r.body === 'string' ? r.body.slice(0, 220) : JSON.stringify(r.body ?? null).slice(0, 220);
          attempts.push({ key: key.kind, method: c.method, path: c.path.replace(base, ''), status: r.status, body });
        } catch (err) {
          attempts.push({ key: key.kind, method: c.method, path: c.path.replace(base, ''), status: null, body: String(err.message).slice(0, 160) });
        }
      }
    }

    const routeExists = attempts.filter((a) => a.status === 400 || a.status === 200);
    return res.json({
      site,
      verdict: routeExists.length ? 'ROUTE_CANDIDATES_FOUND' : 'NO_ROUTE_RESPONDED',
      note: routeExists.length
        ? 'A 400 or 200 means the route EXISTS. Read the bodies: a validation list names what a real write must send.'
        : 'Every candidate answered 404/405 — the Consumer API has no booking-update path under these spellings.',
      routeExists,
      attempts,
    });
  });

  /**
   * POST /resdiary/booking-note-probe — can the booking's note be WRITTEN?
   *
   * Body: { reference }  — a REAL booking. Use a CANCELLED one with no note.
   *
   * Route existence is already settled (`/resdiary/booking-write-probe`):
   * `PUT .../Booking/{reference}` answers 400 "The 'request' parameter must be
   * specified", i.e. it exists and wants JSON, and it keys on the REFERENCE —
   * `GET .../Booking/{bookingId}` throws while the reference reads 200.
   *
   * This asks the only remaining question: does a note sent that way STICK?
   * EPOS answered 200 and silently ignored `Comments` while applying `Covers`
   * from the same payload, so a 200 here proves nothing on its own — every
   * attempt is read back and compared.
   *
   * Two body shapes are tried because the 400 is ambiguous about which it
   * wants: the record on its own, and wrapped as `{ request: … }`. And two
   * field names, because a booking payload carries BOTH `SpecialRequests` and
   * `Comments` and nothing says which one the diary screen shows.
   *
   * Restores the original value in a `finally`, whatever happens.
   */
  app.post('/resdiary/booking-note-probe', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.hasCreds()) return res.status(503).json({ error: 'resdiary_creds_not_configured' });
    const site = rd.micrositeName();
    if (!site) return res.status(503).json({ error: 'RESDIARY_MICROSITE_NAME not set' });

    const reference = String((req.body || {}).reference || '').trim();
    if (!reference) return res.status(400).json({ error: 'expected { reference } — a REAL, ideally cancelled, booking' });

    const base = `/api/ConsumerApi/v1/Restaurant/${encodeURIComponent(site)}`;
    const path = `${base}/Booking/${encodeURIComponent(reference)}`;
    const MARKER = `probe-note-${Date.now()}`;
    const steps = [];
    let before = null;

    const read = async (label) => {
      try {
        const b = await rd.rdGet(path);
        steps.push({ name: `read:${label}`, ok: true, specialRequests: b?.SpecialRequests ?? null, comments: b?.Comments ?? null });
        return b;
      } catch (err) {
        steps.push({ name: `read:${label}`, ok: false, error: String(err.message).slice(0, 160) });
        return null;
      }
    };

    try {
      before = await read('before');
      if (!before) return res.status(502).json({ error: `could not read booking ${reference}` });

      const ATTEMPTS = [
        { name: 'raw:SpecialRequests', wrap: false, field: 'SpecialRequests' },
        { name: 'wrapped:SpecialRequests', wrap: true, field: 'SpecialRequests' },
        { name: 'raw:Comments', wrap: false, field: 'Comments' },
        { name: 'wrapped:Comments', wrap: true, field: 'Comments' },
      ];

      let landed = null;
      for (const a of ATTEMPTS) {
        const record = { ...before, [a.field]: MARKER };
        const payload = a.wrap ? { request: record } : record;
        let put;
        try {
          put = await rd.rdSendJson('PUT', path, payload);
        } catch (err) {
          steps.push({ name: `put:${a.name}`, ok: false, error: String(err.message).slice(0, 160) });
          continue;
        }
        // A 200 is NOT the answer — EPOS returned 200 and ignored the field.
        const after = await read(`after:${a.name}`);
        const stuck = after && (after.SpecialRequests === MARKER || after.Comments === MARKER);
        steps.push({
          name: `put:${a.name}`, ok: put.ok, status: put.status, stuck: !!stuck,
          body: typeof put.body === 'string' ? put.body.slice(0, 200) : JSON.stringify(put.body ?? null).slice(0, 200),
        });
        if (stuck) { landed = a; break; }
      }

      return res.json({
        reference,
        verdict: landed ? 'BOOKING_NOTE_WRITABLE' : 'BOOKING_NOTE_NOT_WRITABLE',
        worksVia: landed ? landed.name : null,
        note: landed
          ? `The booking note CAN be written, via ${landed.name}. Read-modify-write the whole record.`
          : 'Every shape answered without the note sticking — the booking note is not writable on the Consumer API either.',
        before: { specialRequests: before?.SpecialRequests ?? null, comments: before?.Comments ?? null },
        steps,
      });
    } finally {
      // Put it back exactly as found, whatever happened above.
      if (before) {
        try {
          await rd.rdSendJson('PUT', path, before);
          const back = await rd.rdGet(path);
          console.log('[booking-note-probe] restored', reference, JSON.stringify({
            specialRequests: back?.SpecialRequests ?? null, comments: back?.Comments ?? null,
          }));
        } catch (err) {
          console.error('[booking-note-probe] RESTORE FAILED', reference, err.message);
        }
      }
    }
  });

  app.post('/resdiary/customer-note-probe', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.hasCreds()) return res.status(503).json({ error: 'resdiary_creds_not_configured' });

    const site = rd.micrositeName();
    if (!site) return res.status(503).json({ error: 'RESDIARY_MICROSITE_NAME not set' });

    const steps = [];
    const step = async (name, fn) => {
      try {
        const r = await fn();
        // The envelope verbatim — never `?? r`, which turns an empty 200 body
        // into the envelope and makes a successful write look like a refusal.
        steps.push({ name, ok: r?.ok === true, status: r?.status ?? null, body: r && 'body' in r ? r.body : null });
        return r;
      } catch (err) {
        // A throw is a TRANSPORT failure (Cloudflare, retries exhausted) and
        // must never be read as the vendor saying no.
        steps.push({ name, ok: false, status: null, transportError: err.message });
        return null;
      }
    };

    const stamp = Date.now();
    const email = `${PROBE_EMAIL_PREFIX}${stamp}@theagorahotel.com`;
    const surname = `Probe ${stamp}`;
    const base = `/api/ConsumerApi/v1/Restaurant/${encodeURIComponent(site)}`;

    let customerId = null;
    let payload = null;

    try {
      const created = await step('createCustomer', () =>
        rd.rdSend('POST', `${base}/Customer`, {
          Title: 'Mr',
          FirstName: 'API',
          Surname: surname,
          Email: email,
          // ResDiary refuses a customer with neither a mobile nor a landline
          // ("Either a mobile or a landline number must be supplied"). Derived
          // from the stamp so two probes never collide, and in a range that
          // cannot be a real Cyprus mobile.
          MobileCountryCode: 357,
          Mobile: `99${String(stamp).slice(-6)}`,
          ReceiveEmailMarketing: false,
          ReceiveSmsMarketing: false,
          Comments: 'probe: first note',
        }));

      if (!created || created.ok !== true) {
        payload = { ok: false, verdict: 'CREATE_FAILED', note: 'The create did not return 2xx — nothing was written.' };
      } else {
        // Prove the row is OURS before touching it. An id alone is not proof.
        const b = created.body || {};
        const echoesUs =
          String(b.Email ?? b.EmailAddress ?? '') === email ||
          String(b.Surname ?? '').includes(String(stamp));
        const id = b.Id ?? b.CustomerId ?? null;

        if (!id || !echoesUs) {
          payload = {
            ok: false,
            verdict: 'CREATE_UNVERIFIED',
            note: 'The create response did not echo our marker back, so we cannot prove this id is the customer we made. Refusing to write to it or delete it.',
          };
        } else {
          customerId = id;
          // Update Customer is a WHOLE-RECORD replace: sending Comments alone
          // returns 400 "The email address must be supplied". So the real usage
          // pattern — and this probe — must echo the record back and change the
          // one field. `AppendComments` then decides whether the note is added
          // to the old one or replaces it, which is the whole question.
          const appended = await step('appendNote', () =>
            rd.rdSend('PUT', `${base}/Customer/${customerId}/`, {
              Title: b.Title ?? 'Mr',
              FirstName: b.FirstName ?? 'API',
              Surname: b.Surname ?? surname,
              Email: b.Email ?? email,
              MobileCountryCode: b.MobileCountryCode ?? 357,
              Mobile: b.Mobile ?? '',
              ReceiveEmailMarketing: b.ReceiveEmailMarketing ?? false,
              ReceiveSmsMarketing: b.ReceiveSmsMarketing ?? false,
              Comments: 'probe: appended note',
              'CustomerOptions[AppendComments]': true,
              // Does the VIP flag travel too? It is on the customer record we
              // get back (`IsVip`), but ResDiary's documented PUT body does not
              // list it — so ask rather than assume.
              IsVip: true,
            }));

          // ── Can Comments be REPLACED at all, by any spelling? ──────────
          //
          // Everything about editing and deleting a note rests on this. Round
          // one omitted `CustomerOptions[AppendComments]` entirely and ResDiary
          // APPENDED anyway (probe 05-09-2026), which kills the feature as
          // designed — so this asks the question every remaining way rather
          // than concluding "impossible" from one spelling. That mistake has
          // already been made once on this API: Reviews was declared
          // unentitled after a 404 that turned out to be our own invented
          // query parameters.
          //
          // Sequenced on the SAME throwaway customer, each attempt reading the
          // echoed Comments back: if the field grew, that spelling appends; if
          // it is exactly what we sent, that spelling replaces.
          const REPLACE_ATTEMPTS = [
            { name: 'omit', extra: {} },
            { name: 'bracketFalse', extra: { 'CustomerOptions[AppendComments]': false } },
            { name: 'dottedFalse', extra: { 'CustomerOptions.AppendComments': false } },
            { name: 'bareFalse', extra: { AppendComments: false } },
            { name: 'bracketZero', extra: { 'CustomerOptions[AppendComments]': 0 } },
          ];

          const replaceTries = [];
          let replaceWinner = null;
          for (const attempt of REPLACE_ATTEMPTS) {
            const marker = `probe: replace-${attempt.name}`;
            const r = await step(`replace:${attempt.name}`, () =>
              rd.rdSend('PUT', `${base}/Customer/${customerId}/`,
                { ...customerFormFrom(b, { comments: marker }), ...attempt.extra },
                { maxAttempts: 1 }));
            const after = typeof r?.body?.Comments === 'string' ? r.body.Comments : null;
            const verdict =
              after === null ? 'UNDETERMINED_NO_BODY'
              : after.trim() === marker ? 'REPLACES'
              : after.includes(marker) ? 'APPENDS'
              : 'IGNORED';
            replaceTries.push({ attempt: attempt.name, verdict, commentsAfter: after });
            if (verdict === 'REPLACES') { replaceWinner = attempt; break; }
          }

          const replaceVerdict = replaceWinner ? 'REPLACE_WORKS'
            : replaceTries.some((t) => t.verdict === 'APPENDS') ? 'REPLACE_STILL_APPENDS'
            : 'UNDETERMINED';

          // ── And can the field be emptied at all? ───────────────────────
          // "Delete the last remaining line" ends in an empty Comments. MEWS
          // ignores an empty string on its own notes field and needs a single
          // space; that is a MEWS fact and says nothing about ResDiary, so ask.
          const cleared = replaceWinner
            ? await step('clearComments', () =>
                rd.rdSend('PUT', `${base}/Customer/${customerId}/`,
                  { ...customerFormFrom(b, { comments: '' }), ...replaceWinner.extra }, { maxAttempts: 1 }))
            : null;
          // An emptied Comments comes back as null, not "" — so null here is
          // the SUCCESS answer, not an unreadable body.
          const rawClear = cleared?.body?.Comments;
          const afterClear = typeof rawClear === 'string' ? rawClear : rawClear === null ? '' : null;
          const clearVerdict =
            cleared === null ? 'NOT_ATTEMPTED'
            : afterClear === null ? 'UNDETERMINED_NO_BODY'
            : afterClear.trim() === '' ? 'CLEAR_WORKS'
            : 'CLEAR_IGNORED';

          // ── And CustomerCodes, the last field the form still drops? ────
          // An array, so bracket notation alone may not be enough — and unlike
          // Address, the values are probably a configured list the microsite
          // owns rather than free text, in which case an invented code is
          // rejected and the honest answer is "cannot be carried".
          const CODE_ATTEMPTS = [
            { name: 'indexBare', extra: { 'CustomerCodes[0]': 'PROBE' } },
            { name: 'indexCode', extra: { 'CustomerCodes[0][Code]': 'PROBE' } },
            { name: 'indexName', extra: { 'CustomerCodes[0][Name]': 'PROBE' } },
            { name: 'bare', extra: { CustomerCodes: 'PROBE' } },
          ];
          const codeTries = [];
          for (const attempt of CODE_ATTEMPTS) {
            const r = await step(`codes:${attempt.name}`, () =>
              rd.rdSend('PUT', `${base}/Customer/${customerId}/`, {
                ...customerFormFrom(b, { comments: 'probe: codes', appendComments: false }),
                ...attempt.extra,
              }, { maxAttempts: 1 }));
            const got = r?.body?.CustomerCodes;
            codeTries.push({
              attempt: attempt.name,
              status: r?.status ?? null,
              carried: Array.isArray(got) && got.length > 0,
              got: Array.isArray(got) ? got.slice(0, 2) : got ?? null,
            });
            if (Array.isArray(got) && got.length > 0) break;
          }
          const codesVerdict = codeTries.some((t) => t.carried) ? 'CODES_CARRIED' : 'CODES_NOT_CARRIED';

          // ── Does the PUT carry a nested Address? ───────────────────────
          // Round one reported Address and CustomerCodes as dropped, and the
          // PUT replaces the whole record — so every note we have ever
          // appended to a real diner has been blanking their address. Ask
          // whether the bracket notation that works for CustomerOptions works
          // here too, so it can be carried instead of lost.
          const addressTry = await step('addressBracket', () =>
            rd.rdSend('PUT', `${base}/Customer/${customerId}/`, {
              ...customerFormFrom(b, { comments: 'probe: address' }),
              'Address[Town]': 'ProbeTown',
              'Address[Street]': 'Probe Street',
            }, { maxAttempts: 1 }));
          const addressAfter = addressTry?.body?.Address ?? null;
          const addressVerdict =
            !addressAfter ? 'UNDETERMINED_NO_BODY'
            : addressAfter.Town === 'ProbeTown' ? 'ADDRESS_BRACKET_WORKS'
            : 'ADDRESS_NOT_CARRIED';

          // Which fields the GET returned that our PUT could not carry. The
          // hand-picked ten-field form was clearing everything outside it on
          // every real diner we appended to; this is how we find out.
          const probeForm = customerFormFrom(b, { comments: 'probe: replacement' });

          payload = {
            ok: true,
            customerId,
            ...verdictOf(appended),
            replace: {
              verdict: replaceVerdict,
              worksVia: replaceWinner ? replaceWinner.name : null,
              tried: replaceTries,
              note: replaceWinner
                ? `Comments CAN be replaced, via "${replaceWinner.name}" — editing and deleting a single note is possible.`
                : 'Comments is APPEND-ONLY by every spelling tried. Editing and deleting a diary line is not possible through this endpoint.',
            },
            address: { verdict: addressVerdict, after: addressAfter },
            customerCodes: { verdict: codesVerdict, tried: codeTries },
            clear: { verdict: clearVerdict, commentsAfter: afterClear },
            form: {
              sentKeys: Object.keys(probeForm),
              droppedFields: unsentScalarKeys(b, probeForm),
            },
            // Reported separately from the note verdict: VIP is a different
            // question and a different answer is perfectly possible.
            vip: {
              sentIsVip: true,
              createdAs: appended && appended.body ? undefined : null,
              cameBack: appended?.body?.IsVip ?? null,
              verdict:
                appended?.body?.IsVip === true ? 'VIP_WRITABLE'
                : appended?.body?.IsVip === false ? 'VIP_IGNORED'
                : 'VIP_UNDETERMINED',
            },
          };
        }
      }
    } finally {
      if (customerId) {
        // Shape is not in the docs section we read, so try both spellings.
        const a = await step('markForDeletion:CustomerIds[0]', () =>
          rd.rdSend('POST', `${base}/Customers/MarkForDeletion`, { 'CustomerIds[0]': customerId }));
        if (!a || a.ok !== true) {
          await step('markForDeletion:CustomerIds', () =>
            rd.rdSend('POST', `${base}/Customers/MarkForDeletion`, { CustomerIds: customerId }));
        }
      }
      const cleanup = steps.filter((x) => x.name.startsWith('markForDeletion'));
      const tidied = cleanup.some((x) => x.ok);
      // Logged as well as returned: if the caller drops the response, Railway
      // still holds the answer to "did we leave a row behind".
      console.log('[resdiary-probe]', JSON.stringify({ customerId, tidied, cleanup }));
      res.json({
        ...(payload || { ok: false, verdict: 'ABORTED' }),
        cleanedUp: customerId ? tidied : 'nothing_to_clean',
        leftBehind: customerId && !tidied ? { customerId, surname, email } : null,
        steps,
      });
    }
  });

  app.get('/resdiary/whoami', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.hasCreds()) return res.status(503).json({ error: 'resdiary_creds_not_configured' });
    try {
      const [user, restaurants] = [await rd.getCurrentUser(), await rd.getRestaurants()];
      res.json({ currentUser: user, restaurants });
    } catch (err) {
      const status = err instanceof rd.CloudflareBlockedError ? 502 : 500;
      res.status(status).json({ error: err.message });
    }
  });

  /**
   * Diner reviews → the app.
   *
   * `?raw=1` returns ResDiary's payload untouched and writes nothing. That is
   * the FIRST call to make once the whitelist lands: the Reviews response shape
   * is not in the portal docs we hold, and seeing one real response is worth
   * more than any amount of guessing. The mapping then lives app-side, where it
   * can be corrected without redeploying this worker.
   */
  app.get('/resdiary/reviews', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.hasCreds()) return res.status(503).json({ error: 'resdiary_creds_not_configured' });
    try {
      // One page per call, caller-driven: until a real payload has been seen we
      // cannot know how this endpoint signals "no more pages", so walking them
      // automatically would be a guess. ?raw=1 returns it verbatim and writes
      // nothing — use that first.
      const payload = await rd.getReviews({
        sortBy: req.query.sortBy || 'Newest',
        page: Number(req.query.page) || 1,
        pageSize: Math.min(Number(req.query.pageSize) || 20, 100),
      });
      if (req.query.raw === '1') return res.json({ ok: true, payload });

      const { url, token } = appTarget();
      if (!token) return res.status(503).json({ error: 'RESDIARY_INGEST_TOKEN not set' });
      const push = await fetch(url.replace(/\/ingest$/, '/reviews'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ payload }),
        signal: AbortSignal.timeout(120_000),
      });
      const body = await push.text();
      if (!push.ok) return res.status(502).json({ error: `app ingest HTTP ${push.status}`, detail: body.slice(0, 300) });
      res.json({ ok: true, app: JSON.parse(body) });
    } catch (err) {
      // A Cloudflare block here means one thing and one thing only: this
      // service's egress IP is not on ResDiary's allowlist yet.
      const blocked = err instanceof rd.CloudflareBlockedError;
      res.status(blocked ? 502 : 500).json({
        error: err.message,
        ...(blocked ? { hint: 'This egress IP is not whitelisted by ResDiary — send them the Railway static IPs.' } : {}),
      });
    }
  });

  // Shape inspection: what a change row, a full booking and a customer-change
  // row actually look like for one date. Read-only; the reconcile bug of
  // 02-09-2026 came from never having seen these.
  app.get('/resdiary/peek', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.isConfigured()) return res.status(503).json({ error: 'resdiary_not_configured' });
    const day = String(req.query.date || toDateIso(Date.now()));
    try {
      // ?bookingId= — one full record, verbatim: settles "what does Data
      // Extract actually say about THIS booking" (service name vs description,
      // promotions shape) without guessing from a date's first change row.
      if (req.query.bookingId) {
        const id = String(req.query.bookingId);
        const one = await rd.getBookingById(id);
        return res.json({ bookingId: id, looksFull: looksLikeFullBooking(one), fullBooking: one });
      }
      const changeRows = unwrapList(await rd.getBookingChanges(day));
      const custRows = unwrapList(await rd.getCustomerChanges(day));
      const first = classifyChangeRow(changeRows[0]);
      const fullBooking = first.kind === 'id' ? await rd.getBookingById(first.id) : null;
      res.json({
        date: day,
        bookingChanges: { count: changeRows.length, sample: changeRows[0] ?? null },
        fullBooking: { sample: fullBooking, looksFull: looksLikeFullBooking(fullBooking) },
        customerChanges: { count: custRows.length, sample: custRows[0] ?? null },
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/resdiary/backfill', (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.isConfigured() || !ingestConfigured()) {
      return res.status(503).json({ error: 'resdiary_not_configured', status: getStatus().configured });
    }
    if (state.running) return res.status(409).json({ error: 'sync_already_running', phase: state.phase });
    const { from, to } = req.body || {};
    runBackfill({ from, to, trigger: 'manual' }).catch((err) => console.error('[resdiary-sync] backfill failed:', err.message));
    res.status(202).json({ ok: true, message: 'Backfill started — poll GET /resdiary/status' });
  });

  app.post('/resdiary/reconcile', (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.isConfigured() || !ingestConfigured()) {
      return res.status(503).json({ error: 'resdiary_not_configured', status: getStatus().configured });
    }
    if (state.running) return res.status(409).json({ error: 'sync_already_running', phase: state.phase });
    const days = req.body?.days;
    runReconcile({ days, trigger: 'manual' }).catch((err) => console.error('[resdiary-sync] reconcile failed:', err.message));
    res.status(202).json({ ok: true, message: 'Reconcile started — poll GET /resdiary/status' });
  });

  // Self-timer (the app has no cron for this). Also one run ~90s after boot:
  // a redeploy used to open a silent gap until the next tick, and a boot run
  // re-proves the whole path — credentials, whitelist, ingest — every deploy.
  const intervalMs = parseInt(process.env.RESDIARY_RECONCILE_INTERVAL_MS || '0', 10);
  if (intervalMs > 0 && rd.isConfigured() && ingestConfigured()) {
    const tick = (trigger) => {
      if (!state.running) {
        runReconcile({ trigger }).catch((err) => console.error(`[resdiary-sync] ${trigger} reconcile failed:`, err.message));
      }
    };
    setTimeout(() => tick('boot'), 90_000).unref?.();
    setInterval(() => tick('timer'), intervalMs);
    console.log(`[resdiary-sync] self-reconcile every ${intervalMs}ms (+ one run 90s after boot)`);
  }
}

module.exports = {
  registerResdiaryRoutes,
  runBackfill,
  runReconcile,
  getStatus,
  // exported for tests
  _internal: { listDatesInclusive, addDaysIso, chunk, unwrapList, classifyChangeRow, classifyCustomerRow, looksLikeFullBooking, looksLikeFullCustomer, pushRun, DEFAULT_RECONCILE_DAYS },
};
