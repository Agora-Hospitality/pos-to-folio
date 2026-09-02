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

function classifyCustomerRow(row) {
  if (!row || typeof row !== 'object') return null;
  const cust = row.Customer && typeof row.Customer === 'object' ? row.Customer : row;
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
      const payload = await rd.getReviews({ fromDate: req.query.from, toDate: req.query.to });
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
