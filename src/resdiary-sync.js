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

/** ResDiary list responses are either bare arrays or a paging envelope. */
function unwrapList(body) {
  if (Array.isArray(body)) return body;
  if (body && Array.isArray(body.Data)) return body.Data;
  return [];
}

/**
 * A BookingChange row's exact shape is undocumented; be liberal. Returns a
 * booking object, or an id for `fetchById`, or null to skip.
 */
function classifyChangeRow(row) {
  if (!row || typeof row !== 'object') return { kind: 'skip' };
  if (row.Booking && typeof row.Booking === 'object') return { kind: 'booking', booking: row.Booking };
  if (row.VisitDateTime || row.BookingReference) return { kind: 'booking', booking: row };
  const id = row.BookingId ?? row.Id;
  if (id !== undefined && id !== null) return { kind: 'id', id };
  return { kind: 'skip' };
}

function classifyCustomerRow(row) {
  if (!row || typeof row !== 'object') return null;
  if (row.Customer && typeof row.Customer === 'object') return row.Customer;
  return row;
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

// ── Sync state (single-flight) ───────────────────────────────────────

const state = {
  running: false,
  phase: null, // 'bookings' | 'customers' | 'reconcile'
  startedAt: null,
  finishedAt: null,
  lastError: null,
  counts: { days: 0, bookings: 0, customers: 0 },
};

async function runBackfill({ from, to } = {}) {
  if (state.running) throw new Error(`A ResDiary sync is already running (phase=${state.phase})`);
  if (!rd.isConfigured()) throw new Error('ResDiary not fully configured (creds + deployment/provider ids)');
  if (!ingestConfigured()) throw new Error('RESDIARY_INGEST_TOKEN not set');

  state.running = true;
  state.phase = 'bookings';
  state.startedAt = new Date().toISOString();
  state.finishedAt = null;
  state.lastError = null;
  state.counts = { days: 0, bookings: 0, customers: 0 };

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
    return { ...state.counts };
  } catch (err) {
    state.lastError = err.message;
    throw err;
  } finally {
    state.running = false;
    state.phase = null;
    state.finishedAt = new Date().toISOString();
  }
}

async function runReconcile({ days = 2 } = {}) {
  if (state.running) throw new Error(`A ResDiary sync is already running (phase=${state.phase})`);
  if (!rd.isConfigured()) throw new Error('ResDiary not fully configured (creds + deployment/provider ids)');
  if (!ingestConfigured()) throw new Error('RESDIARY_INGEST_TOKEN not set');

  const n = Math.min(Math.max(parseInt(days, 10) || 2, 1), 30);
  state.running = true;
  state.phase = 'reconcile';
  state.startedAt = new Date().toISOString();
  state.lastError = null;

  const counts = { days: n, bookings: 0, customers: 0 };
  try {
    const today = toDateIso(Date.now());
    for (let i = n - 1; i >= 0; i--) {
      const day = addDaysIso(today, -i);

      const changeRows = unwrapList(await rd.getBookingChanges(day));
      const bookings = [];
      for (const row of changeRows) {
        const c = classifyChangeRow(row);
        if (c.kind === 'booking') bookings.push(c.booking);
        else if (c.kind === 'id') {
          try {
            bookings.push(await rd.getBookingById(c.id));
          } catch (err) {
            console.warn(`[resdiary-sync] booking ${c.id} fetch failed:`, err.message);
          }
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
    return counts;
  } catch (err) {
    state.lastError = err.message;
    throw err;
  } finally {
    state.running = false;
    state.phase = null;
    state.finishedAt = new Date().toISOString();
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

function getStatus() {
  return {
    configured: {
      creds: rd.hasCreds(),
      ids: rd.isConfigured(),
      ingestToken: ingestConfigured(),
      appUrl: appTarget().url,
    },
    tokenCached: rd.hasCreds() ? rd.tokenCached() : false,
    cursor: readCursor(),
    sync: { ...state },
  };
}

/** Mount /resdiary/* on the bridge's Express app. Additive only. */
function registerResdiaryRoutes(app) {
  app.get('/resdiary/status', (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    res.json(getStatus());
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

  app.post('/resdiary/backfill', (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.isConfigured() || !ingestConfigured()) {
      return res.status(503).json({ error: 'resdiary_not_configured', status: getStatus().configured });
    }
    if (state.running) return res.status(409).json({ error: 'sync_already_running', phase: state.phase });
    const { from, to } = req.body || {};
    runBackfill({ from, to }).catch((err) => console.error('[resdiary-sync] backfill failed:', err.message));
    res.status(202).json({ ok: true, message: 'Backfill started — poll GET /resdiary/status' });
  });

  app.post('/resdiary/reconcile', (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!rd.isConfigured() || !ingestConfigured()) {
      return res.status(503).json({ error: 'resdiary_not_configured', status: getStatus().configured });
    }
    if (state.running) return res.status(409).json({ error: 'sync_already_running', phase: state.phase });
    const days = req.body?.days;
    runReconcile({ days }).catch((err) => console.error('[resdiary-sync] reconcile failed:', err.message));
    res.status(202).json({ ok: true, message: 'Reconcile started — poll GET /resdiary/status' });
  });

  // Optional self-timer while the app-side cron isn't shipped yet.
  const intervalMs = parseInt(process.env.RESDIARY_RECONCILE_INTERVAL_MS || '0', 10);
  if (intervalMs > 0 && rd.isConfigured() && ingestConfigured()) {
    setInterval(() => {
      if (!state.running) {
        runReconcile({}).catch((err) => console.error('[resdiary-sync] scheduled reconcile failed:', err.message));
      }
    }, intervalMs);
    console.log(`[resdiary-sync] self-reconcile every ${intervalMs}ms`);
  }
}

module.exports = {
  registerResdiaryRoutes,
  runBackfill,
  runReconcile,
  getStatus,
  // exported for tests
  _internal: { listDatesInclusive, addDaysIso, chunk, unwrapList, classifyChangeRow, classifyCustomerRow },
};
