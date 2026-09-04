/**
 * ResDiary API client (Data Extract + account endpoints).
 *
 * This service owns EVERY outbound ResDiary call because its Railway egress
 * is the set of static IPs ResDiary whitelists (prod API is IP-restricted).
 * The Vercel app never calls ResDiary directly — it receives our pushes on
 * /api/resdiary/ingest and ResDiary's webhooks on /api/resdiary/webhook.
 *
 * Facts confirmed against the live API portal (04-08-2026):
 *   - Auth: POST /api/Jwt/v2/Authenticate {Username, Password}
 *     → { Status: "Success", Token, TokenExpiryUtc }. Tokens last 24h and the
 *     server RE-ISSUES THE SAME TOKEN until it is expired or within 5 min of
 *     expiry — so we must cache and reuse, never token-per-call.
 *   - TokenExpiryUtc arrives WITHOUT a zone suffix ("2026-08-05T17:23:06.2470000")
 *     but is UTC — parse accordingly or every expiry check is off by the TZ.
 *   - api.resdiary.com sits behind Cloudflare bot protection: default
 *     python/undici user-agents can be banned (403 "error code: 1010") and
 *     non-whitelisted IPs get an HTML "Attention Required" page on data calls.
 *     Send a real User-Agent and detect HTML responses explicitly.
 *   - Data Extract is keyed on {deploymentId}/{providerId}; "bookingDate"
 *     means the date the booking was CREATED, not the visit date.
 *   - Rate limit: 200 req/min per account — throttled + backoff below.
 */

const fs = require('node:fs');
const path = require('node:path');

const BASE = process.env.RESDIARY_BASE_URL || 'https://api.resdiary.com';
const USER_AGENT =
  process.env.RESDIARY_USER_AGENT ||
  'AgoraPosToFolio/1.0 (+https://theagorahotel.com; resdiary-integration)';

const DATA_DIR = process.env.DATA_DIR
  ? path.resolve(process.env.DATA_DIR)
  : path.resolve(process.cwd(), 'data');
const TOKEN_FILE = path.join(DATA_DIR, 'resdiary-token.json');

// ~2.8 req/s keeps a long backfill far under the 200 req/min account cap
// while leaving headroom for whatever else the account is doing.
const MIN_INTERVAL_MS = 350;
const REFRESH_MARGIN_MS = 10 * 60_000; // re-auth when <10 min of life left

function creds() {
  return {
    username: process.env.RESDIARY_USERNAME || '',
    password: process.env.RESDIARY_PASSWORD || '',
  };
}

function ids() {
  return {
    deploymentId: process.env.RESDIARY_DEPLOYMENT_ID || '',
    providerId: process.env.RESDIARY_PROVIDER_ID || '',
  };
}

/** Credentials present — enough for auth + discovery (whoami). */
function hasCreds() {
  const c = creds();
  return !!(c.username && c.password);
}

/** Fully configured for Data Extract (creds + path ids). */
function isConfigured() {
  const i = ids();
  return hasCreds() && !!(i.deploymentId && i.providerId);
}

class CloudflareBlockedError extends Error {
  constructor(status, snippet) {
    super(
      `ResDiary request blocked at Cloudflare (HTTP ${status}). ` +
        `Data calls stay blocked until our static IPs are whitelisted by ResDiary; ` +
        `a "1010" code means the User-Agent was banned instead. Snippet: ${snippet}`
    );
    this.name = 'CloudflareBlockedError';
    this.status = status;
  }
}

/**
 * ResDiary timestamps come zoneless but are UTC. Trim >3 fraction digits
 * (V8 tolerates them, but be explicit) and pin the zone.
 */
function parseUtc(s) {
  if (!s || typeof s !== 'string') return null;
  let v = s.trim().replace(/(\.\d{3})\d+/, '$1');
  if (!/[zZ]$|[+-]\d\d:?\d\d$/.test(v)) v += 'Z';
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
}

/** True when a cached token should be replaced. */
function tokenNeedsRefresh(entry, now = Date.now()) {
  if (!entry || !entry.token || !entry.expiresAtMs) return true;
  return now >= entry.expiresAtMs - REFRESH_MARGIN_MS;
}

// ── Token cache (module memory + volume, survives redeploys) ────────

let _token = null; // { token, expiresAtMs }

function loadPersistedToken() {
  try {
    if (fs.existsSync(TOKEN_FILE)) {
      const raw = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf-8'));
      if (raw && raw.token && raw.expiresAtMs) return raw;
    }
  } catch (err) {
    console.warn('[resdiary] Could not read persisted token:', err.message);
  }
  return null;
}

function persistToken(entry) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(TOKEN_FILE, JSON.stringify(entry), { mode: 0o600 });
  } catch (err) {
    console.warn('[resdiary] Could not persist token:', err.message);
  }
}

function looksBlocked(status, text) {
  if (typeof text !== 'string') return false;
  const t = text.slice(0, 300).toLowerCase();
  return (
    (status === 403 || status === 503) &&
    (t.startsWith('<!doctype') || t.startsWith('<html') || /error code: 10\d\d/.test(t))
  );
}

async function getToken(force = false) {
  if (!hasCreds()) throw new Error('ResDiary credentials not configured (RESDIARY_USERNAME/RESDIARY_PASSWORD)');

  if (!_token) _token = loadPersistedToken();
  if (!force && !tokenNeedsRefresh(_token)) return _token.token;

  const c = creds();
  const res = await fetch(`${BASE}/api/Jwt/v2/Authenticate`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'User-Agent': USER_AGENT,
      Accept: 'application/json, text/plain, */*',
    },
    body: JSON.stringify({ Username: c.username, Password: c.password }),
    signal: AbortSignal.timeout(30_000),
  });
  const text = await res.text();
  if (looksBlocked(res.status, text)) throw new CloudflareBlockedError(res.status, text.slice(0, 120));
  if (!res.ok) throw new Error(`ResDiary auth HTTP ${res.status}: ${text.slice(0, 200)}`);

  let body;
  try {
    body = JSON.parse(text);
  } catch {
    throw new Error(`ResDiary auth returned non-JSON: ${text.slice(0, 120)}`);
  }
  if (body.Status !== 'Success' || !body.Token) {
    // The API answers Status:"Fail" for bad credentials (200 OK, no detail).
    throw new Error(`ResDiary auth rejected (Status=${body.Status ?? 'unknown'}) — check RESDIARY_USERNAME/RESDIARY_PASSWORD`);
  }

  const exp = parseUtc(body.TokenExpiryUtc);
  _token = {
    token: body.Token,
    // Fall back to +23h if the expiry ever fails to parse — better a slightly
    // early refresh than a stuck client.
    expiresAtMs: exp ? exp.getTime() : Date.now() + 23 * 3600_000,
  };
  persistToken(_token);
  return _token.token;
}

// ── Throttled, retrying GET ──────────────────────────────────────────

let _lastCallAt = 0;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function throttle() {
  const wait = _lastCallAt + MIN_INTERVAL_MS - Date.now();
  if (wait > 0) await sleep(wait);
  _lastCallAt = Date.now();
}

/**
 * GET a ResDiary API path with auth, throttle, one 401 re-auth, and
 * exponential backoff on 429/5xx/network errors.
 */
async function rdGet(apiPath, { query } = {}) {
  const url = new URL(BASE + apiPath);
  for (const [k, v] of Object.entries(query || {})) {
    if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
  }

  let reauthed = false;
  let lastErr = null;
  for (let attempt = 0; attempt < 4; attempt++) {
    if (attempt > 0) await sleep(1000 * 3 ** (attempt - 1)); // 1s, 3s, 9s
    await throttle();
    let res, text;
    try {
      res = await fetch(url, {
        headers: {
          Authorization: `Bearer ${await getToken()}`,
          'User-Agent': USER_AGENT,
          Accept: 'application/json, text/plain, */*',
        },
        signal: AbortSignal.timeout(60_000),
      });
      text = await res.text();
    } catch (err) {
      lastErr = err; // network/timeout — retry
      continue;
    }

    if (looksBlocked(res.status, text)) throw new CloudflareBlockedError(res.status, text.slice(0, 120));

    if (res.status === 401 && !reauthed) {
      reauthed = true;
      await getToken(true); // token invalidated server-side — force re-auth once
      attempt--; // does not consume a retry
      continue;
    }
    if (res.status === 429 || res.status >= 500) {
      lastErr = new Error(`ResDiary ${apiPath} HTTP ${res.status}`);
      continue;
    }
    if (!res.ok) throw new Error(`ResDiary ${apiPath} HTTP ${res.status}: ${text.slice(0, 300)}`);

    try {
      return JSON.parse(text);
    } catch {
      throw new Error(`ResDiary ${apiPath} returned non-JSON (${text.slice(0, 120)})`);
    }
  }
  throw lastErr || new Error(`ResDiary ${apiPath}: retries exhausted`);
}

// ── Endpoints ────────────────────────────────────────────────────────

/** Account discovery — works with creds alone, no deployment/provider ids. */
async function getCurrentUser() {
  return rdGet('/api/ConsumerApi/v1/CurrentUser');
}

/** All restaurants this API account can reach (includes microsite names). */
async function getRestaurants() {
  return rdGet('/api/ConsumerApi/v1/Restaurants');
}

/**
 * The diner reviews ResDiary holds for one microsite.
 *
 * ── Why this is a different shape to everything else here ──────────────────
 * Reviews live on the CONSUMER API, which is keyed on `micrositeName` — not on
 * the {deploymentId}/{providerId} pair Data Extract uses. That is genuinely
 * good news: the microsite name rides free on every webhook notification and is
 * printed in the diary's own settings, whereas the Data Extract ids can only be
 * discovered through a call that the IP whitelist blocks. So reviews need one
 * value we can already get without ResDiary's help.
 *
 * ── Why we need it at all ──────────────────────────────────────────────────
 * ResDiary emails the venue a daily feedback digest, and that digest carries no
 * per-review score — its only number is the batch average, which cannot be
 * spread across the reviews without inventing scores nobody gave. The score is
 * the point, so it has to come from here.
 *
 * The response shape is NOT in the portal docs we have read, so the caller gets
 * the payload verbatim and normalises it. Guessing a schema here would bake a
 * wrong assumption into the one place that is hard to see into.
 */
async function getReviews({ micrositeName, sortBy = 'Newest', page = 1, pageSize = 20 } = {}) {
  const site = micrositeName || process.env.RESDIARY_MICROSITE_NAME || '';
  if (!site) {
    throw new Error(
      'RESDIARY_MICROSITE_NAME not set — read it from Diary → Settings → Alter Restaurant Details, ' +
      'or from any webhook payload (MicrositeName), or via /resdiary/whoami once data calls work',
    );
  }
  // sortBy/page/pageSize are the ONLY parameters this endpoint takes.
  //
  // This used to send fromDate/toDate, which we invented — ResDiary has no such
  // parameters — and the endpoint answered 404 for two days while we concluded
  // the account was not entitled to Reviews at all. It is: Ciorstaidh MacLeod
  // (The Access Group, 03-09-2026) confirmed the endpoint is available to us and
  // gave the canonical form, which is exactly the three below. Do not add a
  // parameter here that is not in ResDiary's own documentation.
  return rdGet(`/api/ConsumerApi/v1/Restaurant/${encodeURIComponent(site)}/Reviews`, {
    query: { sortBy, page, pageSize },
  });
}

function dePath(rest) {
  const i = ids();
  if (!i.deploymentId || !i.providerId) {
    throw new Error('ResDiary ids not configured (RESDIARY_DEPLOYMENT_ID/RESDIARY_PROVIDER_ID) — use /resdiary/whoami to discover them');
  }
  return `/api/ConsumerApi/v1/DataExtract/Restaurant/${i.deploymentId}/${i.providerId}/${rest}`;
}

/** First date any booking was CREATED — the backfill floor. */
async function getEarliestBookingDate() {
  return rdGet(dePath('Booking/EarliestDate'));
}

/** Bookings CREATED on the given date (current state of each). */
async function getBookingsForDate(dateIso) {
  return rdGet(dePath(`Booking/${dateIso}`));
}

async function getBookingById(bookingId) {
  return rdGet(dePath(`Booking/${bookingId}`));
}

/** Changes made to bookings on the given date. */
async function getBookingChanges(dateIso) {
  return rdGet(dePath(`BookingChange/${dateIso}`));
}

/** Customer records page (pageSize max 100 per the docs). */
async function getCustomersPage(fromCreationDate, pageNumber, pageSize = 100) {
  return rdGet(dePath('Customers'), {
    query: {
      fromCreationDate,
      includeBlockedCustomers: true,
      pageNumber,
      pageSize,
    },
  });
}

/** Changes made to customers on the given date. */
async function getCustomerChanges(dateIso) {
  return rdGet(dePath(`CustomerChange/${dateIso}`));
}

function tokenCached() {
  if (!_token) _token = loadPersistedToken();
  return !!(_token && !tokenNeedsRefresh(_token));
}

module.exports = {
  hasCreds,
  isConfigured,
  getToken,
  getCurrentUser,
  getRestaurants,
  getReviews,
  getEarliestBookingDate,
  getBookingsForDate,
  getBookingById,
  getBookingChanges,
  getCustomersPage,
  getCustomerChanges,
  tokenCached,
  CloudflareBlockedError,
  // exported for tests
  _internal: { parseUtc, tokenNeedsRefresh, looksBlocked },
};
