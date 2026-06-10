/**
 * Goodtill (SumUp POS) API client
 * Handles JWT authentication and sales retrieval
 */

const BASE = 'https://api.thegoodtill.com/api';
let _token = null;
let _tokenExpiry = null;

async function getToken() {
  if (_token && _tokenExpiry && Date.now() < _tokenExpiry - 30_000) return _token;

  const res = await fetch(`${BASE}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      subdomain: process.env.GOODTILL_SUBDOMAIN,
      username: process.env.GOODTILL_USERNAME,
      password: process.env.GOODTILL_PASSWORD,
    }),
    signal: AbortSignal.timeout(15_000),
  });

  if (!res.ok) throw new Error(`Goodtill login failed: ${res.status} ${res.statusText}`);
  const data = await res.json();
  _token = data.token;
  const payload = JSON.parse(Buffer.from(_token.split('.')[1], 'base64').toString());
  _tokenExpiry = payload.exp * 1000;
  return _token;
}

async function gt(path, params = {}) {
  const token = await getToken();
  const url = new URL(`${BASE}${path}`);
  for (const [k, v] of Object.entries(params)) {
    if (v != null) url.searchParams.set(k, String(v));
  }
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(20_000),
  });
  if (!res.ok) throw new Error(`Goodtill ${path} failed: ${res.status} ${res.statusText}`);
  return res.json();
}

/**
 * POST/PUT to a Goodtill API endpoint with JSON body
 */
async function gtWrite(method, path, body = {}) {
  const token = await getToken();
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(20_000),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Goodtill ${method} ${path} failed: ${res.status} — ${text}`);
  }
  return res.json();
}

// ─── Customer management ──────────────────────────────────────────────

/**
 * List all customers from Goodtill
 * @returns {Promise<any[]>}
 */
async function listCustomers() {
  const data = await gt('/customers');
  return data.data || [];
}

/**
 * Create a customer in Goodtill
 * @param {object} fields - { name, extra_notes, custom_field_1, ... }
 * @returns {Promise<any>} The created customer
 */
async function createCustomer(fields) {
  const data = await gtWrite('POST', '/customers', fields);
  return data.data;
}

/**
 * Update a customer in Goodtill
 * @param {string} id - Customer ID
 * @param {object} fields - Fields to update
 * @returns {Promise<any>} The updated customer
 */
async function updateCustomer(id, fields) {
  const data = await gtWrite('PUT', `/customers/${id}`, fields);
  return data.data;
}

/**
 * Activate a customer (make visible on POS iPad)
 */
async function activateCustomer(id) {
  return updateCustomer(id, { active: 1 });
}

/**
 * Deactivate a customer (hide from POS iPad)
 */
async function deactivateCustomer(id) {
  return updateCustomer(id, { active: 0 });
}

/**
 * Fetch all completed sales in a time range (paginated)
 * @param {string} from - "YYYY-MM-DD HH:MM:SS"
 * @param {string} to   - "YYYY-MM-DD HH:MM:SS"
 * @returns {Promise<any[]>} Array of sale objects
 */
async function fetchSales(from, to) {
  const fromStr = from instanceof Date ? formatDateTime(from) : from;
  const toStr = to instanceof Date ? formatDateTime(to) : to;
  const all = [];
  let offset = 0;
  const limit = 200;
  while (true) {
    const page = await gt('/external/get_sales_details', { from: fromStr, to: toStr, limit, offset });
    if (!page.data || !Array.isArray(page.data) || page.data.length === 0) break;
    all.push(...page.data);
    if (page.data.length < limit) break;
    offset += page.data.length;
  }
  return all;
}

/**
 * Format a Date to "YYYY-MM-DD HH:MM:SS" in Cyprus local time for Goodtill API.
 * Goodtill stores/returns timestamps in the property's local timezone (Europe/Nicosia).
 * Using Intl so it works correctly regardless of the server's system timezone (e.g. UTC on Railway).
 *
 * `hourCycle: 'h23'` pins the hour range to 00–23. Without it, some ICU builds
 * (e.g. the Node version running on Railway) emit "24" for midnight, producing
 * invalid timestamps like "2026-04-23 24:00:37" that Goodtill rejects with 422.
 */
function formatDateTime(d) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Nicosia',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(d);
  const get = (type) => parts.find((p) => p.type === type)?.value || '00';
  return `${get('year')}-${get('month')}-${get('day')} ${get('hour')}:${get('minute')}:${get('second')}`;
}

/**
 * Offset of Europe/Nicosia vs UTC at the given UTC instant, in milliseconds.
 */
function nicosiaOffsetMs(utcMs) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Nicosia',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(new Date(utcMs));
  const get = (type) => Number(parts.find((p) => p.type === type)?.value || 0);
  return Date.UTC(get('year'), get('month') - 1, get('day'), get('hour'), get('minute'), get('second')) - utcMs;
}

/**
 * Convert a Goodtill local timestamp ("YYYY-MM-DD HH:MM:SS", Europe/Nicosia)
 * to a true-UTC ISO string. The inverse of formatDateTime().
 *
 * The bridge used to do `new Date(sale.sales_date_time).toISOString()`, which
 * parses the zone-less local string as UTC on a UTC host (Railway). Every MEWS
 * ConsumedUtc came out as the Cyprus wall clock relabeled "Z" — +3h ahead of
 * the true instant in DST, +2h in winter. Any UTC-windowed consumer (the
 * agora-app guest-folio reconciliation) then saw 07:00–09:59-local sales in
 * the NEXT service day, and bridge posts looked ~3h late in MEWS.
 *
 * Returns null for unparsable input so callers can choose their fallback.
 */
function cyprusLocalToUtcIso(localStr) {
  // Fully anchored: a zone-suffixed timestamp ("...Z", "...+03:00") must fall
  // through to the caller's fallback rather than be silently re-shifted by 3h.
  const m = String(localStr || '').match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?$/);
  if (!m) return null;
  const wallUtc = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
  // Guess-and-correct: take the offset at the wall-clock instant, then refine
  // once so timestamps next to a DST transition resolve with the right offset.
  let utc = wallUtc - nicosiaOffsetMs(wallUtc);
  utc = wallUtc - nicosiaOffsetMs(utc);
  return new Date(utc).toISOString();
}

/**
 * Filter sales that used "Guest Folio" as a payment method.
 * Returns objects with { sale, roomNumber } where roomNumber is extracted
 * from the guest/customer name field.
 *
 * Goodtill sales_details contains sales_payments array with tender_type or
 * payment_type field. We match on name containing "guest folio" (case-insensitive).
 *
 * The room number is expected in the sale's customer_name or staff_name field
 * (waiter enters room number as the guest name in SumUp).
 */
function extractGuestFolioSales(sales) {
  const results = [];

  for (const sale of sales) {
    const status = (sale.order_status || '').toUpperCase();
    if (status !== 'COMPLETED' && status !== 'VOIDED') continue;

    // sales_payments is a top-level object keyed by payment type name
    // e.g. { "CUSTOM_1": { payment_total: "8.00", ... } }
    const payments = sale.sales_payments || {};
    const paymentKeys = Object.keys(payments);

    const isFolioKey = (key) => {
      const k = key.toLowerCase();
      return k === 'custom_1' || k.includes('guest folio') || k.includes('room charge');
    };

    const isGuestFolio = paymentKeys.some(isFolioKey);
    if (!isGuestFolio) continue;

    // Sum folio-keyed payment amounts (what actually hits the room folio) and
    // collect non-folio payments (Card/Cash) so the bridge can tell split bills
    // from full-folio sales.
    let folioAmount = 0;
    const otherPayments = [];
    for (const [key, p] of Object.entries(payments)) {
      const amount = parseFloat(p.payment_total || p.payment_amount || '0');
      if (amount <= 0) continue;
      if (isFolioKey(key)) {
        folioAmount += amount;
      } else {
        otherPayments.push({ method: key, amount });
      }
    }
    folioAmount = Math.round(folioAmount * 100) / 100;

    results.push({ sale, voided: status === 'VOIDED', folioAmount, otherPayments });
  }

  return results;
}

/**
 * Parse a room number from a string.
 * Accepts formats like "101", "Room 101", "room101", "R101", "#101", etc.
 * Returns the numeric room string or null.
 */
function parseRoomNumber(str) {
  if (!str) return null;
  const cleaned = str.replace(/^(room|r|#)\s*/i, '').trim();
  // Must be a reasonable room number (1-999)
  const match = cleaned.match(/^(\d{1,3})$/);
  return match ? match[1] : null;
}

/**
 * Fetch a single sale by ID.
 *
 * Uses /sales/<id> — NOT /external/get_sale_details/<id>. The "external"
 * path only exists for the plural list endpoint; the singular /external
 * form 404s, which silently broke webhook handlers until 2026-04-21.
 *
 * Returns null when the sale does not exist (404) so callers can distinguish
 * "sale gone / hard-deleted" from "network/API error".
 * @param {string} saleId
 * @returns {Promise<any|null>} The sale object, or null if not found
 */
async function fetchSaleById(saleId) {
  const token = await getToken();
  const res = await fetch(`${BASE}/sales/${saleId}`, {
    headers: { Authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(20_000),
  });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`Goodtill sales/${saleId} failed: ${res.status} ${res.statusText}`);
  const data = await res.json();
  return data.data || null;
}

/**
 * Cheapest possible Goodtill health probe — hits /login and discards the token.
 * Proves API is reachable AND credentials are valid, without pulling any data.
 */
async function ping() {
  const res = await fetch(`${BASE}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      subdomain: process.env.GOODTILL_SUBDOMAIN,
      username: process.env.GOODTILL_USERNAME,
      password: process.env.GOODTILL_PASSWORD,
    }),
    signal: AbortSignal.timeout(10_000),
  });
  if (!res.ok) throw new Error(`Goodtill login failed: ${res.status} ${res.statusText}`);
}

module.exports = {
  fetchSales,
  fetchSaleById,
  formatDateTime,
  cyprusLocalToUtcIso,
  extractGuestFolioSales,
  parseRoomNumber,
  listCustomers,
  createCustomer,
  updateCustomer,
  activateCustomer,
  deactivateCustomer,
  ping,
};
