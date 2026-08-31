/**
 * ResDiary EPOS API client — 2-legged OAuth 1.0a (per the EPOS docs in the
 * API portal: Deployments table gives per-region OAuth + API hosts; requests
 * need consumer key/secret, the per-restaurant SECOND SECRET, and a scope).
 *
 * Purpose: push till receipts onto ResDiary bookings — the only way the
 * booking's spend field is ever populated — plus diary reads. Access released
 * sandbox EPOS credentials on 31-08-2026; the PRODUCTION Second Secret is
 * handed over only after we demonstrate receipts posting successfully in
 * sandbox. `runEposDemo()` below IS that demonstration, end to end.
 *
 * Payload shapes for Create Booking / Receipt are not in the docs we hold, so
 * the demo accepts overrides and returns every vendor response VERBATIM —
 * iterate against sandbox with one curl, no redeploys.
 *
 * Everything is DORMANT until the RESDIARY_EPOS_* env is set.
 */

const crypto = require('node:crypto');

function cfg() {
  return {
    oauthUrl: process.env.RESDIARY_EPOS_OAUTH_URL || '', // e.g. https://uk.resdiary.com/OAuth/V10a
    apiUrl: (process.env.RESDIARY_EPOS_API_URL || '').replace(/\/$/, ''), // e.g. https://uk.resdiary.com/WebServices/Epos/v1
    consumerKey: process.env.RESDIARY_EPOS_CONSUMER_KEY || '',
    consumerSecret: process.env.RESDIARY_EPOS_CONSUMER_SECRET || '',
    secondSecret: process.env.RESDIARY_EPOS_SECOND_SECRET || '',
    restaurantId: process.env.RESDIARY_EPOS_RESTAURANT_ID || '',
    scope: process.env.RESDIARY_EPOS_SCOPE || 'http://rd.resdiary.com/api/epos',
  };
}

function eposConfigured() {
  const c = cfg();
  return !!(c.oauthUrl && c.apiUrl && c.consumerKey && c.consumerSecret && c.secondSecret && c.restaurantId);
}

// ── OAuth 1.0a primitives (RFC 5849, HMAC-SHA1) ─────────────────────

/** RFC 3986 percent-encoding — encodeURIComponent plus !'()* . */
function pct(v) {
  return encodeURIComponent(String(v)).replace(/[!'()*]/g, (c) => '%' + c.charCodeAt(0).toString(16).toUpperCase());
}

/** The signature base string: METHOD & enc(url) & enc(sorted params). */
function baseString(method, url, params) {
  const pairs = Object.entries(params)
    .map(([k, v]) => [pct(k), pct(v)])
    .sort(([ak, av], [bk, bv]) => (ak === bk ? (av < bv ? -1 : 1) : ak < bk ? -1 : 1))
    .map(([k, v]) => `${k}=${v}`)
    .join('&');
  return `${method.toUpperCase()}&${pct(url)}&${pct(pairs)}`;
}

function hmacSign(base, consumerSecret, tokenSecret = '') {
  return crypto.createHmac('sha1', `${pct(consumerSecret)}&${pct(tokenSecret)}`).update(base).digest('base64');
}

function oauthBase(consumerKey) {
  return {
    oauth_consumer_key: consumerKey,
    oauth_nonce: crypto.randomBytes(16).toString('hex'),
    oauth_timestamp: String(Math.floor(Date.now() / 1000)),
    oauth_signature_method: 'HMAC-SHA1',
    oauth_version: '1.0',
  };
}

/** Token responses are classically form-encoded; tolerate JSON too. */
function parseTokenResponse(text) {
  const t = String(text).trim();
  if (t.startsWith('{')) {
    try {
      const j = JSON.parse(t);
      return { token: j.oauth_token || j.Token || null, secret: j.oauth_token_secret || j.TokenSecret || null, raw: t };
    } catch {
      return { token: null, secret: null, raw: t };
    }
  }
  const p = new URLSearchParams(t);
  return { token: p.get('oauth_token'), secret: p.get('oauth_token_secret'), raw: t };
}

async function oauthTokenCall(url, params, consumerSecret, tokenSecret = '') {
  const signed = { ...params, oauth_signature: hmacSign(baseString('POST', url, params), consumerSecret, tokenSecret) };
  const body = new URLSearchParams(signed).toString();
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
    signal: AbortSignal.timeout(30_000),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`OAuth ${url} HTTP ${res.status}: ${text.slice(0, 300)}`);
  const parsed = parseTokenResponse(text);
  if (!parsed.token) throw new Error(`OAuth ${url}: no oauth_token in response: ${text.slice(0, 200)}`);
  return parsed;
}

// Access token cached per process; EPOS token lifetime is undocumented, so a
// 401 on an API call clears it and the next call re-runs the dance.
let _access = null;

async function getAccessToken(force = false) {
  if (_access && !force) return _access;
  const c = cfg();
  if (!eposConfigured()) throw new Error('EPOS not configured (RESDIARY_EPOS_* env)');

  const request = await oauthTokenCall(
    c.oauthUrl,
    { ...oauthBase(c.consumerKey), second_secret: c.secondSecret, scope: c.scope },
    c.consumerSecret
  );
  const access = await oauthTokenCall(
    c.oauthUrl,
    { ...oauthBase(c.consumerKey), oauth_token: request.token, second_secret: c.secondSecret, scope: c.scope },
    c.consumerSecret,
    request.secret || ''
  );
  _access = access;
  return _access;
}

/** Signed EPOS API request. JSON in/out; OAuth params travel in the header. */
async function eposFetch(method, apiPath, jsonBody) {
  const c = cfg();
  const token = await getAccessToken();
  const url = `${c.apiUrl}${apiPath}`;

  const attempt = async (tok) => {
    const params = { ...oauthBase(c.consumerKey), oauth_token: tok.token };
    params.oauth_signature = hmacSign(baseString(method, url, params), c.consumerSecret, tok.secret || '');
    const header =
      'OAuth ' +
      Object.entries(params)
        .map(([k, v]) => `${pct(k)}="${pct(v)}"`)
        .join(', ');
    const res = await fetch(url, {
      method,
      headers: {
        Authorization: header,
        Accept: 'application/json',
        ...(jsonBody !== undefined ? { 'Content-Type': 'application/json' } : {}),
      },
      body: jsonBody !== undefined ? JSON.stringify(jsonBody) : undefined,
      signal: AbortSignal.timeout(60_000),
    });
    const text = await res.text();
    let body;
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      body = text;
    }
    return { status: res.status, ok: res.ok, body };
  };

  let out = await attempt(token);
  if (out.status === 401) {
    out = await attempt(await getAccessToken(true)); // stale token — one re-dance
  }
  return out;
}

// ── Demo: the exact flow Access wants to see before releasing prod ──

/**
 * End-to-end sandbox proof: token dance → read today's diary → create a
 * booking (unless `bookingId` is supplied) → push a receipt onto it → read it
 * back. Returns a step-by-step transcript with every vendor response, so a
 * failing shape is fixed by re-running with `booking`/`receipt` overrides.
 */
async function runEposDemo({ date, bookingId, covers = 2, booking, receipt } = {}) {
  const c = cfg();
  const day = date || new Date().toISOString().slice(0, 10);
  const steps = [];
  const step = async (name, fn) => {
    try {
      const out = await fn();
      const ok = out && out.ok !== false;
      steps.push({ name, ok, status: out?.status ?? null, body: out?.body ?? out ?? null });
      return ok ? out : null;
    } catch (err) {
      steps.push({ name, ok: false, error: err.message });
      return null;
    }
  };

  const tok = await step('oauth', async () => {
    await getAccessToken(true);
    return { ok: true, status: 200, body: 'request+access token dance completed' };
  });
  if (!tok) return { ok: false, steps };

  await step('diaryData', () => eposFetch('GET', `/Restaurant/${c.restaurantId}/DiaryData/${day}`));

  let targetId = bookingId ?? null;
  if (!targetId) {
    const defaults = {
      VisitDate: day,
      VisitTime: '19:00:00',
      PartySize: covers,
      Customer: { FirstName: 'Agora', Surname: 'EposDemo', Email: 'epos-demo@theagorahotel.com' },
    };
    const created = await step('createBooking', () =>
      eposFetch('POST', `/Restaurant/${c.restaurantId}/Booking?overrideCovers=true`, booking || defaults)
    );
    targetId = created?.body?.Id ?? created?.body?.BookingId ?? created?.body?.Booking?.Id ?? null;
    if (!targetId) return { ok: false, steps, note: 'no booking id from createBooking — adjust the `booking` payload and re-run' };
  }

  const defaultReceipt = {
    Total: 47.5,
    Items: [
      { Name: 'Tasting menu', Quantity: 2, UnitPrice: 20.0 },
      { Name: 'Wine pairing', Quantity: 1, UnitPrice: 7.5 },
    ],
  };
  await step('postReceipt', () =>
    eposFetch('POST', `/Restaurant/${c.restaurantId}/Booking/${targetId}/Receipt`, receipt || defaultReceipt)
  );
  await step('readBack', () => eposFetch('GET', `/Restaurant/${c.restaurantId}/Booking/${targetId}`));

  return { ok: steps.every((s) => s.ok), bookingId: targetId, steps };
}

/** Auth mirror of resdiary-sync's route gate (admin header or worker bearer). */
function authorized(req) {
  const safeEq = (a, b) => {
    const ba = Buffer.from(String(a), 'utf8');
    const bb = Buffer.from(String(b), 'utf8');
    return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
  };
  const admin = process.env.BRIDGE_ADMIN_TOKEN || '';
  const worker = process.env.RESDIARY_WORKER_TOKEN || '';
  const xToken = req.get('X-Bridge-Token') || '';
  const bearer = (req.get('Authorization') || '').replace(/^Bearer\s+/i, '');
  return (admin && xToken && safeEq(admin, xToken)) || (worker && bearer && safeEq(worker, bearer));
}

function registerEposRoutes(app) {
  app.get('/resdiary/epos-status', (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    const c = cfg();
    res.json({
      configured: eposConfigured(),
      pieces: {
        oauthUrl: !!c.oauthUrl,
        apiUrl: !!c.apiUrl,
        consumerKey: !!c.consumerKey,
        consumerSecret: !!c.consumerSecret,
        secondSecret: !!c.secondSecret,
        restaurantId: !!c.restaurantId,
      },
      tokenCached: !!_access,
    });
  });

  app.post('/resdiary/epos-demo', async (req, res) => {
    if (!authorized(req)) return res.status(403).json({ error: 'forbidden' });
    if (!eposConfigured()) return res.status(503).json({ error: 'epos_not_configured' });
    try {
      res.json(await runEposDemo(req.body || {}));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });
}

module.exports = {
  eposConfigured,
  runEposDemo,
  registerEposRoutes,
  // exported for tests
  _internal: { pct, baseString, hmacSign, parseTokenResponse, oauthBase },
};
