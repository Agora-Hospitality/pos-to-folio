/**
 * ResDiary EPOS API client — 2-legged OAuth 1.0a.
 *
 * Shapes verified against ResDiary's own Postman collection
 * (/Content/postman/ResDiary-EPOS-API-v1.postman_collection.json — served
 * unauthenticated from the portal, fetched 31-08-2026). The facts that bit:
 *
 *   · OAuth params travel in the AUTHORIZATION HEADER (addParamsToHeader),
 *     never the body. A form-encoded body POST to /OAuth/V10a is answered
 *     with a redirect to an HTML error page — which fetch() follows, so the
 *     failure surfaces as "200 with HTML" unless redirects are refused.
 *   · Request token: POST {oauthUrl}?second_secret=..&scope=.. — the TWO
 *     QUERY params are part of the signature base. Access token: POST the
 *     bare oauthUrl with oauth_token from step 1, signed with its secret.
 *   · scope is the constant http://app.restaurantdiary.com/WebServices/Epos/v1
 *     (same across environments — restaurantdiary.com, not resdiary.com).
 *   · Token responses are form-encoded: oauth_token=..&oauth_token_secret=..
 *   · DiaryData / BookingChanges take ?date= QUERY params (signed!), not
 *     path segments.
 *   · Add Booking is JSON with .NET dates ("/Date(1513247400000)/"), field
 *     `Covers` (not PartySize), Type "Internal", nested Customer; response
 *     carries BookingId + Customer.CustomerId.
 *   · Add Receipt is XML: <Receipt><Items><Item><Description/><Quantity/>
 *     <Price/></Item>…</Items></Receipt>.
 *
 * Purpose: push till receipts onto bookings — the only way booking spend is
 * ever populated. Access released sandbox credentials 31-08-2026; production
 * Second Secret follows a successful sandbox receipt demo, which is exactly
 * what runEposDemo() performs. Dormant until RESDIARY_EPOS_* env is set.
 */

const crypto = require('node:crypto');

const DEFAULT_SCOPE = 'http://app.restaurantdiary.com/WebServices/Epos/v1';

function cfg() {
  return {
    oauthUrl: process.env.RESDIARY_EPOS_OAUTH_URL || '', // e.g. https://app.rdbranch.com/OAuth/V10a
    apiUrl: (process.env.RESDIARY_EPOS_API_URL || '').replace(/\/$/, ''), // e.g. https://app.rdbranch.com/WebServices/Epos/v1
    consumerKey: process.env.RESDIARY_EPOS_CONSUMER_KEY || '',
    consumerSecret: process.env.RESDIARY_EPOS_CONSUMER_SECRET || '',
    secondSecret: process.env.RESDIARY_EPOS_SECOND_SECRET || '',
    restaurantId: process.env.RESDIARY_EPOS_RESTAURANT_ID || '',
    scope: process.env.RESDIARY_EPOS_SCOPE || DEFAULT_SCOPE,
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

/** The signature base string: METHOD & enc(bare url) & enc(sorted params). */
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

/** Split a URL into its bare form and query params (both signed under OAuth1). */
function splitUrl(fullUrl) {
  const u = new URL(fullUrl);
  const query = {};
  for (const [k, v] of u.searchParams.entries()) query[k] = v;
  u.search = '';
  return { bareUrl: u.toString(), query };
}

/**
 * Sign a request and return the Authorization header. Query params are part
 * of the signature base; oauth_* params travel ONLY in the header.
 */
function buildOAuthHeader(method, fullUrl, { consumerKey, consumerSecret, token, tokenSecret }) {
  const { bareUrl, query } = splitUrl(fullUrl);
  const oauth = { ...oauthBase(consumerKey) };
  if (token) oauth.oauth_token = token;
  const base = baseString(method, bareUrl, { ...query, ...oauth });
  oauth.oauth_signature = hmacSign(base, consumerSecret, tokenSecret || '');
  const header =
    'OAuth ' +
    Object.entries(oauth)
      .map(([k, v]) => `${pct(k)}="${pct(v)}"`)
      .join(', ');
  return header;
}

/** Token responses are form-encoded; tolerate JSON too. */
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

/** undici hides the network cause behind "fetch failed" — dig it out. */
function causeOf(err) {
  return err?.cause?.code || err?.cause?.message || err?.message || 'unknown';
}

async function oauthTokenCall(fullUrl, creds) {
  let lastErr = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    if (attempt > 0) await new Promise((r) => setTimeout(r, 2000));
    // Fresh header per attempt — nonce/timestamp must not be reused.
    const header = buildOAuthHeader('POST', fullUrl, creds);
    let res, text;
    try {
      // redirect:"manual" — a rejected OAuth call answers with a redirect to
      // an HTML error page; following it hides the real failure as "200+HTML".
      res = await fetch(fullUrl, {
        method: 'POST',
        headers: { Authorization: header, 'Content-Length': '0' },
        redirect: 'manual',
        signal: AbortSignal.timeout(30_000),
      });
      text = await res.text();
    } catch (err) {
      lastErr = new Error(`OAuth network error: ${causeOf(err)} (if this recurs, boot with NODE_OPTIONS=--network-family-autoselection-attempt-timeout=10000)`);
      continue;
    }
    if (res.status >= 300 && res.status < 400) {
      throw new Error(`OAuth rejected (redirect ${res.status} → ${res.headers.get('location') || '?'}) — check consumer key/secret, second_secret and scope`);
    }
    if (!res.ok) throw new Error(`OAuth HTTP ${res.status}: ${text.slice(0, 300)}`);
    const parsed = parseTokenResponse(text);
    if (!parsed.token) throw new Error(`OAuth: no oauth_token in response: ${text.slice(0, 200)}`);
    return parsed;
  }
  throw lastErr;
}

// Access token cached per process; lifetime is undocumented, so a 401 on an
// API call clears it and the next call re-runs the dance.
let _access = null;

async function getAccessToken(force = false) {
  if (_access && !force) return _access;
  const c = cfg();
  if (!eposConfigured()) throw new Error('EPOS not configured (RESDIARY_EPOS_* env)');

  // Step 1 — request token: second_secret + scope as SIGNED query params.
  const requestUrl = `${c.oauthUrl}?second_secret=${pct(c.secondSecret)}&scope=${pct(c.scope)}`;
  const request = await oauthTokenCall(requestUrl, {
    consumerKey: c.consumerKey,
    consumerSecret: c.consumerSecret,
  });

  // Step 2 — exchange for the access token, signed with the request secret.
  const access = await oauthTokenCall(c.oauthUrl, {
    consumerKey: c.consumerKey,
    consumerSecret: c.consumerSecret,
    token: request.token,
    tokenSecret: request.secret || '',
  });
  _access = access;
  return _access;
}

/**
 * Signed EPOS API request. `body` is {json} OR {xml}; query params on the
 * path are signed automatically. Returns {status, ok, body} with the vendor
 * response verbatim (JSON-parsed when possible).
 */
async function eposFetch(method, apiPath, body) {
  const c = cfg();
  const token = await getAccessToken();
  const url = `${c.apiUrl}${apiPath}`;

  const attempt = async (tok) => {
    const header = buildOAuthHeader(method, url, {
      consumerKey: c.consumerKey,
      consumerSecret: c.consumerSecret,
      token: tok.token,
      tokenSecret: tok.secret || '',
    });
    const headers = { Authorization: header, Accept: 'application/json' };
    let payload;
    if (body?.json !== undefined) {
      headers['Content-Type'] = 'application/json';
      payload = JSON.stringify(body.json);
    } else if (body?.xml !== undefined) {
      headers['Content-Type'] = 'application/xml';
      payload = body.xml;
    }
    let res;
    try {
      res = await fetch(url, {
        method,
        headers,
        body: payload,
        redirect: 'manual',
        signal: AbortSignal.timeout(60_000),
      });
    } catch (err) {
      throw new Error(`network error calling ${apiPath}: ${causeOf(err)}`);
    }
    const text = await res.text();
    let parsed;
    try {
      parsed = text ? JSON.parse(text) : null;
    } catch {
      parsed = text.slice(0, 500);
    }
    const redirected = res.status >= 300 && res.status < 400;
    return {
      status: res.status,
      ok: res.ok,
      body: redirected ? `redirect → ${res.headers.get('location') || '?'} (auth rejected?)` : parsed,
    };
  };

  let out = await attempt(token);
  if (out.status === 401 || out.status === 302) {
    out = await attempt(await getAccessToken(true)); // stale token — one re-dance
  }
  return out;
}

// ── Demo helpers ─────────────────────────────────────────────────────

/** .NET JSON date: "/Date(ms)/" — what Add Booking expects. */
function dotNetDate(ms) {
  return `/Date(${ms})/`;
}

const xmlEsc = (s) => String(s).replace(/[<>&'"]/g, (ch) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', "'": '&apos;', '"': '&quot;' }[ch]));

/** [{description, quantity, price}] → the Receipt XML the API expects. */
function buildReceiptXml(items) {
  const rows = items
    .map(
      (i) =>
        `    <Item>\n      <Description>${xmlEsc(i.description)}</Description>\n      <Quantity>${Number(i.quantity) || 1}</Quantity>\n      <Price>${Number(i.price).toFixed(2)}</Price>\n    </Item>`
    )
    .join('\n');
  return `<Receipt>\n  <Items>\n${rows}\n  </Items>\n</Receipt>`;
}

/**
 * Hunt a usable table id anywhere in a restaurant-setup/diary response:
 * the first entry of any non-empty `Tables` array (numbers, or objects with
 * an Id). Sandbox bookings require at least one table.
 */
function findFirstTableId(node, depth = 0) {
  if (!node || typeof node !== 'object' || depth > 8) return null;
  if (Array.isArray(node)) {
    for (const item of node) {
      const hit = findFirstTableId(item, depth + 1);
      if (hit !== null) return hit;
    }
    return null;
  }
  for (const [k, v] of Object.entries(node)) {
    if (k === 'Tables' && Array.isArray(v) && v.length) {
      const first = v[0];
      if (typeof first === 'number') return first;
      if (first && typeof first === 'object') {
        const id = first.Id ?? first.TableId;
        if (id !== undefined && id !== null) return id;
      }
    }
  }
  for (const v of Object.values(node)) {
    const hit = findFirstTableId(v, depth + 1);
    if (hit !== null) return hit;
  }
  return null;
}

// ── Demo: the exact flow Access wants to see before releasing prod ──

/**
 * End-to-end sandbox proof: token dance → read the diary → create a booking
 * (unless `bookingId` is supplied) → push a receipt onto it → read it back.
 * Every vendor response is returned verbatim; `booking` (JSON object) and
 * `receipt` ({items:[{description,quantity,price}]} or raw `receiptXml`)
 * override the defaults so shapes are iterated with curl, not redeploys.
 */
async function runEposDemo({ date, bookingId, covers = 2, booking, receipt, receiptXml, tableId } = {}) {
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
    return { ok: true, status: 200, body: 'request + access token dance completed' };
  });
  if (!tok) return { ok: false, steps };

  await step('diaryData', () =>
    eposFetch('GET', `/Restaurant/${c.restaurantId}/DiaryData?date=${day}&includeUnallocatedBookings=true`)
  );

  let targetId = bookingId ?? null;
  if (!targetId) {
    // Sandbox validation: "At least one table must be specified" — discover
    // one from the restaurant setup unless the caller named it.
    let table = tableId ?? null;
    if (!table && !booking) {
      const setup = await step('restaurantSetup', () => eposFetch('GET', `/Restaurant/${c.restaurantId}`));
      table = findFirstTableId(setup?.body);
      if (!table) return { ok: false, steps, note: 'no table id found in the Restaurant setup — pass {"tableId": N} and re-run' };
      steps.push({ name: 'tableDiscovery', ok: true, status: null, body: `using table ${table}` });
    }

    const visitMs = Date.parse(`${day}T19:00:00Z`);
    const defaults = {
      VisitDateTime: dotNetDate(visitMs),
      Covers: covers,
      AreaId: 0,
      ServiceId: 0,
      MenuId: 0,
      ChannelId: 0,
      Type: 'Internal',
      Comments: 'Agora EPOS integration demo',
      Customer: {
        CustomerId: 0,
        Title: 'Mr',
        FirstName: 'Agora',
        Surname: 'EposDemo',
        Email: 'epos-demo@theagorahotel.com',
        MobileNumber: '99000000',
      },
      Promotions: [],
      Payments: [],
      Extras: [],
      Tables: table ? [table] : [],
    };
    const created = await step('createBooking', () =>
      eposFetch('POST', `/Restaurant/${c.restaurantId}/Booking?overrideCovers=true`, { json: booking || defaults })
    );
    targetId = created?.body?.BookingId ?? created?.body?.Id ?? null;
    if (!targetId) return { ok: false, steps, note: 'no BookingId from createBooking — adjust the `booking` payload and re-run' };
  }

  const xml =
    receiptXml ||
    buildReceiptXml(
      receipt?.items || [
        { description: 'Tasting menu', quantity: 2, price: 20.0 },
        { description: 'Wine pairing', quantity: 1, price: 7.5 },
      ]
    );
  await step('postReceipt', () => eposFetch('POST', `/Restaurant/${c.restaurantId}/Booking/${targetId}/Receipt`, { xml }));
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
  _internal: { pct, baseString, hmacSign, parseTokenResponse, oauthBase, splitUrl, buildOAuthHeader, dotNetDate, buildReceiptXml, findFirstTableId },
};
