const { test } = require('node:test');
const assert = require('node:assert');

const { _internal: e } = require('./resdiary-epos');
const eposModule = require('./resdiary-epos');

test('pct is RFC 3986: space %20, asterisk %2A, tilde untouched', () => {
  assert.strictEqual(e.pct('a b'), 'a%20b');
  assert.strictEqual(e.pct('a*b'), 'a%2Ab');
  assert.strictEqual(e.pct("a!'()"), 'a%21%27%28%29');
  assert.strictEqual(e.pct('a~b-c_d.e'), 'a~b-c_d.e');
});

test('baseString sorts encoded params and joins with & once', () => {
  const base = e.baseString('post', 'https://uk.resdiary.com/OAuth/V10a', {
    oauth_version: '1.0',
    oauth_consumer_key: 'key',
    scope: 'http://rd.resdiary.com/api/epos',
  });
  assert.ok(base.startsWith('POST&https%3A%2F%2Fuk.resdiary.com%2FOAuth%2FV10a&'));
  const paramPart = decodeURIComponent(base.split('&').slice(2).join('&'));
  // sorted: oauth_consumer_key < oauth_version < scope
  assert.ok(paramPart.indexOf('oauth_consumer_key=') < paramPart.indexOf('oauth_version='));
  assert.ok(paramPart.indexOf('oauth_version=') < paramPart.indexOf('scope='));
  // the scope value is itself percent-encoded inside the (encoded) param string
  assert.ok(base.includes('scope%3Dhttp%253A%252F%252Frd.resdiary.com'));
});

test('hmacSign is deterministic and key = enc(consumer)&enc(tokenSecret)', () => {
  const a = e.hmacSign('BASE', 'cs', 'ts');
  const b = e.hmacSign('BASE', 'cs', 'ts');
  const c = e.hmacSign('BASE', 'cs', '');
  assert.strictEqual(a, b);
  assert.notStrictEqual(a, c);
  // matches an independent computation of the same RFC construction
  const crypto = require('node:crypto');
  assert.strictEqual(a, crypto.createHmac('sha1', 'cs&ts').update('BASE').digest('base64'));
});

test('parseTokenResponse reads form-encoded and JSON shapes', () => {
  assert.deepStrictEqual(
    e.parseTokenResponse('oauth_token=T&oauth_token_secret=S'),
    { token: 'T', secret: 'S', raw: 'oauth_token=T&oauth_token_secret=S' }
  );
  const j = e.parseTokenResponse('{"oauth_token":"T2","oauth_token_secret":"S2"}');
  assert.strictEqual(j.token, 'T2');
  assert.strictEqual(j.secret, 'S2');
  assert.strictEqual(e.parseTokenResponse('nonsense').token, null);
});

test('buildOAuthHeader signs query params and keeps them out of the header', () => {
  const h = e.buildOAuthHeader('POST', 'https://app.rdbranch.com/OAuth/V10a?second_secret=SS&scope=http://x/y', {
    consumerKey: 'ck',
    consumerSecret: 'cs',
  });
  assert.ok(h.startsWith('OAuth '));
  assert.ok(h.includes('oauth_consumer_key="ck"'));
  assert.ok(h.includes('oauth_signature="'));
  assert.ok(!h.includes('second_secret')); // query params are signed, never carried in the header
  assert.ok(!h.includes('scope='));
});

test('splitUrl separates bare url from query for the signature base', () => {
  const { bareUrl, query } = e.splitUrl('https://h/x/y?b=2&a=1');
  assert.strictEqual(bareUrl, 'https://h/x/y');
  assert.deepStrictEqual(query, { b: '2', a: '1' });
});

test('dotNetDate and receipt XML match the Postman collection shapes', () => {
  assert.strictEqual(e.dotNetDate(1513247400000), '/Date(1513247400000)/');
  const xml = e.buildReceiptXml([{ description: 'Fish & chips', quantity: 1, price: 10.99 }]);
  assert.ok(xml.startsWith('<Receipt>'));
  assert.ok(xml.includes('<Description>Fish &amp; chips</Description>'));
  assert.ok(xml.includes('<Quantity>1</Quantity>'));
  assert.ok(xml.includes('<Price>10.99</Price>'));
  assert.ok(xml.trim().endsWith('</Receipt>'));
});

test('oauthBase carries the five required params with fresh nonces', () => {
  const a = e.oauthBase('k');
  const b = e.oauthBase('k');
  assert.strictEqual(a.oauth_consumer_key, 'k');
  assert.strictEqual(a.oauth_signature_method, 'HMAC-SHA1');
  assert.strictEqual(a.oauth_version, '1.0');
  assert.ok(/^\d+$/.test(a.oauth_timestamp));
  assert.notStrictEqual(a.oauth_nonce, b.oauth_nonce);
});

test('epos routes: 403 without token, 503 unconfigured, status shape', async (t) => {
  const express = require('express');
  const app = express();
  app.use(express.json());

  process.env.BRIDGE_ADMIN_TOKEN = 'epos-test-admin';
  for (const k of Object.keys(process.env)) if (k.startsWith('RESDIARY_EPOS_')) delete process.env[k];

  eposModule.registerEposRoutes(app);
  const server = app.listen(0);
  t.after(() => server.close());
  const base = `http://127.0.0.1:${server.address().port}`;

  const noAuth = await fetch(base + '/resdiary/epos-status');
  assert.strictEqual(noAuth.status, 403);

  const st = await fetch(base + '/resdiary/epos-status', { headers: { 'X-Bridge-Token': 'epos-test-admin' } });
  assert.strictEqual(st.status, 200);
  const body = await st.json();
  assert.strictEqual(body.configured, false);
  assert.strictEqual(body.pieces.secondSecret, false);

  const demo = await fetch(base + '/resdiary/epos-demo', {
    method: 'POST',
    headers: { 'X-Bridge-Token': 'epos-test-admin', 'Content-Type': 'application/json' },
    body: '{}',
  });
  assert.strictEqual(demo.status, 503);
});
