const { test } = require('node:test');
const assert = require('node:assert');

const { _internal: rd } = require('./resdiary');
const sync = require('./resdiary-sync');
const { _internal: s } = sync;

// ── resdiary.js pure helpers ─────────────────────────────────────────

test('parseUtc treats zoneless ResDiary timestamps as UTC', () => {
  const d = rd.parseUtc('2026-08-05T17:23:06.2470000');
  assert.ok(d);
  assert.strictEqual(d.toISOString(), '2026-08-05T17:23:06.247Z');
});

test('parseUtc keeps explicit zones and rejects garbage', () => {
  assert.strictEqual(rd.parseUtc('2026-08-05T17:23:06Z').toISOString(), '2026-08-05T17:23:06.000Z');
  assert.strictEqual(rd.parseUtc('not-a-date'), null);
  assert.strictEqual(rd.parseUtc(null), null);
});

test('tokenNeedsRefresh honours the 10-minute margin', () => {
  const now = Date.now();
  assert.strictEqual(rd.tokenNeedsRefresh(null, now), true);
  assert.strictEqual(rd.tokenNeedsRefresh({ token: 't', expiresAtMs: now + 60 * 60_000 }, now), false);
  assert.strictEqual(rd.tokenNeedsRefresh({ token: 't', expiresAtMs: now + 5 * 60_000 }, now), true);
  assert.strictEqual(rd.tokenNeedsRefresh({ token: 't', expiresAtMs: now - 1000 }, now), true);
});

test('looksBlocked spots Cloudflare HTML but not API errors', () => {
  assert.strictEqual(rd.looksBlocked(403, '<!DOCTYPE html><html>Attention Required'), true);
  assert.strictEqual(rd.looksBlocked(403, 'error code: 1010'), true);
  assert.strictEqual(rd.looksBlocked(403, '{"error":"denied"}'), false);
  assert.strictEqual(rd.looksBlocked(200, '<!DOCTYPE html>'), false);
});

// ── resdiary-sync.js pure helpers ────────────────────────────────────

test('listDatesInclusive walks inclusive UTC dates across month ends', () => {
  assert.deepStrictEqual(s.listDatesInclusive('2026-02-27', '2026-03-02'), [
    '2026-02-27',
    '2026-02-28',
    '2026-03-01',
    '2026-03-02',
  ]);
  assert.deepStrictEqual(s.listDatesInclusive('2026-08-04', '2026-08-04'), ['2026-08-04']);
});

test('listDatesInclusive rejects inverted and absurd ranges', () => {
  assert.throws(() => s.listDatesInclusive('2026-08-05', '2026-08-04'), /inverted/);
  assert.throws(() => s.listDatesInclusive('1990-01-01', '2026-01-01'), /exceeds/);
});

test('chunk splits into bounded batches', () => {
  assert.deepStrictEqual(s.chunk([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  assert.deepStrictEqual(s.chunk([], 10), []);
});

test('unwrapList handles arrays, paging envelopes, and junk', () => {
  assert.deepStrictEqual(s.unwrapList([1, 2]), [1, 2]);
  assert.deepStrictEqual(s.unwrapList({ TotalPages: 3, Data: ['a'] }), ['a']);
  assert.deepStrictEqual(s.unwrapList({ nope: true }), []);
  assert.deepStrictEqual(s.unwrapList(null), []);
});

test('classifyChangeRow: nested booking, inline booking, bare id, junk', () => {
  assert.deepStrictEqual(s.classifyChangeRow({ Booking: { Id: 9 } }), { kind: 'booking', booking: { Id: 9 } });
  const inline = { Id: 4, VisitDateTime: '2026-08-04T19:00:00', CoversBooked: 2 };
  assert.deepStrictEqual(s.classifyChangeRow(inline), { kind: 'booking', booking: inline });
  assert.deepStrictEqual(s.classifyChangeRow({ BookingId: 77 }), { kind: 'id', id: 77 });
  assert.deepStrictEqual(s.classifyChangeRow({ Id: 12 }), { kind: 'id', id: 12 });
  assert.deepStrictEqual(s.classifyChangeRow('x'), { kind: 'skip' });
  assert.deepStrictEqual(s.classifyChangeRow(null), { kind: 'skip' });
});

test('classifyCustomerRow unwraps nested Customer', () => {
  assert.deepStrictEqual(s.classifyCustomerRow({ Customer: { Id: 1 } }), { Id: 1 });
  assert.deepStrictEqual(s.classifyCustomerRow({ Id: 2, Email: 'x@y.z' }), { Id: 2, Email: 'x@y.z' });
  assert.strictEqual(s.classifyCustomerRow(null), null);
});

// ── HTTP surface, mounted on a bare express app (no bridge init) ────

test('resdiary routes: forbidden without token, 503 when unconfigured, status shape', async (t) => {
  const express = require('express');
  const app = express();
  app.use(express.json());

  process.env.BRIDGE_ADMIN_TOKEN = 'test-admin-token';
  delete process.env.RESDIARY_USERNAME;
  delete process.env.RESDIARY_PASSWORD;
  delete process.env.RESDIARY_DEPLOYMENT_ID;
  delete process.env.RESDIARY_PROVIDER_ID;
  delete process.env.RESDIARY_INGEST_TOKEN;

  sync.registerResdiaryRoutes(app);
  const server = app.listen(0);
  t.after(() => server.close());
  const base = `http://127.0.0.1:${server.address().port}`;

  // no token → 403 on every surface
  for (const [method, p] of [
    ['GET', '/resdiary/status'],
    ['GET', '/resdiary/whoami'],
    ['POST', '/resdiary/backfill'],
    ['POST', '/resdiary/reconcile'],
  ]) {
    const res = await fetch(base + p, { method });
    assert.strictEqual(res.status, 403, `${method} ${p} without token`);
  }

  // wrong token → still 403
  const bad = await fetch(base + '/resdiary/status', { headers: { 'X-Bridge-Token': 'wrong' } });
  assert.strictEqual(bad.status, 403);

  // admin header → status OK and reports unconfigured
  const ok = await fetch(base + '/resdiary/status', { headers: { 'X-Bridge-Token': 'test-admin-token' } });
  assert.strictEqual(ok.status, 200);
  const body = await ok.json();
  assert.strictEqual(body.configured.creds, false);
  assert.strictEqual(body.configured.ids, false);
  assert.strictEqual(body.configured.ingestToken, false);
  assert.strictEqual(body.sync.running, false);

  // worker bearer also accepted
  process.env.RESDIARY_WORKER_TOKEN = 'test-worker-token';
  const bearer = await fetch(base + '/resdiary/status', { headers: { Authorization: 'Bearer test-worker-token' } });
  assert.strictEqual(bearer.status, 200);

  // unconfigured → whoami 503, backfill/reconcile 503
  const who = await fetch(base + '/resdiary/whoami', { headers: { 'X-Bridge-Token': 'test-admin-token' } });
  assert.strictEqual(who.status, 503);
  const bf = await fetch(base + '/resdiary/backfill', { method: 'POST', headers: { 'X-Bridge-Token': 'test-admin-token' } });
  assert.strictEqual(bf.status, 503);
  const rc = await fetch(base + '/resdiary/reconcile', { method: 'POST', headers: { 'X-Bridge-Token': 'test-admin-token' } });
  assert.strictEqual(rc.status, 503);
});
