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

test('classifyChangeRow only ever yields an id — change records are never forwarded as bookings', () => {
  // The 02-09-2026 clobber: an inline "booking-looking" change row must NOT come back as a booking.
  const inline = { Id: 4, VisitDateTime: '2026-08-04T19:00:00', CoversBooked: 2 };
  assert.deepStrictEqual(s.classifyChangeRow(inline), { kind: 'id', id: 4 });
  assert.deepStrictEqual(s.classifyChangeRow({ Booking: { Id: 9 } }), { kind: 'id', id: 9 });
  assert.deepStrictEqual(s.classifyChangeRow({ BookingId: 77, ChangeType: 'PartySize' }), { kind: 'id', id: 77 });
  assert.deepStrictEqual(s.classifyChangeRow({ Id: 12 }), { kind: 'id', id: 12 });
  assert.deepStrictEqual(s.classifyChangeRow({ ChangeType: 'x' }), { kind: 'skip' });
  assert.deepStrictEqual(s.classifyChangeRow('x'), { kind: 'skip' });
  assert.deepStrictEqual(s.classifyChangeRow(null), { kind: 'skip' });
});

test('looksLikeFullBooking requires a party size AND a visit instant', () => {
  assert.strictEqual(s.looksLikeFullBooking({ Id: 1, CoversBooked: 2, VisitDateTime: '2026-08-29T19:45:00' }), true);
  assert.strictEqual(s.looksLikeFullBooking({ Id: 1, PartySize: 0, VisitDate: '2026-08-29' }), true); // 0 covers is a value
  assert.strictEqual(s.looksLikeFullBooking({ Id: 1, VisitDateTime: '2026-08-29T19:45:00' }), false); // stub: no covers
  assert.strictEqual(s.looksLikeFullBooking({ Id: 1, CoversBooked: 2 }), false); // stub: no visit
  assert.strictEqual(s.looksLikeFullBooking(null), false);
});

test('classifyCustomerRow forwards only records with real contact/name fields', () => {
  assert.deepStrictEqual(s.classifyCustomerRow({ Customer: { Id: 1, Email: 'a@b.c' } }), { Id: 1, Email: 'a@b.c' });
  assert.deepStrictEqual(s.classifyCustomerRow({ Id: 2, Email: 'x@y.z' }), { Id: 2, Email: 'x@y.z' });
  assert.strictEqual(s.classifyCustomerRow({ Id: 3, ChangeType: 'CustomerEmailChanged' }), null); // stub
  assert.strictEqual(s.classifyCustomerRow({ Customer: { Id: 1 } }), null); // stub
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
    ['GET', '/resdiary/booking-reviews?from=2026-09-01&to=2026-09-02'],
    ['GET', '/resdiary/booking-earliest'],
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
  const br = await fetch(base + '/resdiary/booking-reviews?from=2026-09-01', { headers: { 'X-Bridge-Token': 'test-admin-token' } });
  assert.strictEqual(br.status, 503);
  const be = await fetch(base + '/resdiary/booking-earliest', { headers: { 'X-Bridge-Token': 'test-admin-token' } });
  assert.strictEqual(be.status, 503);
});

test('run history: newest first, capped at 50, default window is 7 days', () => {
  const runs = [];
  let list = [];
  for (let i = 0; i < 55; i++) list = s.pushRun(list, { kind: 'reconcile', startedAt: String(i) });
  assert.strictEqual(list.length, 50);
  assert.strictEqual(list[0].startedAt, '54');
  assert.strictEqual(list[49].startedAt, '5');
  assert.deepStrictEqual(s.pushRun(null, { kind: 'backfill' }), [{ kind: 'backfill' }]);
  assert.strictEqual(s.DEFAULT_RECONCILE_DAYS, 7);
  assert.ok(Array.isArray(runs));
});

// ── booking reviews (Data Extract) ───────────────────────────────────

// Synthetic — the shape of a real Data Extract booking, none of its values.
const reviewedBooking = (id, extra = {}) => ({
  Id: id,
  BookingReference: `REF${id}`,
  CustomerId: 70000 + id,
  CustomerFirstName: ' Test ',
  CustomerSurname: 'Diner',
  Status: 'Closed',
  VisitDateTime: '2026-09-23T19:00:00.0000000',
  Review: {
    VisitDateTime: '2026-09-23T19:00:00.0000000',
    ReviewDateTime: '2026-09-24T11:31:25.6870000',
    Review: 'Lovely evening.\r\nSlow service.',
    AverageRating: 3.6,
    LikelyToRecommendRating: 3,
    FoodAndDrinkRating: 5,
    AtmosphereRating: 3,
    ServiceRating: 3,
    ValueRating: 4,
  },
  ...extra,
});

test('pickBookingReview keeps the review verbatim beside the booking identity', () => {
  const out = s.pickBookingReview(reviewedBooking(1));
  assert.deepStrictEqual(out, {
    bookingId: '1',
    reference: 'REF1',
    customerId: 70001,
    customerName: 'Test Diner',
    status: 'Closed',
    visitDateTime: '2026-09-23T19:00:00.0000000',
    review: reviewedBooking(1).Review,
  });
});

test('pickBookingReview skips bookings with no review, an empty one, or no id', () => {
  assert.strictEqual(s.pickBookingReview(null), null);
  assert.strictEqual(s.pickBookingReview(reviewedBooking(2, { Review: null })), null);
  assert.strictEqual(s.pickBookingReview(reviewedBooking(3, { Review: { Review: '', AverageRating: null } })), null);
  assert.strictEqual(s.pickBookingReview(reviewedBooking(4, { Id: undefined })), null);
  // A change row names the booking BookingId and carries no customer.
  const change = s.pickBookingReview({ BookingId: 5, Review: { AverageRating: 5 } });
  assert.strictEqual(change.bookingId, '5');
  assert.strictEqual(change.customerName, null);
});

test('parseEarliestDate reads every shape EarliestDate has arrived in', () => {
  assert.strictEqual(s.parseEarliestDate({ Result: '2022-12-28T17:07:43.4300000' }), '2022-12-28'); // the live shape
  assert.strictEqual(s.parseEarliestDate('2022-03-01T00:00:00'), '2022-03-01');
  assert.strictEqual(s.parseEarliestDate({ EarliestDate: '2022-03-01T00:00:00' }), '2022-03-01');
  assert.strictEqual(s.parseEarliestDate({ Date: '2022-03-01' }), '2022-03-01');
  assert.strictEqual(s.parseEarliestDate({ nope: 1 }), null);
  assert.strictEqual(s.parseEarliestDate('garbage'), null);
});

test('GET /resdiary/booking-reviews walks the range, dedupes bookings, reports where it stopped', async (t) => {
  const express = require('express');
  const rdMod = require('./resdiary');
  const saved = { isConfigured: rdMod.isConfigured, getBookingsForDate: rdMod.getBookingsForDate, getBookingChanges: rdMod.getBookingChanges };
  t.after(() => Object.assign(rdMod, saved));

  const calls = [];
  rdMod.isConfigured = () => true;
  rdMod.getBookingsForDate = async (day) => {
    calls.push(day);
    if (day === '2026-09-23') return [reviewedBooking(1), reviewedBooking(2, { Review: null })];
    if (day === '2026-09-24') return { Data: [reviewedBooking(3)] };
    if (day === '2026-09-27') throw new Error('upstream 500');
    return [];
  };
  // Two edits to the same booking in one walk: the review is reported once.
  rdMod.getBookingChanges = async () => [
    { BookingId: 9, Review: { AverageRating: 4 } },
    { BookingId: 9, Review: { AverageRating: 5 } },
  ];

  process.env.BRIDGE_ADMIN_TOKEN = 'test-admin-token';
  const app = express();
  sync.registerResdiaryRoutes(app);
  const server = app.listen(0);
  t.after(() => server.close());
  const base = `http://127.0.0.1:${server.address().port}`;
  const get = (q) => fetch(`${base}/resdiary/booking-reviews?${q}`, { headers: { 'X-Bridge-Token': 'test-admin-token' } });

  const ok = await get('from=2026-09-23&to=2026-09-25');
  assert.strictEqual(ok.status, 200);
  const body = await ok.json();
  assert.strictEqual(body.by, 'created');
  assert.strictEqual(body.days, 3);
  assert.strictEqual(body.rowsSeen, 3);
  assert.deepStrictEqual(body.reviews.map((r) => r.bookingId), ['1', '3']);
  assert.deepStrictEqual(body.perDay, [
    { date: '2026-09-23', rows: 2, reviews: 1 },
    { date: '2026-09-24', rows: 1, reviews: 1 },
    { date: '2026-09-25', rows: 0, reviews: 0 },
  ]);
  assert.deepStrictEqual(calls, ['2026-09-23', '2026-09-24', '2026-09-25']);

  const change = await (await get('from=2026-09-24&by=change')).json();
  assert.deepStrictEqual(change.reviews.map((r) => [r.bookingId, r.review.AverageRating]), [['9', 5]]);

  const failed = await get('from=2026-09-26&to=2026-09-28');
  assert.strictEqual(failed.status, 500);
  const fb = await failed.json();
  assert.strictEqual(fb.failedDate, '2026-09-27');
  assert.strictEqual(fb.lastDateDone, '2026-09-26');

  assert.strictEqual((await get('from=2026-09-28&to=2026-09-01')).status, 400);
  assert.strictEqual((await get('from=nope')).status, 400);
  assert.strictEqual((await get('from=2026-09-01&by=sideways')).status, 400);
  assert.strictEqual((await get('from=2026-09-01&by=visit')).status, 400, 'BookingDate 404s on our account');
  assert.strictEqual((await get(`from=2026-01-01&to=2026-12-31`)).status, 400, 'span over the cap');
  assert.strictEqual(s.REVIEW_SWEEP_MAX_DAYS, 62);
});
