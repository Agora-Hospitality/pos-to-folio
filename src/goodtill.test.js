const { test } = require('node:test');
const assert = require('node:assert/strict');
const { formatDateTime, cyprusLocalToUtcIso } = require('./goodtill');

test('formatDateTime emits 00 at midnight Cyprus local (never 24)', () => {
  // 2026-04-23T21:00:00Z is 2026-04-24T00:00:00 in Europe/Nicosia (UTC+3 DST).
  const midnight = new Date('2026-04-23T21:00:00Z');
  assert.equal(formatDateTime(midnight), '2026-04-24 00:00:00');
});

test('formatDateTime hour component is always 00-23', () => {
  for (let h = 0; h < 24; h += 1) {
    const d = new Date(Date.UTC(2026, 3, 23, h, 0, 0));
    const out = formatDateTime(d);
    const hour = out.slice(11, 13);
    assert.match(hour, /^(0\d|1\d|2[0-3])$/, `hour "${hour}" from ${d.toISOString()} must be 00-23, got "${out}"`);
  }
});

test('formatDateTime returns Cyprus wall-clock, not UTC', () => {
  // Mid-afternoon UTC maps to +3h local during DST.
  const noonUtc = new Date('2026-04-23T12:00:00Z');
  assert.equal(formatDateTime(noonUtc), '2026-04-23 15:00:00');
});

test('cyprusLocalToUtcIso: DST (UTC+3) maps 3h back — the Joakim GRD17 case', () => {
  assert.equal(cyprusLocalToUtcIso('2026-06-07 09:56:54'), '2026-06-07T06:56:54.000Z');
});

test('cyprusLocalToUtcIso: winter (UTC+2) maps 2h back', () => {
  assert.equal(cyprusLocalToUtcIso('2026-01-15 09:00:00'), '2026-01-15T07:00:00.000Z');
});

test('cyprusLocalToUtcIso: local just after midnight rolls the UTC date back', () => {
  assert.equal(cyprusLocalToUtcIso('2026-06-07 00:30:00'), '2026-06-06T21:30:00.000Z');
});

test('cyprusLocalToUtcIso roundtrips with formatDateTime, incl. DST-transition days', () => {
  for (const local of ['2026-10-25 12:00:00', '2026-03-29 12:00:00', '2026-06-06 23:39:45']) {
    const iso = cyprusLocalToUtcIso(local);
    assert.equal(formatDateTime(new Date(iso)), local);
  }
});

test('cyprusLocalToUtcIso accepts a T separator', () => {
  assert.equal(cyprusLocalToUtcIso('2026-06-07T09:56:54'), '2026-06-07T06:56:54.000Z');
});

test('cyprusLocalToUtcIso: unparsable input returns null', () => {
  assert.equal(cyprusLocalToUtcIso(''), null);
  assert.equal(cyprusLocalToUtcIso(undefined), null);
  assert.equal(cyprusLocalToUtcIso('not a date'), null);
});

test('cyprusLocalToUtcIso: zone-suffixed input is rejected, not re-shifted', () => {
  // A true-UTC string must not be treated as Cyprus local (-3h error);
  // returning null routes the caller to its safe fallback.
  assert.equal(cyprusLocalToUtcIso('2026-06-07T06:56:54Z'), null);
  assert.equal(cyprusLocalToUtcIso('2026-06-07 06:56:54+03:00'), null);
});

test('cyprusLocalToUtcIso: hour right before the DST spring-forward gap', () => {
  // Cyprus DST 2026 starts 29 Mar at 01:00 UTC (03:00→04:00 local).
  // 02:30 local exists (still UTC+2) — protects the second guess-and-correct
  // iteration, which is load-bearing exactly here.
  assert.equal(cyprusLocalToUtcIso('2026-03-29 02:30:00'), '2026-03-29T00:30:00.000Z');
});

test('cyprusLocalToUtcIso: ambiguous fall-back hour pins the second occurrence', () => {
  // 25 Oct 2026, 03:30 local occurs twice (DST end); the EET (second)
  // occurrence is the documented convention — both bucket to the same
  // 10:00-10:00 service day, so either would reconcile identically.
  assert.equal(cyprusLocalToUtcIso('2026-10-25 03:30:00'), '2026-10-25T01:30:00.000Z');
});
