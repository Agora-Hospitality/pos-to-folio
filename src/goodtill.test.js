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
