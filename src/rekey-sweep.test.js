const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { DeadLetterStore } = require('./store');
const { rekeyedToResolutions, sweepRekeyed } = require('./rekey-sweep');

const tmpStore = () => new DeadLetterStore(path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'rekey-')), 'dl.json'));

test('the app response becomes receipt → resolution, skipping rows without a receipt', () => {
  const m = rekeyedToResolutions({ rekeyed: [{ receipt: '111', date: '2026-07-24' }, { date: '2026-07-25' }] });
  assert.deepEqual([...m.keys()], ['111']);
  assert.match(m.get('111'), /reconciliation 2026-07-24/);
});

test('a sweep asks from the oldest open entry and closes the matches', async () => {
  const dl = tmpStore();
  dl.record('s1', { receipt: '111', totalAtStake: 49 }, 'no room matched');
  dl.record('s2', { receipt: '222', totalAtStake: 28 }, 'no room matched');
  let askedSince = null;
  const n = await sweepRekeyed(dl, async (since) => { askedSince = since; return new Map([['222', 're-keyed']]); });
  assert.equal(n, 1);
  assert.match(askedSince, /^\d{4}-\d{2}-\d{2}$/);
  assert.deepEqual(dl.unresolved().map((e) => e.saleId), ['s1']);
});

test('an unreachable app closes nothing and does not throw', async () => {
  const dl = tmpStore();
  dl.record('s1', { receipt: '111', totalAtStake: 49 }, 'no room matched');
  assert.equal(await sweepRekeyed(dl, async () => null), 0);
  assert.equal(dl.unresolved().length, 1);
});

test('an empty queue never calls the app', async () => {
  let called = false;
  assert.equal(await sweepRekeyed(tmpStore(), async () => { called = true; return new Map(); }), 0);
  assert.equal(called, false);
});
