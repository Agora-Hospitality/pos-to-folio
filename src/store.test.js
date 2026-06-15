const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { SalesStore, DeadLetterStore, DATA_DIR } = require('./store');

// Each call returns a path inside a fresh temp dir, so tests never touch the
// real ./data store or each other.
function tmpFile(name) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'pos-store-'));
  return path.join(dir, name);
}

test('DATA_DIR is exported as an absolute path, defaulting to ./data', () => {
  assert.ok(path.isAbsolute(DATA_DIR));
  assert.equal(DATA_DIR, path.resolve(process.cwd(), 'data'));
});

test('processed sales survive a restart (reload from the same file)', () => {
  const file = tmpFile('processed-sales.json');
  const before = new SalesStore(file);
  before.add('uuid-A', 'mews-order-1');
  before.add('uuid-B', 'mews-order-2');
  assert.equal(before.size, 2);

  // A new instance over the same file == a process restart with a persisted
  // volume. This is exactly what was failing on Railway's ephemeral disk.
  const after = new SalesStore(file);
  assert.equal(after.size, 2);
  assert.equal(after.get('uuid-A').mewsOrderId, 'mews-order-1');
  assert.ok(after.has('uuid-B'));
});

test('markReversed persists across a reload', () => {
  const file = tmpFile('processed-sales.json');
  const before = new SalesStore(file);
  before.add('uuid-A', 'mews-order-1');
  before.markReversed('uuid-A');

  const after = new SalesStore(file);
  assert.ok(after.get('uuid-A').reversedAt, 'reversedAt must survive a reload');
});

test('getUnreversedCandidates excludes reversed, order-less and aged entries', () => {
  const file = tmpFile('processed-sales.json');
  const s = new SalesStore(file);
  s.add('fresh', 'order-fresh');        // candidate
  s.add('no-order', undefined);         // excluded: never got a MEWS order id
  s.add('reversed', 'order-rev');
  s.markReversed('reversed');           // excluded: already reversed
  s.processed.set('aged', { postedAt: '2000-01-01T00:00:00.000Z', mewsOrderId: 'order-aged' }); // excluded: too old

  const ids = s.getUnreversedCandidates(48 * 60 * 60 * 1000).map(([id]) => id);
  assert.deepEqual(ids, ['fresh']);
});

test('a missing store file loads empty without throwing (wiped-volume path)', () => {
  const file = tmpFile('does-not-exist.json');
  const s = new SalesStore(file);
  assert.equal(s.size, 0);
});

test('dead-letter entries survive a reload', () => {
  const file = tmpFile('dead-letter.json');
  const before = new DeadLetterStore(file);
  before.record('sale-x', { receipt: 'R1' }, 'no room matched');
  assert.equal(before.unresolved().length, 1);

  const after = new DeadLetterStore(file);
  assert.equal(after.unresolved().length, 1);
  assert.equal(after.unresolved()[0].saleId, 'sale-x');
});

test('DATA_DIR honours the DATA_DIR env override at load time', () => {
  // DATA_DIR is a load-time const, so prove the override in a fresh process.
  const storePath = path.resolve(__dirname, 'store.js');
  const probe = `process.stdout.write(require(${JSON.stringify(storePath)}).DATA_DIR)`;
  const out = execFileSync(process.execPath, ['-e', probe], {
    env: { ...process.env, DATA_DIR: '/tmp/agora-volume' },
    encoding: 'utf-8',
  });
  assert.equal(out, path.resolve('/tmp/agora-volume'));
});
