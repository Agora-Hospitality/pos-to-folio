/**
 * Every case here is a way this worker could have destroyed a note a member of
 * restaurant staff typed into ResDiary. They are the failures an adversarial
 * review found in the design before it was built, plus the ones the app's
 * TypeScript twin covers, so the two implementations stay honest about the
 * same rules.
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const { findBlockOnBoundary, spliceBlock, verifySplice, withCustomerLock } = require('./blob');

// The realistic shape: our appends are CRLF, the portal's typing is not.
const MIXED = 'Window table please\r\nNo nuts\r\nBirthday 12/09';

test('an LF needle finds a CRLF-separated line', () => {
  const hits = findBlockOnBoundary(MIXED, 'No nuts');
  assert.equal(hits.length, 1);
  assert.equal(MIXED.slice(hits[0].index, hits[0].index + hits[0].length), 'No nuts');
});

test('the cut does not shift — neighbours survive byte for byte, CRLF included', () => {
  const span = findBlockOnBoundary(MIXED, 'No nuts')[0];
  assert.equal(
    spliceBlock(MIXED, span, 'No nuts or shellfish'),
    'Window table please\r\nNo nuts or shellfish\r\nBirthday 12/09',
  );
});

test('a longer line does NOT match the shorter note inside it', () => {
  const blob = 'Window table please\r\nNo nuts or shellfish\r\nBirthday 12/09';
  assert.deepEqual(findBlockOnBoundary(blob, 'No nuts'), []);
});

test('verifySplice is boundary-aware, so an additive edit reports as landed', () => {
  const after = 'Window table\r\nNo nuts or shellfish';
  // The trap: the old text is still present as a substring of the new one.
  assert.ok(after.includes('No nuts'));
  assert.equal(verifySplice(after, 'No nuts', 'No nuts or shellfish'), true);
});

test('verifySplice fails when the new text did not actually arrive', () => {
  assert.equal(verifySplice('Window table\r\nNo nuts', 'No nuts', 'No nuts or shellfish'), false);
});

test('verifySplice on a delete only asks that the old line is gone', () => {
  assert.equal(verifySplice('Window table', 'No nuts', null), true);
  assert.equal(verifySplice('Window table\r\nNo nuts', 'No nuts', null), false);
});

test('a response body with no Comments is UNDETERMINED, never false', () => {
  assert.equal(verifySplice(undefined, 'No nuts', null), null);
  assert.equal(verifySplice(null, 'No nuts', null), null);
});

test('two identical lines are a refusal, not a coin toss', () => {
  assert.equal(findBlockOnBoundary('No nuts\r\nWindow\r\nNo nuts', 'No nuts').length, 2);
});

test('a blank needle matches nothing', () => {
  assert.deepEqual(findBlockOnBoundary(MIXED, ''), []);
  assert.deepEqual(findBlockOnBoundary(MIXED, '  \n '), []);
});

test('a multi-line note is one block', () => {
  const blob = 'First\r\nOurs one\r\nOurs two\r\nLast';
  const span = findBlockOnBoundary(blob, 'Ours one\nOurs two')[0];
  assert.ok(span);
  assert.equal(spliceBlock(blob, span, null), 'First\r\nLast');
});

test('deleting a middle line leaves no blank behind', () => {
  const span = findBlockOnBoundary(MIXED, 'No nuts')[0];
  assert.equal(spliceBlock(MIXED, span, null), 'Window table please\r\nBirthday 12/09');
});

test('deleting the last line leaves no trailing newline', () => {
  const span = findBlockOnBoundary(MIXED, 'Birthday 12/09')[0];
  assert.equal(spliceBlock(MIXED, span, null), 'Window table please\r\nNo nuts');
});

test('deleting the only line empties the blob', () => {
  const span = findBlockOnBoundary('Only this', 'Only this')[0];
  assert.equal(spliceBlock('Only this', span, null), '');
});

test('the per-diner lock serialises read-modify-write', async () => {
  // Without the lock these interleave as read-A, read-B, write-A, write-B, and
  // write-B reinstates the line write-A removed.
  const order = [];
  const slow = (tag, ms) => withCustomerLock('190687369', async () => {
    order.push(`read-${tag}`);
    await new Promise((r) => setTimeout(r, ms));
    order.push(`write-${tag}`);
  });
  await Promise.all([slow('A', 30), slow('B', 1)]);
  assert.deepEqual(order, ['read-A', 'write-A', 'read-B', 'write-B']);
});

test('a rejection does not poison the lock for the next caller', async () => {
  const boom = withCustomerLock('1', async () => { throw new Error('nope'); });
  await assert.rejects(boom, /nope/);
  assert.equal(await withCustomerLock('1', async () => 'fine'), 'fine');
});

test('different diners do not block each other', async () => {
  const order = [];
  await Promise.all([
    withCustomerLock('1', async () => { await new Promise((r) => setTimeout(r, 20)); order.push('slow'); }),
    withCustomerLock('2', async () => { order.push('fast'); }),
  ]);
  assert.deepEqual(order, ['fast', 'slow']);
});
