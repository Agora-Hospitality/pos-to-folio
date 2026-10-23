const { test } = require('node:test');
const assert = require('node:assert/strict');
const { summarizeMewsEvents, formatMewsEventSummary } = require('./mews-events');

const so = (Id) => ({ Discriminator: 'ServiceOrderUpdated', Value: { Id } });

test('summarizeMewsEvents keeps ServiceOrderUpdated ids, in order, deduplicated', () => {
  const s = summarizeMewsEvents([so('b'), so('a'), so('b')]);
  assert.deepEqual(s.serviceOrderIds, ['b', 'a']);
  assert.equal(s.total, 3);
  assert.deepEqual(s.others, {});
});

test('summarizeMewsEvents counts every other discriminator instead of dropping it silently', () => {
  const s = summarizeMewsEvents([
    { Discriminator: 'CustomerUpdated', Value: { Id: 'c1' } },
    { Discriminator: 'CustomerUpdated', Value: { Id: 'c2' } },
    { Discriminator: 'PaymentUpdated', Value: { Id: 'p1' } },
    so('r1'),
  ]);
  assert.deepEqual(s.serviceOrderIds, ['r1']);
  assert.deepEqual(s.others, { CustomerUpdated: 2, PaymentUpdated: 1 });
});

test('summarizeMewsEvents drops a ServiceOrderUpdated with no usable id (the old loop passed undefined on)', () => {
  const s = summarizeMewsEvents([
    { Discriminator: 'ServiceOrderUpdated' },
    { Discriminator: 'ServiceOrderUpdated', Value: {} },
    { Discriminator: 'ServiceOrderUpdated', Value: { Id: 42 } },
    so('ok'),
  ]);
  assert.deepEqual(s.serviceOrderIds, ['ok']);
});

test('summarizeMewsEvents tolerates a body with no Events', () => {
  for (const events of [undefined, null, 'nope', {}, []]) {
    assert.deepEqual(summarizeMewsEvents(events), { total: 0, serviceOrderIds: [], others: {} });
  }
});

test('summarizeMewsEvents labels a missing discriminator rather than crashing', () => {
  const s = summarizeMewsEvents([{ Value: { Id: 'x' } }, null]);
  assert.deepEqual(s.others, { '(none)': 2 });
});

test('formatMewsEventSummary keeps the historical prefix and appends the ids', () => {
  const line = formatMewsEventSummary(summarizeMewsEvents([so('a7abe5b1'), so('2c1574ff')]));
  assert.equal(line, '[webhook] Received 2 ServiceOrderUpdated event(s): a7abe5b1, 2c1574ff');
});

test('formatMewsEventSummary names the other events delivered', () => {
  const line = formatMewsEventSummary(summarizeMewsEvents([
    { Discriminator: 'CustomerUpdated', Value: { Id: 'c' } },
    so('r'),
    { Discriminator: 'CustomerUpdated', Value: { Id: 'd' } },
  ]));
  assert.equal(line, '[webhook] Received 1 ServiceOrderUpdated event(s): r · also CustomerUpdated×2');
});

test('formatMewsEventSummary: a delivery with no reservation in it is still a line', () => {
  const line = formatMewsEventSummary(summarizeMewsEvents([{ Discriminator: 'ProductUpdated', Value: { Id: 'p' } }]));
  assert.equal(line, '[webhook] Received 0 ServiceOrderUpdated event(s) · also ProductUpdated×1');
});
