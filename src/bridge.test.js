/**
 * Regression tests for the webhook-vs-poll double-post race.
 *
 * Confirmed incidents: 2026-06-25 (€5.80, two MEWS orders 1s apart) and
 * 2026-07-02 receipt #1782990088859 (€55, two orders the same second —
 * reception posted a manual −€55 correction). The sale.completed webhook
 * (handleCompletedSale) used to call processSale with no serialization
 * against the poll loop, and processSale never re-checked the store after
 * its first await, so every sale raced webhook-vs-poll.
 *
 * bridge.js requires its collaborators at load time, so Goodtill/MEWS/roster
 * are stubbed via require.cache BEFORE the bridge is loaded. The SalesStore
 * is real, pointed at a temp DATA_DIR.
 */

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Module = require('node:module');

// Must be set before ./store (required by ./bridge) loads — DATA_DIR is a
// load-time const. Keeps the test off the real ./data store.
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pos-bridge-'));

const { test } = require('node:test');
const assert = require('node:assert/strict');

// ─── Module stubs (installed before ./bridge is required) ──────────────

function stubModule(relPath, exports) {
  const resolved = require.resolve(relPath);
  const m = new Module(resolved);
  m.filename = resolved;
  m.exports = exports;
  m.loaded = true;
  require.cache[resolved] = m;
}

// In-memory Goodtill: sales keyed by ID. Parsing/formatting helpers stay real
// so the fixtures go through the genuine extractGuestFolioSales path.
const realGoodtill = require('./goodtill');
const goodtillSales = new Map();
stubModule('./goodtill', {
  ...realGoodtill,
  fetchSales: async () => {
    await new Promise(setImmediate); // yield, like a real network call
    return [...goodtillSales.values()];
  },
  fetchSaleById: async (id) => {
    await new Promise(setImmediate);
    return goodtillSales.get(String(id)) || null;
  },
});

// MEWS stub records every order/cancellation. The reservation lookup is
// deliberately slow so both racing paths sit inside the window between their
// store.has check and addOrder — exactly the production race.
const mewsCalls = { addOrder: [], cancelOrderItems: [] };
let onReservationLookup = null;
stubModule('./mews', {
  getResourcesAndRoomMap: async () => ({ roomMap: new Map([['11', 'resource-11']]) }),
  findFBService: async () => 'svc-fb',
  findFBAccountingCategory: async () => 'cat-fb',
  getActiveReservationForRoom: async () => {
    if (onReservationLookup) await onReservationLookup();
    await new Promise((resolve) => setTimeout(resolve, 20));
    return { Id: 'res-1', AccountId: 'acct-1' };
  },
  addOrder: async (order) => {
    mewsCalls.addOrder.push(order);
    await new Promise(setImmediate);
    return { OrderId: `order-${mewsCalls.addOrder.length}` };
  },
  getOrderItems: async (orderIds) => orderIds.map((id) => ({ Id: `item-of-${id}`, CanceledUtc: null })),
  cancelOrderItems: async (itemIds) => {
    mewsCalls.cancelOrderItems.push(itemIds);
    await new Promise(setImmediate);
    return {};
  },
  deleteExternalPayments: async () => ({}),
});

stubModule('./roster', {
  fullSync: async () => {},
  getRoomByCustomerId: (gtCustomerId) =>
    gtCustomerId === 'cust-room-11' ? { roomNumber: '11', reservationId: 'res-1' } : null,
});

const bridge = require('./bridge');

// ─── Fixtures ───────────────────────────────────────────────────────────

function guestFolioSale(id, { total = '55.00', status = 'COMPLETED' } = {}) {
  return {
    id,
    order_status: status,
    receipt_no: `R-${id}`,
    customer_id: 'cust-room-11',
    sales_date_time: '2026-07-02 14:01:37',
    sales_payments: { CUSTOM_1: { payment_total: total } },
    sales_details: {
      total_after_discount: total,
      sales_items: [{ product_name: 'Halloumi', quantity: '1', line_total_after_discount: total }],
    },
  };
}

// init() once for the whole file — tests share the bridge module state, so
// they each use distinct sale IDs and reset the MEWS call recorders.
let initPromise;
function ensureInit() {
  if (!initPromise) initPromise = bridge.init();
  return initPromise;
}

// ─── Tests ──────────────────────────────────────────────────────────────

test('sale.completed webhook racing the poll loop posts exactly one MEWS order (receipt #1782990088859)', async () => {
  await ensureInit();
  mewsCalls.addOrder.length = 0;
  goodtillSales.set('race-1', guestFolioSale('race-1'));

  // Webhook and poll tick fire together, as on 2026-07-02 11:01:37Z. Both
  // pass their store.has pre-check before either reaches addOrder.
  await Promise.all([
    bridge.pollOnce(),
    bridge.handleCompletedSale('race-1'),
  ]);

  assert.equal(mewsCalls.addOrder.length, 1,
    `expected exactly 1 MEWS order, got ${mewsCalls.addOrder.length} — the double-post race is back`);
  assert.ok(bridge._getStoreForTests().get('race-1').mewsOrderId, 'winning path must record the MEWS order');
});

test('duplicate sale.completed webhook deliveries post exactly one MEWS order', async () => {
  await ensureInit();
  mewsCalls.addOrder.length = 0;
  goodtillSales.set('dup-hook', guestFolioSale('dup-hook'));

  await Promise.all([
    bridge.handleCompletedSale('dup-hook'),
    bridge.handleCompletedSale('dup-hook'),
  ]);

  assert.equal(mewsCalls.addOrder.length, 1);
});

test('processSale re-checks the store immediately before addOrder (last line of defence)', async () => {
  await ensureInit();
  mewsCalls.addOrder.length = 0;
  goodtillSales.set('mid-flight', guestFolioSale('mid-flight'));

  // Simulate a path the in-flight guard cannot see (another replica, manual
  // re-key) marking the sale processed while this one awaits MEWS.
  onReservationLookup = async () => {
    bridge._getStoreForTests().add('mid-flight', 'order-posted-elsewhere');
  };
  try {
    await bridge.handleCompletedSale('mid-flight');
  } finally {
    onReservationLookup = null;
  }

  assert.equal(mewsCalls.addOrder.length, 0,
    'addOrder must not fire when the sale landed in the store mid-flight');
});

test('concurrent void signals cancel the MEWS order items exactly once', async () => {
  await ensureInit();
  mewsCalls.addOrder.length = 0;
  mewsCalls.cancelOrderItems.length = 0;

  // Post a sale, then void it on the POS.
  goodtillSales.set('void-race', guestFolioSale('void-race'));
  await bridge.handleCompletedSale('void-race');
  assert.equal(mewsCalls.addOrder.length, 1, 'setup: the sale must post first');
  goodtillSales.set('void-race', guestFolioSale('void-race', { status: 'VOIDED' }));

  // sale.voided webhook racing a redelivery of itself (same shape as racing
  // detectVoids or the poll's voided branch — all funnel into reverseSale).
  await Promise.all([
    bridge.handleVoidedSale('void-race'),
    bridge.handleVoidedSale('void-race'),
  ]);

  assert.equal(mewsCalls.cancelOrderItems.length, 1,
    `expected exactly 1 cancellation, got ${mewsCalls.cancelOrderItems.length}`);
  assert.ok(bridge._getStoreForTests().get('void-race').reversedAt, 'sale must be marked reversed');
});
