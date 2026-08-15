/**
 * Regression tests for the roster "chosen record rotation" bug.
 *
 * Confirmed live 2026-08-15 (reported by FOH 2026-08-12): Goodtill returns
 * /customers sorted by updated_at ASCENDING, and fullSync picked the first
 * record per room from response order — i.e. the STALEST one. Every 10-minute
 * sync renamed an ancient record to the current guest, activated it, and
 * deactivated the previously-active record (~2 writes/room/sync, ~5k/day).
 * The POS iPad cached whichever record was active at each of its own syncs,
 * so waiters saw several past guests for one room (DEM11 showed 4 names).
 *
 * These tests feed fullSync room records in stalest-first order and assert
 * the selection is sticky and steady-state syncs write nothing.
 *
 * roster.js requires its collaborators at load time, so Goodtill/MEWS are
 * stubbed via require.cache BEFORE the roster is loaded.
 */

const Module = require('node:module');
const { test, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

// ─── Module stubs (installed before ./roster is required) ──────────────

function stubModule(relPath, exports) {
  const resolved = require.resolve(relPath);
  const m = new Module(resolved);
  m.filename = resolved;
  m.exports = exports;
  m.loaded = true;
  require.cache[resolved] = m;
}

// In-memory Goodtill customer table + write log
let gtCustomers = [];
const calls = { create: [], update: [], deactivate: [] };
stubModule('./goodtill', {
  listCustomers: async () => gtCustomers,
  createCustomer: async (fields) => {
    calls.create.push(fields);
    const c = { id: `new-${calls.create.length}`, ...fields };
    gtCustomers.push(c);
    return c;
  },
  updateCustomer: async (id, fields) => {
    calls.update.push({ id, fields });
    return { id, ...fields };
  },
  activateCustomer: async (id) => calls.update.push({ id, fields: { active: 1 } }),
  deactivateCustomer: async (id) => {
    calls.deactivate.push(id);
    return { id, active: 0 };
  },
});

// MEWS stub: checked-in reservations + guest names, configurable per test
let mewsReservations = [];
let mewsGuests = [];
stubModule('./mews', {
  getResourcesAndRoomMap: async () => ({ roomMap: new Map() }),
  getAllCheckedInReservations: async () => mewsReservations,
  getActiveReservationForRoom: async () => null,
  getCustomers: async () => mewsGuests,
  getAll: async () => [],
});

const { fullSync, roomCustomerMap, customerRoomMap } = require('./roster');

// room DEM11 is MEWS resource "res-dem11"
const resourceToRoom = new Map([['res-dem11', 'DEM11']]);
const roomMap = new Map([['DEM11', 'res-dem11']]);

function reservation(id, guestId = 'guest-1') {
  return { Id: id, State: 'Started', AssignedResourceIds: ['res-dem11'], CustomerId: guestId };
}

// Records listed stalest-first, exactly like Goodtill's updated_at-asc order
function gtRecord(id, name, { active = 0, ref = null, updated }) {
  return { id, name, active, custom_field_1: ref, source: 'mews-bridge', updated_at: updated };
}

beforeEach(() => {
  gtCustomers = [];
  mewsReservations = [];
  mewsGuests = [];
  calls.create.length = 0;
  calls.update.length = 0;
  calls.deactivate.length = 0;
  roomCustomerMap.clear();
  customerRoomMap.clear();
});

test('steady state: correct record already active → zero writes, even when stale records come first', async () => {
  mewsReservations = [reservation('res-A')];
  mewsGuests = [{ Id: 'guest-1', FirstName: 'Nataliya', LastName: 'Jungi' }];
  gtCustomers = [
    // three ancient checked-out records, stalest first (rotation bait)
    gtRecord('old-1', 'DEM11 — Kevin Anselme', { updated: '2026-07-01 10:00:00' }),
    gtRecord('old-2', 'DEM11 — Steve Birchall', { updated: '2026-07-10 10:00:00' }),
    gtRecord('old-3', 'DEM11 — Άννα Παύλου', { updated: '2026-07-20 10:00:00' }),
    // the correct, currently-active record is LAST in response order
    gtRecord('cur-1', 'DEM11 — Nataliya Jungi', { active: 1, ref: 'mews:res-A', updated: '2026-08-15 18:57:01' }),
  ];

  await fullSync(roomMap, resourceToRoom);

  assert.equal(calls.update.length, 0, `no updates expected, got ${JSON.stringify(calls.update)}`);
  assert.equal(calls.deactivate.length, 0, `no deactivations expected, got ${JSON.stringify(calls.deactivate)}`);
  assert.equal(calls.create.length, 0);
  assert.equal(roomCustomerMap.get('DEM11'), 'cur-1');
});

test('new stay in same room: reuses the active record (not the stalest), one write', async () => {
  mewsReservations = [reservation('res-B', 'guest-2')];
  mewsGuests = [{ Id: 'guest-2', FirstName: 'New', LastName: 'Guest' }];
  gtCustomers = [
    gtRecord('old-1', 'DEM11 — Kevin Anselme', { updated: '2026-07-01 10:00:00' }),
    gtRecord('prev-1', 'DEM11 — Nataliya Jungi', { active: 1, ref: 'mews:res-A', updated: '2026-08-15 18:57:01' }),
  ];

  await fullSync(roomMap, resourceToRoom);

  assert.equal(calls.update.length, 1);
  assert.equal(calls.update[0].id, 'prev-1');
  assert.equal(calls.update[0].fields.name, 'DEM11 — New Guest');
  assert.equal(calls.update[0].fields.custom_field_1, 'mews:res-B');
  assert.equal(calls.deactivate.length, 0);
  assert.equal(calls.create.length, 0);
});

test('checkout: deactivates the active record once, leaves inactive history untouched', async () => {
  mewsReservations = []; // room no longer occupied
  gtCustomers = [
    gtRecord('old-1', 'DEM11 — Kevin Anselme', { updated: '2026-07-01 10:00:00' }),
    gtRecord('cur-1', 'DEM11 — Nataliya Jungi', { active: 1, ref: 'mews:res-A', updated: '2026-08-15 18:57:01' }),
  ];

  await fullSync(roomMap, resourceToRoom);

  assert.deepEqual(calls.deactivate, ['cur-1']);
  assert.equal(calls.update.length, 0);
});

test('duplicate actives (webhook-created after restart): keeps the one on the current reservation, deactivates the other', async () => {
  mewsReservations = [reservation('res-C')];
  mewsGuests = [{ Id: 'guest-1', FirstName: 'Nataliya', LastName: 'Jungi' }];
  gtCustomers = [
    // stray active from a previous stay whose checkout was missed
    gtRecord('stray-1', 'DEM11 — Kevin Anselme', { active: 1, ref: 'mews:res-old', updated: '2026-07-01 10:00:00' }),
    gtRecord('cur-1', 'DEM11 — Nataliya Jungi', { active: 1, ref: 'mews:res-C', updated: '2026-08-15 12:00:00' }),
  ];

  await fullSync(roomMap, resourceToRoom);

  assert.deepEqual(calls.deactivate, ['stray-1']);
  assert.equal(calls.update.length, 0, `chosen record already correct, got ${JSON.stringify(calls.update)}`);
  assert.equal(roomCustomerMap.get('DEM11'), 'cur-1');
});

test('string "1" active flags are treated as active (no needless reactivation writes)', async () => {
  mewsReservations = [reservation('res-A')];
  mewsGuests = [{ Id: 'guest-1', FirstName: 'Nataliya', LastName: 'Jungi' }];
  gtCustomers = [
    gtRecord('cur-1', 'DEM11 — Nataliya Jungi', { active: '1', ref: 'mews:res-A', updated: '2026-08-15 18:57:01' }),
  ];

  await fullSync(roomMap, resourceToRoom);

  assert.equal(calls.update.length, 0);
  assert.equal(calls.deactivate.length, 0);
});

test('room impostors from other integrations: active unmanaged record named after a known room is deactivated', async () => {
  mewsReservations = [reservation('res-A')];
  mewsGuests = [{ Id: 'guest-1', FirstName: 'Nataliya', LastName: 'Jungi' }];
  gtCustomers = [
    // cloned back by the ResDiary customer sync — foreign source, no mews ref
    { id: 'ghost-1', name: 'DEM11 — Kevin Anselme', active: 1, custom_field_1: null, source: 'ResDiary', updated_at: '2026-08-15 07:51:36' },
    // real CRM customer, not room-named — must never be touched
    { id: 'crm-1', name: 'Kevin Anselme', active: 1, custom_field_1: null, source: null, updated_at: '2026-08-15 07:52:00' },
    // room-named but already inactive — invisible, leave alone
    { id: 'ghost-2', name: 'DEM11 — Steve Birchall', active: 0, custom_field_1: null, source: null, updated_at: '2026-07-26 05:11:04' },
    // named after a room MEWS doesn't know — could be anything, leave alone
    { id: 'odd-1', name: 'ZZZ99 — Mystery', active: 1, custom_field_1: null, source: null, updated_at: '2026-08-01 10:00:00' },
    gtRecord('cur-1', 'DEM11 — Nataliya Jungi', { active: 1, ref: 'mews:res-A', updated: '2026-08-15 18:57:01' }),
  ];

  await fullSync(roomMap, resourceToRoom);

  assert.deepEqual(calls.deactivate, ['ghost-1']);
  assert.equal(calls.update.length, 0);
  assert.equal(roomCustomerMap.get('DEM11'), 'cur-1');
});

test('occupied room with no record at all: creates exactly one', async () => {
  mewsReservations = [reservation('res-A')];
  mewsGuests = [{ Id: 'guest-1', FirstName: 'Nataliya', LastName: 'Jungi' }];
  gtCustomers = [];

  await fullSync(roomMap, resourceToRoom);

  assert.equal(calls.create.length, 1);
  assert.equal(calls.create[0].name, 'DEM11 — Nataliya Jungi');
  assert.equal(calls.create[0].custom_field_1, 'mews:res-A');
});
