const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createBikeRentalWatcher, parseServiceIds, DEFAULTS } = require('./bike-rental-watch');

const BIKE = 'svc-bike';
const T0 = Date.parse('2026-10-23T08:00:00Z');
const order = (Id, UpdatedUtc = '2026-10-23T07:59:30Z') => ({ Id, UpdatedUtc, ServiceId: BIKE });

/** Watcher with every clock and I/O injected. `h.orders` is what MEWS returns next. */
function harness({ services, secret = 's3cret', serviceIds = [BIKE], appOk = true } = {}) {
  const h = { t: T0, orders: [], mewsCalls: [], fetches: [], timers: [], repeats: [], logs: [], warns: [], mewsError: null };
  h.w = createBikeRentalWatcher({
    mewsPost: async (ep, body) => {
      h.mewsCalls.push({ ep, body });
      if (h.mewsError) throw h.mewsError;
      if (ep === 'services/getAll') return { Services: services || [] };
      if (ep === 'productServiceOrders/getAll') return { ProductServiceOrders: h.orders };
      throw new Error(`unexpected ${ep}`);
    },
    fetchImpl: async (url, init) => {
      h.fetches.push({ url, init });
      return appOk
        ? { ok: true, status: 200, json: async () => ({ added: 1, renters: 2 }) }
        : { ok: false, status: 500, json: async () => ({ error: 'boom' }) };
    },
    secret,
    serviceIds,
    appUrl: 'https://app.test/',
    now: () => h.t,
    setTimer: (fn, ms) => { const timer = { fn, ms }; h.timers.push(timer); return timer; },
    clearTimer: (timer) => { timer.cleared = true; },
    setRepeat: (fn, ms) => { h.repeats.push({ fn, ms }); return 'repeat'; },
    clearRepeat: () => { h.repeatsCleared = true; },
    log: { log: (m) => h.logs.push(m), warn: (m) => h.warns.push(m) },
  });
  h.advance = (ms) => { h.t += ms; };
  return h;
}

test('parseServiceIds splits a comma list and drops blanks', () => {
  assert.deepEqual(parseServiceIds(' a, b,,c '), ['a', 'b', 'c']);
  assert.deepEqual(parseServiceIds(undefined), []);
  assert.deepEqual(parseServiceIds(''), []);
});

test('a new bike order triggers one app call: run=1, bearer secret, GET', async () => {
  const h = harness();
  h.orders = [order('o1')];
  assert.deepEqual(await h.w.tick(), ['o1']);
  assert.equal(h.fetches.length, 1);
  assert.equal(h.fetches[0].url, 'https://app.test/api/cron/cycling-renters?run=1');
  assert.equal(h.fetches[0].init.method, 'GET');
  assert.equal(h.fetches[0].init.headers.Authorization, 'Bearer s3cret');
  assert.match(h.logs.at(-1), /✓ app synced — 1 added, 2 renter/);
});

test('an order already seen is silent; a later update to it counts as new', async () => {
  const h = harness();
  h.orders = [order('o1', '2026-10-23T07:59:30Z')];
  await h.w.tick();
  assert.deepEqual(await h.w.tick(), []);
  assert.equal(h.fetches.length, 1);
  h.advance(61_000);
  h.orders = [order('o1', '2026-10-23T08:01:00Z')];
  assert.deepEqual(await h.w.tick(), ['o1']);
  assert.equal(h.fetches.length, 2);
});

test('at most one app call a minute: a burst coalesces into one trailing call carrying every id', async () => {
  const h = harness();
  h.orders = [order('o1')];
  await h.w.tick(); // call 1 at T0
  assert.equal(h.fetches.length, 1);

  h.advance(10_000);
  h.orders = [order('o1'), order('o2')];
  assert.deepEqual(await h.w.tick(), ['o2']);
  assert.equal(h.fetches.length, 1, 'inside the minute: nothing fires yet');
  assert.equal(h.timers.length, 1);
  assert.equal(h.timers[0].ms, 50_000, 'trailing call lands exactly when the minute is up');

  h.advance(10_000);
  h.orders = [order('o1'), order('o2'), order('o3')];
  assert.deepEqual(await h.w.tick(), ['o3']);
  assert.equal(h.timers.length, 1, 'a second burst does not queue a second call');

  h.advance(40_000);
  await h.timers[0].fn(); // the clock fires the trailing call
  assert.equal(h.fetches.length, 2);
  assert.match(h.logs.at(-1), /orders o2, o3\)/);
});

test('a change after the minute has passed fires at once, with no timer', async () => {
  const h = harness();
  h.orders = [order('o1')];
  await h.w.tick();
  h.advance(61_000);
  h.orders = [order('o1'), order('o2')];
  await h.w.tick();
  assert.equal(h.fetches.length, 2);
  assert.equal(h.timers.length, 0);
});

test('MEWS is asked for the bike services only, over a trailing 3-minute UpdatedUtc window', async () => {
  const h = harness();
  await h.w.tick();
  const call = h.mewsCalls[0];
  assert.equal(call.ep, 'productServiceOrders/getAll');
  assert.deepEqual(call.body.ServiceIds, [BIKE]);
  assert.equal(call.body.UpdatedUtc.EndUtc, new Date(T0).toISOString());
  assert.equal(call.body.UpdatedUtc.StartUtc, new Date(T0 - DEFAULTS.lookbackMs).toISOString());
  assert.equal(call.body.Limitation.Count, 100);
  assert.equal(h.fetches.length, 0, 'nothing changed → the app is not called');
});

test('without BIKE_RENTAL_SERVICE_IDS the services are found by name, active only, once', async () => {
  const h = harness({
    serviceIds: [],
    services: [
      { Id: 'a', Name: 'Bicycle Rental', IsActive: true },
      { Id: 'b', Name: 'Bike hire (old)', IsActive: false },
      { Id: 'c', Name: 'Restaurant', IsActive: true },
      { Id: 'd', Name: 'E-Bikes', IsActive: true },
    ],
  });
  await h.w.tick();
  assert.deepEqual(h.w.serviceIds, ['a', 'd']);
  assert.equal(h.mewsCalls[0].ep, 'services/getAll');
  assert.deepEqual(h.mewsCalls[1].body.ServiceIds, ['a', 'd']);
  await h.w.tick();
  assert.equal(h.mewsCalls.filter((c) => c.ep === 'services/getAll').length, 1, 'resolved once, then cached');
});

test('no bike service in MEWS: one warning, no order query, resolution retried next tick', async () => {
  const h = harness({ serviceIds: [], services: [{ Id: 'c', Name: 'Restaurant', IsActive: true }] });
  await h.w.tick();
  assert.equal(h.mewsCalls.length, 1);
  assert.match(h.warns[0], /BIKE_RENTAL_SERVICE_IDS/);
  await h.w.tick();
  assert.equal(h.mewsCalls.filter((c) => c.ep === 'services/getAll').length, 2);
  assert.equal(h.fetches.length, 0);
});

test('a MEWS failure is a warning, never a throw, and the next tick works', async () => {
  const h = harness();
  h.mewsError = new Error('MEWS 503');
  assert.deepEqual(await h.w.tick(), []);
  assert.match(h.warns[0], /poll failed \(MEWS 503\)/);
  h.mewsError = null;
  h.orders = [order('o1')];
  assert.deepEqual(await h.w.tick(), ['o1']);
});

test('an app failure is a warning that names the safety net; nothing throws', async () => {
  const h = harness({ appOk: false });
  h.orders = [order('o1')];
  await h.w.tick();
  assert.equal(h.fetches.length, 1);
  assert.match(h.warns[0], /app sync call failed \(HTTP 500 boom\).*10-minute cron/);
});

test('start() without the secret is OFF with one warning and never polls', () => {
  const h = harness({ secret: '' });
  assert.equal(h.w.start(), false);
  assert.match(h.warns[0], /AGORA_CRON_SECRET not set/);
  assert.equal(h.repeats.length, 0);
  assert.equal(h.mewsCalls.length, 0);
});

test('start() polls at once and then every pollMs; stop() clears the clocks', async () => {
  const h = harness();
  assert.equal(h.w.start(), true);
  assert.equal(h.w.start(), true, 'idempotent');
  assert.equal(h.repeats.length, 1);
  assert.equal(h.repeats[0].ms, DEFAULTS.pollMs);
  await new Promise((r) => setImmediate(r));
  assert.equal(h.mewsCalls.length, 1, 'first poll ran immediately');
  h.w.stop();
  assert.equal(h.repeatsCleared, true);
});
