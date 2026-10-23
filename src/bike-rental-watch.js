/**
 * Bike rentals → the Agora app's Cycling list, within a minute of the sale.
 *
 * Bike hire is sold in MEWS as product service orders on the Bicycle Rental
 * service(s), each linked to a stay. The MEWS General Webhook never reports
 * product orders (30 days of logs, 19-10-2026 — see README), so this watcher
 * polls productServiceOrders/getAll every BIKE_RENTAL_POLL_MS for orders
 * updated in the last few minutes and, on anything not seen before, asks the
 * app to run its Cycling sync: GET /api/cron/cycling-renters?run=1 with
 * Authorization: Bearer AGORA_CRON_SECRET. At most one app call a minute;
 * bursts coalesce into one trailing call. The app's own 10-minute cron stays
 * as the safety net, and its sync is idempotent, so a spare trigger costs
 * nothing.
 *
 * Never throws out of the timer: a MEWS or app failure is a warning and the
 * next tick retries. Disabled with one warning when AGORA_CRON_SECRET is unset.
 */

const { post: mewsPostLive } = require('./mews');

const DEFAULTS = {
  pollMs: 60_000,
  lookbackMs: 3 * 60_000, // wider than the poll so a slow tick never leaves a gap
  minGapMs: 60_000, // at most one app call per minute
  seenCap: 1000,
  namePattern: /bicycl|\bbike/i, // same rule the app uses to find the services
};

/** "a, b,,c " → ["a", "b", "c"] */
function parseServiceIds(raw) {
  return String(raw || '').split(',').map((s) => s.trim()).filter(Boolean);
}

/**
 * @param {object} opts
 * @param {(endpoint: string, body: object) => Promise<any>} [opts.mewsPost]
 * @param {typeof fetch} [opts.fetchImpl]
 * @param {string} [opts.appUrl]
 * @param {string} [opts.secret] - the app's CRON_SECRET
 * @param {string[]} [opts.serviceIds] - bike services; resolved from MEWS by name when empty
 * @param {number} [opts.pollMs]
 * @param {number} [opts.lookbackMs]
 * @param {number} [opts.minGapMs]
 * @param {() => number} [opts.now]
 * @param {Function} [opts.setTimer] / [opts.clearTimer] / [opts.setRepeat] / [opts.clearRepeat] - injectable clocks
 * @param {{ log: Function, warn: Function }} [opts.log]
 */
function createBikeRentalWatcher(opts = {}) {
  const {
    mewsPost = mewsPostLive,
    fetchImpl = globalThis.fetch,
    appUrl = 'https://theagorahotel.app',
    secret = '',
    serviceIds: configuredIds = [],
    pollMs = DEFAULTS.pollMs,
    lookbackMs = DEFAULTS.lookbackMs,
    minGapMs = DEFAULTS.minGapMs,
    now = () => Date.now(),
    setTimer = setTimeout,
    clearTimer = clearTimeout,
    setRepeat = setInterval,
    clearRepeat = clearInterval,
    log = console,
  } = opts;

  let serviceIds = [...configuredIds];
  const seen = new Map(); // `${Id}:${UpdatedUtc}` → true, insertion-ordered so the oldest is evicted
  let pollTimer = null;
  let trailingTimer = null;
  let lastCallAt = -Infinity;
  let inFlight = false;
  let pendingIds = new Set();

  async function resolveServiceIds() {
    if (serviceIds.length) return serviceIds;
    const data = await mewsPost('services/getAll', {});
    serviceIds = (data.Services || [])
      .filter((s) => s.IsActive && DEFAULTS.namePattern.test(s.Name || ''))
      .map((s) => s.Id);
    if (serviceIds.length) log.log(`[cycling] watching bike-rental services: ${serviceIds.join(', ')}`);
    else log.warn('[cycling] no active MEWS service named like a bike — set BIKE_RENTAL_SERVICE_IDS; retrying next tick');
    return serviceIds;
  }

  function remember(key) {
    seen.set(key, true);
    if (seen.size > DEFAULTS.seenCap) seen.delete(seen.keys().next().value);
  }

  /** Call the app once. Resolves when the call has finished (success or warning). */
  async function fire() {
    const ids = [...pendingIds];
    pendingIds = new Set();
    lastCallAt = now();
    inFlight = true;
    const url = `${appUrl.replace(/\/$/, '')}/api/cron/cycling-renters?run=1`;
    try {
      const res = await fetchImpl(url, {
        method: 'GET',
        headers: { Authorization: `Bearer ${secret}` },
        signal: AbortSignal.timeout(120_000),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(`HTTP ${res.status}${body.error ? ` ${body.error}` : ''}`);
      log.log(`[cycling] ✓ app synced — ${body.added ?? '?'} added, ${body.renters ?? '?'} renter(s) in its window (orders ${ids.join(', ')})`);
    } catch (err) {
      log.warn(`[cycling] ✗ app sync call failed (${err.message}) — the app's 10-minute cron will catch up`);
    } finally {
      inFlight = false;
    }
  }

  /**
   * Ask the app to sync for these order ids: now if the last call is over a
   * minute old, else one trailing call when the minute is up. Returns the
   * in-progress call's promise when it fires at once, else undefined.
   */
  function requestSync(ids) {
    for (const id of ids) pendingIds.add(id);
    if (trailingTimer) return undefined; // already queued; it carries these ids too
    const wait = lastCallAt + minGapMs - now();
    if (wait <= 0 && !inFlight) return fire();
    trailingTimer = setTimer(() => {
      trailingTimer = null;
      return fire();
    }, wait > 0 ? wait : minGapMs);
    return undefined;
  }

  /** One poll. Resolves to the ids of orders not seen before (for tests and logs). */
  async function tick() {
    try {
      const ids = await resolveServiceIds();
      if (!ids.length) return [];
      const end = now();
      const data = await mewsPost('productServiceOrders/getAll', {
        ServiceIds: ids,
        UpdatedUtc: { StartUtc: new Date(end - lookbackMs).toISOString(), EndUtc: new Date(end).toISOString() },
        Limitation: { Count: 100 },
      });
      const fresh = [];
      for (const o of data.ProductServiceOrders || []) {
        if (!o?.Id) continue;
        const key = `${o.Id}:${o.UpdatedUtc || ''}`;
        if (seen.has(key)) continue;
        remember(key);
        fresh.push(o.Id);
      }
      if (fresh.length) {
        log.log(`[cycling] ${fresh.length} bike-rental order(s) changed: ${fresh.join(', ')}`);
        await requestSync(fresh);
      }
      return fresh;
    } catch (err) {
      log.warn(`[cycling] poll failed (${err.message}) — next tick retries`);
      return [];
    }
  }

  /** Start polling. False (and one warning) when the secret is missing. */
  function start() {
    if (!secret) {
      log.warn("[cycling] AGORA_CRON_SECRET not set — bike-rental watch OFF (the app's 10-minute cron still runs)");
      return false;
    }
    if (pollTimer) return true;
    log.log(`[cycling] bike-rental watch every ${Math.round(pollMs / 1000)}s → ${appUrl}`);
    tick();
    pollTimer = setRepeat(() => {
      tick();
    }, pollMs);
    return true;
  }

  function stop() {
    if (pollTimer) clearRepeat(pollTimer);
    pollTimer = null;
    if (trailingTimer) clearTimer(trailingTimer);
    trailingTimer = null;
  }

  return {
    start,
    stop,
    tick,
    requestSync,
    get serviceIds() {
      return [...serviceIds];
    },
  };
}

// ─── Process-wide instance built from env ────────────────────────────

let instance = null;

function startBikeRentalWatch() {
  if (!instance) {
    instance = createBikeRentalWatcher({
      appUrl: process.env.AGORA_APP_URL || 'https://theagorahotel.app',
      secret: process.env.AGORA_CRON_SECRET || '',
      serviceIds: parseServiceIds(process.env.BIKE_RENTAL_SERVICE_IDS),
      pollMs: parseInt(process.env.BIKE_RENTAL_POLL_MS || '', 10) || DEFAULTS.pollMs,
    });
  }
  return instance.start();
}

function stopBikeRentalWatch() {
  if (instance) instance.stop();
}

module.exports = { DEFAULTS, parseServiceIds, createBikeRentalWatcher, startBikeRentalWatch, stopBikeRentalWatch };
