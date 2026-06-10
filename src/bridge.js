/**
 * Bridge Orchestrator
 * Polls Goodtill for Guest Folio sales and posts them as orders in MEWS.
 *
 * Flow:
 * 1. Fetch recent sales from Goodtill
 * 2. Filter for completed sales paid via "Guest Folio" payment method
 * 3. Look up the room from the sale's customer_id (synced roster profile)
 * 4. Find the active (checked-in) reservation for that room
 * 5. Post the order to MEWS linked to that reservation
 */

const { fetchSales, fetchSaleById, formatDateTime, extractGuestFolioSales, parseRoomNumber } = require('./goodtill');
const { getResourcesAndRoomMap, getActiveReservationForRoom, findFBService, findFBAccountingCategory, addOrder, deleteExternalPayments, getOrderItems, cancelOrderItems } = require('./mews');
const { SalesStore, DeadLetterStore } = require('./store');
const { fullSync, getRoomByCustomerId } = require('./roster');

/** @type {Map<string, string>} room name → MEWS resource ID */
let roomMap = new Map();
/** @type {Map<string, string>} MEWS resource ID → room name */
let resourceToRoom = new Map();
/** @type {string|null} */
let fbServiceId = null;
/** @type {string|null} */
let fbAccountingCategoryId = null;
/** @type {SalesStore} */
let store;
/** @type {DeadLetterStore} */
let deadLetter;
/** @type {ReturnType<typeof setInterval>|null} */
let pollTimer = null;

// How far back to look on each poll cycle (in minutes)
const LOOKBACK_MINUTES = parseInt(process.env.POLL_LOOKBACK_MINUTES || '30', 10);
// Unpostable sales (dead-letter) are retried this long after first being seen...
const DEAD_LETTER_RETRY_WINDOW_HOURS = parseInt(process.env.DEAD_LETTER_RETRY_WINDOW_HOURS || '24', 10);
// ...at most once per interval (a guest checking in mid-morning makes the
// room postable, so frequent early retries pay off; hammering doesn't)...
const DEAD_LETTER_RETRY_INTERVAL_MS = parseInt(process.env.DEAD_LETTER_RETRY_INTERVAL_MS || String(15 * 60 * 1000), 10);
// ...and at most this many times in total (bounds duplicate-charge exposure
// if MEWS orders/add ever commits server-side but errors client-side).
const DEAD_LETTER_MAX_ATTEMPTS = parseInt(process.env.DEAD_LETTER_MAX_ATTEMPTS || '40', 10);
// Reentrancy guard: setInterval ticks, POST /poll, and webhooks can otherwise
// overlap, and processSale never re-checks store.has after its first await —
// concurrent runs are the realistic double-post path.
let _polling = false;
// How far back to check for voids (in hours) — posted sales within this window are re-queried each void-detection pass
const VOID_DETECT_WINDOW_HOURS = parseInt(process.env.VOID_DETECT_WINDOW_HOURS || '48', 10);
// Run void detection at most once every N ms (throttle, since it makes one API call per unreversed sale)
const VOID_DETECT_INTERVAL_MS = parseInt(process.env.VOID_DETECT_INTERVAL_MS || String(5 * 60 * 1000), 10);
let _lastVoidDetectAt = 0;

/**
 * Initialise the bridge: load MEWS config (rooms, services, categories),
 * then run a full roster sync to push checked-in rooms to Goodtill.
 */
async function init() {
  store = new SalesStore();
  deadLetter = new DeadLetterStore();

  console.log('[bridge] Loading MEWS configuration...');
  const [roomData, serviceId, catId] = await Promise.all([
    getResourcesAndRoomMap(),
    findFBService(),
    findFBAccountingCategory(),
  ]);

  roomMap = roomData.roomMap;
  fbServiceId = serviceId;
  fbAccountingCategoryId = catId;

  // Build reverse map: resource ID → room name
  resourceToRoom = new Map();
  for (const [name, id] of roomMap) {
    resourceToRoom.set(id, name);
  }

  console.log(`[bridge] Loaded ${roomMap.size} room mappings`);
  console.log(`[bridge] F&B Service ID: ${fbServiceId}`);
  console.log(`[bridge] F&B Accounting Category ID: ${fbAccountingCategoryId}`);

  // Sync checked-in rooms to Goodtill as customer profiles
  await fullSync(roomMap, resourceToRoom);
}

/** Expose maps for webhook handler */
function getRoomMap() { return roomMap; }
function getResourceToRoom() { return resourceToRoom; }

/**
 * Run a single poll cycle
 */
async function pollOnce() {
  if (_polling) {
    console.log('[bridge] Poll already in flight — skipping this tick');
    return;
  }
  _polling = true;
  try {
    await pollOnceInner();
  } finally {
    _polling = false;
  }
}

async function pollOnceInner() {
  const now = new Date();
  const from = new Date(now.getTime() - LOOKBACK_MINUTES * 60 * 1000);

  console.log(`[bridge] Polling Goodtill sales from ${formatDateTime(from)} to ${formatDateTime(now)}`);

  let sales;
  try {
    sales = await fetchSales(from, now);
  } catch (err) {
    console.error('[bridge] Failed to fetch Goodtill sales:', err.message);
    return;
  }

  const guestFolioSales = extractGuestFolioSales(sales);
  console.log(`[bridge] Found ${sales.length} total sales, ${guestFolioSales.length} Guest Folio sales`);

  let successCount = 0;
  let errorCount = 0;
  let reversedCount = 0;

  for (const { sale, voided, folioAmount, otherPayments } of guestFolioSales) {
    const saleId = String(sale.id || sale.sale_id);

    if (voided) {
      // Check if this voided sale was previously posted to MEWS
      const entry = store.get(saleId);
      if (!entry || entry.reversedAt) continue; // not posted or already reversed
      try {
        const reversed = await reverseSale(sale, saleId, entry);
        if (reversed) reversedCount++;
      } catch (err) {
        errorCount++;
        console.error(`[bridge] Error reversing sale ${saleId}:`, err.message);
      }
      continue;
    }

    if (store.has(saleId)) continue;

    try {
      const posted = await processSale(sale, saleId, folioAmount, otherPayments);
      if (posted) successCount++;
    } catch (err) {
      errorCount++;
      console.error(`[bridge] Error processing sale ${saleId}:`, err.message);
    }
  }

  if (successCount || errorCount || reversedCount) {
    console.log(`[bridge] Poll complete: ${successCount} posted, ${reversedCount} reversed, ${errorCount} errors`);
  }

  // Void-detection fallback: Goodtill's /external/get_sales_details list endpoint
  // only returns COMPLETED sales, so voids never appear in pollOnce above. And
  // Goodtill's sale.voided webhook is unreliable — we've seen zero webhook
  // deliveries despite subscribing. So we also poll each unreversed store entry
  // individually and check its current order_status. Throttled to avoid hammering
  // the Goodtill API.
  if (Date.now() - _lastVoidDetectAt >= VOID_DETECT_INTERVAL_MS) {
    _lastVoidDetectAt = Date.now();
    try {
      await detectVoids();
    } catch (err) {
      console.error('[bridge] Void detection error:', err.message);
    }
  }

  // Retry sales the bridge previously could not post (no customer attached,
  // room missing, no checked-in reservation). Without this, a sale that aged
  // out of the poll lookback was dropped forever with only a console.warn.
  try {
    await retryDeadLetters();
  } catch (err) {
    console.error('[bridge] Dead-letter retry error:', err.message);
  }
}

/**
 * Re-attempt every unresolved dead-letter sale that is due for a retry.
 * Resolution paths: posted (processSale succeeds), voided on POS, no longer a
 * folio sale, or already in the processed store. Entries older than the retry
 * window stay listed at GET /dead-letter for ops but are not retried.
 */
async function retryDeadLetters() {
  const candidates = deadLetter.retryCandidates(
    DEAD_LETTER_RETRY_WINDOW_HOURS * 60 * 60 * 1000,
    DEAD_LETTER_RETRY_INTERVAL_MS,
    DEAD_LETTER_MAX_ATTEMPTS,
  );
  if (candidates.length === 0) return;

  let recovered = 0;
  for (const [saleId] of candidates) {
    if (store.has(saleId)) {
      deadLetter.resolve(saleId, 'already posted');
      continue;
    }

    let sale;
    try {
      sale = await fetchSaleById(saleId);
    } catch (err) {
      console.error(`[bridge] dead-letter: error fetching sale ${saleId}:`, err.message);
      continue;
    }
    if (!sale) {
      deadLetter.record(saleId, {}, 'sale not found in Goodtill (404)');
      continue;
    }

    const matches = extractGuestFolioSales([sale]);
    if (matches.length === 0) {
      // Only conclude "no longer a folio sale" from positive evidence: a
      // COMPLETED sale whose payments visibly lack a folio tender. The by-id
      // response shape may omit sales_payments — that proves nothing.
      const status = String(sale.order_status || '').toUpperCase();
      const payments = sale.sales_payments && typeof sale.sales_payments === 'object' ? sale.sales_payments : null;
      const keys = payments ? Object.keys(payments) : [];
      const hasFolioKey = keys.some((k) => {
        const kk = k.toLowerCase();
        return kk === 'custom_1' || kk.includes('guest folio') || kk.includes('room charge');
      });
      if (keys.length > 0 && !hasFolioKey && status === 'COMPLETED') {
        deadLetter.resolve(saleId, 'no longer a guest-folio sale');
      } else {
        deadLetter.record(saleId, {}, `by-id response inconclusive (status ${status || 'unknown'}, ${keys.length} payment keys) — left open`);
      }
      continue;
    }
    if (matches[0].voided) {
      deadLetter.resolve(saleId, 'voided on POS');
      continue;
    }

    try {
      // processSale resolves the entry on success and re-records (with a fresh
      // lastTriedAt, which throttles the next attempt) on failure.
      const posted = await processSale(sale, saleId, matches[0].folioAmount, matches[0].otherPayments);
      if (posted) recovered++;
    } catch (err) {
      // Bump lastTriedAt/attempts so a thrown failure (MEWS lookup/addOrder
      // error) is throttled like any other — not re-attempted every 60s tick.
      console.error(`[bridge] dead-letter: error reprocessing sale ${saleId}:`, err.message);
      deadLetter.record(saleId, {}, `retry error: ${err.message}`);
    }
  }

  if (recovered) {
    console.log(`[bridge] Dead-letter retry: recovered ${recovered} sale(s)`);
  }
}

/**
 * Ping Uptime Kuma's push monitor to signal a successful poll cycle.
 * Uses UPTIME_KUMA_PUSH_URL env var — silent no-op if unset.
 * Failures never throw — a monitoring hiccup must not break the bridge.
 */
async function pushHeartbeat(status, msg) {
  const url = process.env.UPTIME_KUMA_PUSH_URL;
  if (!url) return;
  try {
    const u = new URL(url);
    u.searchParams.set('status', status);
    u.searchParams.set('msg', msg);
    await fetch(u, { signal: AbortSignal.timeout(5000) });
  } catch (err) {
    console.warn('[bridge] push heartbeat failed:', err.message);
  }
}

/**
 * Re-check every recently-posted sale in the store and reverse any that have
 * since been voided on the POS. Idempotent — entries already marked reversedAt
 * are filtered out by the store.
 */
async function detectVoids() {
  const candidates = store.getUnreversedCandidates(VOID_DETECT_WINDOW_HOURS * 60 * 60 * 1000);
  if (candidates.length === 0) return;

  let reversed = 0;
  let errors = 0;
  for (const [saleId, entry] of candidates) {
    let sale;
    try {
      sale = await fetchSaleById(saleId);
    } catch (err) {
      errors++;
      console.error(`[bridge] void-detect: error fetching sale ${saleId}:`, err.message);
      continue;
    }

    // Treat 404 (null) as a warning, not an auto-reversal — Goodtill occasionally
    // returns transient 404s, and we don't want to cancel real charges on MEWS
    // because of a flaky lookup. Log so ops can investigate.
    if (!sale) {
      console.warn(`[bridge] void-detect: sale ${saleId} returned 404 — possible void or hard-delete. Skipping auto-reverse.`);
      continue;
    }

    const status = String(sale.order_status || '').toUpperCase();
    if (status === 'VOIDED') {
      try {
        const ok = await reverseSale(sale, saleId, entry);
        if (ok) reversed++;
      } catch (err) {
        errors++;
        console.error(`[bridge] void-detect: error reversing sale ${saleId}:`, err.message);
      }
    }
  }

  if (reversed || errors) {
    console.log(`[bridge] Void detection: checked ${candidates.length}, reversed ${reversed}, errors ${errors}`);
  }
}

/**
 * Process a single Guest Folio sale
 * @param {any} sale - The Goodtill sale object
 * @param {string} saleId
 * @param {number} folioAmount - Sum of folio-keyed entries in sale.sales_payments.
 *   Excludes tips — Goodtill stores tips in sales_details.metadata.tips_obj, never as a
 *   sales_payments line, so folioAmount is bill-only even when the tip is also folio-routed.
 * @param {Array<{method: string, amount: number}>} [otherPayments] - Non-folio payments (Card/Cash)
 */
async function processSale(sale, saleId, folioAmount, otherPayments = []) {
  // Primary: look up room from the sale's customer_id (roster-synced profile)
  const gtCustomerId = sale.customer_id;
  let roomNumber = null;
  let reservation = null;

  if (gtCustomerId) {
    const roomInfo = getRoomByCustomerId(gtCustomerId);
    if (roomInfo) {
      roomNumber = roomInfo.roomNumber;
      // We already have the reservation ID from the roster
      const resourceId = roomMap.get(roomNumber);
      if (resourceId) {
        reservation = await getActiveReservationForRoom(resourceId);
      }
    }
  }

  // Fallback: extract room code from the customer name (e.g. "DEM11 — Guest" or legacy "Room DEM11 — Guest")
  if (!roomNumber) {
    const custName = sale.customer?.name || sale.customer_name || sale.sales_details?.customer_name || '';
    const match = custName.match(/^(?:Room\s+)?(\S+)\s*—/i);
    if (match) {
      roomNumber = match[1];
      console.log(`[bridge] Sale ${saleId}: resolved room ${roomNumber} from customer name "${custName}"`);
    }
  }

  if (!roomNumber) {
    const custName = sale.customer?.name || sale.customer_name || sale.sales_details?.customer_name || '';
    console.warn(`[bridge] Sale ${saleId}: no room matched — customer_id="${gtCustomerId || 'null'}", customer_name="${custName}", skipping`);
    deadLetter.record(saleId, deadLetterInfo(sale, folioAmount), 'no room matched (no customer attached to sale)');
    return false;
  }

  // If we didn't get a reservation from the roster, look it up
  if (!reservation) {
    const resourceId = roomMap.get(roomNumber);
    if (!resourceId) {
      console.error(`[bridge] Sale ${saleId}: room "${roomNumber}" not found in MEWS`);
      deadLetter.record(saleId, deadLetterInfo(sale, folioAmount), `room "${roomNumber}" not found in MEWS`);
      return false;
    }
    reservation = await getActiveReservationForRoom(resourceId);
  }

  if (!reservation) {
    console.error(`[bridge] Sale ${saleId}: no checked-in reservation found for room ${roomNumber}`);
    deadLetter.record(saleId, deadLetterInfo(sale, folioAmount), `no checked-in reservation for room ${roomNumber}`);
    return false;
  }

  // 3. Build order items
  //
  // Split bill (Card + Guest Folio) posts ONLY the folio portion to MEWS —
  // the Card is settled at POS and never touches MEWS. We construct the folio
  // lines as food-portion + tip-portion (rather than a single summary line) so
  // the Tips accounting category stays separate from F&B for reconciliation.
  //
  // Full-folio sales take the existing itemized path unchanged.
  const saleRef = sale.receipt_no || sale.receipt_number || saleId;
  const saleTotal = parseFloat(
    sale.sales_details?.total_after_discount ?? sale.sales_details?.total ?? sale.total ?? '0'
  );
  const tipTotal = extractTipAmount(sale);
  // A sale is a split only when there's a non-folio payment (Card/Cash) on the
  // sales_payments list. Don't trigger split on a folioAmount/saleTotal
  // mismatch: tips are in tips_obj metadata and never appear in sales_payments,
  // so a full-folio bill with a folio-routed tip always has that mismatch and
  // would be wrongly carved up (sale #1777054624155 posted €71+€7 instead of €78+€7).
  const isSplit = otherPayments.length > 0;

  let items;
  if (isSplit) {
    if (folioAmount <= 0) {
      console.warn(`[bridge] Sale ${saleId}: split-bill folio amount is zero, skipping`);
      return false;
    }
    // Heuristic: tips go to the folio first (true for the common case where
    // the guest routes the tip to their room). If a future sale routes tip to
    // card, the total folio amount is still correct — only the food-vs-tip
    // split within the folio lines would be off.
    const tipFolio = Math.min(tipTotal, folioAmount);
    const foodFolio = Math.round((folioAmount - tipFolio) * 100) / 100;
    items = [];
    if (foodFolio >= 0.005) {
      items.push({
        name: `POS Sale #${saleRef} — Food/Drinks`,
        unitCount: 1,
        grossValue: foodFolio,
      });
    }
    if (tipFolio >= 0.005) {
      items.push({
        name: 'Tip',
        unitCount: 1,
        grossValue: tipFolio,
        taxCode: 'CY-Z',
        accountingCategoryId: process.env.MEWS_TIPS_CATEGORY_ID || '88951db5-3deb-4e66-b108-b213013e2a08',
      });
    }
  } else {
    items = buildOrderItems(sale);
    if (items.length === 0) {
      if (saleTotal <= 0) {
        console.warn(`[bridge] Sale ${saleId}: zero total after discount, skipping`);
        return false;
      }
      console.warn(`[bridge] Sale ${saleId}: no line items found, posting total as single item`);
      items.push({ name: 'POS Charge', unitCount: 1, grossValue: saleTotal });
    }
  }

  // 4. Post to MEWS
  console.log(`[bridge] Posting sale ${saleRef} (room ${roomNumber}${isSplit ? `, split — folio €${folioAmount.toFixed(2)}` : ''}) to reservation ${reservation.Id}`);

  const result = await addOrder({
    serviceId: fbServiceId,
    accountId: reservation.AccountId || reservation.CustomerId,
    reservationId: reservation.Id,
    accountingCategoryId: fbAccountingCategoryId,
    items,
    notes: `POS Sale #${saleRef} — ${roomNumber}`,
    consumptionUtc: sale.sales_date_time
      ? new Date(sale.sales_date_time).toISOString()
      : new Date().toISOString(),
    currency: process.env.CURRENCY || 'EUR',
  });

  const mewsOrderId = result?.OrderId || result?.Id;

  // 5. Mark as processed. paymentIds stays empty for new entries — kept in the
  // store schema for back-compat with pre-fix entries that still have them.
  store.add(saleId, mewsOrderId, []);
  deadLetter.resolve(saleId, 'posted');
  console.log(`[bridge] ✓ Sale ${saleRef} posted to MEWS (order ${mewsOrderId || 'ok'})`);
  return true;
}

/**
 * Context captured with a dead-letter entry so ops can identify the sale
 * without opening Goodtill.
 */
function deadLetterInfo(sale, folioAmount) {
  // folioAmount is bill-only (tips live in tips_obj metadata) — report the tip
  // too so a tip-only sale doesn't read as "nothing owed".
  const tipAmount = extractTipAmount(sale);
  return {
    receipt: sale.receipt_no || sale.receipt_number || '',
    customer: sale.customer?.name || sale.customer_name || sale.sales_details?.customer_name || '',
    saleTimeLocal: sale.sales_date_time || '',
    folioAmount,
    tipAmount,
    totalAtStake: Math.round((folioAmount + tipAmount) * 100) / 100,
  };
}

/**
 * Reverse a voided sale by cancelling its order items in MEWS
 * @param {any} sale - The Goodtill sale object (now voided)
 * @param {string} saleId
 * @param {{ mewsOrderId?: string }} entry - The store entry for this sale
 */
async function reverseSale(sale, saleId, entry) {
  const saleRef = sale.receipt_no || sale.receipt_number || saleId;

  if (!entry.mewsOrderId) {
    console.warn(`[bridge] Sale ${saleRef} was voided but no MEWS order ID stored — cannot auto-reverse`);
    return false;
  }

  console.log(`[bridge] Sale ${saleRef} voided on POS — cancelling MEWS order ${entry.mewsOrderId}`);

  // Fetch order items for this order
  const items = await getOrderItems([entry.mewsOrderId]);
  const activeItems = items.filter((i) => !i.CanceledUtc);

  if (activeItems.length === 0) {
    console.log(`[bridge] Sale ${saleRef}: no active order items to cancel in MEWS`);
    store.markReversed(saleId);
    return true;
  }

  // Cancel all active items
  await cancelOrderItems(activeItems.map((i) => i.Id));

  // Also reverse any external payments (split-bill prepayments)
  const paymentIds = entry.paymentIds || [];
  if (paymentIds.length > 0) {
    try {
      await deleteExternalPayments(paymentIds);
      console.log(`[bridge]   Reversed ${paymentIds.length} external payment(s)`);
    } catch (err) {
      console.error(`[bridge]   Failed to reverse external payments for ${saleRef}:`, err.message);
    }
  }

  store.markReversed(saleId);
  console.log(`[bridge] ✓ Sale ${saleRef} reversed — cancelled ${activeItems.length} item(s) in MEWS`);
  return true;
}

/**
 * Build MEWS order items from a Goodtill sale
 * @param {any} sale
 * @returns {Array<{name: string, unitCount: number, grossValue: number}>}
 */
function buildOrderItems(sale) {
  const saleItems = sale.sales_details?.sales_items || sale.sales_items || [];
  const items = [];

  for (const item of saleItems) {
    const name = item.product_name || item.item_name || item.name || 'Item';
    const qty = parseInt(item.quantity || item.qty || '1', 10);

    // Prefer post-discount price. Use ?? so a legitimate 0 (fully discounted line)
    // doesn't fall through to the pre-discount price — that would charge the guest
    // the original amount instead of the discounted amount.
    const postDiscountRaw = item.line_total_after_discount ?? item.line_total_after_line_discount;
    const price = parseFloat(
      postDiscountRaw ?? item.total ?? item.price_inc_vat_per_item ?? item.price ?? item.amount ?? '0'
    );

    // Skip zero lines, and round floating-point near-zero (e.g. -7e-15) to zero.
    if (Math.abs(price) < 0.005) continue;

    items.push({
      name,
      unitCount: qty || 1,
      grossValue: price / (qty || 1), // MEWS wants per-unit gross value
    });
  }

  // Extract tips from metadata
  const tipTotal = extractTipAmount(sale);
  if (tipTotal > 0) {
    items.push({
      name: 'Tip',
      unitCount: 1,
      grossValue: tipTotal,
      taxCode: 'CY-Z', // Tips are 0% VAT
      accountingCategoryId: process.env.MEWS_TIPS_CATEGORY_ID || '88951db5-3deb-4e66-b108-b213013e2a08',
    });
  }

  return items;
}

/**
 * Extract total tip amount from a Goodtill sale's metadata
 */
function extractTipAmount(sale) {
  try {
    const metaStr = sale.sales_details?.metadata;
    if (!metaStr) return 0;
    const meta = typeof metaStr === 'string' ? JSON.parse(metaStr) : metaStr;
    const tipsObj = meta.tips_obj;
    if (!tipsObj || typeof tipsObj !== 'object') return 0;
    let total = 0;
    for (const tip of Object.values(tipsObj)) {
      total += parseFloat(tip.amount || '0');
    }
    return total;
  } catch {
    return 0;
  }
}

/**
 * Start the polling loop
 * @param {number} [intervalMs] - Polling interval in milliseconds
 */
function startPolling(intervalMs) {
  const interval = intervalMs || parseInt(process.env.POLL_INTERVAL_MS || '60000', 10);
  console.log(`[bridge] Starting poll loop every ${interval / 1000}s`);

  // Heartbeat fires every tick regardless of pollOnce outcome — the push
  // monitor signals "polling loop is alive", not "external APIs worked".
  // Goodtill/MEWS outages are covered by their own HTTP health checks.
  const tick = async () => {
    try {
      await pollOnce();
    } catch (err) {
      console.error('[bridge] Poll error:', err.message);
    }
    await pushHeartbeat('up', 'tick');
  };

  tick();
  pollTimer = setInterval(tick, interval);
}

/**
 * Stop the polling loop
 */
function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
    console.log('[bridge] Polling stopped');
  }
}

// ─── Webhook handlers ────────────────────────────────────────────────

/**
 * Handle a sale.voided webhook from Goodtill.
 * Fetches the sale, checks if it was posted to MEWS, and cancels the order items.
 * @param {string} saleId - Goodtill sale ID
 */
async function handleVoidedSale(saleId) {
  const entry = store.get(saleId);
  if (!entry) {
    console.log(`[bridge] Voided sale ${saleId} was not in store (not a guest folio or not yet posted), ignoring`);
    return;
  }
  if (entry.reversedAt) {
    console.log(`[bridge] Voided sale ${saleId} already reversed, ignoring`);
    return;
  }

  // Fetch the sale to get the receipt number for logging
  let saleRef = saleId;
  try {
    const sale = await fetchSaleById(saleId);
    if (sale) saleRef = sale.receipt_no || sale.receipt_number || saleId;
  } catch { /* use saleId as fallback ref */ }

  await reverseSale({ receipt_no: saleRef }, saleId, entry);
}

/**
 * Handle a sale.completed webhook from Goodtill.
 * Fetches the sale details and processes it if it's a guest folio payment.
 * @param {string} saleId - Goodtill sale ID
 */
async function handleCompletedSale(saleId) {
  if (store.has(saleId)) {
    console.log(`[bridge] Completed sale ${saleId} already processed, ignoring`);
    return;
  }

  let sale;
  try {
    sale = await fetchSaleById(saleId);
  } catch (err) {
    console.error(`[bridge] Failed to fetch sale ${saleId}:`, err.message);
    return;
  }

  if (!sale) {
    console.warn(`[bridge] Sale ${saleId} not found in Goodtill`);
    return;
  }

  // Check if it's a guest folio sale
  const matches = extractGuestFolioSales([sale]);
  if (matches.length === 0 || matches[0].voided) return;

  console.log(`[bridge] Webhook: processing completed guest folio sale ${saleId}`);
  try {
    await processSale(sale, saleId, matches[0].folioAmount, matches[0].otherPayments);
  } catch (err) {
    console.error(`[bridge] Error processing webhook sale ${saleId}:`, err.message);
  }
}

/** Unresolved dead-letter entries for the /dead-letter endpoint */
function getDeadLetterEntries() {
  return deadLetter ? deadLetter.unresolved() : [];
}

module.exports = { init, pollOnce, startPolling, stopPolling, getRoomMap, getResourceToRoom, handleVoidedSale, handleCompletedSale, getDeadLetterEntries };
