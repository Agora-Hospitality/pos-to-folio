/**
 * Fire-and-forget notify to the Agora app when a sale lands on a folio:
 * POSTs the full sale lines to /api/pos/folio-sales so the Guest 360 can show
 * lifetime F&B spend + favourite items (Phase 2 of the guest platform).
 *
 * Never throws and never blocks the bridge — the folio charge is the money
 * path; this is telemetry. The app endpoint is idempotent (goodtillSaleId),
 * so retries/replays are safe. One retry after 5s on network failure.
 *
 * Env: AGORA_APP_URL (default https://theagorahotel.app), POS_INGEST_SECRET.
 */

async function postOnce(url, secret, payload) {
  const res = await fetch(`${url.replace(/\/$/, '')}/api/pos/folio-sales`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-pos-secret': secret },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`app ingest HTTP ${res.status}`);
}

/**
 * @param {any} sale - full Goodtill sale
 * @param {string} saleId
 * @param {object} ctx - { roomNumber, reservation, fnbGross, tipGross, saleTimeUtcIso }
 */
async function notifyAppOfFolioSale(sale, saleId, ctx) {
  const url = process.env.AGORA_APP_URL || 'https://theagorahotel.app';
  const secret = process.env.POS_INGEST_SECRET;
  if (!secret) {
    console.warn('[app-ingest] POS_INGEST_SECRET not set — skipping app notify (spend/favourites will lag until backfill)');
    return;
  }
  const items = sale?.sales_details?.sales_items || [];
  const payload = {
    goodtillSaleId: String(saleId),
    receiptNo: sale.receipt_no || sale.receipt_number || null,
    soldAtUtc: ctx.saleTimeUtcIso,
    roomNumber: ctx.roomNumber || null,
    mewsReservationId: ctx.reservation?.Id || null,
    mewsCustomerId: ctx.reservation?.AccountId || ctx.reservation?.CustomerId || null,
    grossTotal: ctx.fnbGross,
    tipGross: ctx.tipGross,
    currency: process.env.CURRENCY || 'EUR',
    source: 'bridge',
    lines: items.map((it) => ({
      sku: it.product_sku || null,
      name: String(it.product_name || it.name || '').trim(),
      qty: parseFloat(it.quantity) || 0,
      gross: (parseFloat(it.price_inc_vat_per_item) || 0) * (parseFloat(it.quantity) || 0),
      category: it.category_name || null,
    })),
  };
  try {
    await postOnce(url, secret, payload);
    console.log(`[app-ingest] ✓ sale ${saleId} lines sent to the app`);
  } catch (e) {
    console.warn(`[app-ingest] first attempt failed (${e.message}) — retrying in 5s`);
    setTimeout(() => {
      postOnce(url, secret, payload)
        .then(() => console.log(`[app-ingest] ✓ sale ${saleId} lines sent on retry`))
        .catch((e2) => console.error(`[app-ingest] ✗ sale ${saleId} not ingested (${e2.message}) — backfill will pick it up`));
    }, 5000);
  }
}

module.exports = { notifyAppOfFolioSale };
