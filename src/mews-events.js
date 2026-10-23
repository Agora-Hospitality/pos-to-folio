/**
 * MEWS General Webhook payload helpers — pure, no I/O.
 *
 * A delivery is { EnterpriseId, IntegrationId, Events: [{ Discriminator,
 * Value: { Id } }] }. Every Value carries only the entity Id — no ServiceId,
 * no state — so anything beyond "which reservation changed" needs a
 * Connector API read.
 *
 * Why the ids are logged (19-10-2026): bike rentals are MEWS product service
 * orders on the Bicycle Rental service, each linked to a stay, and the app's
 * Cycling list wants them within a minute of the sale. Thirty days of Railway
 * logs show no ServiceOrderUpdated for a product order's own id (the roster
 * would log "Reservation … not found" — zero lines) and none for the linked
 * reservation either; the 294 F&B orders the bridge itself posted were
 * followed by a webhook no more often than chance. So this webhook cannot
 * drive the Cycling list until Mews confirms product orders raise an event.
 * The summary line in index.js settles that from the logs: match a bike
 * sale's time to the ids delivered around it.
 */

const SERVICE_ORDER_UPDATED = 'ServiceOrderUpdated';

/**
 * @param {unknown} events - req.body.Events as delivered
 * @returns {{ total: number, serviceOrderIds: string[], others: Record<string, number> }}
 *   serviceOrderIds — distinct ServiceOrderUpdated ids in delivery order,
 *   missing / non-string ids dropped; others — count per other Discriminator.
 */
function summarizeMewsEvents(events) {
  const list = Array.isArray(events) ? events : [];
  const serviceOrderIds = [];
  const seen = new Set();
  const others = {};
  for (const evt of list) {
    const disc = typeof evt?.Discriminator === 'string' && evt.Discriminator ? evt.Discriminator : '(none)';
    if (disc === SERVICE_ORDER_UPDATED) {
      const id = evt.Value?.Id;
      if (typeof id === 'string' && id && !seen.has(id)) {
        seen.add(id);
        serviceOrderIds.push(id);
      }
      continue;
    }
    others[disc] = (others[disc] || 0) + 1;
  }
  return { total: list.length, serviceOrderIds, others };
}

/**
 * One log line per delivery. Keeps the historical prefix
 * "[webhook] Received N ServiceOrderUpdated event(s)" so existing greps hold.
 */
function formatMewsEventSummary({ serviceOrderIds, others }) {
  let line = `[webhook] Received ${serviceOrderIds.length} ${SERVICE_ORDER_UPDATED} event(s)`;
  if (serviceOrderIds.length) line += `: ${serviceOrderIds.join(', ')}`;
  const rest = Object.entries(others).map(([disc, n]) => `${disc}×${n}`);
  if (rest.length) line += ` · also ${rest.join(', ')}`;
  return line;
}

module.exports = { SERVICE_ORDER_UPDATED, summarizeMewsEvents, formatMewsEventSummary };
