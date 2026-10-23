/**
 * Close dead-letter entries that reception has already keyed into MEWS by hand.
 *
 * The bridge can only see its own postings. When a Guest Folio sale fails (no
 * room attached, guest not checked in) and someone re-keys the charge on the
 * folio, nothing tells the bridge — the entry stays open forever. By 23-10-2026
 * 142 open entries (€33,128.80) were charges the Agora app's daily guest-folio
 * reconciliation had already matched to a manual MEWS posting.
 *
 * The reconciliation is the one place that knows, so this asks it:
 * GET {AGORA_APP_URL}/api/pos/folio-rekeyed?since=YYYY-MM-DD → { rekeyed: [{ receipt, date }] }
 * Read-only on both sides, never throws; a failed fetch just waits for the next run.
 *
 * Env: AGORA_APP_URL (default https://theagorahotel.app), POS_INGEST_SECRET.
 */

/**
 * @param {string} since - YYYY-MM-DD, the oldest open entry's day
 * @returns {Promise<Map<string, string>|null>} receipt → resolution, or null when unavailable
 */
async function fetchRekeyedReceipts(since) {
  const url = (process.env.AGORA_APP_URL || 'https://theagorahotel.app').replace(/\/$/, '');
  const secret = process.env.POS_INGEST_SECRET;
  if (!secret) {
    console.warn('[rekey-sweep] POS_INGEST_SECRET not set — cannot ask the app which sales were re-keyed');
    return null;
  }
  try {
    const res = await fetch(`${url}/api/pos/folio-rekeyed?since=${encodeURIComponent(since)}`, {
      headers: { 'x-pos-secret': secret },
      signal: AbortSignal.timeout(20000),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.json();
    return rekeyedToResolutions(body);
  } catch (e) {
    console.error(`[rekey-sweep] could not read re-keyed sales from the app (${e.message})`);
    return null;
  }
}

/**
 * @param {{ rekeyed?: Array<{ receipt?: string, date?: string }> }} body
 * @returns {Map<string, string>}
 */
function rekeyedToResolutions(body) {
  const out = new Map();
  for (const r of body?.rekeyed || []) {
    if (!r?.receipt) continue;
    out.set(String(r.receipt), `re-keyed in MEWS by hand (reconciliation ${r.date || '?'})`);
  }
  return out;
}

/**
 * One sweep against a DeadLetterStore.
 * @param {import('./store').DeadLetterStore} deadLetter
 * @param {(since: string) => Promise<Map<string, string>|null>} [fetcher]
 * @returns {Promise<number>} entries closed
 */
async function sweepRekeyed(deadLetter, fetcher = fetchRekeyedReceipts) {
  const oldest = deadLetter.oldestOpenFirstSeen();
  if (!oldest) return 0;
  const map = await fetcher(oldest.slice(0, 10));
  if (!map || map.size === 0) return 0;
  const n = deadLetter.resolveByReceipts(map);
  if (n) console.log(`[rekey-sweep] closed ${n} dead-letter entr${n === 1 ? 'y' : 'ies'} the reconciliation shows re-keyed by hand`);
  return n;
}

module.exports = { fetchRekeyedReceipts, rekeyedToResolutions, sweepRekeyed };
