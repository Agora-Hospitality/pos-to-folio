/**
 * Processed-sales tracker
 * Keeps track of Goodtill sale IDs that have been successfully posted to MEWS.
 * Persists to a local JSON file so the service survives restarts without duplicating charges.
 */

const fs = require('node:fs');
const path = require('node:path');

const DEFAULT_PATH = path.resolve(process.cwd(), 'data', 'processed-sales.json');

class SalesStore {
  /**
   * @param {string} [filePath] - Path to the JSON persistence file
   */
  constructor(filePath = DEFAULT_PATH) {
    this.filePath = filePath;
    /** @type {Map<string, { postedAt: string, mewsOrderId?: string, paymentIds?: string[] }>} */
    this.processed = new Map();
    this._load();
  }

  _load() {
    try {
      const dir = path.dirname(this.filePath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

      if (fs.existsSync(this.filePath)) {
        const raw = JSON.parse(fs.readFileSync(this.filePath, 'utf-8'));
        for (const [k, v] of Object.entries(raw)) {
          this.processed.set(k, v);
        }
        console.log(`[store] Loaded ${this.processed.size} processed sales from disk`);
      }
    } catch (err) {
      console.error('[store] Failed to load processed sales:', err.message);
    }
  }

  _save() {
    try {
      const obj = Object.fromEntries(this.processed);
      fs.writeFileSync(this.filePath, JSON.stringify(obj, null, 2));
    } catch (err) {
      console.error('[store] Failed to save processed sales:', err.message);
    }
  }

  /**
   * Check whether a sale ID has already been processed
   * @param {string} saleId
   * @returns {boolean}
   */
  has(saleId) {
    return this.processed.has(String(saleId));
  }

  /**
   * Get the stored data for a sale
   * @param {string} saleId
   * @returns {{ postedAt: string, mewsOrderId?: string, paymentIds?: string[], reversedAt?: string } | undefined}
   */
  get(saleId) {
    return this.processed.get(String(saleId));
  }

  /**
   * Mark a sale as successfully processed
   * @param {string} saleId
   * @param {string} [mewsOrderId]
   * @param {string[]} [paymentIds] - MEWS external payment IDs (for split-bill prepayments)
   */
  add(saleId, mewsOrderId, paymentIds = []) {
    const entry = {
      postedAt: new Date().toISOString(),
      mewsOrderId,
    };
    if (paymentIds.length > 0) entry.paymentIds = paymentIds;
    this.processed.set(String(saleId), entry);
    this._save();
  }

  /**
   * Mark a previously processed sale as reversed (voided)
   * @param {string} saleId
   */
  markReversed(saleId) {
    const entry = this.processed.get(String(saleId));
    if (entry) {
      entry.reversedAt = new Date().toISOString();
      this._save();
    }
  }

  /** Number of processed sales */
  get size() {
    return this.processed.size;
  }

  /**
   * Return entries that are candidates for void-polling:
   * posted within maxAgeMs, have a MEWS order ID, and aren't already reversed.
   * @param {number} maxAgeMs - Max age of entries to return
   * @returns {Array<[string, { postedAt: string, mewsOrderId: string, paymentIds?: string[] }]>}
   */
  getUnreversedCandidates(maxAgeMs) {
    const cutoff = Date.now() - maxAgeMs;
    const out = [];
    for (const [id, entry] of this.processed) {
      if (entry.reversedAt) continue;
      if (!entry.mewsOrderId) continue;
      const postedAt = Date.parse(entry.postedAt);
      if (!Number.isFinite(postedAt) || postedAt < cutoff) continue;
      out.push([id, entry]);
    }
    return out;
  }
}

const DEAD_LETTER_PATH = path.resolve(process.cwd(), 'data', 'dead-letter.json');

/**
 * Dead-letter tracker for Guest Folio sales the bridge could NOT post
 * (no customer attached, room not in MEWS, no checked-in reservation).
 *
 * Before this existed, such sales were retried only while they remained inside
 * the poll lookback window (default 30 min) and then silently dropped forever —
 * console.warn was the only trace. Entries here survive restarts, are retried
 * on a schedule, and stay visible via GET /dead-letter until resolved.
 */
class DeadLetterStore {
  /**
   * @param {string} [filePath] - Path to the JSON persistence file
   */
  constructor(filePath = DEAD_LETTER_PATH) {
    this.filePath = filePath;
    /** @type {Map<string, { firstSeenAt: string, lastTriedAt: string, attempts: number, reason: string, receipt?: string, customer?: string, saleTimeLocal?: string, folioAmount?: number, resolvedAt?: string, resolution?: string }>} */
    this.entries = new Map();
    this._load();
  }

  _load() {
    try {
      const dir = path.dirname(this.filePath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

      if (fs.existsSync(this.filePath)) {
        const raw = JSON.parse(fs.readFileSync(this.filePath, 'utf-8'));
        for (const [k, v] of Object.entries(raw)) {
          this.entries.set(k, v);
        }
        const open = [...this.entries.values()].filter((e) => !e.resolvedAt).length;
        console.log(`[dead-letter] Loaded ${this.entries.size} entries (${open} unresolved) from disk`);
      }
    } catch (err) {
      console.error('[dead-letter] Failed to load:', err.message);
    }
  }

  _save() {
    try {
      const obj = Object.fromEntries(this.entries);
      fs.writeFileSync(this.filePath, JSON.stringify(obj, null, 2));
    } catch (err) {
      console.error('[dead-letter] Failed to save:', err.message);
    }
  }

  /**
   * Record (or refresh) a failed sale. Keeps firstSeenAt, bumps attempts and
   * lastTriedAt. Re-recording a previously resolved entry reopens it.
   * @param {string} saleId
   * @param {{ receipt?: string, customer?: string, saleTimeLocal?: string, folioAmount?: number }} info
   * @param {string} reason
   */
  record(saleId, info, reason) {
    const id = String(saleId);
    const prev = this.entries.get(id);
    const nowIso = new Date().toISOString();
    // Merge over the previous entry so a sparse re-record (e.g. a transient
    // 404 path passing info={}) never wipes receipt/customer/amount context.
    const entry = {
      ...prev,
      ...info,
      firstSeenAt: prev?.firstSeenAt || nowIso,
      reason,
      attempts: (prev?.attempts || 0) + 1,
      lastTriedAt: nowIso,
    };
    // Re-recording deliberately reopens a resolved entry — the sale failed again.
    delete entry.resolvedAt;
    delete entry.resolution;
    this.entries.set(id, entry);
    this._save();
  }

  /**
   * Mark an entry as resolved (posted, voided, no longer a folio sale, ...).
   * No-op for unknown sale IDs.
   * @param {string} saleId
   * @param {string} resolution
   */
  resolve(saleId, resolution) {
    const entry = this.entries.get(String(saleId));
    if (entry && !entry.resolvedAt) {
      entry.resolvedAt = new Date().toISOString();
      entry.resolution = resolution;
      this._save();
    }
  }

  /** @returns {boolean} */
  has(saleId) {
    return this.entries.has(String(saleId));
  }

  /**
   * All unresolved entries, newest first.
   * @returns {Array<{ saleId: string } & Record<string, any>>}
   */
  unresolved() {
    return [...this.entries]
      .filter(([, e]) => !e.resolvedAt)
      .map(([saleId, e]) => ({ saleId, ...e }))
      .sort((a, b) => String(b.firstSeenAt).localeCompare(String(a.firstSeenAt)));
  }

  /**
   * Unresolved entries due for a retry: first seen within maxAgeMs, not tried
   * within the last minIntervalMs, and under the attempts cap (bounds the
   * MEWS double-submit exposure when addOrder commits server-side but throws
   * client-side). Capped/aged-out entries stay visible in unresolved() but are
   * no longer retried automatically.
   * @param {number} maxAgeMs
   * @param {number} minIntervalMs
   * @param {number} [maxAttempts]
   * @returns {Array<[string, Record<string, any>]>}
   */
  retryCandidates(maxAgeMs, minIntervalMs, maxAttempts = 40) {
    const now = Date.now();
    const out = [];
    for (const [id, e] of this.entries) {
      if (e.resolvedAt) continue;
      if ((e.attempts || 0) >= maxAttempts) continue;
      const firstSeen = Date.parse(e.firstSeenAt);
      const lastTried = Date.parse(e.lastTriedAt || e.firstSeenAt);
      if (!Number.isFinite(firstSeen) || now - firstSeen > maxAgeMs) continue;
      if (Number.isFinite(lastTried) && now - lastTried < minIntervalMs) continue;
      out.push([id, e]);
    }
    return out;
  }
}

module.exports = { SalesStore, DeadLetterStore };
