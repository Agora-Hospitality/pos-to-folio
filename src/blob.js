/**
 * Finding and splicing ONE line inside a ResDiary diner's comments box.
 *
 * ResDiary has no per-note rows — a diner has one free-text "Customer Comments"
 * field, shared by everything the restaurant has ever typed about them and
 * everything the app has pushed. Editing or removing a single note therefore
 * means surgery on a shared string, and the rule is narrow on purpose:
 *
 *   **The only bytes we may change are bytes we have just proved are there,
 *   matched whole-line, byte-for-byte, in the blob ResDiary handed us
 *   milliseconds ago. Everything else is copied back verbatim. Zero matches and
 *   two matches are both REFUSALS that write nothing.**
 *
 * ── Why offsets are computed on the ORIGINAL string ───────────────────────
 * Our appends write CRLF; staff typing in the portal produce their own; a note
 * edited in the app's textarea comes back LF-only. The blob is genuinely mixed.
 * Normalise it to a copy, find an offset there, then splice that offset into
 * the original, and every `\r\n` before it has shifted the cut by one byte per
 * line: the cut lands mid-word in the neighbouring lines and we PUT that
 * corruption back as the whole customer record. Normalising the original
 * instead silently rewrites every line ending in a field the restaurant owns.
 *
 * So nothing here ever compares raw strings. The blob is split into SEGMENTS
 * and SEPARATORS, offsets are recorded against the original, and matching
 * compares segment text only — `\r\n` versus `\n` can affect neither a match
 * nor a cut.
 *
 * ── No trimming, no fuzz ──────────────────────────────────────────────────
 * A note whose wording someone changed in the portal is NOT found, and that is
 * the correct answer: it means we no longer know which line was ours, and
 * guessing would rewrite a line a manager wrote.
 *
 * The app holds a TypeScript twin of these rules in
 * `agora-app/src/lib/guest360/resdiary-blob.ts`, so the screen and the write
 * agree about what is in the blob. Change one, change the other.
 */

const SEPARATOR = /(\r\n|\r|\n)/;

/** Lines, each remembering where it began in the ORIGINAL string. */
function segmentsOf(blob) {
  const parts = String(blob ?? '').split(SEPARATOR);
  const out = [];
  let offset = 0;
  for (let i = 0; i < parts.length; i++) {
    if (i % 2 === 0) out.push({ text: parts[i], start: offset });
    offset += parts[i].length;
  }
  return out;
}

/** The needle as lines, with leading/trailing blank lines dropped. */
function needleSegments(text) {
  const segs = String(text ?? '').split(SEPARATOR).filter((_, i) => i % 2 === 0);
  while (segs.length && segs[0].trim() === '') segs.shift();
  while (segs.length && segs[segs.length - 1].trim() === '') segs.pop();
  return segs;
}

/**
 * Every whole-line occurrence of `text` in `blob`, as {index, length} into the
 * ORIGINAL string.
 *
 * A match begins at the start of a line and ends at the end of one, so
 * "No nuts" does NOT match inside "No nuts or shellfish" — the commonest edit
 * shape, and the one a substring test gets wrong in both directions.
 */
function findBlockOnBoundary(blob, text) {
  const needle = needleSegments(text);
  if (!needle.length || needle.every((s) => s.trim() === '')) return [];
  const segs = segmentsOf(blob);
  const hits = [];
  for (let i = 0; i + needle.length <= segs.length; i++) {
    let ok = true;
    for (let k = 0; k < needle.length; k++) {
      if (segs[i + k].text !== needle[k]) { ok = false; break; }
    }
    if (!ok) continue;
    const first = segs[i];
    const last = segs[i + needle.length - 1];
    hits.push({ index: first.start, length: last.start + last.text.length - first.start });
  }
  return hits;
}

/**
 * Replace or remove one located block, copying every other byte verbatim.
 *
 * `newText` null/blank means remove, and a removal takes exactly one adjacent
 * separator with it — the one after, or the one before when the block is last —
 * so no blank line is left behind and no trailing newline is created.
 */
function spliceBlock(blob, span, newText) {
  const s = String(blob ?? '');
  const end = span.index + span.length;
  const replacement = String(newText ?? '').trim();
  if (replacement) return s.slice(0, span.index) + newText + s.slice(end);

  const after = s.slice(end);
  const sepAfter = /^(\r\n|\r|\n)/.exec(after);
  if (sepAfter) return s.slice(0, span.index) + after.slice(sepAfter[0].length);

  const before = s.slice(0, span.index);
  const sepBefore = /(\r\n|\r|\n)$/.exec(before);
  if (sepBefore) return before.slice(0, before.length - sepBefore[0].length) + after;

  return before + after;
}

/**
 * Did the write do what we asked, judged by the SAME matcher that cut.
 *
 * The naive check — `after.includes(newText) && !after.includes(previousText)`
 * — is wrong for every additive edit: growing "No nuts" into "No nuts or
 * shellfish" leaves the old text present as a substring, so a perfectly good
 * write reports as a failure and the user re-ticks it, planting a duplicate.
 */
function verifySplice(after, previousText, newText) {
  if (typeof after !== 'string') return null; // no body echoed — UNDETERMINED
  const gone = findBlockOnBoundary(after, previousText).length === 0;
  if (newText == null || String(newText).trim() === '') return gone;
  return gone && findBlockOnBoundary(after, newText).length === 1;
}

/**
 * Serialise read-modify-write per diner, in-process.
 *
 * The throttle in resdiary.js spaces individual HTTP calls; it is not a mutex.
 * Two rows actioned from one guest profile interleave as read-A, read-B,
 * write-A, write-B — and write-B, computed from a blob that predates write-A,
 * silently reinstates the line write-A removed. The worker is a single Railway
 * process, so one Map of promises is a real lock.
 */
const _locks = new Map();
function withCustomerLock(customerId, work) {
  const key = String(customerId);
  const prev = _locks.get(key) || Promise.resolve();
  const run = prev.then(work, work);
  // Never let a rejection poison the chain for the next caller.
  _locks.set(key, run.then(() => {}, () => {}));
  return run;
}

module.exports = {
  findBlockOnBoundary,
  spliceBlock,
  verifySplice,
  withCustomerLock,
  _internal: { segmentsOf, needleSegments },
};
