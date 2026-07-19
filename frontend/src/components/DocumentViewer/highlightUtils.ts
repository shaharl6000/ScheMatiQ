/**
 * Locate a cell's grounding excerpt(s) inside the raw source-document text so the
 * preview can highlight them. Excerpts rarely match the document byte-for-byte
 * (collapsed whitespace, curly vs straight quotes, surrounding quotes, trailing
 * ellipsis, truncation), and a single excerpt may stitch together several
 * non-contiguous passages joined by an internal ellipsis ("A... B"). Matching is
 * therefore tolerant and ellipsis-aware:
 *   - fold curly quotes/dashes and collapse whitespace before comparing;
 *   - split each excerpt on internal ellipsis and locate every fragment;
 *   - exact match first, then whitespace-normalized, then a leading-prefix
 *     fallback that still lands the reader near the right place.
 *
 * Returned indices are offsets into the ORIGINAL `text`, end-exclusive.
 */

type Range = [number, number];

/** Single-character folds applied to the normalized (comparison) text only.
 *  All are 1:1 (one char in, one char out) so the norm→original index map that
 *  normalizeWithMap builds stays valid. */
const CHAR_FOLD: Record<string, string> = {
  '\u2018': "'", '\u2019': "'", '\u201a': "'", '\u201b': "'", '\u2032': "'",
  '\u0060': "'", '\u00b4': "'",
  '\u201c': '"', '\u201d': '"', '\u201e': '"', '\u201f': '"', '\u2033': '"',
  '\u2013': '-', '\u2014': '-', '\u2212': '-',
};

const foldChar = (ch: string): string => CHAR_FOLD[ch] ?? ch;

/** Lowercased+folded copy with runs of whitespace collapsed to one space, plus a
 *  map from each normalized-string index back to its original-string index. */
function normalizeWithMap(text: string): { norm: string; map: number[] } {
  const normChars: string[] = [];
  const map: number[] = [];
  let prevWasSpace = false;
  const lower = text.toLowerCase();
  for (let i = 0; i < lower.length; i += 1) {
    const ch = lower[i];
    if (/\s/.test(ch)) {
      if (prevWasSpace) continue;
      normChars.push(' ');
      map.push(i);
      prevWasSpace = true;
    } else {
      normChars.push(foldChar(ch));
      map.push(i);
      prevWasSpace = false;
    }
  }
  return { norm: normChars.join(''), map };
}

/** Fold + collapse a query the same way normalizeWithMap folds the text. */
function foldNormalized(s: string): string {
  const out: string[] = [];
  let prevWasSpace = false;
  const lower = s.toLowerCase();
  for (let i = 0; i < lower.length; i += 1) {
    const ch = lower[i];
    if (/\s/.test(ch)) {
      if (prevWasSpace) continue;
      out.push(' ');
      prevWasSpace = true;
    } else {
      out.push(foldChar(ch));
      prevWasSpace = false;
    }
  }
  return out.join('').trim();
}

/** Strip surrounding quotes and trailing ellipsis that excerpts often carry. */
function cleanQuery(raw: string): string {
  let q = raw.trim();
  q = q.replace(/^["'\u201c\u201d\u2018\u2019]+/, '').replace(/["'\u201c\u201d\u2018\u2019]+$/, '');
  q = q.replace(/\s*(?:\.{3,}|\u2026)\s*$/, '');
  return q.trim();
}

/**
 * Split an excerpt into fragments on an internal ellipsis ("...", "....", or the
 * "\u2026" character). Only 3+ dots split, so citation punctuation like "U.S."
 * or "Apr." is left intact. Fragments shorter than 4 chars are dropped as noise.
 */
export function splitExcerptFragments(raw: string): string[] {
  return raw
    .split(/\s*(?:\.{3,}|\u2026)\s*/)
    .map((s) => s.trim())
    .filter((s) => s.length >= 4);
}

export function findHighlightRange(
  text: string | null | undefined,
  rawQuery: string | null | undefined,
): Range | null {
  if (!text || !rawQuery) return null;
  const query = cleanQuery(rawQuery);
  if (query.length < 3) return null;

  // 1) Direct case-insensitive match against the original text.
  const directIdx = text.toLowerCase().indexOf(query.toLowerCase());
  if (directIdx >= 0) return [directIdx, directIdx + query.length];

  // 2) Folded + whitespace-normalized match, mapped back to original offsets.
  const { norm, map } = normalizeWithMap(text);
  const normQuery = foldNormalized(query);

  const mapRange = (nIdx: number, len: number): Range => {
    const start = map[nIdx];
    const lastOriginal = map[nIdx + len - 1];
    return [start, lastOriginal + 1];
  };

  const normIdx = norm.indexOf(normQuery);
  if (normIdx >= 0) return mapRange(normIdx, normQuery.length);

  // 3) Leading-prefix fallback: match the first several words so the reader is
  //    still scrolled to the right region even if the tail diverges.
  const prefix = normQuery.split(' ').slice(0, 8).join(' ').slice(0, 60).trim();
  if (prefix.length >= 8) {
    const prefixIdx = norm.indexOf(prefix);
    if (prefixIdx >= 0) return mapRange(prefixIdx, prefix.length);
  }

  return null;
}

/**
 * Locate every excerpt in `queries` within `text`, splitting each on internal
 * ellipsis so multi-passage excerpts highlight all their parts. Returns ranges
 * sorted by start offset with overlaps removed (so we never render nested
 * marks). The caller scrolls to the first; the rest are found by scrolling.
 */
export function findHighlightRanges(
  text: string | null | undefined,
  queries: ReadonlyArray<string | null | undefined> | null | undefined,
): Range[] {
  if (!text || !queries || queries.length === 0) return [];

  const found: Range[] = [];
  for (const q of queries) {
    if (!q) continue;
    for (const fragment of splitExcerptFragments(q)) {
      const range = findHighlightRange(text, fragment);
      if (range) found.push(range);
    }
  }
  if (found.length === 0) return [];

  found.sort((a, b) => a[0] - b[0]);

  // Drop ranges that overlap one already kept (first-wins after sorting).
  const result: Range[] = [];
  let lastEnd = -1;
  for (const [start, end] of found) {
    if (start >= lastEnd) {
      result.push([start, end]);
      lastEnd = end;
    }
  }
  return result;
}
