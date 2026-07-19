/**
 * Locate a cell's grounding excerpt inside the raw source-document text so the
 * preview can highlight it. Excerpts rarely match the document byte-for-byte
 * (collapsed whitespace, surrounding quotes, trailing ellipsis, truncation), so
 * matching is tolerant: exact match first, then whitespace-normalized, then a
 * leading-prefix fallback that still lands the reader near the right place.
 *
 * Returned indices are offsets into the ORIGINAL `text`, end-exclusive.
 */

type Range = [number, number];

/** Lowercased copy with runs of whitespace collapsed to one space, plus a map
 *  from each normalized-string index back to its original-string index. */
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
      normChars.push(ch);
      map.push(i);
      prevWasSpace = false;
    }
  }
  return { norm: normChars.join(''), map };
}

/** Strip surrounding quotes and trailing ellipsis that excerpts often carry. */
function cleanQuery(raw: string): string {
  let q = raw.trim();
  q = q.replace(/^["'\u201c\u201d\u2018\u2019]+/, '').replace(/["'\u201c\u201d\u2018\u2019]+$/, '');
  q = q.replace(/\s*(?:\.{3}|\u2026)\s*$/, '');
  return q.trim();
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

  // 2) Whitespace-normalized match, mapped back to original offsets.
  const { norm, map } = normalizeWithMap(text);
  const normQuery = query.toLowerCase().replace(/\s+/g, ' ').trim();

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
 * Locate every excerpt in `queries` within `text`, returning ranges sorted by
 * start offset with overlaps removed (so we never render nested marks). Used to
 * highlight all of a cell's grounding excerpts at once — the caller scrolls to
 * the first, and the rest are visible as the user scrolls the document.
 */
export function findHighlightRanges(
  text: string | null | undefined,
  queries: ReadonlyArray<string | null | undefined> | null | undefined,
): Range[] {
  if (!text || !queries || queries.length === 0) return [];

  const found: Range[] = [];
  for (const q of queries) {
    const range = findHighlightRange(text, q);
    if (range) found.push(range);
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
