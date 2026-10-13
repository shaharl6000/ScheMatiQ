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

/** Lowercased+folded copy with ALL whitespace removed (not just collapsed to
 *  one space), plus a map from each kept-char's index back to its
 *  original-string index. Real extracted PDF text can drop inter-word
 *  spacing entirely across a stretch (a font/encoding artifact, distinct
 *  from the double-space noise normalizeWithMap's collapsing already
 *  handles) -- collapsing a whitespace RUN to one space does nothing when
 *  there's no whitespace there to collapse in the first place.
 *
 *  Also drops a hyphen immediately followed by a whitespace run that
 *  contains a newline -- a print line-wrap artifact (e.g. source
 *  "recombina-\ntion" should match excerpt "recombination"). Mirrors the
 *  backend's _normalize_for_matching (excerpt_grounder.py) so the two
 *  stay in sync -- scoped narrowly (hyphen immediately followed by
 *  newline-containing whitespace) so real compound words ("T-cell",
 *  hyphen with no adjacent whitespace) are untouched. */
function stripWithMap(text: string): { stripped: string; map: number[] } {
  const chars: string[] = [];
  const map: number[] = [];
  const lower = text.toLowerCase();
  const n = lower.length;
  let i = 0;
  while (i < n) {
    const ch = lower[i];
    const folded = foldChar(ch);
    if (folded === '-') {
      let j = i + 1;
      let sawNewline = false;
      while (j < n && /\s/.test(lower[j])) {
        if (lower[j] === '\n') sawNewline = true;
        j += 1;
      }
      if (sawNewline && j > i + 1) {
        // Line-wrap hyphen: drop it and the whitespace run after it.
        i = j;
        continue;
      }
    }
    if (/\s/.test(ch)) {
      i += 1;
      continue;
    }
    chars.push(folded);
    map.push(i);
    i += 1;
  }
  return { stripped: chars.join(''), map };
}

/** Fold + strip a query the same way stripWithMap strips the text -- shares
 *  the same per-character logic (via stripWithMap itself) rather than
 *  duplicating it, so the two can't drift out of sync again. */
function foldStripped(s: string): string {
  return stripWithMap(s).stripped;
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

  // 1.5) Whitespace-STRIPPED match: handles source text that has lost
  // inter-word spacing entirely across a stretch (seen in real extracted
  // PDF text, e.g. "resultswithmice" instead of "results with mice") --
  // step 2 below only collapses whitespace RUNS, which can't fix this
  // since there's often no whitespace there to collapse.
  const { stripped, map: stripMap } = stripWithMap(text);
  const strippedQuery = foldStripped(query);
  if (strippedQuery.length >= 3) {
    const strippedIdx = stripped.indexOf(strippedQuery);
    if (strippedIdx >= 0) {
      const start = stripMap[strippedIdx];
      const lastOriginal = stripMap[strippedIdx + strippedQuery.length - 1];
      return [start, lastOriginal + 1];
    }
  }

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

/** Minimal shape findHighlightRanges needs from a grounding excerpt. */
export interface ExcerptLike {
  text?: string | null;
  char_start?: number | null;
  char_end?: number | null;
}

/** First "real" (letter/digit-containing) word of a normalized string. */
function firstWord(s: string): string {
  return foldNormalized(s).split(' ').find((w) => /[a-z0-9]/i.test(w)) ?? '';
}

/**
 * Cheap sanity check that a stored [char_start, char_end) offset still points
 * at roughly the right place -- guards against stale offsets if the document
 * on disk was ever replaced. Deliberately loose (first word only, not a full
 * comparison): the backend's own fuzzy grounding can legitimately diverge
 * from the excerpt's exact wording past the first word.
 *
 * Both sides are run through cleanQuery first: excerpts routinely carry a
 * surrounding quote (see this file's module docstring) that would otherwise
 * stick to the first word (e.g. `"the` vs `the`) and make a perfectly valid
 * backend-computed offset fail this check every time.
 */
function offsetRoughlyMatches(sourceSlice: string, excerptText: string): boolean {
  const a = firstWord(cleanQuery(sourceSlice));
  const b = firstWord(cleanQuery(excerptText));
  if (a.length === 0) return false;
  if (a === b) return true;
  // A source-side line-wrap hyphen (e.g. "recombina-" continuing as
  // "tion" on the next line -- see stripWithMap / the backend's
  // _normalize_for_matching) makes the source's first "word" end in a
  // trailing hyphen that the excerpt's own (unbroken) word doesn't have.
  // Treat that as a prefix match instead of demanding exact equality, so
  // a valid backend-computed offset for one of these isn't rejected here.
  if (a.endsWith('-')) {
    const aDehyphenated = a.slice(0, -1);
    return aDehyphenated.length > 0 && b.startsWith(aDehyphenated);
  }
  return false;
}

/**
 * Locate every excerpt in `excerpts` within `text`, splitting each on
 * internal ellipsis so multi-passage excerpts highlight all their parts.
 * Returns ranges sorted by start offset with overlaps removed (so we never
 * render nested marks). The caller scrolls to the first; the rest are found
 * by scrolling.
 *
 * When an excerpt carries a valid `char_start`/`char_end` (the backend
 * already located it via ExcerptGrounder, whose fuzzy matching tolerates
 * paraphrasing this module's own text search does not), that span is used
 * directly instead of re-searching for `text`.
 */
export function findHighlightRanges(
  text: string | null | undefined,
  excerpts: ReadonlyArray<ExcerptLike | null | undefined> | null | undefined,
): Range[] {
  if (!text || !excerpts || excerpts.length === 0) return [];

  const found: Range[] = [];
  for (const exc of excerpts) {
    if (!exc || !exc.text) continue;
    const excerptText = exc.text;

    const { char_start: start, char_end: end } = exc;
    if (
      typeof start === 'number' && typeof end === 'number' &&
      start >= 0 && end > start && end <= text.length &&
      offsetRoughlyMatches(text.slice(start, end), excerptText)
    ) {
      found.push([start, end]);
      continue;
    }

    const before = found.length;
    for (const fragment of splitExcerptFragments(excerptText)) {
      const range = findHighlightRange(text, fragment);
      if (range) found.push(range);
    }

    // Last resort: the backend's own fuzzy grounding already picked this
    // offset and can legitimately diverge from the excerpt's exact wording
    // (see offsetRoughlyMatches's docstring). Rather than dropping the
    // citation entirely when neither the sanity check nor a fresh text
    // search panned out, trust the stored (in-bounds) offset anyway so the
    // reader still gets a highlight and a scroll target.
    if (
      found.length === before &&
      typeof start === 'number' && typeof end === 'number' &&
      start >= 0 && end > start && end <= text.length
    ) {
      found.push([start, end]);
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
