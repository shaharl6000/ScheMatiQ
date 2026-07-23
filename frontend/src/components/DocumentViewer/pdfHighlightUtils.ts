/**
 * Map grounding excerpts onto a PDF page's text layer.
 *
 * PDF.js exposes a page's text as many separate text items (roughly one per
 * text run), not one continuous string, so we cannot wrap a match the way the
 * plain-text preview does. Instead we:
 *   1. concatenate the page's items into a single string with a separator
 *      between items, recording the `[start, end)` span each item occupies;
 *   2. reuse `findHighlightRanges` (ellipsis-split + quote/whitespace folding +
 *      overlap removal) to locate the excerpts in that string;
 *   3. intersect each matched range with the item spans to get per-item
 *      character intervals so the caller can wrap the covered substrings in
 *      <mark>.
 *
 * This module is pure (no DOM, no pdfjs imports) and unit-testable. The React
 * component feeds it the items from `getTextContent()` and uses the result in
 * react-pdf's `customTextRenderer` (which renders per item and expects an HTML
 * string), so highlights live inside the already-positioned text-layer spans
 * and stay aligned automatically.
 */

import { findHighlightRanges } from './highlightUtils';

/** The subset of a PDF.js text-content item we rely on. The array from
 *  `getTextContent()`/`onGetTextSuccess` also contains marked-content items that
 *  have no `str`; those occupy an index but contribute no text. */
export interface PdfTextItemLike {
  str?: string;
  /** PDF.js marks the item that ends a line; we join those with a newline. */
  hasEOL?: boolean;
}

/** A text item's span within the concatenated page text: `[start, end)`. */
interface Segment {
  item: number;
  start: number;
  end: number;
}

export interface PageText {
  text: string;
  segments: Segment[];
}

/**
 * Concatenate a page's text items into one string, inserting a separator after
 * each text item (newline after an end-of-line item, otherwise a space), and
 * return the `[start, end)` span each item occupies in that string. Item indices
 * are the item's position in the ORIGINAL items array so they line up with the
 * `itemIndex` react-pdf passes to `customTextRenderer`; marked-content items
 * (no `str`) are skipped but still consume their index. The separators let an
 * excerpt that spans two items match under the whitespace-tolerant matcher.
 */
export function buildPageText(items: PdfTextItemLike[]): PageText {
  const parts: string[] = [];
  const segments: Segment[] = [];
  let offset = 0;

  items.forEach((item, itemIndex) => {
    const str = typeof item.str === 'string' ? item.str : '';
    if (!str) return;
    parts.push(str);
    segments.push({ item: itemIndex, start: offset, end: offset + str.length });
    offset += str.length;
    const separator = item.hasEOL ? '\n' : ' ';
    parts.push(separator);
    offset += separator.length;
  });

  return { text: parts.join(''), segments };
}

/**
 * Locate every excerpt in `queries` on this page and return, per item index,
 * the list of `[start, end)` character intervals within that item's `str` to
 * highlight. Intervals per item are sorted and merged so wrapping is simple.
 */
export function computeItemHighlightIntervals(
  items: PdfTextItemLike[],
  queries: ReadonlyArray<string | null | undefined> | null | undefined,
): Map<number, Array<[number, number]>> {
  const result = new Map<number, Array<[number, number]>>();
  if (!items.length || !queries || queries.length === 0) return result;

  const { text, segments } = buildPageText(items);
  const ranges = findHighlightRanges(text, queries);
  if (ranges.length === 0) return result;

  // Intersect each matched range with the item segments it overlaps. Segments
  // are sorted by start, so we can stop once a segment begins past the range.
  ranges.forEach(([rangeStart, rangeEnd]) => {
    for (let s = 0; s < segments.length; s += 1) {
      const seg = segments[s];
      if (seg.end <= rangeStart) continue;
      if (seg.start >= rangeEnd) break;
      const overlapStart = Math.max(rangeStart, seg.start);
      const overlapEnd = Math.min(rangeEnd, seg.end);
      if (overlapEnd <= overlapStart) continue;
      const list = result.get(seg.item) ?? [];
      list.push([overlapStart - seg.start, overlapEnd - seg.start]);
      result.set(seg.item, list);
    }
  });

  // Merge/sort intervals within each item (adjacent ranges can touch).
  result.forEach((intervals, item) => {
    intervals.sort((a, b) => a[0] - b[0]);
    const merged: Array<[number, number]> = [];
    intervals.forEach(([start, end]) => {
      const last = merged[merged.length - 1];
      if (last && start <= last[1]) {
        last[1] = Math.max(last[1], end);
      } else {
        merged.push([start, end]);
      }
    });
    result.set(item, merged);
  });

  return result;
}

const escapeHtml = (s: string): string =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/**
 * Build the HTML string for one text item, wrapping the given character
 * intervals in <mark>. Returned to react-pdf's `customTextRenderer`. The mark
 * inherits the text layer's transparent text color, so only the yellow
 * background paints over the canvas glyphs (the standard PDF marker look).
 */
export function wrapItemHtml(str: string, intervals: Array<[number, number]> | undefined): string {
  if (!intervals || intervals.length === 0) return escapeHtml(str);

  const out: string[] = [];
  let cursor = 0;
  for (const [start, end] of intervals) {
    const s = Math.max(0, Math.min(start, str.length));
    const e = Math.max(s, Math.min(end, str.length));
    if (e <= s) continue;
    if (s > cursor) out.push(escapeHtml(str.slice(cursor, s)));
    out.push(
      '<mark style="background-color:rgba(253,224,71,0.45);color:inherit;padding:0;border-radius:2px;">',
      escapeHtml(str.slice(s, e)),
      '</mark>',
    );
    cursor = e;
  }
  if (cursor < str.length) out.push(escapeHtml(str.slice(cursor)));
  return out.join('');
}
