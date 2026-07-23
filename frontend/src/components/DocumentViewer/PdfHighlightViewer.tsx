import React, {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { Document, Page, pdfjs } from 'react-pdf';
import type { PDFDocumentProxy } from 'pdfjs-dist';

import 'react-pdf/dist/Page/TextLayer.css';
import {
  computeItemHighlightIntervals,
  wrapItemHtml,
  PdfTextItemLike,
} from './pdfHighlightUtils';

// Self-host the pdfjs worker: `new URL(..., import.meta.url)` makes webpack
// (CRA 5) emit the worker as a hashed asset served from our own origin, so it is
// version-locked to the installed pdfjs-dist and needs no CDN (no CSP concerns).
pdfjs.GlobalWorkerOptions.workerSrc = new URL(
  'pdfjs-dist/build/pdf.worker.min.mjs',
  import.meta.url,
).toString();

/** itemIndex -> character intervals to highlight within that item's str. */
type PageIntervals = Map<number, Array<[number, number]>>;

interface HighlightedPageProps {
  pageNumber: number;
  width?: number;
  intervals?: PageIntervals;
  onItems: (pageNumber: number, items: PdfTextItemLike[]) => void;
}

/**
 * One PDF page. Its `customTextRenderer`/`onGetTextSuccess` are memoized on the
 * page's own inputs, so react-pdf only rebuilds this page's text layer when its
 * highlights actually change — not on unrelated parent re-renders (e.g. a
 * same-cell re-click that only bumps the scroll nonce).
 */
const HighlightedPage: React.FC<HighlightedPageProps> = ({
  pageNumber,
  width,
  intervals,
  onItems,
}) => {
  const handleGetTextSuccess = useCallback(
    (textContent: { items: unknown[] }) => {
      onItems(pageNumber, textContent.items as PdfTextItemLike[]);
    },
    [pageNumber, onItems],
  );

  const customTextRenderer = useCallback(
    (item: { str: string; itemIndex: number }): string =>
      wrapItemHtml(item.str, intervals?.get(item.itemIndex)),
    [intervals],
  );

  return (
    <Page
      pageNumber={pageNumber}
      width={width || undefined}
      renderTextLayer
      renderAnnotationLayer={false}
      onGetTextSuccess={handleGetTextSuccess}
      customTextRenderer={customTextRenderer}
      className="shadow-sm"
    />
  );
};

interface PdfHighlightViewerProps {
  /** URL the PDF is served from (same-origin content endpoint). */
  fileUrl: string;
  /** Grounding excerpts to highlight; null/empty renders the PDF unmarked. */
  highlightTexts?: string[] | null;
  /** Bumped on each grounded-cell click to re-scroll to the first highlight. */
  scrollNonce?: number;
}

/**
 * Renders a PDF with react-pdf and highlights a cell's grounding excerpts in the
 * text layer. Each page's text items are captured via `onGetTextSuccess` (the
 * exact array react-pdf indexes, so `itemIndex` lines up), then matched in
 * pdfHighlightUtils; react-pdf's per-item `customTextRenderer` wraps the covered
 * substrings in <mark>, so highlights stay aligned inside the positioned spans.
 * Intervals are derived with useMemo so they recompute when the selected cell
 * (highlightTexts) changes, not only on document load. Scanned PDFs (no text
 * layer) render normally but cannot be marked.
 */
const PdfHighlightViewer: React.FC<PdfHighlightViewerProps> = ({
  fileUrl,
  highlightTexts,
  scrollNonce,
}) => {
  const [numPages, setNumPages] = useState(0);
  const [itemsByPage, setItemsByPage] = useState<Map<number, PdfTextItemLike[]>>(new Map());
  const [error, setError] = useState(false);
  const [pageWidth, setPageWidth] = useState(0);

  const containerRef = useRef<HTMLDivElement | null>(null);

  // Fit pages to the panel width.
  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return undefined;
    const measure = () => setPageWidth(Math.max(0, el.clientWidth - 24));
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  // Forget cached page text when the document changes.
  useEffect(() => {
    setItemsByPage(new Map());
    setNumPages(0);
    setError(false);
  }, [fileUrl]);

  const onLoadSuccess = useCallback((pdf: PDFDocumentProxy) => {
    setError(false);
    setNumPages(pdf.numPages);
  }, []);

  // Cache each page's items once (page text is stable for a document). Ignoring
  // repeat calls also keeps this stable callback loop-safe if react-pdf
  // re-invokes onGetTextSuccess.
  const handleItems = useCallback((pageNumber: number, items: PdfTextItemLike[]) => {
    setItemsByPage((prev) => {
      if (prev.has(pageNumber)) return prev;
      const next = new Map(prev);
      next.set(pageNumber, items);
      return next;
    });
  }, []);

  // Recompute highlight intervals whenever the cached text or the selected
  // cell's excerpts change.
  const intervalsByPage = useMemo(() => {
    const result = new Map<number, PageIntervals>();
    if (!highlightTexts || highlightTexts.length === 0) return result;
    itemsByPage.forEach((items, pageNumber) => {
      const intervals = computeItemHighlightIntervals(items, highlightTexts);
      if (intervals.size > 0) result.set(pageNumber, intervals);
    });
    return result;
  }, [itemsByPage, highlightTexts]);

  // Scroll to the first mark when a new selection's highlights become available
  // or on an explicit re-click. A guard keyed on the current selection prevents
  // repeated jumps as pages stream in during load, and avoids re-scrolling on
  // resize. The poll waits for the text layer to paint the mark.
  const scrolledForRef = useRef<{ texts: string[] | null | undefined; nonce: number | undefined }>({
    texts: undefined,
    nonce: undefined,
  });
  useEffect(() => {
    if (intervalsByPage.size === 0) return undefined;
    const done = scrolledForRef.current;
    if (done.texts === highlightTexts && done.nonce === scrollNonce) return undefined;

    let raf = 0;
    let tries = 0;
    const tryScroll = () => {
      const mark = containerRef.current?.querySelector('mark');
      if (mark) {
        mark.scrollIntoView({ block: 'center', behavior: 'smooth' });
        scrolledForRef.current = { texts: highlightTexts, nonce: scrollNonce };
        return;
      }
      tries += 1;
      if (tries < 30) raf = requestAnimationFrame(tryScroll);
    };
    raf = requestAnimationFrame(tryScroll);
    return () => cancelAnimationFrame(raf);
  }, [intervalsByPage, scrollNonce, highlightTexts]);

  const pages = useMemo(
    () => Array.from({ length: numPages }, (_, i) => i + 1),
    [numPages],
  );

  // A highlight was requested, every page's text has been scanned, and nothing
  // matched — either a scanned/image PDF (no text layer) or wording that differs
  // from the document. Tell the user rather than showing the PDF silently.
  const highlightRequested = Boolean(highlightTexts && highlightTexts.length > 0);
  const allPagesScanned = numPages > 0 && itemsByPage.size >= numPages;
  const showNoMatchNote = highlightRequested && allPagesScanned && intervalsByPage.size === 0;

  if (error) {
    return (
      <div className="h-full flex items-center justify-center text-sm text-muted-foreground text-center px-4">
        Could not load this PDF for preview.
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className="h-full w-full overflow-auto bg-muted/20 flex flex-col items-center gap-3 py-3"
    >
      {showNoMatchNote && (
        <div className="sticky top-0 z-10 w-full px-3 py-1.5 text-[11px] text-muted-foreground bg-muted/80 backdrop-blur border-b border-border text-center">
          Couldn&apos;t locate this excerpt in the PDF text (it may be scanned or worded differently).
        </div>
      )}
      <Document
        file={fileUrl}
        onLoadSuccess={onLoadSuccess}
        onLoadError={() => setError(true)}
        loading={<div className="text-sm text-muted-foreground p-4">Loading…</div>}
        error={<div className="text-sm text-muted-foreground p-4">Could not load this PDF.</div>}
      >
        {pages.map((pageNumber) => (
          <HighlightedPage
            key={pageNumber}
            pageNumber={pageNumber}
            width={pageWidth}
            intervals={intervalsByPage.get(pageNumber)}
            onItems={handleItems}
          />
        ))}
      </Document>
    </div>
  );
};

export default PdfHighlightViewer;
