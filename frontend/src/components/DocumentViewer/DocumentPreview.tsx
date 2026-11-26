import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Download,
  ExternalLink,
  FileText,
  FileX,
  Loader2,
  Quote,
  RotateCw,
  Search,
  Upload,
  X,
} from 'lucide-react';

import { unitsAPI } from '../../services/api';
import type { TextExcerpt } from '../../types';
import { findHighlightRanges } from './highlightUtils';

/** Extensions the browser can render inline in an <iframe>. */
const INLINE_EXTENSIONS = new Set([
  'pdf', 'html', 'htm', 'txt', 'md', 'csv', 'json',
  'png', 'jpg', 'jpeg', 'gif', 'webp', 'svg',
]);

/** Inline content that can carry executable markup, so the iframe is sandboxed. */
const SANDBOX_EXTENSIONS = new Set(['html', 'htm', 'svg']);

/**
 * Text formats we render ourselves (in a DOM we control) when highlight mode is
 * enabled, so a cell's grounding excerpt can be marked and scrolled into view.
 * Non-text formats (pdf/images/html) keep the native <iframe> rendering, which
 * cannot be annotated from the parent document.
 */
const TEXT_EXTENSIONS = new Set(['txt', 'text', 'md', 'markdown', 'csv', 'tsv', 'json', 'log']);

/**
 * Binary office formats DocumentUpload accepts that aren't inline-renderable
 * or text-renderable, but are still real, known extensions that should
 * legitimately fall to the download-only branch below (as opposed to an
 * arbitrary alphanumeric tail that merely looks like an extension).
 */
const OTHER_KNOWN_EXTENSIONS = new Set(['doc', 'docx', 'rtf']);

const extensionOf = (name: string): string => {
  const dot = name.lastIndexOf('.');
  if (dot < 0) return '';
  const ext = name.slice(dot + 1).toLowerCase();
  // Only treat the suffix as a real extension if it's one we actually know —
  // not just "looks like one". Document/paper names often contain internal
  // periods with a short alphanumeric tail that isn't a real extension (an
  // arXiv-style ID's version suffix like "2512.10004v2", a docket number's
  // trailing fragment, etc.); a generic shape-based regex was previously
  // fooled by these. Those still resolve to '' so they take the
  // text-renderable path instead of the false "preview unavailable" state.
  const isKnownExtension =
    INLINE_EXTENSIONS.has(ext) || TEXT_EXTENSIONS.has(ext) || OTHER_KNOWN_EXTENSIONS.has(ext);
  return isKnownExtension ? ext : '';
};

type Availability = 'idle' | 'checking' | 'ok' | 'unavailable';

/** A figure citation's image + optional caption, ready to render. */
interface FigureItem {
  url: string;
  caption?: string;
}

/** Metadata describing the currently selected Data cell shown in the Evidence Inspector. */
export interface EvidenceCellMeta {
  rowName: string;
  columnKey: string;
  columnLabel: string;
  value: string;
  status: 'grounded' | 'ungrounded' | 'confirmed_empty' | 'empty' | 'edited';
  isVerified: boolean;
  verifyKey: string;
}

interface DocumentPreviewProps {
  sessionId: string | null | undefined;
  /** Source-document name (raw value, as returned by the documents endpoint). */
  documentName: string | null;
  /** Message shown when no document and no figures are selected. */
  emptyHint?: string;
  /**
   * Bump this to force a fresh availability probe and iframe reload — e.g. after
   * the user uploads previously-missing source files for an imported project.
   */
  reloadToken?: number;
  /**
   * When provided, the "not available" state offers a button that invokes this
   * (typically opens a file picker to re-attach the original source documents).
   */
  onRequestUpload?: () => void;
  /**
   * True while re-attached source files are being uploaded and processed on the
   * server. Converting several documents can take a while, so the "not
   * available" state shows a spinner instead of a static message — otherwise the
   * panel looks frozen and the user assumes the upload silently failed.
   */
  uploading?: boolean;
  /**
   * Grounding excerpts to highlight in the document. Passing this prop at all
   * (even `null`/`[]`) enables the highlight-capable inline text renderer for
   * text-based formats; the Documents tab omits it and keeps the plain iframe.
   */
  highlightTexts?: TextExcerpt[] | null;
  /**
   * Changes on every user click of a grounded cell. Re-scrolls to the first
   * highlight even when the excerpt set is unchanged (e.g. the user scrolled the
   * document away and clicked the same cell again).
   */
  scrollNonce?: number;
  /**
   * The selected cell's figure citations (if any), rendered as a stacked
   * gallery below the document (or on their own, full-height, when there's no
   * document to show).
   */
  figures?: FigureItem[] | null;
  /** Enables the upgraded Evidence Inspector header, 1/N stepper, in-doc search, PDF text toggle, and select-to-fill. */
  enhancedEvidence?: boolean;
  /** Selected cell context displayed in the Evidence Inspector header. */
  evidenceMeta?: EvidenceCellMeta | null;
  /** Toggle human-verified state on the selected cell. */
  onToggleVerify?: (verifyKey: string) => void;
  /** Trigger "Wrong, try again" on the selected cell with an optional user hint. */
  onRetryCell?: (hint?: string) => void;
  /** Direct manipulation: set selected cell's value from text highlighted in the document. */
  onUseSelectionAsValue?: (selectedText: string) => void;
  /** Disable retry controls while an extraction is already running. */
  retryBusy?: boolean;
}

/**
 * Renders a single source document inline (native browser rendering for
 * PDF/HTML/images/text) with an "Open full" link to a new tab, and a download
 * fallback for formats the browser cannot render.
 *
 * Before rendering, the content URL is probed with a HEAD request: if the
 * document cannot be served (e.g. an imported project whose files were never
 * re-attached, where the endpoint returns an error), a friendly "not available"
 * message is shown instead of letting the iframe render the raw error body. The
 * message can offer an upload action so the user can supply the original files.
 *
 * When `highlightTexts` is supplied (highlight mode), text-based documents are
 * fetched and rendered in a controlled scroll container so the grounding
 * excerpts can be marked; the first match is scrolled into view and the rest
 * are visible as the user scrolls. PDFs/images/HTML cannot be annotated inside
 * their native iframe, so they keep the iframe and show a brief note when a
 * highlight was requested.
 *
 * When the selected cell also (or only) cites figures, they render as a
 * gallery stacked below the document body, separated by a divider — or on
 * their own when there's no document (e.g. a figure-only citation).
 */
const DocumentPreview: React.FC<DocumentPreviewProps> = ({
  sessionId,
  documentName,
  emptyHint,
  reloadToken,
  onRequestUpload,
  uploading,
  highlightTexts,
  scrollNonce,
  figures,
  enhancedEvidence,
  evidenceMeta,
  onToggleVerify,
  onRetryCell,
  onUseSelectionAsValue,
  retryBusy,
}) => {
  const contentUrl = useMemo(() => {
    if (!sessionId || !documentName) return null;
    const base = unitsAPI.getDocumentContentUrl(sessionId, documentName);
    // Cache-bust so a freshly uploaded file isn't masked by a cached 404/response.
    return reloadToken ? `${base}&_t=${reloadToken}` : base;
  }, [sessionId, documentName, reloadToken]);

  const [availability, setAvailability] = useState<Availability>('idle');

  useEffect(() => {
    if (!contentUrl) {
      setAvailability('idle');
      return undefined;
    }
    let cancelled = false;
    setAvailability('checking');
    fetch(contentUrl, { method: 'HEAD' })
      .then((res) => {
        if (!cancelled) setAvailability(res.ok ? 'ok' : 'unavailable');
      })
      .catch(() => {
        if (!cancelled) setAvailability('unavailable');
      });
    return () => {
      cancelled = true;
    };
  }, [contentUrl]);

  const ext = documentName ? extensionOf(documentName) : '';
  const canRenderInline = ext === '' || INLINE_EXTENSIONS.has(ext);
  const needsSandbox = SANDBOX_EXTENSIONS.has(ext);

  // Highlight mode is opt-in (the prop is present) and only applies to text
  // formats we can render and annotate ourselves — plus PDFs when enhancedEvidence
  // switches into extracted-text mode (`pdfViewMode === 'text'`).
  const highlightEnabled = highlightTexts !== undefined;
  const isPdf = ext === 'pdf';
  const [pdfViewMode, setPdfViewMode] = useState<'text' | 'pdf'>('text');
  const [activeExcerptIdx, setActiveExcerptIdx] = useState(0);
  const [docSearchOpen, setDocSearchOpen] = useState(false);
  const [docSearchQuery, setDocSearchQuery] = useState('');
  const [docSearchIdx, setDocSearchIdx] = useState(0);
  const [retryHintOpen, setRetryHintOpen] = useState(false);
  const [retryHint, setRetryHint] = useState('');
  const [selectedSnippet, setSelectedSnippet] = useState('');

  // Reset per-cell transient controls when cell or document changes.
  useEffect(() => {
    setActiveExcerptIdx(0);
    setRetryHintOpen(false);
    setRetryHint('');
    setSelectedSnippet('');
  }, [documentName, evidenceMeta?.columnKey, evidenceMeta?.rowName, highlightTexts]);

  useEffect(() => {
    setDocSearchQuery('');
    setDocSearchIdx(0);
  }, [documentName]);

  const isTextRenderable = ext === '' || TEXT_EXTENSIONS.has(ext);
  const usePdfTextMode = Boolean(enhancedEvidence && highlightEnabled && isPdf && pdfViewMode === 'text');
  const useInlineText = highlightEnabled && (isTextRenderable || usePdfTextMode);

  const hasDocument = Boolean(contentUrl);
  const hasFigures = Boolean(figures && figures.length > 0);
  const hasTextGrounding = Boolean(highlightTexts && highlightTexts.length > 0);
  const showDocumentSection = hasDocument && !(highlightEnabled && hasFigures && !hasTextGrounding);
  const showOpenFull = showDocumentSection && availability === 'ok';

  const [text, setText] = useState<string | null>(null);
  const [textError, setTextError] = useState(false);
  const [contentIsBinary, setContentIsBinary] = useState(false);
  const textDocumentRef = useRef<string | null>(null);

  useEffect(() => {
    if (!useInlineText || !sessionId || !documentName || availability !== 'ok') {
      setText(null);
      setTextError(false);
      textDocumentRef.current = null;
      setContentIsBinary(false);
      return undefined;
    }
    let cancelled = false;
    setText(null);
    setTextError(false);
    setContentIsBinary(false);
    textDocumentRef.current = null;
    unitsAPI
      .getDocumentContentText(sessionId, documentName, usePdfTextMode ? 'text' : undefined)
      .then((content) => {
        if (cancelled) return;
        if (content.startsWith('%PDF-') || content.indexOf('\u0000') !== -1) {
          setContentIsBinary(true);
        } else {
          setText(content);
          textDocumentRef.current = documentName;
        }
      })
      .catch(() => {
        if (!cancelled) setTextError(true);
      });
    return () => {
      cancelled = true;
    };
  }, [useInlineText, usePdfTextMode, sessionId, documentName, availability, reloadToken]);

  const highlightRanges = useMemo(
    () => (useInlineText ? findHighlightRanges(text, highlightTexts) : []),
    [useInlineText, text, highlightTexts],
  );

  // In-document plain-text search matches (case-insensitive).
  const docSearchRanges = useMemo<Array<[number, number]>>(() => {
    const q = docSearchQuery.trim().toLowerCase();
    if (!enhancedEvidence || !useInlineText || !text || q.length < 2) return [];
    const lower = text.toLowerCase();
    const out: Array<[number, number]> = [];
    let pos = 0;
    while (pos < lower.length && out.length < 200) {
      const idx = lower.indexOf(q, pos);
      if (idx < 0) break;
      out.push([idx, idx + q.length]);
      pos = idx + q.length;
    }
    return out;
  }, [docSearchQuery, enhancedEvidence, text, useInlineText]);

  useEffect(() => {
    setDocSearchIdx(0);
  }, [docSearchQuery]);

  useEffect(() => {
    const textMatchesCurrentDocument = textDocumentRef.current === documentName;
    if (
      useInlineText && text && textMatchesCurrentDocument &&
      highlightTexts && highlightTexts.length > 0 && highlightRanges.length === 0
    ) {
      console.warn(
        '[DocumentPreview] No highlight match found for excerpt(s):',
        highlightTexts.map((e) => e.text),
      );
    }
  }, [useInlineText, text, documentName, highlightTexts, highlightRanges]);

  const markRefs = useRef<Array<HTMLElement | null>>([]);
  const searchMarkRefs = useRef<Array<HTMLElement | null>>([]);
  const preRef = useRef<HTMLPreElement | null>(null);

  useEffect(() => {
    const idx = Math.min(activeExcerptIdx, Math.max(0, highlightRanges.length - 1));
    const target = markRefs.current[idx];
    if (target) {
      target.scrollIntoView({ block: 'center', behavior: 'smooth' });
    }
  }, [activeExcerptIdx, highlightRanges, text, scrollNonce]);

  useEffect(() => {
    if (docSearchRanges.length === 0) return;
    const idx = Math.min(docSearchIdx, docSearchRanges.length - 1);
    const target = searchMarkRefs.current[idx];
    if (target) {
      target.scrollIntoView({ block: 'center', behavior: 'smooth' });
    }
  }, [docSearchIdx, docSearchRanges]);

  const handleTextMouseUp = () => {
    if (!enhancedEvidence || !onUseSelectionAsValue) return;
    const sel = window.getSelection();
    if (!sel || sel.isCollapsed) {
      setSelectedSnippet('');
      return;
    }
    const anchorInside = preRef.current && sel.anchorNode && preRef.current.contains(sel.anchorNode);
    if (!anchorInside) {
      setSelectedSnippet('');
      return;
    }
    const cleaned = sel.toString().replace(/\s+/g, ' ').trim();
    setSelectedSnippet(cleaned.length >= 1 ? cleaned : '');
  };

  const renderInlineText = () => {
    if (textError) {
      return (
        <div className="h-full flex items-center justify-center text-sm text-muted-foreground text-center px-4">
          Could not load this document&apos;s text.
        </div>
      );
    }
    if (text === null) {
      return (
        <div className="h-full flex items-center justify-center text-sm text-muted-foreground">
          Loading…
        </div>
      );
    }

    // Merge grounding highlights and in-document search highlights cleanly.
    type StyledSpan = { start: number; end: number; kind: 'excerpt' | 'search'; index: number };
    const spans: StyledSpan[] = [];
    highlightRanges.forEach(([start, end], i) => spans.push({ start, end, kind: 'excerpt', index: i }));
    docSearchRanges.forEach(([start, end], i) => spans.push({ start, end, kind: 'search', index: i }));
    spans.sort((a, b) => a.start - b.start || (a.kind === 'excerpt' ? -1 : 1));

    const merged: StyledSpan[] = [];
    let lastEnd = -1;
    for (const span of spans) {
      if (span.start >= lastEnd) {
        merged.push(span);
        lastEnd = span.end;
      }
    }

    markRefs.current = [];
    searchMarkRefs.current = [];

    let body: React.ReactNode = text;
    if (merged.length > 0) {
      const nodes: React.ReactNode[] = [];
      let cursor = 0;
      merged.forEach((span) => {
        if (span.start > cursor) nodes.push(text.slice(cursor, span.start));
        if (span.kind === 'excerpt') {
          const isCurrent = enhancedEvidence && span.index === activeExcerptIdx;
          nodes.push(
            <mark
              key={`mark-${span.start}-${span.end}`}
              ref={(el) => { markRefs.current[span.index] = el; }}
              style={{
                backgroundColor: isCurrent ? '#facc15' : '#fde047',
                color: '#1f2937',
                padding: '0 2px',
                borderRadius: 3,
                outline: isCurrent && highlightRanges.length > 1 ? '2px solid #d97706' : undefined,
              }}
            >
              {text.slice(span.start, span.end)}
            </mark>,
          );
        } else {
          const isCurrentSearch = span.index === docSearchIdx;
          nodes.push(
            <mark
              key={`search-${span.start}-${span.end}`}
              ref={(el) => { searchMarkRefs.current[span.index] = el; }}
              style={{
                backgroundColor: isCurrentSearch ? '#a78bfa' : '#ddd6fe',
                color: '#1f2937',
                padding: '0 1px',
                borderRadius: 2,
              }}
            >
              {text.slice(span.start, span.end)}
            </mark>,
          );
        }
        cursor = span.end;
      });
      if (cursor < text.length) nodes.push(text.slice(cursor));
      body = <>{nodes}</>;
    }

    return (
      <div className="relative h-full w-full flex flex-col">
        {enhancedEvidence && selectedSnippet && onUseSelectionAsValue && evidenceMeta && (
          <div className="flex items-center gap-2 px-3 py-1.5 bg-primary/10 border-b border-primary/25 text-xs shrink-0">
            <Quote className="h-3.5 w-3.5 shrink-0 text-primary" />
            <span className="truncate flex-1 text-foreground">
              &ldquo;{selectedSnippet}&rdquo;
            </span>
            <button
              type="button"
              onClick={() => {
                onUseSelectionAsValue(selectedSnippet);
                setSelectedSnippet('');
                window.getSelection()?.removeAllRanges();
              }}
              className="shrink-0 rounded bg-primary px-2 py-0.5 text-[11px] font-medium text-primary-foreground hover:opacity-90"
            >
              Use as value
            </button>
            <button
              type="button"
              onClick={() => setSelectedSnippet('')}
              className="shrink-0 text-muted-foreground hover:text-foreground"
              aria-label="Clear selected quote"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          </div>
        )}
        <pre
          ref={preRef}
          dir="auto"
          onMouseUp={handleTextMouseUp}
          className="flex-1 min-h-0 w-full overflow-auto m-0 px-4 py-3 text-xs leading-relaxed text-foreground"
          style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', fontFamily: 'inherit' }}
        >
          {body}
        </pre>
      </div>
    );
  };

  /** Renders the document body. Only called when contentUrl is set. */
  const renderDocumentBody = () => {
    if (availability === 'checking' || availability === 'idle') {
      return (
        <div className="h-full flex items-center justify-center text-sm text-muted-foreground">
          Loading…
        </div>
      );
    }
    if (availability === 'unavailable') {
      // Uploading/converting the re-attached files can take a while. Show a
      // spinner instead of the static prompt so the panel doesn't look frozen
      // (the button is hidden while uploading, so without this it appears as if
      // nothing happened and the upload silently failed).
      if (uploading) {
        return (
          <div className="h-full flex flex-col items-center justify-center gap-2 text-sm text-muted-foreground text-center px-6">
            <Loader2 className="h-8 w-8 opacity-60 animate-spin" />
            <span>Uploading and processing documents&hellip;</span>
            <span className="text-xs">This can take a moment for large files.</span>
          </div>
        );
      }
      return (
        <div className="h-full flex flex-col items-center justify-center gap-2 text-sm text-muted-foreground text-center px-6">
          <FileX className="h-8 w-8 opacity-40" />
          <span>This document isn&apos;t available to preview.</span>
          <span className="text-xs">
            Imported projects don&apos;t include the original files. Upload them to enable preview.
          </span>
          {onRequestUpload && (
            <button
              type="button"
              onClick={onRequestUpload}
              className="mt-1 inline-flex items-center gap-1 px-3 py-1.5 rounded-md border border-border hover:bg-muted/50 transition-colors text-foreground"
            >
              <Upload className="h-4 w-4" />
              Upload source documents
            </button>
          )}
        </div>
      );
    }
    if (useInlineText && !contentIsBinary) {
      return renderInlineText();
    }
    if (canRenderInline) {
      return (
        <iframe
          key={contentUrl}
          src={contentUrl as string}
          title={documentName || 'Document preview'}
          className="w-full h-full border-0"
          {...(needsSandbox ? { sandbox: '' } : {})}
        />
      );
    }
    return (
      <div className="h-full flex flex-col items-center justify-center gap-3 text-sm text-muted-foreground">
        <FileText className="h-8 w-8 opacity-40" />
        <span>Preview isn&apos;t available for .{ext} files.</span>
        <a
          href={contentUrl as string}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 px-3 py-1.5 rounded-md border border-border hover:bg-muted/50 transition-colors text-foreground"
        >
          <Download className="h-4 w-4" />
          Open / download
        </a>
      </div>
    );
  };

  // A highlight was requested but this format can't be annotated inline.
  const showHighlightUnsupportedNote =
    highlightEnabled &&
    Boolean(highlightTexts && highlightTexts.length > 0) &&
    !useInlineText &&
    availability === 'ok' &&
    canRenderInline;

  const figureLabel = hasFigures
    ? (figures!.length === 1 ? (figures![0].caption || 'Figure') : `${figures!.length} figures`)
    : null;
  const headerLabel = showDocumentSection
    ? (documentName || 'Document')
    : figureLabel || documentName || 'No document selected';

  const statusMeta: Record<EvidenceCellMeta['status'], { label: string; cls: string }> = {
    grounded: { label: 'Grounded', cls: 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border-emerald-500/30' },
    ungrounded: { label: 'Ungrounded', cls: 'bg-amber-500/15 text-amber-700 dark:text-amber-300 border-amber-500/30' },
    confirmed_empty: { label: 'Not found', cls: 'bg-muted text-muted-foreground border-border' },
    empty: { label: 'Blank', cls: 'bg-muted text-muted-foreground border-border' },
    edited: { label: 'Edited', cls: 'bg-blue-500/15 text-blue-700 dark:text-blue-300 border-blue-500/30' },
  };

  return (
    <div className="flex-1 min-w-0 h-full flex flex-col">
      <div className="flex items-center gap-1.5 px-3 py-2 border-b border-border text-xs">
        <span className="truncate font-medium text-foreground" title={headerLabel}>
          {headerLabel}
        </span>
        <span className="flex-1" />
        {enhancedEvidence && isPdf && highlightEnabled && availability === 'ok' && (
          <div className="inline-flex items-center rounded-md border border-border overflow-hidden text-[11px]">
            <button
              type="button"
              onClick={() => setPdfViewMode('text')}
              className={`px-2 py-0.5 transition-colors ${
                pdfViewMode === 'text' ? 'bg-primary text-primary-foreground font-medium' : 'text-muted-foreground hover:bg-muted'
              }`}
              title="View extracted text with highlighted quotes"
            >
              Text
            </button>
            <button
              type="button"
              onClick={() => setPdfViewMode('pdf')}
              className={`px-2 py-0.5 transition-colors ${
                pdfViewMode === 'pdf' ? 'bg-primary text-primary-foreground font-medium' : 'text-muted-foreground hover:bg-muted'
              }`}
              title="View original PDF"
            >
              PDF
            </button>
          </div>
        )}
        {enhancedEvidence && useInlineText && availability === 'ok' && (
          <button
            type="button"
            onClick={() => setDocSearchOpen((v) => !v)}
            className={`inline-flex items-center justify-center h-6 w-6 rounded-md border border-border transition-colors ${
              docSearchOpen ? 'bg-accent text-foreground' : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
            }`}
            title="Search inside this document"
            aria-label="Search inside document"
          >
            <Search className="h-3.5 w-3.5" />
          </button>
        )}
        {showOpenFull && (
          <a
            href={contentUrl as string}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 px-2 py-1 rounded-md border border-border hover:bg-muted/50 transition-colors text-foreground"
          >
            <ExternalLink className="h-3.5 w-3.5" />
            Open full
          </a>
        )}
      </div>

      {/* Pinned Evidence Inspector card (selected cell + 1/N excerpt stepper + Verify/Retry) */}
      {enhancedEvidence && evidenceMeta && (
        <div className="px-3 py-2 border-b border-border bg-muted/35 text-xs space-y-1.5 shrink-0">
          <div className="flex items-center gap-1.5 min-w-0">
            <span className="font-semibold text-foreground truncate">{evidenceMeta.columnLabel}</span>
            <span className={`shrink-0 rounded-full border px-1.5 py-0.5 text-[10px] font-medium leading-none ${statusMeta[evidenceMeta.status].cls}`}>
              {statusMeta[evidenceMeta.status].label}
            </span>
            {evidenceMeta.isVerified && (
              <span className="shrink-0 inline-flex items-center gap-0.5 rounded-full border border-emerald-500/40 bg-emerald-500/15 px-1.5 py-0.5 text-[10px] font-semibold leading-none text-emerald-700 dark:text-emerald-300">
                ✓ Verified
              </span>
            )}
            <span className="flex-1" />
            {highlightRanges.length > 0 && (
              <div className="inline-flex items-center gap-0.5 rounded-md border border-border bg-background px-1 py-0.5 text-[11px] tabular-nums">
                <span className="px-1 text-muted-foreground">
                  Quote {Math.min(activeExcerptIdx + 1, highlightRanges.length)}/{highlightRanges.length}
                </span>
                {highlightRanges.length > 1 && (
                  <>
                    <button
                      type="button"
                      onClick={() => setActiveExcerptIdx((i) => (i - 1 + highlightRanges.length) % highlightRanges.length)}
                      className="rounded p-0.5 hover:bg-muted"
                      title="Previous excerpt"
                      aria-label="Previous excerpt"
                    >
                      <ChevronLeft className="h-3 w-3" />
                    </button>
                    <button
                      type="button"
                      onClick={() => setActiveExcerptIdx((i) => (i + 1) % highlightRanges.length)}
                      className="rounded p-0.5 hover:bg-muted"
                      title="Next excerpt"
                      aria-label="Next excerpt"
                    >
                      <ChevronRight className="h-3 w-3" />
                    </button>
                  </>
                )}
              </div>
            )}
          </div>

          <div className="flex items-start justify-between gap-2">
            <div dir="auto" className="min-w-0 flex-1 text-foreground/90 line-clamp-2">
              {evidenceMeta.value ? (
                evidenceMeta.value
              ) : (
                <span className="italic text-muted-foreground">
                  {evidenceMeta.status === 'confirmed_empty' ? 'Not found in document' : 'No value'}
                </span>
              )}
            </div>
            <div className="flex items-center gap-1 shrink-0">
              {onToggleVerify && (
                <button
                  type="button"
                  onClick={() => onToggleVerify(evidenceMeta.verifyKey)}
                  className={`inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[11px] font-medium transition-colors ${
                    evidenceMeta.isVerified
                      ? 'border-emerald-500/40 bg-emerald-500/15 text-emerald-700 dark:text-emerald-300'
                      : 'border-border bg-background hover:bg-muted text-foreground'
                  }`}
                  title={evidenceMeta.isVerified ? 'Unmark verified' : 'Mark verified'}
                >
                  <CheckCircle2 className="h-3 w-3" />
                  {evidenceMeta.isVerified ? 'Verified' : 'Verify'}
                </button>
              )}
              {onRetryCell && (
                <button
                  type="button"
                  disabled={retryBusy}
                  onClick={() => setRetryHintOpen((v) => !v)}
                  className="inline-flex items-center gap-1 rounded-md border border-border bg-background px-2 py-0.5 text-[11px] font-medium text-foreground hover:bg-muted disabled:opacity-50"
                  title="Re-extract this cell (with optional hint)"
                >
                  <RotateCw className="h-3 w-3" />
                  Retry
                </button>
              )}
            </div>
          </div>

          {retryHintOpen && onRetryCell && (
            <form
              className="flex items-center gap-1.5 pt-1"
              onSubmit={(e) => {
                e.preventDefault();
                onRetryCell(retryHint.trim() || undefined);
                setRetryHintOpen(false);
                setRetryHint('');
              }}
            >
              <input
                type="text"
                value={retryHint}
                onChange={(e) => setRetryHint(e.target.value)}
                placeholder="Optional hint (e.g. 'Check footnote 3')…"
                className="flex-1 min-w-0 h-7 rounded-md border border-border bg-background px-2 text-xs outline-none focus:ring-1 focus:ring-ring"
                autoFocus
              />
              <button
                type="submit"
                disabled={retryBusy}
                className="shrink-0 h-7 rounded-md bg-primary px-2.5 text-[11px] font-medium text-primary-foreground hover:opacity-90 disabled:opacity-50"
              >
                Re-run
              </button>
            </form>
          )}
        </div>
      )}

      {/* In-document text search bar */}
      {enhancedEvidence && docSearchOpen && useInlineText && (
        <div className="flex items-center gap-1.5 px-3 py-1.5 border-b border-border bg-background text-xs shrink-0">
          <Search className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
          <input
            type="text"
            value={docSearchQuery}
            onChange={(e) => setDocSearchQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Escape') {
                setDocSearchOpen(false);
                setDocSearchQuery('');
              } else if (e.key === 'Enter' && docSearchRanges.length > 0) {
                e.preventDefault();
                const delta = e.shiftKey ? -1 : 1;
                setDocSearchIdx((i) => (i + delta + docSearchRanges.length) % docSearchRanges.length);
              }
            }}
            placeholder="Find in document…"
            className="flex-1 min-w-0 bg-transparent text-xs outline-none"
            autoFocus
          />
          {docSearchQuery.trim().length >= 2 && (
            <span className="text-[11px] text-muted-foreground tabular-nums shrink-0">
              {docSearchRanges.length === 0
                ? '0'
                : `${Math.min(docSearchIdx + 1, docSearchRanges.length)}/${docSearchRanges.length}`}
            </span>
          )}
          <button
            type="button"
            disabled={docSearchRanges.length === 0}
            onClick={() => setDocSearchIdx((i) => (i - 1 + docSearchRanges.length) % docSearchRanges.length)}
            className="rounded p-0.5 hover:bg-muted disabled:opacity-35"
            aria-label="Previous document match"
          >
            <ChevronLeft className="h-3.5 w-3.5" />
          </button>
          <button
            type="button"
            disabled={docSearchRanges.length === 0}
            onClick={() => setDocSearchIdx((i) => (i + 1) % docSearchRanges.length)}
            className="rounded p-0.5 hover:bg-muted disabled:opacity-35"
            aria-label="Next document match"
          >
            <ChevronRight className="h-3.5 w-3.5" />
          </button>
          <button
            type="button"
            onClick={() => {
              setDocSearchOpen(false);
              setDocSearchQuery('');
            }}
            className="rounded p-0.5 hover:bg-muted"
            aria-label="Close document search"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      {showHighlightUnsupportedNote && (
        <div className="px-3 py-1.5 text-[11px] text-muted-foreground border-b border-border bg-muted/20">
          Source highlighting isn&apos;t available in the native .{ext || 'file'} viewer
          {hasFigures ? ' — see the figures below for image citations.' : '.'}
        </div>
      )}

      <div className="flex-1 min-h-0 bg-muted/20 overflow-y-auto flex flex-col">
        {!showDocumentSection && !hasFigures && (
          <div className="h-full flex items-center justify-center text-sm text-muted-foreground text-center px-4">
            {emptyHint || 'Select a document to preview it.'}
          </div>
        )}
        {showDocumentSection && (
          <div className={hasFigures ? 'h-[50vh] shrink-0' : 'flex-1 min-h-0'}>
            {renderDocumentBody()}
          </div>
        )}
        {showDocumentSection && hasFigures && <div className="border-t border-border shrink-0" />}
        {hasFigures && (
          <div className="p-3 flex flex-col gap-4">
            {figures!.map((fig) => (
              <div key={fig.url}>
                <img
                  src={fig.url}
                  alt={fig.caption || 'Figure'}
                  className="max-w-full rounded-md border border-border"
                />
                {fig.caption && (
                  <p dir="auto" className="text-sm leading-relaxed mt-2 text-muted-foreground">
                    {fig.caption}
                  </p>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default DocumentPreview;
