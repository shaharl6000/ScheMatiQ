import React, { useEffect, useMemo, useRef, useState } from 'react';
import { FileText, ExternalLink, Download, FileX, Upload, Loader2 } from 'lucide-react';

import { unitsAPI } from '../../services/api';
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

const extensionOf = (name: string): string => {
  const dot = name.lastIndexOf('.');
  if (dot < 0) return '';
  const ext = name.slice(dot + 1).toLowerCase();
  // Only treat the suffix as a real extension if it looks like one. Document
  // names often contain internal periods (e.g. "... Order No. 19-cv-7151"),
  // where the text after the last dot is not an extension; those resolve to ''
  // so they take the text-renderable path instead of the download fallback.
  return /^[a-z0-9]{1,8}$/.test(ext) && /[a-z]/.test(ext) ? ext : '';
};

type Availability = 'idle' | 'checking' | 'ok' | 'unavailable';

interface DocumentPreviewProps {
  sessionId: string | null | undefined;
  /** Source-document name (raw value, as returned by the documents endpoint). */
  documentName: string | null;
  /** Message shown when no document is selected. */
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
   * All excerpts are marked; the first found match is scrolled into view.
   */
  highlightTexts?: string[] | null;
  /**
   * Changes on every user click of a grounded cell. Re-scrolls to the first
   * highlight even when the excerpt set is unchanged (e.g. the user scrolled the
   * document away and clicked the same cell again).
   */
  scrollNonce?: number;
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
  const showOpenFull = Boolean(contentUrl) && availability === 'ok';

  // Highlight mode is opt-in (the prop is present) and only applies to text
  // formats we can render and annotate ourselves.
  const highlightEnabled = highlightTexts !== undefined;
  const isTextRenderable = ext === '' || TEXT_EXTENSIONS.has(ext);
  const useInlineText = highlightEnabled && isTextRenderable;

  const [text, setText] = useState<string | null>(null);
  const [textError, setTextError] = useState(false);
  // A recorded name may lack a usable extension (internal periods), so the
  // text path can be reached for a file the server actually serves as binary
  // (e.g. a raw PDF whose conversion failed). Detect that and fall back to the
  // native iframe instead of rendering raw bytes as gibberish.
  const [contentIsBinary, setContentIsBinary] = useState(false);

  useEffect(() => {
    if (!useInlineText || !sessionId || !documentName || availability !== 'ok') {
      setText(null);
      setTextError(false);
      setContentIsBinary(false);
      return undefined;
    }
    let cancelled = false;
    setText(null);
    setTextError(false);
    setContentIsBinary(false);
    unitsAPI
      .getDocumentContentText(sessionId, documentName)
      .then((content) => {
        if (cancelled) return;
        if (content.startsWith('%PDF-') || content.indexOf('\u0000') !== -1) {
          setContentIsBinary(true);
        } else {
          setText(content);
        }
      })
      .catch(() => {
        if (!cancelled) setTextError(true);
      });
    return () => {
      cancelled = true;
    };
  }, [useInlineText, sessionId, documentName, availability, reloadToken]);

  const highlightRanges = useMemo(
    () => (useInlineText ? findHighlightRanges(text, highlightTexts) : []),
    [useInlineText, text, highlightTexts],
  );

  const firstMarkRef = useRef<HTMLElement | null>(null);
  useEffect(() => {
    if (firstMarkRef.current) {
      firstMarkRef.current.scrollIntoView({ block: 'center', behavior: 'smooth' });
    }
  }, [highlightRanges, text, scrollNonce]);

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

    let body: React.ReactNode = text;
    if (highlightRanges.length > 0) {
      const nodes: React.ReactNode[] = [];
      let cursor = 0;
      highlightRanges.forEach(([start, end], i) => {
        if (start > cursor) nodes.push(text.slice(cursor, start));
        nodes.push(
          <mark
            key={`mark-${start}-${end}`}
            ref={i === 0 ? firstMarkRef : undefined}
            style={{ backgroundColor: '#fde047', color: '#1f2937', padding: '0 1px', borderRadius: 2 }}
          >
            {text.slice(start, end)}
          </mark>,
        );
        cursor = end;
      });
      if (cursor < text.length) nodes.push(text.slice(cursor));
      body = <>{nodes}</>;
    }

    return (
      <pre
        className="h-full w-full overflow-auto m-0 px-4 py-3 text-xs leading-relaxed text-foreground"
        style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', fontFamily: 'inherit' }}
      >
        {body}
      </pre>
    );
  };

  const renderBody = () => {
    if (!contentUrl) {
      return (
        <div className="h-full flex items-center justify-center text-sm text-muted-foreground text-center px-4">
          {emptyHint || 'Select a document to preview it.'}
        </div>
      );
    }
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
          src={contentUrl}
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
          href={contentUrl}
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
    !isTextRenderable &&
    availability === 'ok' &&
    canRenderInline;

  return (
    <div className="flex-1 min-w-0 h-full flex flex-col">
      <div className="flex items-center gap-2 px-3 py-2 border-b border-border text-xs">
        <span className="truncate text-muted-foreground" title={documentName || ''}>
          {documentName || 'No document selected'}
        </span>
        <span className="flex-1" />
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

      {showHighlightUnsupportedNote && (
        <div className="px-3 py-1.5 text-[11px] text-muted-foreground border-b border-border bg-muted/20">
          Source highlighting isn&apos;t available for .{ext || 'this'} files — see the grounding popup for the excerpt.
        </div>
      )}

      <div className="flex-1 min-h-0 bg-muted/20">{renderBody()}</div>
    </div>
  );
};

export default DocumentPreview;
