import React, { useEffect, useMemo, useState } from 'react';
import { FileText, ExternalLink, Download, FileX, Upload } from 'lucide-react';

import { unitsAPI } from '../../services/api';

/** Extensions the browser can render inline in an <iframe>. */
const INLINE_EXTENSIONS = new Set([
  'pdf', 'html', 'htm', 'txt', 'md', 'csv', 'json',
  'png', 'jpg', 'jpeg', 'gif', 'webp', 'svg',
]);

/** Inline content that can carry executable markup, so the iframe is sandboxed. */
const SANDBOX_EXTENSIONS = new Set(['html', 'htm', 'svg']);

const extensionOf = (name: string): string => {
  const dot = name.lastIndexOf('.');
  return dot >= 0 ? name.slice(dot + 1).toLowerCase() : '';
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
 */
const DocumentPreview: React.FC<DocumentPreviewProps> = ({
  sessionId,
  documentName,
  emptyHint,
  reloadToken,
  onRequestUpload,
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

      <div className="flex-1 min-h-0 bg-muted/20">{renderBody()}</div>
    </div>
  );
};

export default DocumentPreview;
