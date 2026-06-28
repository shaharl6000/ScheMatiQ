import React, { useMemo } from 'react';
import { FileText, ExternalLink, Download } from 'lucide-react';

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

interface DocumentPreviewProps {
  sessionId: string | null | undefined;
  /** Source-document name (raw value, as returned by the documents endpoint). */
  documentName: string | null;
  /** Message shown when no document is selected. */
  emptyHint?: string;
}

/**
 * Renders a single source document inline (native browser rendering for
 * PDF/HTML/images/text) with an "Open full" link to a new tab, and a download
 * fallback for formats the browser cannot render. The browser uses the HTTP
 * Content-Type to decide how to render, so a name without an extension is still
 * attempted inline.
 */
const DocumentPreview: React.FC<DocumentPreviewProps> = ({ sessionId, documentName, emptyHint }) => {
  const contentUrl = useMemo(
    () => (sessionId && documentName ? unitsAPI.getDocumentContentUrl(sessionId, documentName) : null),
    [sessionId, documentName],
  );

  const ext = documentName ? extensionOf(documentName) : '';
  const canRenderInline = ext === '' || INLINE_EXTENSIONS.has(ext);
  const needsSandbox = SANDBOX_EXTENSIONS.has(ext);

  return (
    <div className="flex-1 min-w-0 h-full flex flex-col">
      <div className="flex items-center gap-2 px-3 py-2 border-b border-border text-xs">
        <span className="truncate text-muted-foreground" title={documentName || ''}>
          {documentName || 'No document selected'}
        </span>
        <span className="flex-1" />
        {contentUrl && (
          <a
            href={contentUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 px-2 py-1 rounded-md border border-border hover:bg-muted/50 transition-colors text-foreground"
          >
            <ExternalLink className="h-3.5 w-3.5" />
            Open full
          </a>
        )}
      </div>

      <div className="flex-1 min-h-0 bg-muted/20">
        {!contentUrl ? (
          <div className="h-full flex items-center justify-center text-sm text-muted-foreground text-center px-4">
            {emptyHint || 'Select a document to preview it.'}
          </div>
        ) : canRenderInline ? (
          <iframe
            key={contentUrl}
            src={contentUrl}
            title={documentName || 'Document preview'}
            className="w-full h-full border-0"
            {...(needsSandbox ? { sandbox: '' } : {})}
          />
        ) : (
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
        )}
      </div>
    </div>
  );
};

export default DocumentPreview;
