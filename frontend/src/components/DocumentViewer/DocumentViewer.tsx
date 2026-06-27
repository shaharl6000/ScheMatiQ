import React, { useEffect, useMemo, useState } from 'react';
import { FileText, ExternalLink, Download } from 'lucide-react';

import { unitsAPI } from '../../services/api';
import { DocumentSummary } from '../../types/unit';

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

interface DocumentViewerProps {
  sessionId: string | null;
}

/**
 * Browse a session's uploaded source documents: a list on the left, an inline
 * preview on the right (native browser rendering for PDF/HTML/images/text), and
 * an "Open full" link that opens the document in a new browser tab.
 */
const DocumentViewer: React.FC<DocumentViewerProps> = ({ sessionId }) => {
  const [documents, setDocuments] = useState<DocumentSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  useEffect(() => {
    if (!sessionId) {
      setDocuments([]);
      setSelected(null);
      return undefined;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    unitsAPI
      .getDocuments(sessionId)
      .then((res) => {
        if (cancelled) return;
        setDocuments(res.documents);
        setSelected((prev) => prev ?? res.documents[0]?.name ?? null);
      })
      .catch(() => {
        if (!cancelled) setError('Could not load the document list.');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [sessionId]);

  const contentUrl = useMemo(
    () => (sessionId && selected ? unitsAPI.getDocumentContentUrl(sessionId, selected) : null),
    [sessionId, selected],
  );

  const selectedExt = selected ? extensionOf(selected) : '';
  const canRenderInline = INLINE_EXTENSIONS.has(selectedExt);
  const needsSandbox = SANDBOX_EXTENSIONS.has(selectedExt);

  if (!sessionId) {
    return (
      <div className="h-full flex items-center justify-center text-sm text-muted-foreground">
        Open a project to view its documents.
      </div>
    );
  }

  return (
    <div className="h-full flex min-h-0">
      {/* Document list */}
      <div className="w-56 flex-shrink-0 border-r border-border overflow-y-auto">
        <div className="px-3 py-2 text-xs text-muted-foreground border-b border-border">
          {loading ? 'Loading\u2026' : `${documents.length} document${documents.length === 1 ? '' : 's'}`}
        </div>
        {error && <div className="px-3 py-2 text-xs text-destructive">{error}</div>}
        {documents.map((doc) => {
          const active = doc.name === selected;
          return (
            <button
              key={doc.name}
              type="button"
              onClick={() => setSelected(doc.name)}
              className={`w-full text-left flex items-center gap-2 px-3 py-2 border-l-2 transition-colors ${
                active ? 'border-primary bg-primary/10' : 'border-transparent hover:bg-muted/50'
              }`}
            >
              <FileText
                className={`h-4 w-4 flex-shrink-0 ${active ? 'text-primary' : 'text-muted-foreground'}`}
              />
              <span className="min-w-0">
                <span className="block text-xs font-medium truncate" title={doc.name}>
                  {doc.name}
                </span>
                <span className="block text-[11px] text-muted-foreground">
                  {doc.rowCount} row{doc.rowCount === 1 ? '' : 's'}
                </span>
              </span>
            </button>
          );
        })}
        {!loading && documents.length === 0 && !error && (
          <div className="px-3 py-6 text-xs text-muted-foreground text-center">
            No documents in this project yet.
          </div>
        )}
      </div>

      {/* Preview pane */}
      <div className="flex-1 min-w-0 flex flex-col">
        <div className="flex items-center gap-2 px-3 py-2 border-b border-border text-xs">
          <span className="truncate text-muted-foreground" title={selected || ''}>
            {selected || 'No document selected'}
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
            <div className="h-full flex items-center justify-center text-sm text-muted-foreground">
              Select a document to preview it.
            </div>
          ) : canRenderInline ? (
            <iframe
              key={contentUrl}
              src={contentUrl}
              title={selected || 'Document preview'}
              className="w-full h-full border-0"
              {...(needsSandbox ? { sandbox: '' } : {})}
            />
          ) : (
            <div className="h-full flex flex-col items-center justify-center gap-3 text-sm text-muted-foreground">
              <FileText className="h-8 w-8 opacity-40" />
              <span>Preview isn&apos;t available for .{selectedExt} files.</span>
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
    </div>
  );
};

export default DocumentViewer;
