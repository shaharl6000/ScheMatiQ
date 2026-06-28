import React, { useEffect, useState } from 'react';
import { FileText } from 'lucide-react';

import { unitsAPI } from '../../services/api';
import { DocumentSummary } from '../../types/unit';
import DocumentPreview from './DocumentPreview';

interface DocumentViewerProps {
  sessionId: string | null | undefined;
  /** Changes when the underlying data updates, so the list refetches as documents arrive. */
  refreshKey?: number;
}

/**
 * Browse a session's uploaded source documents: a list on the left and an
 * inline preview on the right (see DocumentPreview). "Open full" opens the
 * document in a new browser tab.
 */
const DocumentViewer: React.FC<DocumentViewerProps> = ({ sessionId, refreshKey }) => {
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
  }, [sessionId, refreshKey]);

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
      <DocumentPreview sessionId={sessionId} documentName={selected} />
    </div>
  );
};

export default DocumentViewer;
