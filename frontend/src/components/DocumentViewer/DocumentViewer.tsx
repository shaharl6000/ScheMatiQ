import React, { useCallback, useEffect, useRef, useState } from 'react';
import { FileText, Upload, Loader2 } from 'lucide-react';

import { unitsAPI } from '../../services/api';
import { DocumentSummary } from '../../types/unit';
import DocumentPreview from './DocumentPreview';

/** Extensions accepted by the source-document re-attach picker (mirrors the backend allow-list). */
const ACCEPTED_UPLOAD_EXTENSIONS = '.txt,.md,.pdf,.doc,.docx,.rtf,.json';

interface DocumentViewerProps {
  sessionId: string | null | undefined;
  /** Changes when the underlying data updates, so the list refetches as documents arrive. */
  refreshKey?: number;
}

const extractErrorMessage = (err: any): string => {
  const detail = err?.response?.data?.detail;
  if (typeof detail === 'string') return detail;
  if (detail && Array.isArray(detail.errors) && detail.errors.length > 0) {
    return detail.errors.join(' ');
  }
  return 'Could not upload documents. Please try again.';
};

/**
 * Browse a session's uploaded source documents: a list on the left and an
 * inline preview on the right (see DocumentPreview). "Open full" opens the
 * document in a new browser tab.
 *
 * Imported projects carry the schema and data but not the original files, so the
 * preview reports documents as unavailable. The header (and the preview's empty
 * state) offer an upload action that re-attaches the original source files via
 * the existing add-documents endpoint, after which previews resolve.
 */
const DocumentViewer: React.FC<DocumentViewerProps> = ({ sessionId, refreshKey }) => {
  const [documents, setDocuments] = useState<DocumentSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  // Bumped after a successful upload to force the list to refetch and the
  // preview to re-probe / reload the now-available file.
  const [reloadToken, setReloadToken] = useState(0);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

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
  }, [sessionId, refreshKey, reloadToken]);

  const openFilePicker = useCallback(() => {
    setUploadError(null);
    fileInputRef.current?.click();
  }, []);

  const handleFilesSelected = useCallback(
    async (e: React.ChangeEvent<HTMLInputElement>) => {
      const files = Array.from(e.target.files || []);
      // Reset so selecting the same files again re-triggers onChange.
      e.target.value = '';
      if (!sessionId || files.length === 0) return;

      setUploading(true);
      setUploadError(null);
      try {
        await unitsAPI.attachSourceDocuments(sessionId, files);
        // Refetch the list and re-probe the preview against the newly stored files.
        setReloadToken((t) => t + 1);
      } catch (err: any) {
        setUploadError(extractErrorMessage(err));
      } finally {
        setUploading(false);
      }
    },
    [sessionId],
  );

  if (!sessionId) {
    return (
      <div className="h-full flex items-center justify-center text-sm text-muted-foreground">
        Open a project to view its documents.
      </div>
    );
  }

  return (
    <div className="h-full flex min-h-0">
      {/* Hidden picker shared by the header button and the preview's empty state. */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        accept={ACCEPTED_UPLOAD_EXTENSIONS}
        onChange={handleFilesSelected}
        className="hidden"
      />

      {/* Document list */}
      <div className="w-56 flex-shrink-0 border-r border-border overflow-y-auto">
        <div className="flex items-center gap-2 px-3 py-2 text-xs text-muted-foreground border-b border-border">
          <span className="truncate">
            {loading ? 'Loading\u2026' : `${documents.length} document${documents.length === 1 ? '' : 's'}`}
          </span>
          <span className="flex-1" />
          <button
            type="button"
            onClick={openFilePicker}
            disabled={uploading}
            title="Upload the original source documents for this project"
            className="inline-flex items-center gap-1 px-1.5 py-1 rounded-md border border-border hover:bg-muted/50 transition-colors text-foreground disabled:opacity-50"
          >
            {uploading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Upload className="h-3 w-3" />}
            <span>Upload</span>
          </button>
        </div>
        {error && <div className="px-3 py-2 text-xs text-destructive">{error}</div>}
        {uploadError && <div className="px-3 py-2 text-xs text-destructive">{uploadError}</div>}
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
      <DocumentPreview
        sessionId={sessionId}
        documentName={selected}
        reloadToken={reloadToken}
        onRequestUpload={openFilePicker}
      />
    </div>
  );
};

export default DocumentViewer;
