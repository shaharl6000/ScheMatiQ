import { useCallback, useMemo, useState } from 'react';

import {
  getAvailableProviders,
  getDefaultModelForProvider,
  type LLMProviderKey,
} from '@/constants';
import { configAPI, loadAPI } from '@/services/api';
import type { DocumentUploadResult, VisualizationSession } from '@/types';
import type { DocumentListResponse } from '@/types/unit';
import { getApiKeyForProvider, getConfiguredProviders } from '@/utils/apiKeyStorage';

import type { SheetId } from '../types';

const normalizeDocName = (s: string) => s.trim().toLowerCase();
const stripDocExt = (s: string) => s.replace(/\.[^.\\/]+$/, '');

type UseAddDocumentsOptions = {
  sessionId?: string;
  documents: DocumentListResponse | null;
  session: VisualizationSession | null;
  refresh: (options?: { silent?: boolean }) => Promise<void>;
  setActiveSheet: (sheet: SheetId) => void;
  toast: (props: {
    title: string;
    description?: string;
    variant?: 'default' | 'destructive';
    duration?: number;
  }) => void;
};

// Owns the "add more documents" upload queue and processing flow on the Documents sheet.
// Parent: Workspace (index.tsx).
export function useAddDocuments({
  sessionId,
  documents,
  session,
  refresh,
  setActiveSheet,
  toast,
}: UseAddDocumentsOptions) {
  const [addDocsFiles, setAddDocsFiles] = useState<File[]>([]);
  const [addDocsUploading, setAddDocsUploading] = useState(false);
  const [addDocsProcessing, setAddDocsProcessing] = useState(false);
  const [addDocsResult, setAddDocsResult] = useState<DocumentUploadResult | null>(null);
  const [addDocsError, setAddDocsError] = useState<string | null>(null);
  const [addDocsNotice, setAddDocsNotice] = useState<string | null>(null);

  // Everything already in this project. Two sources, unioned:
  //  - documents.documents: documents that already have extracted rows. This is
  //    authoritative for a loaded/saved project, where the source files may no
  //    longer be "available" for preview but the document is still in the table.
  //  - session.metadata.uploaded_documents: documents uploaded this session that
  //    may not have been processed into rows yet.
  const existingDocs = useMemo(() => {
    const map = new Map<string, { name: string; label: string; status?: string }>();
    for (const d of documents?.documents ?? []) {
      const key = normalizeDocName(d.name);
      if (!map.has(key)) {
        map.set(key, {
          name: d.name,
          label: d.name,
          status: d.rowCount ? `${d.rowCount} row${d.rowCount === 1 ? '' : 's'}` : undefined,
        });
      }
    }
    const meta = session?.metadata?.document_metadata;
    for (const doc of session?.metadata?.uploaded_documents ?? []) {
      const label = meta?.[doc]?.original_filename || doc;
      const key = normalizeDocName(label);
      const status = meta?.[doc]?.extraction_status;
      const existing = map.get(key);
      if (existing) {
        if (status && !existing.status) existing.status = status;
      } else {
        map.set(key, { name: doc, label, status });
      }
    }
    return Array.from(map.values());
  }, [documents, session]);

  const existingDocNames = useMemo(() => {
    const names = new Set<string>();
    for (const d of existingDocs) {
      names.add(normalizeDocName(d.label));
      names.add(normalizeDocName(d.name));
      names.add(normalizeDocName(stripDocExt(d.label)));
      names.add(normalizeDocName(stripDocExt(d.name)));
    }
    return names;
  }, [existingDocs]);

  // Step 1: upload extra documents into the session's pending queue.
  const uploadAddDocuments = useCallback(async () => {
    if (!sessionId || addDocsFiles.length === 0 || addDocsUploading) return;
    setAddDocsUploading(true);
    setAddDocsError(null);
    setAddDocsNotice(null);
    try {
      const result = await loadAPI.addDocuments(sessionId, addDocsFiles);
      setAddDocsResult(result);
      setAddDocsFiles([]);
      await refresh({ silent: true });
    } catch (err: any) {
      const detail = err?.response?.data?.detail;
      const message =
        detail && typeof detail === 'object' && Array.isArray(detail.errors)
          ? detail.errors.join('\n')
          : typeof detail === 'string'
            ? detail
            : err?.message || 'Failed to upload documents';
      setAddDocsError(message);
    } finally {
      setAddDocsUploading(false);
    }
  }, [addDocsFiles, addDocsUploading, refresh, sessionId]);

  // Step 2: extract the queued documents with the existing schema. The LLM
  // config is resolved exactly like re-extraction; the backend additionally
  // falls back to the session's stored value_extraction_backend when none is
  // passed. New rows stream into the table via the existing WebSocket handler.
  const processAddDocuments = useCallback(async () => {
    if (!sessionId || addDocsProcessing) return;
    setAddDocsProcessing(true);
    setAddDocsError(null);
    try {
      const cfg = await configAPI.getConfig().catch(() => ({ allow_llm_config: true }));
      const configured = await getConfiguredProviders();
      const available = getAvailableProviders(configured);
      const provider: LLMProviderKey = !cfg.allow_llm_config
        ? 'gemini'
        : (available[0] ?? 'gemini');
      const model = getDefaultModelForProvider(provider);
      const apiKey = await getApiKeyForProvider(provider);
      const llmConfig: Record<string, unknown> = { provider, model, temperature: 0 };
      if (apiKey) llmConfig.api_key = apiKey;

      await loadAPI.processDocuments(sessionId, llmConfig);
      setAddDocsResult(null);
      setActiveSheet('data');
      toast({
        title: 'Processing new documents',
        description: 'New rows will appear in the table as they are extracted.',
        duration: 4000,
      });
      await refresh({ silent: true });
    } catch (err: any) {
      const detail = err?.response?.data?.detail;
      const message = err?.response?.status === 503
        ? (detail || 'The server is busy. Please try again in a few minutes.')
        : (typeof detail === 'string' ? detail : err?.message || 'Failed to start processing');
      setAddDocsError(message);
    } finally {
      setAddDocsProcessing(false);
    }
  }, [addDocsProcessing, refresh, sessionId, setActiveSheet, toast]);

  // Reject files whose name already exists in the project so an already-extracted
  // source is never uploaded and re-processed. Matches on the full name and on the
  // extension-stripped base name (a loaded project may store the document without
  // its original extension). Also de-dupes repeated names within a selection.
  // Client-side guard; the backend remains the source of truth.
  const handleAddDocsFilesChange = useCallback((incoming: File[]) => {
    const seen = new Set<string>();
    const accepted: File[] = [];
    const skipped: string[] = [];
    for (const file of incoming) {
      const key = normalizeDocName(file.name);
      const baseKey = normalizeDocName(stripDocExt(file.name));
      if (existingDocNames.has(key) || existingDocNames.has(baseKey)) {
        skipped.push(file.name);
        continue;
      }
      if (seen.has(key)) continue;
      seen.add(key);
      accepted.push(file);
    }
    setAddDocsFiles(accepted);
    setAddDocsNotice(
      skipped.length > 0
        ? `Skipped ${skipped.length} document${skipped.length !== 1 ? 's' : ''} already in this project (already extracted, not re-processed): ${Array.from(new Set(skipped)).join(', ')}`
        : null,
    );
  }, [existingDocNames]);

  const addDocsPending = (addDocsResult?.uploaded_files?.length ?? 0) > 0;

  return {
    addDocsFiles,
    addDocsUploading,
    addDocsProcessing,
    addDocsResult,
    addDocsError,
    addDocsNotice,
    addDocsPending,
    handleAddDocsFilesChange,
    uploadAddDocuments,
    processAddDocuments,
  };
}
