import { useCallback, useEffect, useState } from 'react';

import {
  getAvailableProviders,
  getDefaultModelForProvider,
  type LLMProviderKey,
} from '@/constants';
import { configAPI, schemaAPI, schematiqAPI } from '@/services/api';
import type {
  DocumentAvailabilityResponse,
  ReextractionRequest,
  SchemaData,
} from '@/types';
import { getApiKeyForProvider, getConfiguredProviders } from '@/utils/apiKeyStorage';

import type { PendingRerunKind, SheetId, WorkspaceReextractionState, WorkspaceSessionMode } from '../types';

type UseReextractionOptions = {
  sessionId?: string;
  sessionMode: WorkspaceSessionMode;
  schema: SchemaData | null;
  refresh: (options?: { silent?: boolean }) => Promise<void>;
  setActiveSheet: (sheet: SheetId) => void;
  cancelChatPendingIfAny: () => Promise<boolean>;
  toast: (props: {
    title: string;
    description?: string;
    variant?: 'default' | 'destructive';
    duration?: number;
  }) => void;
};

// Owns re-extraction progress, pending-rerun banners, and confirm-dialog state.
// Parent: Workspace (index.tsx). Exposes setReextraction for WebSocket updates.
export function useReextraction({
  sessionId,
  sessionMode,
  schema,
  refresh,
  setActiveSheet,
  cancelChatPendingIfAny,
  toast,
}: UseReextractionOptions) {
  const [pendingRerunKind, setPendingRerunKind] = useState<PendingRerunKind | null>(null);
  const [pendingSchemaColumns, setPendingSchemaColumns] = useState<string[]>([]);
  const [rerunStarting, setRerunStarting] = useState(false);
  const [reextraction, setReextraction] = useState<WorkspaceReextractionState | null>(null);
  const [reextractConfirm, setReextractConfirm] = useState<{ columns: string[] } | null>(null);
  const [reextractAvailability, setReextractAvailability] = useState<DocumentAvailabilityResponse | null>(null);
  const [reextractAvailabilityLoading, setReextractAvailabilityLoading] = useState(false);
  const [stoppingReextraction, setStoppingReextraction] = useState(false);

  useEffect(() => {
    setPendingRerunKind(null);
    setPendingSchemaColumns([]);
    setRerunStarting(false);
    setReextraction(null);
  }, [sessionId]);

  const clearPendingRerun = useCallback(() => {
    setPendingRerunKind(null);
    setPendingSchemaColumns([]);
  }, []);

  const markRerunNeeded = useCallback((kind: PendingRerunKind, columns: string[] = []) => {
    if (kind === 'unit') {
      setPendingRerunKind('unit');
      setPendingSchemaColumns([]);
      return;
    }

    setPendingRerunKind((current) => current === 'unit' ? 'unit' : 'schema');
    setPendingSchemaColumns((current) => {
      const merged = new Set(current);
      columns
        .map((column) => column.trim())
        .filter(Boolean)
        .forEach((column) => merged.add(column));
      return Array.from(merged);
    });
  }, []);

  // Pre-check source-document availability for the current session. Used by the
  // re-extract confirm card so the user can upload missing originals (e.g. after
  // loading a project from JSON, where documents are not bundled) before running
  // the extraction model. Failures leave availability null and fall back to the
  // backend gate in start_gated_reextraction, which still blocks and reports.
  const runReextractPrecheck = useCallback(async () => {
    if (!sessionId) return;
    setReextractAvailabilityLoading(true);
    try {
      const availability = await schemaAPI.precheckDocuments(sessionId, {
        operation_type: 'reextraction',
      });
      setReextractAvailability(availability);
    } catch {
      setReextractAvailability(null);
    } finally {
      setReextractAvailabilityLoading(false);
    }
  }, [sessionId]);

  // Resolve the requested columns to a concrete, non-empty target set and open
  // the confirm card. Both routes (manual button + chat tool) re-extract only
  // after an explicit confirm; the backend gated action owns baseline capture
  // and document precheck so this stays purely presentational.
  const requestReextraction = useCallback((columns?: string[]) => {
    if (!sessionId || rerunStarting) return;

    const schemaColumnNames = new Set(
      (schema?.schema || [])
        .map((column) => column.name)
        .filter((name): name is string => Boolean(name)),
    );
    const requestedColumns = (columns && columns.length > 0 ? columns : pendingSchemaColumns)
      .map((name) => name.trim())
      .filter((name) => Boolean(name) && !name.toLowerCase().endsWith('_excerpt'));
    const targetColumns = (requestedColumns.length > 0 ? requestedColumns : Array.from(schemaColumnNames))
      .filter((name) => schemaColumnNames.has(name));

    if (targetColumns.length === 0) {
      toast({
        title: 'No columns to re-extract',
        description: 'Add schema columns first, then try again.',
        variant: 'destructive',
      });
      return;
    }

    setReextractConfirm({ columns: targetColumns });
    setReextractAvailability(null);
    void runReextractPrecheck();
  }, [pendingSchemaColumns, rerunStarting, runReextractPrecheck, schema?.schema, sessionId, toast]);

  const startReextraction = useCallback(async (targetColumns: string[]) => {
    if (!sessionId || rerunStarting || targetColumns.length === 0) return;

    const chatPendingCleared = await cancelChatPendingIfAny();
    if (!chatPendingCleared) {
      toast({
        title: 'Chat confirmation still pending',
        description: 'Cancel the chat confirmation card first, then try again.',
        variant: 'destructive',
      });
      return;
    }

    setRerunStarting(true);
    try {
      const cfg = await configAPI.getConfig().catch(() => ({ allow_llm_config: true }));
      const configured = await getConfiguredProviders();
      const available = getAvailableProviders(configured);
      const provider: LLMProviderKey = !cfg.allow_llm_config
        ? 'gemini'
        : (available[0] ?? 'gemini');
      const model = getDefaultModelForProvider(provider);
      const apiKey = await getApiKeyForProvider(provider);
      const request: ReextractionRequest = { columns: targetColumns };
      if (apiKey) {
        request.llm_config = { provider, model, api_key: apiKey, temperature: 0 };
      }

      const response = await schemaAPI.startReextraction(sessionId, request);
      const docCount = response.rows_to_process || response.estimated_papers || 0;
      if (docCount === 0) {
        toast({
          title: 'No source documents',
          description: 'Upload documents or open a ScheMatiQ project with source files, then try again.',
          variant: 'destructive',
        });
        return;
      }

      clearPendingRerun();
      setActiveSheet('data');
      setReextraction({
        operationId: response.operation_id,
        columns: response.columns,
        progress: 0,
        processedDocuments: 0,
        totalDocuments: docCount,
        currentColumn: response.columns[0],
      });
      toast({
        title: 'Re-extraction started',
        description: `Re-extracting ${response.columns.join(', ')} across ${docCount} document(s). Other columns stay unchanged.`,
        duration: 4000,
      });
    } catch (err: any) {
      toast({
        title: 'Re-extraction failed to start',
        description: err?.response?.data?.detail || err?.message || 'Could not start re-extraction',
        variant: 'destructive',
      });
    } finally {
      setRerunStarting(false);
    }
  }, [cancelChatPendingIfAny, clearPendingRerun, rerunStarting, sessionId, toast]);

  const confirmReextraction = useCallback(async () => {
    if (!reextractConfirm) return;
    // Belt-and-suspenders: the Confirm button is disabled when no documents are
    // available, but guard here too so a stale click can't bypass the gate.
    if (reextractAvailability && !reextractAvailability.can_proceed) return;
    const { columns } = reextractConfirm;
    setReextractConfirm(null);
    setReextractAvailability(null);
    await startReextraction(columns);
  }, [reextractAvailability, reextractConfirm, startReextraction]);

  // Cancel a running re-extraction. Reuses the same backend stop mechanism as
  // the classic Visualizer (POST /schema/stop-reextraction); the WebSocket
  // 'reextraction_stopped' handler clears the spinner.
  const stopReextraction = useCallback(async () => {
    if (!sessionId || !reextraction?.operationId || stoppingReextraction) return;
    setStoppingReextraction(true);
    try {
      await schemaAPI.stopReextraction(sessionId, reextraction.operationId);
      toast({
        title: 'Stopping re-extraction',
        description: 'Finishing the current document, then stopping. Partial results are kept.',
      });
    } catch (err: any) {
      toast({
        title: 'Could not stop re-extraction',
        description: err?.response?.data?.detail || err?.message || 'Stop request failed.',
        variant: 'destructive',
      });
      setStoppingReextraction(false);
    }
  }, [reextraction?.operationId, sessionId, stoppingReextraction, toast]);

  useEffect(() => {
    if (!reextraction) setStoppingReextraction(false);
  }, [reextraction]);

  const startSchemaRediscovery = useCallback(async () => {
    if (!sessionId || rerunStarting) return;

    if (sessionMode !== 'schematiq') {
      toast({
        title: 'Rediscovery needs a ScheMatiQ run',
        description: 'Imported static projects can edit the observation unit, but rediscovering schema requires a ScheMatiQ project with source documents.',
        variant: 'destructive',
      });
      return;
    }

    setRerunStarting(true);
    try {
      const chatPendingCleared = await cancelChatPendingIfAny();
      if (!chatPendingCleared) {
        toast({
          title: 'Chat confirmation still pending',
          description: 'Cancel the chat confirmation card first, then try again.',
          variant: 'destructive',
        });
        return;
      }
      await schematiqAPI.resume(sessionId);
      clearPendingRerun();
      toast({
        title: 'Schema rediscovery started',
        description: 'Rediscovering schema from the updated observation unit.',
        duration: 4000,
      });
      await refresh({ silent: true });
    } catch (err: any) {
      toast({
        title: 'Schema rediscovery failed',
        description: err?.response?.data?.detail || err?.message || 'Could not start schema rediscovery',
        variant: 'destructive',
      });
    } finally {
      setRerunStarting(false);
    }
  }, [cancelChatPendingIfAny, clearPendingRerun, refresh, rerunStarting, sessionId, sessionMode, toast]);

  const notifyEditFollowUp = useCallback((kind: PendingRerunKind, columns: string[] = []) => {
    markRerunNeeded(kind, columns);

    if (kind === 'unit') {
      // The persistent top banner (driven by markRerunNeeded above) already
      // surfaces the "Rediscover schema & re-extract" action, so we do not fire a
      // competing toast. Exception: in an imported (non-ScheMatiQ) project
      // rediscovery is impossible and the banner's action only errors, so we
      // surface a proactive, action-less explanation instead.
      if (sessionMode !== 'schematiq') {
        toast({
          title: 'Observation unit updated',
          description: 'Imported static projects can edit the unit, but rediscovery needs a ScheMatiQ project with source documents.',
        });
      }
      return;
    }

    // Schema edits: the persistent "Schema changed" banner is the single,
    // always-fresh entry point for re-extraction, so we raise no duplicate toast
    // here. The old toast was also broken — its action captured a stale
    // `requestReextraction` closure (schema not yet refreshed when the toast was
    // created), producing a false "No columns to re-extract" error on click.
  }, [markRerunNeeded, sessionMode, toast]);

  const runPendingEdits = useCallback(async () => {
    if (!sessionId || !pendingRerunKind || rerunStarting) return;
    if (pendingRerunKind === 'unit') {
      await startSchemaRediscovery();
      return;
    }
    requestReextraction(pendingSchemaColumns);
  }, [pendingRerunKind, pendingSchemaColumns, rerunStarting, requestReextraction, sessionId, startSchemaRediscovery]);

  return {
    pendingRerunKind,
    pendingSchemaColumns,
    rerunStarting,
    reextraction,
    setReextraction,
    reextractConfirm,
    setReextractConfirm,
    reextractAvailability,
    setReextractAvailability,
    reextractAvailabilityLoading,
    stoppingReextraction,
    clearPendingRerun,
    runReextractPrecheck,
    requestReextraction,
    confirmReextraction,
    stopReextraction,
    startSchemaRediscovery,
    notifyEditFollowUp,
    runPendingEdits,
  };
}
