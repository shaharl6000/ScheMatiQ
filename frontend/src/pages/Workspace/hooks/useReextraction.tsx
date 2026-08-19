import { useCallback, useEffect, useState } from 'react';

import {
  getAvailableProviders,
  getDefaultModelForProvider,
  type LLMProviderKey,
} from '@/constants';
import { configAPI, loadAPI, schemaAPI, schematiqAPI } from '@/services/api';
import { ToastAction, type ToastActionElement } from '@/components/ui/toast';
import type {
  DocumentAvailabilityResponse,
  ReextractionRequest,
  SchemaData,
} from '@/types';
import { getApiKeyForProvider, getConfiguredProviders } from '@/utils/apiKeyStorage';

import { describeRequestError } from '../helpers';
import type { PendingRerunKind, SheetId, WorkspaceReextractionState, WorkspaceSessionMode } from '../types';

type UseReextractionOptions = {
  sessionId?: string;
  sessionMode: WorkspaceSessionMode;
  // Whether the session has any source documents attached (uploaded at import
  // time, or added afterward via add-documents). sessionMode alone only
  // reflects the '?mode=load' URL param at page-load time, so an imported
  // project that later gets documents attached would otherwise stay blocked.
  hasSourceDocuments: boolean;
  schema: SchemaData | null;
  refresh: (options?: { silent?: boolean }) => Promise<void>;
  setActiveSheet: (sheet: SheetId) => void;
  cancelChatPendingIfAny: () => Promise<boolean>;
  // Called the moment a schema rediscovery is confirmed, before the (possibly
  // slow) resume request returns. Lets the parent clear the stale schema columns
  // so the Schema tab reflects the reset immediately instead of showing the
  // previous run's columns for the duration of the request.
  onRediscoveryStart?: () => void;
  toast: (props: {
    title: string;
    description?: string;
    variant?: 'default' | 'destructive';
    duration?: number;
    action?: ToastActionElement;
  }) => void;
};

// Owns re-extraction progress, pending-rerun banners, and confirm-dialog state.
// Parent: Workspace (index.tsx). Exposes setReextraction for WebSocket updates.
export function useReextraction({
  sessionId,
  sessionMode,
  hasSourceDocuments,
  schema,
  refresh,
  setActiveSheet,
  cancelChatPendingIfAny,
  onRediscoveryStart,
  toast,
}: UseReextractionOptions) {
  // Rediscovery needs source documents to re-run against. A freshly created
  // ScheMatiQ session always has them; an imported/dual-file session only has
  // them if documents were attached afterward via add-documents.
  const canRediscoverSchema = sessionMode === 'schematiq' || hasSourceDocuments;
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

  const startReextraction = useCallback(async (
    targetColumns: string[],
    // `rows` narrows to specific observation-unit rows (e.g. "Fill empty
    // cells" on a grid selection); `onlyEmpty` never overwrites a cell that
    // already holds a value; `retryConfirmedEmpty` (only meaningful with
    // onlyEmpty) also targets cells the model already checked and explicitly
    // confirmed empty, normally skipped to avoid re-billing -- set on the
    // "Check again" retry offered after a first onlyEmpty call comes back
    // with nothing to fill for that specific reason. All three map straight
    // onto the backend's ReextractionRequest.rows / .only_empty /
    // .retry_confirmed_empty.
    scope?: { rows?: string[]; onlyEmpty?: boolean; retryConfirmedEmpty?: boolean },
  ) => {
    if (!sessionId || rerunStarting || targetColumns.length === 0) return;
    // A re-extraction is already running for this session (rerunStarting only
    // covers the brief window until the start request itself returns -- the
    // background operation can run for minutes after that). Pressing the same
    // button again in that window would just 409 from the backend's
    // single-slot-per-session concurrency guard; give a quiet heads-up instead
    // of surfacing that as a failure.
    if (reextraction) {
      toast({
        title: 'Already filling cells',
        description: 'Please wait a few seconds for the current run to finish, then try again.',
        duration: 3000,
      });
      return;
    }

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
      if (scope?.rows?.length) request.rows = scope.rows;
      if (scope?.onlyEmpty) request.only_empty = true;
      if (scope?.retryConfirmedEmpty) request.retry_confirmed_empty = true;

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
        title: scope?.onlyEmpty ? 'Filling empty cells' : 'Re-extraction started',
        description: scope?.onlyEmpty
          ? `Filling ${response.columns.join(', ')} for ${scope.rows?.length ?? 0} row(s). Existing values stay unchanged.`
          : `Re-extracting ${response.columns.join(', ')} across ${docCount} document(s). Other columns stay unchanged.`,
        duration: 4000,
      });
    } catch (err: any) {
      const { message, isBusy, code } = describeRequestError(err, 'Could not start re-extraction');
      if (code === 'confirmed_empty_only' && !scope?.retryConfirmedEmpty) {
        // The selected cell(s) are blank on screen but were already checked
        // and explicitly confirmed empty by a prior run -- only_empty skips
        // them by default to avoid re-billing the model. Offer a one-click
        // way to force a recheck instead of just reporting a dead end.
        toast({
          title: 'Nothing to fill yet',
          description: message,
          duration: 10000,
          action: (
            <ToastAction
              altText="Check again"
              onClick={() => {
                void startReextraction(targetColumns, { ...scope, retryConfirmedEmpty: true });
              }}
            >
              Check again
            </ToastAction>
          ),
        });
        return;
      }
      toast({
        title: isBusy ? 'Server busy' : 'Re-extraction failed to start',
        description: message,
        variant: isBusy ? 'default' : 'destructive',
      });
    } finally {
      setRerunStarting(false);
    }
  }, [cancelChatPendingIfAny, clearPendingRerun, reextraction, rerunStarting, sessionId, toast]);

  // Entry point for the Data-sheet "Fill empty cells" menu item: the scope
  // (unit-row names + schema-column keys) is resolved by
  // SpreadsheetSurface's selectedEmptyCellScope/emptyCellScope helper, which
  // only tracks *which* rows/columns contain a blank cell, not exact (row,
  // col) pairs -- so onlyEmpty:true is required here to keep the backend from
  // overwriting already-filled cells that share a row or column with a blank
  // one.
  const fillEmptyCells = useCallback((scope: { rows: string[]; columns: string[] }) => {
    if (scope.rows.length === 0 || scope.columns.length === 0) return;
    void startReextraction(scope.columns, { rows: scope.rows, onlyEmpty: true });
  }, [startReextraction]);

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

    if (!canRediscoverSchema) {
      toast({
        title: 'Rediscovery needs source documents',
        description: 'Imported static projects can edit the observation unit, but rediscovering schema requires source documents attached to this project.',
        variant: 'destructive',
      });
      return;
    }

    setRerunStarting(true);
    try {
      // The enabled-state gate (canRediscoverSchema) only confirms the rows
      // reference source documents by name. An imported project can carry those
      // names without the underlying files being retrievable (a dual-file
      // import saves no raw documents; a bundle may ship without a documents/
      // folder). Confirm real availability against the same precheck the
      // backend uses BEFORE the destructive optimistic clear below, so a run
      // that is bound to 404/400 does not blank the Schema tab on its way out.
      // A failing precheck falls through to the authoritative backend gate.
      if (sessionMode === 'load') {
        const availability = await schemaAPI
          .precheckDocuments(sessionId, { operation_type: 'reextraction' })
          .catch(() => null);
        if (availability && !availability.can_proceed) {
          toast({
            title: 'Rediscovery needs source documents',
            description: 'No source documents are available for this project. Add the original source documents from the Documents tab, then try again.',
            variant: 'destructive',
          });
          return;
        }
      }

      const chatPendingCleared = await cancelChatPendingIfAny();
      if (!chatPendingCleared) {
        toast({
          title: 'Chat confirmation still pending',
          description: 'Cancel the chat confirmation card first, then try again.',
          variant: 'destructive',
        });
        return;
      }
      // Drop the previous run's columns now, before the resume round-trip. The
      // backend resets synchronously inside resume, but that request can be slow
      // (it stops any in-flight pipeline first), and until it returns the Schema
      // tab would otherwise keep showing stale columns. The observation unit is
      // preserved by the parent so the Observation Unit tab stays populated.
      onRediscoveryStart?.();
      // Imported (load-mode) sessions never went through /schematiq/configure,
      // so they have no config.json and schematiqAPI.resume would 404 (it's
      // scoped to SessionType.SCHEMATIQ). Route them through the dedicated
      // endpoint that synthesizes a config for the existing session instead.
      if (sessionMode === 'load') {
        await loadAPI.rediscoverImported(sessionId);
      } else {
        await schematiqAPI.resume(sessionId);
      }
      clearPendingRerun();
      toast({
        title: 'Schema rediscovery started',
        description: 'Rediscovering schema from the updated observation unit.',
        duration: 4000,
      });
      await refresh({ silent: true });
    } catch (err: any) {
      // The optimistic clear above may have already run; re-sync from the server
      // so a failed resume does not leave the Schema tab wrongly emptied.
      await refresh({ silent: true }).catch(() => { /* keep the error toast as the primary signal */ });
      const { message, isBusy } = describeRequestError(err, 'Could not start schema rediscovery');
      toast({
        title: isBusy ? 'Server busy' : 'Schema rediscovery failed',
        description: message,
        variant: isBusy ? 'default' : 'destructive',
      });
    } finally {
      setRerunStarting(false);
    }
  }, [cancelChatPendingIfAny, canRediscoverSchema, clearPendingRerun, onRediscoveryStart, refresh, rerunStarting, sessionId, sessionMode, toast]);

  const notifyEditFollowUp = useCallback((kind: PendingRerunKind, columns: string[] = []) => {
    markRerunNeeded(kind, columns);

    if (kind === 'unit') {
      // The persistent top banner (driven by markRerunNeeded above) already
      // surfaces the "Rediscover schema & re-extract" action, so we do not fire a
      // competing toast. Exception: when rediscovery is impossible (no source
      // documents attached) the banner's action only errors, so we surface a
      // proactive, action-less explanation instead.
      if (!canRediscoverSchema) {
        toast({
          title: 'Observation unit updated',
          description: 'Imported static projects can edit the unit, but rediscovery needs source documents attached to this project.',
        });
      }
      return;
    }

    // Schema edits: the persistent "Schema changed" banner is the single,
    // always-fresh entry point for re-extraction, so we raise no duplicate toast
    // here. The old toast was also broken — its action captured a stale
    // `requestReextraction` closure (schema not yet refreshed when the toast was
    // created), producing a false "No columns to re-extract" error on click.
  }, [canRediscoverSchema, markRerunNeeded, toast]);

  const runPendingEdits = useCallback(async () => {
    if (!sessionId || !pendingRerunKind || rerunStarting) return;
    if (pendingRerunKind === 'unit') {
      await startSchemaRediscovery();
      return;
    }
    requestReextraction(pendingSchemaColumns);
  }, [pendingRerunKind, pendingSchemaColumns, rerunStarting, requestReextraction, sessionId, startSchemaRediscovery]);

  return {
    canRediscoverSchema,
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
    fillEmptyCells,
    stopReextraction,
    startSchemaRediscovery,
    notifyEditFollowUp,
    runPendingEdits,
  };
}
