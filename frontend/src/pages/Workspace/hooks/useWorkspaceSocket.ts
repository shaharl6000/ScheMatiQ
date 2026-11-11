import { type Dispatch, type SetStateAction, useCallback, useEffect, useRef, useState } from 'react';

import { WS_DISCONNECTED_REFRESH_INTERVAL } from '@/constants';
import { schemaAPI } from '@/services/api';
import webSocketService from '@/services/websocket';
import type {
  PaginatedData,
  ReextractionCompletedData,
  ReextractionFailedData,
  ReextractionProgressData,
  ReextractionStartedData,
  WebSocketMessage,
} from '@/types';

import type { SheetId, WorkspaceReextractionState } from '../types';

const REEXTRACTION_POLL_INTERVAL_MS = 2500;

type ReextractionSettleOutcome = 'completed' | 'failed' | 'stopped' | 'cleaned_up';

type UseWorkspaceSocketOptions = {
  sessionId?: string;
  reextraction?: WorkspaceReextractionState | null;
  refresh: (options?: { silent?: boolean; onCommitted?: (data: PaginatedData) => void }) => Promise<void>;
  refreshSilent: () => Promise<void> | void;
  setActiveSheet: (sheet: SheetId) => void;
  setReextraction: Dispatch<SetStateAction<WorkspaceReextractionState | null>>;
  toast: (props: {
    title: string;
    description?: string;
    variant?: 'default' | 'destructive';
    duration?: number;
  }) => void;
  // A background rewrite (re-extraction / fill-empty-cells) is about to
  // overwrite existing cell values: drop now-stale undo history and snapshot
  // the current data so the rewrite itself can become one undo step.
  onReextractionStart?: () => void;
  // The rewrite landed (completed, or stopped with partial results): diff the
  // snapshot taken at onReextractionStart against the freshly-committed data
  // and push the result as a single undo/redo command.
  onReextractionSettled?: (data: PaginatedData) => void;
  // The rewrite produced no data (failed): discard the snapshot with no diff.
  onReextractionDiscard?: () => void;
};

// Owns WebSocket connection lifecycle, message routing, and disconnected polling.
// Parent: Workspace (index.tsx).
export function useWorkspaceSocket({
  sessionId,
  reextraction = null,
  refresh,
  refreshSilent,
  setActiveSheet,
  setReextraction,
  toast,
  onReextractionStart,
  onReextractionSettled,
  onReextractionDiscard,
}: UseWorkspaceSocketOptions) {
  const [wsConnected, setWsConnected] = useState(false);
  const wsConnectedRef = useRef(false);
  // A 'connected' message only means "catch up on what you missed" when it
  // follows a drop. The first connect of a session lands right after the
  // initial load has already fetched everything, and the subscription below is
  // re-registered whenever refresh/refreshSilent change identity during mount,
  // so the service replays 'connected' to each new subscriber. Refreshing on
  // every one of those made a single project open issue three identical
  // POST /api/load/data requests for the same 500-row page.
  const sawDisconnectRef = useRef(false);
  const prevSessionIdRef = useRef<string | undefined>(sessionId);
  const startedOpIdRef = useRef<string | null>(null);
  const settledOpIdRef = useRef<string | null>(null);
  const activeOpIdRef = useRef<string | null>(null);
  // Tracks any operationId carried over from the previous session during the
  // single transitional render before useReextraction's session-reset effect
  // clears `reextraction` to null.
  const staleSessionOpIdRef = useRef<string | null>(null);

  const activeOperationId = reextraction?.operationId ?? null;

  useEffect(() => {
    if (prevSessionIdRef.current !== sessionId) {
      prevSessionIdRef.current = sessionId;
      wsConnectedRef.current = false;
      setWsConnected(false);
      sawDisconnectRef.current = false;
      startedOpIdRef.current = null;
      settledOpIdRef.current = null;
      activeOpIdRef.current = null;
      staleSessionOpIdRef.current = activeOperationId;
      return;
    }
    if (staleSessionOpIdRef.current && activeOperationId !== staleSessionOpIdRef.current) {
      staleSessionOpIdRef.current = null;
    }
  }, [activeOperationId, sessionId]);

  const markOperationStarted = useCallback(
    (operationId: string) => {
      if (!operationId || startedOpIdRef.current === operationId) return;
      startedOpIdRef.current = operationId;
      onReextractionStart?.();
    },
    [onReextractionStart],
  );

  // Also observe HTTP-started runs (set via useReextraction.startReextraction)
  // so the undo snapshot is captured even if the WebSocket is disconnected when
  // the run starts, while deduplicating against WS `reextraction_started`.
  useEffect(() => {
    if (!sessionId || !activeOperationId) return;
    if (prevSessionIdRef.current !== sessionId) return;
    if (staleSessionOpIdRef.current === activeOperationId) return;
    if (settledOpIdRef.current === activeOperationId) return;
    activeOpIdRef.current = activeOperationId;
    markOperationStarted(activeOperationId);
  }, [activeOperationId, markOperationStarted, sessionId]);

  const settleOperation = useCallback(
    (
      operationId: string,
      outcome: ReextractionSettleOutcome,
      details?: { columns?: string[]; error?: string },
    ) => {
      if (operationId && settledOpIdRef.current === operationId) return;
      if (operationId) settledOpIdRef.current = operationId;

      setReextraction(null);

      if (outcome === 'completed') {
        void refresh({ silent: true, onCommitted: onReextractionSettled });
        toast({
          title: 'Re-extraction completed',
          description: details?.columns?.length
            ? `Updated ${details.columns.length} column(s) from source documents.`
            : 'Table values were refreshed from source documents.',
        });
        return;
      }

      if (outcome === 'stopped') {
        // A stopped run still keeps whatever it processed before stopping, so
        // that partial rewrite is worth one undo step too.
        void refresh({ silent: true, onCommitted: onReextractionSettled });
        return;
      }

      if (outcome === 'failed') {
        // Nothing was written, so there is nothing to diff: discard the
        // snapshot rather than leaving it to be diffed against unrelated data.
        onReextractionDiscard?.();
        toast({
          title: 'Re-extraction failed',
          description: details?.error || 'Could not re-extract values from source documents.',
          variant: 'destructive',
        });
        return;
      }

      // 'cleaned_up' (404 from getReextractionStatus): backend _cleanup_operation
      // already ran after finishing. Refresh with onReextractionSettled so the
      // snapshot is cleared and any non-zero cell diff is recorded for undo,
      // while omitting a toast since the exact terminal status is unknown.
      void refresh({ silent: true, onCommitted: onReextractionSettled });
    },
    [onReextractionDiscard, onReextractionSettled, refresh, setReextraction, toast],
  );

  useEffect(() => {
    if (!sessionId || wsConnected) return undefined;
    const interval = window.setInterval(() => refreshSilent(), WS_DISCONNECTED_REFRESH_INTERVAL);
    return () => window.clearInterval(interval);
  }, [refreshSilent, sessionId, wsConnected]);

  // Fallback status polling while a re-extraction operation is active: if the
  // WebSocket drops briefly and misses the terminal event (and/or the backend
  // cleans up the finished operation and returns 404), clear `reextraction`
  // and settle the undo snapshot so the bottom-bar banner never hangs forever.
  useEffect(() => {
    if (!sessionId || !activeOperationId) return undefined;
    if (prevSessionIdRef.current !== sessionId) return undefined;
    if (staleSessionOpIdRef.current === activeOperationId) return undefined;

    let cancelled = false;

    const poll = async () => {
      try {
        const st = await schemaAPI.getReextractionStatus(sessionId, activeOperationId);
        if (cancelled || settledOpIdRef.current === activeOperationId) return;

        if (st.status === 'running' || st.status === 'starting') {
          const totalDocuments = st.total_documents || 0;
          const processedDocuments = Math.min(
            st.processed_documents ?? 0,
            totalDocuments || (st.processed_documents ?? 0),
          );
          setReextraction((current) => {
            if (!current || current.operationId !== activeOperationId) return current;
            return {
              ...current,
              columns: st.columns?.length ? st.columns : current.columns,
              progress: st.progress ?? current.progress,
              processedDocuments,
              totalDocuments: totalDocuments || current.totalDocuments,
              currentColumn: st.current_column || current.currentColumn,
            };
          });
          if (!wsConnectedRef.current) {
            void refreshSilent();
          }
          return;
        }

        if (st.status === 'completed' || st.status === 'failed' || st.status === 'stopped') {
          settleOperation(activeOperationId, st.status, {
            columns: st.columns,
            error: st.error,
          });
        }
      } catch (err) {
        const status = (err as { response?: { status?: number } })?.response?.status;
        if (!cancelled && status === 404) {
          settleOperation(activeOperationId, 'cleaned_up');
        }
        // Ignore transient network errors and keep polling.
      }
    };

    const intervalId = window.setInterval(() => {
      void poll();
    }, REEXTRACTION_POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [activeOperationId, refreshSilent, sessionId, setReextraction, settleOperation]);

  useEffect(() => {
    if (!sessionId) return undefined;

    const handler = (message: WebSocketMessage) => {
      if (message.type === 'connected') {
        wsConnectedRef.current = true;
        setWsConnected(true);
        if (sawDisconnectRef.current) {
          sawDisconnectRef.current = false;
          void refreshSilent();
        }
        return;
      }

      if (message.type === 'disconnected' || message.type === 'reconnecting') {
        wsConnectedRef.current = false;
        setWsConnected(false);
        sawDisconnectRef.current = true;
        return;
      }

      if (message.type === 'heartbeat' || message.type === 'pong') {
        return;
      }

      if (message.type === 'reextraction_started' && message.data) {
        const payload = message.data as ReextractionStartedData;
        if (payload.operation_id && settledOpIdRef.current === payload.operation_id) return;
        if (payload.operation_id) activeOpIdRef.current = payload.operation_id;
        setReextraction({
          operationId: payload.operation_id,
          columns: payload.columns || [],
          progress: 0,
          processedDocuments: 0,
          totalDocuments: payload.total_documents || 0,
        });
        setActiveSheet('data');
        // A re-extraction overwrites existing cell values, so any recorded undo
        // of a manual edit would now restore a value the model has replaced.
        // Snapshot the current data first so the rewrite itself can become one
        // undo step once it settles (see onReextractionSettled below).
        markOperationStarted(payload.operation_id);
        void refresh({ silent: true });
        return;
      }

      if (message.type === 'reextraction_progress' && message.data) {
        const payload = message.data as ReextractionProgressData;
        if (payload.operation_id && settledOpIdRef.current === payload.operation_id) return;
        if (payload.operation_id) activeOpIdRef.current = payload.operation_id;
        setReextraction((current) => ({
          operationId: payload.operation_id,
          columns: current?.columns || (payload.column ? [payload.column] : []),
          progress: payload.progress ?? current?.progress ?? 0,
          processedDocuments: payload.processed_documents ?? current?.processedDocuments ?? 0,
          totalDocuments: payload.total_documents ?? current?.totalDocuments ?? 0,
          currentColumn: payload.column || current?.currentColumn,
        }));
        // Progress fires once per cell, alongside a `cell_extracted` event that
        // already triggers a data-only refresh. Re-extraction never changes the
        // schema/status/config/session, so a full `refresh` here re-fetches and
        // re-sets four structural payloads per cell for no benefit, flooding the
        // backend and churning the grid. Fetch only the rows that actually change.
        void refreshSilent();
        return;
      }

      if (message.type === 'reextraction_completed' && message.data) {
        const payload = message.data as ReextractionCompletedData;
        const opId = payload.operation_id ?? activeOpIdRef.current ?? '';
        settleOperation(opId, 'completed', { columns: payload.columns });
        return;
      }

      if (message.type === 'reextraction_failed' && message.data) {
        const payload = message.data as ReextractionFailedData;
        const opId = payload.operation_id ?? activeOpIdRef.current ?? '';
        settleOperation(opId, 'failed', { error: payload.error });
        return;
      }

      if (message.type === 'reextraction_stopped') {
        const opId =
          (message.data as { operation_id?: string } | undefined)?.operation_id
          ?? activeOpIdRef.current
          ?? '';
        settleOperation(opId, 'stopped');
        return;
      }

      // Terminal completion events change the session's status and statistics —
      // progress reaches 100%, the run leaves the "processing" state, and the
      // skipped-documents list is finalized. refreshSilent only re-fetches row
      // data ("no status/schema/session churn"), so routing completion through
      // it leaves the bar stuck at the last processing value (50%) and never
      // surfaces the skipped-documents banner. Use the full refresh here.
      if (
        message.type === 'completed' ||
        message.type === 'reprocessing_completed'
      ) {
        void refresh({ silent: true });
        return;
      }

      if (
        message.type === 'progress' ||
        message.type === 'cell_extracted' ||
        message.type === 'row_completed' ||
        message.type === 'reprocessing_progress'
      ) {
        void refreshSilent();
        return;
      }

      if (
        message.type === 'schema_completed' ||
        message.type === 'schema_updated' ||
        message.type === 'schema_progress' ||
        message.type === 'observation_unit_definition_updated'
      ) {
        void refresh({ silent: true });
      }
    };

    webSocketService.addMessageHandler(handler);
    webSocketService.connect(sessionId, 'progress');

    return () => {
      webSocketService.removeMessageHandler(handler);
      webSocketService.disconnect();
      wsConnectedRef.current = false;
      setWsConnected(false);
    };
  }, [
    markOperationStarted,
    refresh,
    refreshSilent,
    sessionId,
    setActiveSheet,
    setReextraction,
    settleOperation,
  ]);

  return { wsConnected };
}
