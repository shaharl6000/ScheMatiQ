import { type Dispatch, type SetStateAction, useEffect, useRef, useState } from 'react';

import { WS_DISCONNECTED_REFRESH_INTERVAL } from '@/constants';
import webSocketService from '@/services/websocket';
import type {
  ReextractionCompletedData,
  ReextractionFailedData,
  ReextractionProgressData,
  ReextractionStartedData,
  WebSocketMessage,
} from '@/types';

import type { SheetId, WorkspaceReextractionState } from '../types';

type UseWorkspaceSocketOptions = {
  sessionId?: string;
  refresh: (options?: { silent?: boolean }) => Promise<void>;
  refreshSilent: () => Promise<void> | void;
  setActiveSheet: (sheet: SheetId) => void;
  setReextraction: Dispatch<SetStateAction<WorkspaceReextractionState | null>>;
  toast: (props: {
    title: string;
    description?: string;
    variant?: 'default' | 'destructive';
    duration?: number;
  }) => void;
};

// Owns WebSocket connection lifecycle, message routing, and disconnected polling.
// Parent: Workspace (index.tsx).
export function useWorkspaceSocket({
  sessionId,
  refresh,
  refreshSilent,
  setActiveSheet,
  setReextraction,
  toast,
}: UseWorkspaceSocketOptions) {
  const [wsConnected, setWsConnected] = useState(false);
  // A 'connected' message only means "catch up on what you missed" when it
  // follows a drop. The first connect of a session lands right after the
  // initial load has already fetched everything, and the subscription below is
  // re-registered whenever refresh/refreshSilent change identity during mount,
  // so the service replays 'connected' to each new subscriber. Refreshing on
  // every one of those made a single project open issue three identical
  // POST /api/load/data requests for the same 500-row page.
  const sawDisconnectRef = useRef(false);

  useEffect(() => {
    setWsConnected(false);
    sawDisconnectRef.current = false;
  }, [sessionId]);

  useEffect(() => {
    if (!sessionId || wsConnected) return undefined;
    const interval = window.setInterval(() => refreshSilent(), WS_DISCONNECTED_REFRESH_INTERVAL);
    return () => window.clearInterval(interval);
  }, [refreshSilent, sessionId, wsConnected]);

  useEffect(() => {
    if (!sessionId) return undefined;

    const handler = (message: WebSocketMessage) => {
      if (message.type === 'connected') {
        setWsConnected(true);
        if (sawDisconnectRef.current) {
          sawDisconnectRef.current = false;
          void refreshSilent();
        }
        return;
      }

      if (message.type === 'disconnected' || message.type === 'reconnecting') {
        setWsConnected(false);
        sawDisconnectRef.current = true;
        return;
      }

      if (message.type === 'heartbeat' || message.type === 'pong') {
        return;
      }

      if (message.type === 'reextraction_started' && message.data) {
        const payload = message.data as ReextractionStartedData;
        setReextraction({
          operationId: payload.operation_id,
          columns: payload.columns || [],
          progress: 0,
          processedDocuments: 0,
          totalDocuments: payload.total_documents || 0,
        });
        setActiveSheet('data');
        void refresh({ silent: true });
        return;
      }

      if (message.type === 'reextraction_progress' && message.data) {
        const payload = message.data as ReextractionProgressData;
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
        setReextraction(null);
        void refresh({ silent: true });
        toast({
          title: 'Re-extraction completed',
          description: payload.columns?.length
            ? `Updated ${payload.columns.length} column(s) from source documents.`
            : 'Table values were refreshed from source documents.',
        });
        return;
      }

      if (message.type === 'reextraction_failed' && message.data) {
        const payload = message.data as ReextractionFailedData;
        setReextraction(null);
        toast({
          title: 'Re-extraction failed',
          description: payload.error || 'Could not re-extract values from source documents.',
          variant: 'destructive',
        });
        return;
      }

      if (message.type === 'reextraction_stopped') {
        setReextraction(null);
        void refresh({ silent: true });
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
      setWsConnected(false);
    };
  }, [refresh, refreshSilent, sessionId, setActiveSheet, setReextraction, toast]);

  return { wsConnected };
}
