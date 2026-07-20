import { type Dispatch, type SetStateAction, useEffect, useState } from 'react';

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

  useEffect(() => {
    setWsConnected(false);
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
        void refreshSilent();
        return;
      }

      if (message.type === 'disconnected' || message.type === 'reconnecting') {
        setWsConnected(false);
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
        void refresh({ silent: true });
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

      if (
        message.type === 'progress' ||
        message.type === 'completed' ||
        message.type === 'schema_completed' ||
        message.type === 'schema_updated' ||
        message.type === 'cell_extracted' ||
        message.type === 'row_completed' ||
        message.type === 'schema_progress' ||
        message.type === 'reprocessing_progress' ||
        message.type === 'reprocessing_completed' ||
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
