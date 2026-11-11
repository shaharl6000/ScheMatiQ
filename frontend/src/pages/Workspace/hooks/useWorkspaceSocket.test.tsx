import React, { act, useState } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import type { PaginatedData, ReextractionOperationStatus, WebSocketMessage } from '@/types';
import type { SheetId, WorkspaceReextractionState } from '../types';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const mockGetReextractionStatus = jest.fn<
  Promise<ReextractionOperationStatus>,
  [string, string]
>();

const mockWsHandlers = new Set<(message: WebSocketMessage) => void>();
const mockWsConnect = jest.fn();
const mockWsDisconnect = jest.fn();

jest.mock('@/constants', () => ({
  WS_DISCONNECTED_REFRESH_INTERVAL: 60_000,
}));

jest.mock('@/services/api', () => ({
  schemaAPI: {
    getReextractionStatus: (...args: [string, string]) => mockGetReextractionStatus(...args),
  },
}));

jest.mock('@/services/websocket', () => ({
  __esModule: true,
  default: {
    addMessageHandler: (handler: (message: WebSocketMessage) => void) => {
      mockWsHandlers.add(handler);
    },
    removeMessageHandler: (handler: (message: WebSocketMessage) => void) => {
      mockWsHandlers.delete(handler);
    },
    connect: (...args: unknown[]) => mockWsConnect(...args),
    disconnect: (...args: unknown[]) => mockWsDisconnect(...args),
  },
}));

import { useWorkspaceSocket } from './useWorkspaceSocket';

function emitWs(message: WebSocketMessage) {
  Array.from(mockWsHandlers).forEach((handler) => handler(message));
}

describe('useWorkspaceSocket re-extraction lifecycle and polling fallback', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    jest.useFakeTimers();
    mockWsHandlers.clear();
    mockGetReextractionStatus.mockReset();
    mockWsConnect.mockReset();
    mockWsDisconnect.mockReset();
    container = document.createElement('div');
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    jest.useRealTimers();
  });

  it('settles via 2500ms status poll when WS misses reextraction_completed and does not double-fire on late WS event', async () => {
    const refresh = jest.fn().mockResolvedValue(undefined);
    const refreshSilent = jest.fn().mockResolvedValue(undefined);
    const setActiveSheet = jest.fn<void, [SheetId]>();
    const toast = jest.fn();
    const onReextractionStart = jest.fn();
    const onReextractionSettled = jest.fn<void, [PaginatedData]>();
    const onReextractionDiscard = jest.fn();

    let currentReextraction = null as WorkspaceReextractionState | null;
    let setExternalReextraction = null as React.Dispatch<
      React.SetStateAction<WorkspaceReextractionState | null>
    > | null;

    function Harness({ sessionId }: { sessionId: string }) {
      const [reextraction, setReextraction] = useState<WorkspaceReextractionState | null>({
        operationId: 'op-missed-ws',
        columns: ['outcome', 'dosage'],
        progress: 0.2,
        processedDocuments: 1,
        totalDocuments: 5,
      });
      currentReextraction = reextraction;
      setExternalReextraction = setReextraction;

      useWorkspaceSocket({
        sessionId,
        reextraction,
        refresh,
        refreshSilent,
        setActiveSheet,
        setReextraction,
        toast,
        onReextractionStart,
        onReextractionSettled,
        onReextractionDiscard,
      });
      return null;
    }

    mockGetReextractionStatus.mockResolvedValue({
      operation_id: 'op-missed-ws',
      session_id: 'session-1',
      status: 'completed',
      progress: 1,
      columns: ['outcome', 'dosage'],
      processed_documents: 5,
      total_documents: 5,
    });

    act(() => {
      root.render(<Harness sessionId="session-1" />);
    });

    expect(onReextractionStart).toHaveBeenCalledTimes(1);
    expect(currentReextraction?.operationId).toBe('op-missed-ws');
    expect(setExternalReextraction).not.toBeNull();

    await act(async () => {
      jest.advanceTimersByTime(2500);
      await Promise.resolve();
    });

    expect(mockGetReextractionStatus).toHaveBeenCalledTimes(1);
    expect(mockGetReextractionStatus).toHaveBeenCalledWith('session-1', 'op-missed-ws');
    expect(currentReextraction).toBeNull();
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(refresh).toHaveBeenCalledWith({
      silent: true,
      onCommitted: onReextractionSettled,
    });
    expect(toast).toHaveBeenCalledTimes(1);
    expect(toast).toHaveBeenCalledWith(
      expect.objectContaining({
        title: 'Re-extraction completed',
      }),
    );

    // A late/duplicate WebSocket completion for the same operationId must be ignored.
    act(() => {
      emitWs({
        type: 'reextraction_completed',
        data: {
          operation_id: 'op-missed-ws',
          columns: ['outcome', 'dosage'],
          status: 'success',
        },
      });
    });

    expect(refresh).toHaveBeenCalledTimes(1);
    expect(toast).toHaveBeenCalledTimes(1);
  });

  it('clears reextraction and refreshes with onReextractionSettled when status poll returns 404 (cleaned up)', async () => {
    const refresh = jest.fn().mockResolvedValue(undefined);
    const refreshSilent = jest.fn().mockResolvedValue(undefined);
    const setActiveSheet = jest.fn<void, [SheetId]>();
    const toast = jest.fn();
    const onReextractionStart = jest.fn();
    const onReextractionSettled = jest.fn<void, [PaginatedData]>();

    let currentReextraction: WorkspaceReextractionState | null = null;

    function Harness() {
      const [reextraction, setReextraction] = useState<WorkspaceReextractionState | null>({
        operationId: 'op-cleaned-up',
        columns: ['colA'],
        progress: 0.5,
        processedDocuments: 2,
        totalDocuments: 4,
      });
      currentReextraction = reextraction;

      useWorkspaceSocket({
        sessionId: 'session-404',
        reextraction,
        refresh,
        refreshSilent,
        setActiveSheet,
        setReextraction,
        toast,
        onReextractionStart,
        onReextractionSettled,
      });
      return null;
    }

    mockGetReextractionStatus.mockRejectedValue({
      response: { status: 404 },
    });

    act(() => {
      root.render(<Harness />);
    });

    await act(async () => {
      jest.advanceTimersByTime(2500);
      await Promise.resolve();
    });

    expect(mockGetReextractionStatus).toHaveBeenCalledWith('session-404', 'op-cleaned-up');
    expect(currentReextraction).toBeNull();
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(refresh).toHaveBeenCalledWith({
      silent: true,
      onCommitted: onReextractionSettled,
    });
    // Exact terminal outcome is unknown on 404 cleanup, so no toast should fire.
    expect(toast).not.toHaveBeenCalled();
  });

  it('calls onReextractionStart once when HTTP sets reextraction before WS reextraction_started arrives', () => {
    const refresh = jest.fn().mockResolvedValue(undefined);
    const refreshSilent = jest.fn().mockResolvedValue(undefined);
    const setActiveSheet = jest.fn<void, [SheetId]>();
    const toast = jest.fn();
    const onReextractionStart = jest.fn();

    let setExternalReextraction: React.Dispatch<
      React.SetStateAction<WorkspaceReextractionState | null>
    > | null = null;

    function Harness() {
      const [reextraction, setReextraction] = useState<WorkspaceReextractionState | null>(null);
      setExternalReextraction = setReextraction;

      useWorkspaceSocket({
        sessionId: 'session-dedup',
        reextraction,
        refresh,
        refreshSilent,
        setActiveSheet,
        setReextraction,
        toast,
        onReextractionStart,
      });
      return null;
    }

    act(() => {
      root.render(<Harness />);
    });

    expect(onReextractionStart).not.toHaveBeenCalled();

    // Simulate HTTP startReextraction setting state first (e.g. before WS event or while WS is down).
    act(() => {
      setExternalReextraction!({
        operationId: 'op-http-first',
        columns: ['colA'],
        progress: 0,
        processedDocuments: 0,
        totalDocuments: 3,
      });
    });

    expect(onReextractionStart).toHaveBeenCalledTimes(1);

    // Subsequent WS reextraction_started for the same operation_id must NOT re-snapshot / clear history again.
    act(() => {
      emitWs({
        type: 'reextraction_started',
        data: {
          operation_id: 'op-http-first',
          columns: ['colA'],
          total_documents: 3,
        },
      });
    });

    expect(onReextractionStart).toHaveBeenCalledTimes(1);
  });
});
