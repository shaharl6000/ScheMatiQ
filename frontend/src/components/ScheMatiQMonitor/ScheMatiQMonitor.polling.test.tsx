import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from 'react-query';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const mockWsHandlers = new Set<(msg: any) => void>();
const mockGetStatus = jest.fn();
const mockGetSession = jest.fn();
const mockGetReextractionStatus = jest.fn();

jest.mock('../../services/api', () => ({
  __esModule: true,
  schematiqAPI: {
    getStatus: (...args: unknown[]) => mockGetStatus(...args),
    run: () => Promise.resolve(),
    stop: () => Promise.resolve(),
    resume: () => Promise.resolve(),
  },
  loadAPI: {
    getSession: (...args: unknown[]) => mockGetSession(...args),
  },
  schemaAPI: {
    getReextractionStatus: (...args: unknown[]) => mockGetReextractionStatus(...args),
    precheckDocuments: () => Promise.resolve({}),
    startReextraction: () => Promise.resolve({}),
    stopReextraction: () => Promise.resolve({}),
  },
  observationUnitAPI: {
    updateDefinition: () => Promise.resolve({}),
  },
  configAPI: {
    getConfig: () => Promise.resolve({ allow_llm_config: true }),
  },
}));

jest.mock('../../services/websocket', () => ({
  __esModule: true,
  webSocketService: {
    connect: () => {},
    disconnect: () => {},
    isConnected: () => false,
    addMessageHandler: (handler: (msg: any) => void) => {
      mockWsHandlers.add(handler);
      return () => {
        mockWsHandlers.delete(handler);
      };
    },
  },
}));

jest.mock('../../utils/apiKeyStorage', () => ({
  getApiKeyForProvider: () => null,
  getConfiguredProviders: () => [],
}));

jest.mock('@/constants/llmModels', () => ({
  getDefaultModelForProvider: () => 'gemini-3.8-flash',
  getAvailableProviders: () => [],
}));

import ScheMatiQMonitor from './ScheMatiQMonitor';

const flushPromises = async (ticks = 4) => {
  for (let i = 0; i < ticks; i += 1) {
    await Promise.resolve();
  }
};

describe('ScheMatiQMonitor polling lifecycle', () => {
  let container: HTMLDivElement;
  let root: Root;
  let queryClient: QueryClient;

  beforeEach(() => {
    jest.useFakeTimers();
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    mockWsHandlers.clear();
    mockGetStatus.mockReset();
    mockGetSession.mockReset();
    mockGetReextractionStatus.mockReset();
    mockGetSession.mockResolvedValue({});
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    queryClient.clear();
    container.remove();
    jest.useRealTimers();
  });

  it('fetches status once but never polls it when canRunPipeline is false', async () => {
    mockGetStatus.mockResolvedValue({ status: 'completed' });

    await act(async () => {
      root.render(
        <QueryClientProvider client={queryClient}>
          <ScheMatiQMonitor sessionId="upload-sess-1" canRunPipeline={false} />
        </QueryClientProvider>,
      );
      await flushPromises();
    });

    await act(async () => {
      jest.advanceTimersByTime(10000);
      await flushPromises();
    });

    // One fetch restores LLM stats and progress for imported sessions. No polling.
    expect(mockGetStatus).toHaveBeenCalledTimes(1);
  });

  it('stops 2s fallback status polling once initial status is completed even with WebSocket disconnected', async () => {
    mockGetStatus.mockResolvedValue({
      status: 'completed',
      schema_completed: true,
      columns_discovered: 3,
      total_documents: 4,
      processed_documents: 4,
    });

    await act(async () => {
      root.render(
        <QueryClientProvider client={queryClient}>
          <ScheMatiQMonitor sessionId="pipe-sess-1" canRunPipeline={true} />
        </QueryClientProvider>,
      );
      await flushPromises();
    });

    expect(mockGetStatus).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(10000);
      await flushPromises();
    });

    expect(mockGetStatus).toHaveBeenCalledTimes(1);
  });

  it('stops 2.5s reextraction polling when getReextractionStatus responds with HTTP 404', async () => {
    mockGetStatus.mockResolvedValue({
      status: 'completed',
      schema_completed: true,
      columns_discovered: 2,
      total_documents: 2,
      processed_documents: 2,
    });
    mockGetReextractionStatus.mockRejectedValue({ response: { status: 404 } });

    await act(async () => {
      root.render(
        <QueryClientProvider client={queryClient}>
          <ScheMatiQMonitor sessionId="pipe-sess-404" canRunPipeline={true} />
        </QueryClientProvider>,
      );
      await flushPromises();
    });

    expect(mockWsHandlers.size).toBeGreaterThan(0);

    await act(async () => {
      mockWsHandlers.forEach((handler) => {
        handler({
          type: 'reextraction_started',
          data: { operation_id: 'op-gone', total_documents: 2 },
        });
      });
      await flushPromises();
    });

    expect(mockGetReextractionStatus).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(10000);
      await flushPromises();
    });

    expect(mockGetReextractionStatus).toHaveBeenCalledTimes(1);
  });
});
