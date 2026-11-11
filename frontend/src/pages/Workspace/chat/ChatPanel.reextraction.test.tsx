import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import type { PaginatedData, ReextractionOperationStatus, WebSocketMessage } from '@/types';
import type { WorkspaceMessage } from '../types';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const mockGetReextractionStatus = jest.fn<
  Promise<ReextractionOperationStatus>,
  [string, string]
>();
const mockWsHandlers = new Set<(message: WebSocketMessage) => void>();

jest.mock('@/constants', () => ({
  getDefaultModelForProvider: () => 'gemini-3.1-flash-lite',
}));

jest.mock('@/components/ModelSelector', () => ({
  ModelSelector: () => null,
}));

jest.mock('@/components/AdvancedSettings/AdvancedSettingsFields', () => ({
  observationUnitFromValue: jest.fn(),
  retrieverIsCustomized: jest.fn(),
  DEFAULT_ADVANCED_SETTINGS: {},
}));

jest.mock('@/services/api', () => ({
  chatAPI: {
    getMessages: () => Promise.resolve({ messages: [] }),
    getTools: () => Promise.resolve([]),
    sendMessage: () => Promise.resolve(null),
    confirmAction: () => Promise.resolve(null),
    cancelAction: () => Promise.resolve(null),
    stopChat: () => Promise.resolve(undefined),
  },
  referenceAPI: {
    list: () => Promise.resolve([]),
    upload: () => Promise.resolve(null),
    remove: () => Promise.resolve(undefined),
  },
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
  },
}));

jest.mock('./ChatMessageBody', () => ({
  ChatMessageBody: ({ message }: { message: WorkspaceMessage }) => (
    <div data-testid="chat-msg">{message.content}</div>
  ),
}));

import { ChatPanel } from './ChatPanel';

const emptyData: PaginatedData = {
  rows: [],
  total_count: 0,
  page: 0,
  page_size: 50,
  has_more: false,
};

function emitWs(message: WebSocketMessage) {
  Array.from(mockWsHandlers).forEach((handler) => handler(message));
}

describe('ChatPanel re-extraction status polling fallback', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    jest.useFakeTimers();
    mockWsHandlers.clear();
    mockGetReextractionStatus.mockReset();
    Element.prototype.scrollTo = jest.fn();
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    container.remove();
    jest.useRealTimers();
  });

  it('clears spinner and posts fallback recap via 2500ms poll when WS misses reextraction_completed, without duplicating on late WS', async () => {
    mockGetReextractionStatus.mockResolvedValue({
      operation_id: 'chat-op-1',
      session_id: 'session-chat',
      status: 'completed',
      progress: 1,
      columns: ['outcome'],
      processed_documents: 3,
      total_documents: 3,
    });

    await act(async () => {
      root.render(
        <ChatPanel
          sessionId="session-chat"
          sessionMode="schematiq"
          status={null}
          schema={null}
          data={emptyData}
          onRefresh={jest.fn()}
          onEditFollowUp={jest.fn()}
          onClose={jest.fn()}
        />,
      );
      await Promise.resolve();
    });

    act(() => {
      emitWs({
        type: 'reextraction_started',
        data: { operation_id: 'chat-op-1', columns: ['outcome'], total_documents: 3 },
      });
    });

    expect(container.textContent).toContain("Re-extracting 'outcome' in the background");

    await act(async () => {
      jest.advanceTimersByTime(2500);
      await Promise.resolve();
    });

    expect(mockGetReextractionStatus).toHaveBeenCalledWith('session-chat', 'chat-op-1');
    expect(container.textContent).not.toContain("Re-extracting 'outcome' in the background");
    expect(container.textContent).toContain("Re-extraction finished for 'outcome'.");

    // Late duplicate WS completion for the same operation_id must not post a second summary.
    act(() => {
      emitWs({
        type: 'reextraction_completed',
        data: { operation_id: 'chat-op-1', columns: ['outcome'], status: 'success' },
      });
    });

    const summaryNodes = Array.from(container.querySelectorAll('[data-testid="chat-msg"]')).filter(
      (node) => node.textContent?.includes('Re-extraction finished'),
    );
    expect(summaryNodes).toHaveLength(1);
  });

  it('clears spinner on HTTP 404 from getReextractionStatus so it never hangs after server cleanup', async () => {
    mockGetReextractionStatus.mockRejectedValue({
      response: { status: 404 },
    });

    await act(async () => {
      root.render(
        <ChatPanel
          sessionId="session-chat-404"
          sessionMode="schematiq"
          status={null}
          schema={null}
          data={emptyData}
          onRefresh={jest.fn()}
          onEditFollowUp={jest.fn()}
          onClose={jest.fn()}
        />,
      );
      await Promise.resolve();
    });

    act(() => {
      emitWs({
        type: 'reextraction_started',
        data: { operation_id: 'chat-op-404', columns: ['dosage'], total_documents: 2 },
      });
    });

    expect(container.textContent).toContain("Re-extracting 'dosage' in the background");

    await act(async () => {
      jest.advanceTimersByTime(2500);
      await Promise.resolve();
    });

    expect(mockGetReextractionStatus).toHaveBeenCalledWith('session-chat-404', 'chat-op-404');
    expect(container.textContent).not.toContain("Re-extracting 'dosage' in the background");
  });
});
