import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import type { DataRow, PaginatedData, ScheMatiQStatus, SchemaData, WebSocketMessage } from '@/types';
import type { CellUpdate } from './types';
import { EMPTY_DATA_RECHECK_MS } from './constants';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

let mockSessionId: string | undefined = 'session-1';
const mockNavigate = jest.fn();
const mockSearchParams = new URLSearchParams();

jest.mock('react-router-dom', () => ({
  useParams: () => ({ sessionId: mockSessionId }),
  useNavigate: () => mockNavigate,
  useSearchParams: () => [mockSearchParams, jest.fn()],
}));

const mockGetStatus = jest.fn<Promise<ScheMatiQStatus>, [string]>();
const mockGetSchema = jest.fn<Promise<SchemaData | null>, [string]>();
const mockGetConfig = jest.fn();
const mockGetData = jest.fn<Promise<PaginatedData>, [string, number, number]>();
const mockGetSession = jest.fn();
const mockGetDocuments = jest.fn();
const mockGetReextractionStatus = jest.fn();

jest.mock('@/services/api', () => ({
  __esModule: true,
  default: { get: () => Promise.resolve({ data: '' }) },
  configAPI: {
    getConfig: () => Promise.resolve({ developer_mode: true }),
    getLLMConfigStatus: () => Promise.resolve({}),
  },
  loadAPI: {
    getSession: (...args: unknown[]) => mockGetSession(...args),
    getData: () =>
      Promise.resolve({ rows: [], total_count: 0, page: 0, page_size: 500, has_more: false }),
  },
  schematiqAPI: {
    getStatus: (...args: [string]) => mockGetStatus(...args),
    getSchema: (...args: [string]) => mockGetSchema(...args),
    getConfig: (...args: unknown[]) => mockGetConfig(...args),
    getData: (...args: [string, number, number]) => mockGetData(...args),
  },
  schemaAPI: {
    getReextractionStatus: (...args: unknown[]) => mockGetReextractionStatus(...args),
  },
  unitsAPI: {
    getDocuments: (...args: unknown[]) => mockGetDocuments(...args),
    getData: () =>
      Promise.resolve({
        rows: [],
        total_count: 0,
        page: 0,
        page_size: 500,
        has_more: false,
      }),
  },
  describeSkippedAttachments: () => '',
}));

const mockWsHandlers = new Set<(message: WebSocketMessage) => void>();

jest.mock('@/services/websocket', () => ({
  __esModule: true,
  default: {
    addMessageHandler: (handler: (message: WebSocketMessage) => void) => {
      mockWsHandlers.add(handler);
    },
    removeMessageHandler: (handler: (message: WebSocketMessage) => void) => {
      mockWsHandlers.delete(handler);
    },
    connect: jest.fn(),
    disconnect: jest.fn(),
  },
}));

jest.mock('@/utils/recentProjects', () => ({
  rememberProject: jest.fn(),
}));

jest.mock('@/utils/saveFile', () => ({
  saveBlobAs: jest.fn(),
}));

jest.mock('@/utils/apiKeyStorage', () => ({
  getApiKeyForProvider: jest.fn(),
  getConfiguredProviders: () => [],
}));

jest.mock('@/components/AdvancedSettings/AdvancedSettingsFields', () => ({
  observationUnitFromValue: jest.fn(),
  retrieverIsCustomized: jest.fn(),
  DEFAULT_ADVANCED_SETTINGS: {},
}));

jest.mock('@/components/ui/tooltip', () => ({
  Tooltip: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  TooltipTrigger: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  TooltipContent: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

jest.mock('@/components/StatsDashboard/StatsDashboard', () => () => null);
jest.mock('@/components/ScheMatiQMonitor/ScheMatiQMonitor', () => () => null);
jest.mock('@/components/DocumentViewer/DocumentViewer', () => () => null);
jest.mock('@/components/DocumentViewer/DocumentPreview', () => () => null);
jest.mock('@/components/DocumentUpload/DocumentUpload', () => () => null);
jest.mock('@/components/DataTable/SkippedDocumentsBanner', () => () => null);
jest.mock('@/components/ViewMode/ViewModeToggle', () => ({
  ViewModeToggle: () => null,
}));
jest.mock('@/components/SchemaEditor/MissingDocumentsSection', () => () => null);
jest.mock('@/components/TableFeedbackWidget/TableFeedbackWidget', () => () => null);
jest.mock('@/components/ReportIssueDialog/ReportIssueDialog', () => () => null);

let mockCapturedChromeRefresh:
  | ((options?: { silent?: boolean; onCommitted?: (data: PaginatedData) => void }) => Promise<void>)
  | null = null;
let mockCapturedUndo: (() => void) | null = null;
const mockApplyCellUpdatesSpy = jest.fn<void, [CellUpdate[]]>();

jest.mock('./SpreadsheetChrome', () => ({
  SpreadsheetChrome: (props: {
    onRefresh: (options?: { silent?: boolean; onCommitted?: (data: PaginatedData) => void }) => Promise<void>;
    onUndo: () => void;
  }) => {
    mockCapturedChromeRefresh = props.onRefresh;
    mockCapturedUndo = props.onUndo;
    return null;
  },
}));

jest.mock('./SpreadsheetSurface', () => ({
  SpreadsheetSurface: (props: {
    applyCellUpdatesRef: React.MutableRefObject<((updates: CellUpdate[]) => void) | null>;
  }) => {
    props.applyCellUpdatesRef.current = mockApplyCellUpdatesSpy;
    return null;
  },
}));

jest.mock('./chat/ChatPanel', () => ({ ChatPanel: () => null }));
jest.mock('./NewProjectDialog', () => ({ NewProjectDialog: () => null }));
jest.mock('./PendingRerunBanner', () => ({ PendingRerunBanner: () => null }));
jest.mock('./CiteDialog', () => ({ CiteDialog: () => null }));
jest.mock('./KeyboardShortcutsDialog', () => ({ KeyboardShortcutsDialog: () => null }));
jest.mock('./ProjectDetailsDialog', () => ({ ProjectDetailsDialog: () => null }));
jest.mock('./DataLoadErrorBanner', () => ({ DataLoadErrorBanner: () => null }));

import Workspace from './index';

function mkPaginated(rows: DataRow[]): PaginatedData {
  return {
    rows,
    total_count: rows.length,
    page: 0,
    page_size: 500,
    has_more: false,
  };
}

function emitWs(message: WebSocketMessage) {
  Array.from(mockWsHandlers).forEach((handler) => handler(message));
}

async function flushMicrotasks() {
  for (let i = 0; i < 6; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve();
  }
}

describe('Workspace scheduleEmptyRecheck callback chaining', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    jest.useFakeTimers();
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: () => ({
        matches: false,
        media: '',
        onchange: null,
        addListener: () => {},
        removeListener: () => {},
        addEventListener: () => {},
        removeEventListener: () => {},
        dispatchEvent: () => false,
      }),
    });
    mockWsHandlers.clear();
    mockCapturedChromeRefresh = null;
    mockCapturedUndo = null;
    mockApplyCellUpdatesSpy.mockReset();
    mockSessionId = 'session-1';

    mockGetStatus.mockReset().mockResolvedValue({
      session_id: 'session-1',
      status: 'completed',
      progress: 1,
      current_step: 'Completed',
      steps_completed: 1,
      total_steps: 1,
    });
    mockGetSchema.mockReset().mockResolvedValue({
      query: 'test query',
      schema: [{ name: 'colA', definition: 'Col A', rationale: 'Rationale' }],
    });
    mockGetConfig.mockReset().mockResolvedValue(null);
    mockGetSession.mockReset().mockResolvedValue(null);
    mockGetDocuments.mockReset().mockResolvedValue({
      documents: [],
      totalDocuments: 0,
      totalRows: 0,
      enrichmentPending: false,
    });
    mockGetData.mockReset();

    container = document.createElement('div');
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    jest.useRealTimers();
  });

  it('chains multiple onCommitted callbacks scheduled during an active empty-recheck window (including onReextractionSettled)', async () => {
    const initialData = mkPaginated([
      { row_name: 'r1', _row_index: 0, data: { colA: { answer: 'before', excerpts: [] } } },
    ]);
    const transientEmpty = mkPaginated([]);
    const updatedData = mkPaginated([
      { row_name: 'r1', _row_index: 0, data: { colA: { answer: 'after', excerpts: [] } } },
    ]);

    mockGetData.mockResolvedValueOnce(initialData);

    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });

    expect(mockGetData).toHaveBeenCalledTimes(1);

    // Start re-extraction so Workspace snapshots `initialData`.
    mockGetData.mockResolvedValueOnce(initialData);
    await act(async () => {
      emitWs({
        type: 'reextraction_started',
        data: { operation_id: 'op-chain', columns: ['colA'], total_documents: 1 },
      });
      await flushMicrotasks();
    });

    const callOrder: string[] = [];
    const firstCommitted = jest.fn((committed: PaginatedData) => {
      const cell = committed.rows[0]?.data?.colA as { answer?: string } | undefined;
      callOrder.push(`first:${cell?.answer}`);
    });

    // Caller #1 hits transient 0-row JSONL rewrite window -> schedules empty recheck timer.
    mockGetData.mockResolvedValueOnce(transientEmpty);
    await act(async () => {
      await mockCapturedChromeRefresh!({ silent: true, onCommitted: firstCommitted });
      await flushMicrotasks();
    });
    expect(firstCommitted).not.toHaveBeenCalled();

    // Caller #2 (`reextraction_completed` with `onReextractionSettled`) lands while the
    // empty-recheck timer is STILL pending and also observes the 0-row payload.
    mockGetData.mockResolvedValueOnce(transientEmpty);
    await act(async () => {
      emitWs({
        type: 'reextraction_completed',
        data: { operation_id: 'op-chain', columns: ['colA'], status: 'success' },
      });
      await flushMicrotasks();
    });

    // When the single empty-recheck timer fires, both callbacks must run against `updatedData`.
    mockGetData.mockResolvedValueOnce(updatedData);
    await act(async () => {
      jest.advanceTimersByTime(EMPTY_DATA_RECHECK_MS);
      await flushMicrotasks();
    });

    expect(firstCommitted).toHaveBeenCalledTimes(1);
    expect(callOrder).toEqual(['first:after']);

    // Verify `onReextractionSettled` also ran and pushed the undo command for `before` <- `after`.
    act(() => {
      mockCapturedUndo!();
    });
    expect(mockApplyCellUpdatesSpy).toHaveBeenCalledTimes(1);
    expect(mockApplyCellUpdatesSpy.mock.calls[0][0]).toEqual([
      expect.objectContaining({
        rowName: 'r1',
        rowIndexId: 0,
        column: 'colA',
        value: 'before',
      }),
    ]);
  });

  it('drops pending empty-recheck callbacks when clearEmptyRecheck runs on session switch', async () => {
    const initialData = mkPaginated([
      { row_name: 'r1', _row_index: 0, data: { colA: { answer: 's1', excerpts: [] } } },
    ]);
    const transientEmpty = mkPaginated([]);
    const session2Data = mkPaginated([
      { row_name: 'r2', _row_index: 0, data: { colA: { answer: 's2', excerpts: [] } } },
    ]);

    mockGetData.mockResolvedValueOnce(initialData);

    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });

    const staleCallback = jest.fn();
    mockGetData.mockResolvedValueOnce(transientEmpty);
    await act(async () => {
      await mockCapturedChromeRefresh!({ silent: true, onCommitted: staleCallback });
      await flushMicrotasks();
    });

    // Switch sessions before EMPTY_DATA_RECHECK_MS fires; clearEmptyRecheck must cancel
    // both the timer and the queued callback.
    mockSessionId = 'session-2';
    mockGetData.mockResolvedValue(session2Data);
    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });

    await act(async () => {
      jest.advanceTimersByTime(EMPTY_DATA_RECHECK_MS * 2);
      await flushMicrotasks();
    });

    expect(staleCallback).not.toHaveBeenCalled();

    // Triggering a fresh empty-recheck in session-2 must not invoke session-1's dropped callback either.
    const session2Callback = jest.fn();
    mockGetData.mockResolvedValueOnce(transientEmpty);
    await act(async () => {
      await mockCapturedChromeRefresh!({ silent: true, onCommitted: session2Callback });
      await flushMicrotasks();
    });

    mockGetData.mockResolvedValueOnce(session2Data);
    await act(async () => {
      jest.advanceTimersByTime(EMPTY_DATA_RECHECK_MS);
      await flushMicrotasks();
    });

    expect(staleCallback).not.toHaveBeenCalled();
    expect(session2Callback).toHaveBeenCalledTimes(1);
    expect(session2Callback).toHaveBeenCalledWith(session2Data);
  });
});
