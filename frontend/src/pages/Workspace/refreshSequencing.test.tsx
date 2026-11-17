import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import type { DataRow, PaginatedData, ScheMatiQStatus, SchemaData, WebSocketMessage } from '@/types';
import type { CellUpdate } from './types';
import { EMPTY_DATA_RECHECK_MS, SILENT_REFRESH_MIN_INTERVAL_MS } from './constants';

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
const mockApplyCellUpdatesSpy = jest.fn<void, [CellUpdate[]]>();

jest.mock('./SpreadsheetChrome', () => ({
  SpreadsheetChrome: (props: {
    onRefresh: (options?: { silent?: boolean; onCommitted?: (data: PaginatedData) => void }) => Promise<void>;
  }) => {
    mockCapturedChromeRefresh = props.onRefresh;
    return null;
  },
}));

let mockRenderedRows: string[] = [];
let mockRenderedSchemaQuery: string | undefined;

jest.mock('./SpreadsheetSurface', () => ({
  SpreadsheetSurface: (props: {
    applyCellUpdatesRef: React.MutableRefObject<((updates: CellUpdate[]) => void) | null>;
    data: PaginatedData;
    schema: SchemaData | null;
  }) => {
    props.applyCellUpdatesRef.current = mockApplyCellUpdatesSpy;
    mockRenderedRows = props.data.rows.map((r) => String(r.row_name));
    mockRenderedSchemaQuery = props.schema?.query;
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


function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
}

const row = (name: string): DataRow => ({ row_name: name, _row_index: 0, data: {} });

describe('Workspace row refresh sequencing', () => {
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
    mockRenderedRows = [];
    mockRenderedSchemaQuery = undefined;
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
      query: 'q',
      schema: [{ name: 'colA', definition: 'A', rationale: 'R' }],
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

  it('drops a response that resolves after a newer one was applied', async () => {
    mockGetData.mockResolvedValueOnce(mkPaginated([row('initial')]));
    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });
    expect(mockRenderedRows).toEqual(['initial']);

    const older = deferred<PaginatedData>();
    const newer = deferred<PaginatedData>();
    mockGetData.mockReturnValueOnce(older.promise).mockReturnValueOnce(newer.promise);

    let first: Promise<void> | undefined;
    let second: Promise<void> | undefined;
    await act(async () => {
      first = mockCapturedChromeRefresh!({ silent: true });
      await flushMicrotasks();
      second = mockCapturedChromeRefresh!({ silent: true });
      await flushMicrotasks();
    });
    expect(mockGetData).toHaveBeenCalledTimes(3);

    await act(async () => {
      newer.resolve(mkPaginated([row('newer')]));
      await second;
      await flushMicrotasks();
    });
    expect(mockRenderedRows).toEqual(['newer']);

    await act(async () => {
      older.resolve(mkPaginated([row('older')]));
      await first;
      await flushMicrotasks();
    });
    expect(mockRenderedRows).toEqual(['newer']);
  });

  it('ignores a response for the project the user switched away from', async () => {
    mockGetData.mockResolvedValueOnce(mkPaginated([row('s1-initial')]));
    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });

    const stale = deferred<PaginatedData>();
    mockGetData.mockReturnValueOnce(stale.promise);
    let pending: Promise<void> | undefined;
    await act(async () => {
      pending = mockCapturedChromeRefresh!({ silent: true });
      await flushMicrotasks();
    });

    mockSessionId = 'session-2';
    mockGetData.mockResolvedValue(mkPaginated([row('s2')]));
    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });
    expect(mockRenderedRows).toEqual(['s2']);

    await act(async () => {
      stale.resolve(mkPaginated([row('s1-late')]));
      await pending;
      await flushMicrotasks();
    });
    expect(mockRenderedRows).toEqual(['s2']);
  });

  it('collapses a burst of websocket cell events into one request plus one trailing request', async () => {
    mockGetData.mockResolvedValue(mkPaginated([row('r')]));
    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });
    const afterMount = mockGetData.mock.calls.length;

    await act(async () => {
      for (let i = 0; i < 5; i += 1) {
        emitWs({ type: 'cell_extracted', data: {} } as WebSocketMessage);
      }
      await flushMicrotasks();
    });
    expect(mockGetData.mock.calls.length).toBe(afterMount + 1);

    await act(async () => {
      jest.advanceTimersByTime(SILENT_REFRESH_MIN_INTERVAL_MS);
      await flushMicrotasks();
    });
    expect(mockGetData.mock.calls.length).toBe(afterMount + 2);

    await act(async () => {
      jest.advanceTimersByTime(SILENT_REFRESH_MIN_INTERVAL_MS * 3);
      await flushMicrotasks();
    });
    expect(mockGetData.mock.calls.length).toBe(afterMount + 2);
  });

  it('a dropped response with onCommitted waits for a pending empty re-check instead of reporting older rows', async () => {
    mockGetData.mockResolvedValueOnce(mkPaginated([row('initial')]));
    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });
    expect(mockRenderedRows).toEqual(['initial']);

    const older = deferred<PaginatedData>();
    const onCommitted = jest.fn<void, [PaginatedData]>();
    mockGetData
      .mockReturnValueOnce(older.promise)
      .mockResolvedValueOnce(mkPaginated([]));

    let first: Promise<void> | undefined;
    await act(async () => {
      first = mockCapturedChromeRefresh!({ silent: true, onCommitted });
      await flushMicrotasks();
      // Newer fetch comes back empty (transient rewrite) -> empty re-check scheduled.
      await mockCapturedChromeRefresh!({ silent: true });
      await flushMicrotasks();
    });
    expect(mockRenderedRows).toEqual(['initial']);

    await act(async () => {
      older.resolve(mkPaginated([row('older')]));
      await first;
      await flushMicrotasks();
    });
    // Not called with the pre-rewrite rows.
    expect(onCommitted).not.toHaveBeenCalled();

    mockGetData.mockResolvedValueOnce(mkPaginated([row('after')]));
    await act(async () => {
      jest.advanceTimersByTime(EMPTY_DATA_RECHECK_MS);
      await flushMicrotasks();
    });
    expect(mockRenderedRows).toEqual(['after']);
    expect(onCommitted).toHaveBeenCalledTimes(1);
    expect(onCommitted.mock.calls[0][0].rows.map((r) => r.row_name)).toEqual(['after']);
  });

  it('ignores a structural response (schema/status) for the project the user switched away from', async () => {
    mockGetSchema.mockImplementation((sid: string) =>
      Promise.resolve({ query: sid, schema: [{ name: 'colA', definition: 'A', rationale: 'R' }] }),
    );
    mockGetData.mockResolvedValue(mkPaginated([row('r')]));
    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });
    expect(mockRenderedSchemaQuery).toBe('session-1');

    const staleStatus = deferred<ScheMatiQStatus>();
    mockGetStatus.mockReturnValueOnce(staleStatus.promise);
    await act(async () => {
      emitWs({ type: 'schema_updated', data: {} } as WebSocketMessage);
      await flushMicrotasks();
    });

    mockSessionId = 'session-2';
    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });
    expect(mockRenderedSchemaQuery).toBe('session-2');

    await act(async () => {
      staleStatus.resolve({
        session_id: 'session-1',
        status: 'completed',
        progress: 1,
        current_step: 'Completed',
        steps_completed: 1,
        total_steps: 1,
      });
      await flushMicrotasks();
    });
    expect(mockRenderedSchemaQuery).toBe('session-2');
  });
});
