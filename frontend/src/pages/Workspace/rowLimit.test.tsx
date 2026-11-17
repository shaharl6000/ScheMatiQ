import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import type { DataRow, PaginatedData, ScheMatiQStatus, SchemaData, WebSocketMessage } from '@/types';
import type { CellUpdate } from './types';

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


const row = (name: string): DataRow => ({ row_name: name, _row_index: 0, data: {} });

describe('Workspace row limit banner', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
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
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    container.remove();
  });

  it('tells the user when the grid holds only part of the rows', async () => {
    mockGetData.mockResolvedValue({
      ...mkPaginated([row('a'), row('b')]),
      total_count: 4321,
      has_more: true,
    });
    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });
    const banner = container.querySelector('[data-testid="row-limit-banner"]');
    expect(banner).not.toBeNull();
    expect(banner!.textContent).toContain('Showing the first 2 of 4,321 rows');
  });

  it('shows no banner when every row is loaded', async () => {
    mockGetData.mockResolvedValue(mkPaginated([row('a'), row('b')]));
    await act(async () => {
      root.render(<Workspace />);
      await flushMicrotasks();
    });
    expect(container.querySelector('[data-testid="row-limit-banner"]')).toBeNull();
  });
});
