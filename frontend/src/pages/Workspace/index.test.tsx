import React from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { act as legacyAct } from 'react-dom/test-utils';
import { MemoryRouter, Route, Routes, useNavigate } from 'react-router-dom';

import type { PaginatedData, ScheMatiQStatus, SchemaData, VisualizationSession } from '@/types';

const mockToast = jest.fn();

const mockGetConfig = jest.fn();
const mockSchematiqGetStatus = jest.fn();
const mockSchematiqGetSchema = jest.fn();
const mockSchematiqGetConfig = jest.fn();
const mockSchematiqGetData = jest.fn();
const mockLoadGetSession = jest.fn();
const mockLoadGetData = jest.fn();
const mockUnitsGetDocuments = jest.fn();

jest.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: mockToast }),
}));

jest.mock('@/components/ui/tooltip', () => ({
  Tooltip: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  TooltipTrigger: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  TooltipContent: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

jest.mock('@/services/api', () => ({
  __esModule: true,
  default: { get: jest.fn() },
  configAPI: {
    getConfig: (...args: unknown[]) => mockGetConfig(...args),
  },
  schematiqAPI: {
    getStatus: (...args: unknown[]) => mockSchematiqGetStatus(...args),
    getSchema: (...args: unknown[]) => mockSchematiqGetSchema(...args),
    getConfig: (...args: unknown[]) => mockSchematiqGetConfig(...args),
    getData: (...args: unknown[]) => mockSchematiqGetData(...args),
    estimateCost: jest.fn(),
  },
  loadAPI: {
    getSession: (...args: unknown[]) => mockLoadGetSession(...args),
    getData: (...args: unknown[]) => mockLoadGetData(...args),
    uploadFile: jest.fn(),
    parseFile: jest.fn(),
  },
  unitsAPI: {
    getDocuments: (...args: unknown[]) => mockUnitsGetDocuments(...args),
    getData: jest.fn(),
    attachSourceDocuments: jest.fn(),
    getFigureContentUrl: jest.fn(),
  },
  schemaAPI: {},
  describeSkippedAttachments: jest.fn(() => ''),
}));

jest.mock('@/services/websocket', () => ({
  __esModule: true,
  default: {
    addMessageHandler: jest.fn(),
    removeMessageHandler: jest.fn(),
    connect: jest.fn(),
    disconnect: jest.fn(),
  },
}));

jest.mock('@/utils/recentProjects', () => ({
  rememberProject: jest.fn(),
}));

let latestSilentRefresh: (() => void) | null = null;

jest.mock('./SpreadsheetChrome', () => ({
  SpreadsheetChrome: (props: { sessionStatus: string }) => (
    <div data-testid="chrome-status">{props.sessionStatus}</div>
  ),
}));

jest.mock('./SpreadsheetSurface', () => ({
  SpreadsheetSurface: (props: { onRefresh: () => void; sessionMissing?: boolean }) => {
    latestSilentRefresh = props.onRefresh;
    return <div data-testid="surface" data-session-missing={String(Boolean(props.sessionMissing))} />;
  },
}));

jest.mock('./chat/ChatPanel', () => ({
  ChatPanel: (props: { sessionMode: string }) => (
    <div data-testid="chat-panel" data-session-mode={props.sessionMode} />
  ),
}));

jest.mock('./NewProjectDialog', () => ({
  NewProjectDialog: () => null,
}));
jest.mock('./PendingRerunBanner', () => ({
  PendingRerunBanner: () => null,
}));
jest.mock('./CiteDialog', () => ({
  CiteDialog: () => null,
}));
jest.mock('./KeyboardShortcutsDialog', () => ({
  KeyboardShortcutsDialog: () => null,
}));
jest.mock('./ProjectDetailsDialog', () => ({
  ProjectDetailsDialog: () => null,
}));
jest.mock('./DataLoadErrorBanner', () => ({
  DataLoadErrorBanner: () => null,
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

import Workspace from './index';

const act: typeof legacyAct = (React as Record<string, any>).act || legacyAct;

const mkPaginatedData = (total = 2): PaginatedData => ({
  rows: Array.from({ length: total }, (_, idx) => ({
    row_name: `row-${idx + 1}`,
    data: { col1: { answer: 'val', excerpts: [] } },
  })),
  total_count: total,
  page: 0,
  page_size: 500,
  has_more: false,
});

const mkSchematiqStatus = (sessionId: string): ScheMatiQStatus => ({
  session_id: sessionId,
  status: 'completed',
  progress: 1,
  current_step: 'Completed',
  steps_completed: 2,
  total_steps: 2,
  schema_completed: true,
  columns_discovered: 1,
});

const mkSchema = (): SchemaData => ({
  query: 'Test query',
  schema: [{ name: 'col1', definition: 'Col 1' }],
});

const mkSession = (
  id: string,
  type: VisualizationSession['type'],
  status: VisualizationSession['status'] = 'completed',
): VisualizationSession => ({
  id,
  type,
  status,
  metadata: {
    source: 'test.csv',
    created: '2026-01-01T00:00:00Z',
    last_modified: '2026-01-01T00:00:00Z',
  },
  columns: [{ name: 'col1', definition: 'Col 1' }],
  schema_query: 'Test query',
});

async function flushPromises(ticks = 6): Promise<void> {
  for (let i = 0; i < ticks; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve();
  }
}

describe('Workspace sessionMode refresh & chromeStatus', () => {
  let container: HTMLDivElement;
  let root: Root;
  let navigateFn: ReturnType<typeof useNavigate> | null = null;

  function RouterProbe() {
    navigateFn = useNavigate();
    return <Workspace />;
  }

  async function mountAt(initialUrl: string): Promise<void> {
    await act(async () => {
      root.render(
        <MemoryRouter
          initialEntries={[initialUrl]}
          future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
        >
          <Routes>
            <Route path="/" element={<RouterProbe />} />
            <Route path="/workspace/:sessionId" element={<RouterProbe />} />
          </Routes>
        </MemoryRouter>,
      );
      await flushPromises();
    });
  }

  async function navigateTo(url: string): Promise<void> {
    await act(async () => {
      navigateFn?.(url);
      await flushPromises();
    });
  }

  beforeEach(() => {
    (globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: jest.fn().mockImplementation((query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: jest.fn(),
        removeListener: jest.fn(),
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
        dispatchEvent: jest.fn(),
      })),
    });
    jest.clearAllMocks();
    latestSilentRefresh = null;
    navigateFn = null;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);

    mockGetConfig.mockResolvedValue({ developer_mode: true });
    mockUnitsGetDocuments.mockResolvedValue({ documents: [], totalDocuments: 0 });
    mockSchematiqGetSchema.mockResolvedValue(mkSchema());
    mockSchematiqGetConfig.mockResolvedValue({ query: 'Test query' });
  });

  afterEach(async () => {
    await act(async () => {
      root.unmount();
    });
    container.remove();
  });

  it('runs refresh only once when a load-mode session promotes inline to schematiq', async () => {
    mockLoadGetSession.mockResolvedValue(mkSession('s-promoted', 'schematiq'));
    mockSchematiqGetStatus.mockResolvedValue(mkSchematiqStatus('s-promoted'));
    mockSchematiqGetData.mockResolvedValue(mkPaginatedData(3));

    await mountAt('/workspace/s-promoted?mode=load');

    // Initial load probe sees type='schematiq', promotes inline, and runs
    // fetchAsSchematiq() once — without re-triggering refresh() on state flip.
    expect(mockSchematiqGetStatus).toHaveBeenCalledTimes(1);
    expect(mockSchematiqGetData).toHaveBeenCalledTimes(1);
    expect(mockLoadGetData).not.toHaveBeenCalled();
    // 1 initial load-mode probe + 1 statsSession fetch inside fetchAsSchematiq
    expect(mockLoadGetSession).toHaveBeenCalledTimes(2);
    expect(container.querySelector('[data-testid="chat-panel"]')?.getAttribute('data-session-mode')).toBe('schematiq');
    expect(container.querySelector('[data-testid="chrome-status"]')?.textContent).toBe(
      'Extracted from documents · 3 rows · 1 column',
    );

    // Subsequent silent refresh goes straight to schematiq mode (no extra load-mode probe).
    mockLoadGetSession.mockClear();
    mockSchematiqGetStatus.mockClear();
    mockSchematiqGetData.mockClear();

    await act(async () => {
      latestSilentRefresh?.();
      await flushPromises();
    });

    expect(mockSchematiqGetStatus).toHaveBeenCalledTimes(1);
    expect(mockSchematiqGetData).toHaveBeenCalledTimes(1);
    expect(mockLoadGetSession).toHaveBeenCalledTimes(1);
  });

  it('runs refresh only once when a schematiq fetch falls back and demotes inline to load', async () => {
    mockSchematiqGetStatus.mockRejectedValue(new Error('404 Not Found'));
    mockLoadGetSession.mockResolvedValue(mkSession('s-upload', 'load'));
    mockLoadGetData.mockResolvedValue(mkPaginatedData(2));

    await mountAt('/workspace/s-upload');

    // Initial schematiq probe fails once, falls back to load mode inline once,
    // and does NOT fire a duplicate refresh() when sessionMode flips to 'load'.
    expect(mockSchematiqGetStatus).toHaveBeenCalledTimes(1);
    expect(mockLoadGetData).toHaveBeenCalledTimes(1);
    // 1 statsSession call in fetchAsSchematiq's Promise.all + 1 fallback call in catch
    expect(mockLoadGetSession).toHaveBeenCalledTimes(2);
    expect(container.querySelector('[data-testid="chat-panel"]')?.getAttribute('data-session-mode')).toBe('load');
    expect(container.querySelector('[data-testid="chrome-status"]')?.textContent).toBe(
      'Imported file · 2 rows · 1 column',
    );

    // Subsequent silent refresh reads sessionModeRef='load' directly without probing schematiq again.
    mockSchematiqGetStatus.mockClear();
    mockLoadGetSession.mockClear();
    mockLoadGetData.mockClear();

    await act(async () => {
      latestSilentRefresh?.();
      await flushPromises();
    });

    expect(mockSchematiqGetStatus).not.toHaveBeenCalled();
    expect(mockLoadGetSession).toHaveBeenCalledTimes(1);
    expect(mockLoadGetData).toHaveBeenCalledTimes(1);
  });

  it('switches from a schematiq session to a ?mode=load route without a stale schematiq fetch or double refresh', async () => {
    mockSchematiqGetStatus.mockImplementation(async (id: string) => mkSchematiqStatus(id));
    mockSchematiqGetData.mockResolvedValue(mkPaginatedData(2));
    mockLoadGetSession.mockImplementation(async (id: string) =>
      id === 'sessionA' ? mkSession('sessionA', 'schematiq') : mkSession(id, 'load'),
    );
    mockLoadGetData.mockResolvedValue(mkPaginatedData(4));

    await mountAt('/workspace/sessionA');

    expect(mockSchematiqGetStatus).toHaveBeenCalledTimes(1);
    expect(mockSchematiqGetStatus).toHaveBeenCalledWith('sessionA');

    mockSchematiqGetStatus.mockClear();
    mockSchematiqGetData.mockClear();
    mockLoadGetSession.mockClear();
    mockLoadGetData.mockClear();

    await navigateTo('/workspace/sessionB?mode=load');

    // Must NOT probe schematiqAPI.getStatus('sessionB') with stale sessionMode='schematiq',
    // and must fetch sessionB in load mode exactly once.
    expect(mockSchematiqGetStatus).not.toHaveBeenCalled();
    expect(mockSchematiqGetData).not.toHaveBeenCalled();
    expect(mockLoadGetSession).toHaveBeenCalledTimes(1);
    expect(mockLoadGetSession).toHaveBeenCalledWith('sessionB');
    expect(mockLoadGetData).toHaveBeenCalledTimes(1);
    expect(mockLoadGetData).toHaveBeenCalledWith('sessionB', 0, 500);
    expect(container.querySelector('[data-testid="chat-panel"]')?.getAttribute('data-session-mode')).toBe('load');
  });

  it('resets sessionModeRef to load when navigating from an inline-promoted ?mode=load session to another ?mode=load session', async () => {
    mockLoadGetSession.mockImplementation(async (id: string) =>
      id === 'promotedA' ? mkSession('promotedA', 'schematiq') : mkSession(id, 'load'),
    );
    mockSchematiqGetStatus.mockImplementation(async (id: string) => mkSchematiqStatus(id));
    mockSchematiqGetData.mockResolvedValue(mkPaginatedData(2));
    mockLoadGetData.mockResolvedValue(mkPaginatedData(5));

    await mountAt('/workspace/promotedA?mode=load');
    expect(container.querySelector('[data-testid="chat-panel"]')?.getAttribute('data-session-mode')).toBe('schematiq');

    mockSchematiqGetStatus.mockClear();
    mockLoadGetSession.mockClear();
    mockLoadGetData.mockClear();

    await navigateTo('/workspace/uploadB?mode=load');

    expect(mockSchematiqGetStatus).not.toHaveBeenCalled();
    expect(mockLoadGetSession).toHaveBeenCalledTimes(1);
    expect(mockLoadGetSession).toHaveBeenCalledWith('uploadB');
    expect(mockLoadGetData).toHaveBeenCalledTimes(1);
    expect(container.querySelector('[data-testid="chat-panel"]')?.getAttribute('data-session-mode')).toBe('load');
  });

  it('recomputes chromeStatus to "Project not found" when a silent refresh sets sessionMissing without toggling loading', async () => {
    mockSchematiqGetStatus.mockResolvedValueOnce(mkSchematiqStatus('s-gone'));
    mockSchematiqGetData.mockResolvedValueOnce(mkPaginatedData(2));
    mockLoadGetSession.mockResolvedValueOnce(mkSession('s-gone', 'schematiq'));

    await mountAt('/workspace/s-gone');

    expect(container.querySelector('[data-testid="chrome-status"]')?.textContent).toBe(
      'Extracted from documents · 2 rows · 1 column',
    );

    // Session disappears server-side; silent refresh fails both schematiq and load lookups
    // without touching `loading`, `status`, `schema`, or `data.total_count`.
    mockSchematiqGetStatus.mockRejectedValue(new Error('404 Not Found'));
    mockLoadGetSession.mockResolvedValue(null);

    await act(async () => {
      latestSilentRefresh?.();
      await flushPromises();
    });

    expect(container.querySelector('[data-testid="chrome-status"]')?.textContent).toBe('Project not found');
    expect(container.querySelector('[data-testid="surface"]')?.getAttribute('data-session-missing')).toBe('true');
  });
});
