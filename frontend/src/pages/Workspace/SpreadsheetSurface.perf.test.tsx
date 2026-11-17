import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import type { PaginatedData, SchemaData } from '@/types';
import type { TableDisplayOptions } from './types';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const mockHotState = {
  renderCount: 0,
  latestProps: null as Record<string, any> | null,
  searchQuery: jest.fn(),
  updateSettings: jest.fn(),
  render: jest.fn(),
};

const mockHotInstance = {
  getSettings: () => ({ _initOnlySettings: [] as string[] }),
  getShortcutManager: () => ({ getContext: () => null }),
  getPlugin: (name: string) =>
    name === 'search' ? { isEnabled: () => true, query: mockHotState.searchQuery } : null,
  updateSettings: mockHotState.updateSettings,
  render: mockHotState.render,
  refreshDimensions: jest.fn(),
  selectCells: jest.fn(),
  countRows: () => 0,
  toPhysicalRow: (row: number) => row,
  toPhysicalColumn: (col: number) => col,
  getSelected: () => null,
  getActiveEditor: () => null,
  rootElement: document.createElement('div'),
};

jest.mock('react-router-dom', () => ({
  useParams: () => ({ sessionId: 'sess-1' }),
}));

jest.mock('@handsontable/react', () => {
  const ReactMod = require('react');
  const HotTable = ReactMod.forwardRef((props: Record<string, any>, ref: any) => {
    mockHotState.renderCount += 1;
    mockHotState.latestProps = props;
    ReactMod.useImperativeHandle(ref, () => ({ hotInstance: mockHotInstance }), []);
    return ReactMod.createElement('div', { 'data-testid': 'mock-hot-table' });
  });
  return { __esModule: true, HotTable };
});

jest.mock('handsontable/registry', () => ({
  registerAllModules: jest.fn(),
}));

jest.mock('handsontable/styles/handsontable.min.css', () => ({}));
jest.mock('handsontable/styles/ht-theme-main.min.css', () => ({}));

jest.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: jest.fn() }),
}));

jest.mock('@/services/api', () => ({
  __esModule: true,
  CELL_BATCH_SIZE: 100,
  observationUnitAPI: { update: jest.fn() },
  schemaAPI: { addColumn: jest.fn(), deleteColumn: jest.fn(), updateColumn: jest.fn() },
  schematiqAPI: { updateCellsBatch: jest.fn(), updateRow: jest.fn() },
}));

import { SpreadsheetSurface, type SheetSearchApi } from './SpreadsheetSurface';

const baseDisplayOptions: TableDisplayOptions = {
  fontFamily: 'Inter',
  fontSize: 12,
  bold: false,
  italic: false,
  underline: false,
  strikethrough: false,
  align: 'left',
};

const baseSchema: SchemaData = {
  query: 'q',
  schema: [
    { name: 'status', definition: 'Status of ruling', rationale: 'r' },
    { name: 'year', definition: 'Decision year', rationale: 'r' },
  ],
  observation_unit: { name: 'Case', definition: 'One court decision', example_names: [] },
};

const makeData = (count: number): PaginatedData => ({
  rows: Array.from({ length: count }, (_, idx) => ({
    row_name: `Unit ${idx + 1}`,
    _source_document: `doc_${idx + 1}.txt`,
    data: {
      status: { answer: 'affirmed', excerpts: [] },
      year: { answer: String(2020 + idx), excerpts: [] },
    },
  })),
  total_count: count,
  page: 0,
  page_size: 2000,
  has_more: false,
});

describe('SpreadsheetSurface render & search performance', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    mockHotState.renderCount = 0;
    mockHotState.latestProps = null;
    mockHotState.searchQuery.mockReset();
    mockHotState.updateSettings.mockReset();
    mockHotState.render.mockReset();
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    container.remove();
  });

  it('skips HotTable re-renders on parent callback churn and keeps columns/colHeaders stable when appending rows', () => {
    const hotTableRef = { current: null };
    const data1 = makeData(2);
    const cellFormats = {};

    const renderSurface = (data: PaginatedData) => (
      <SpreadsheetSurface
        activeSheet="data"
        data={data}
        schema={baseSchema}
        displayOptions={baseDisplayOptions}
        cellFormats={cellFormats}
        formatVersion={0}
        hotTableRef={hotTableRef}
        onSelectionChange={() => {}}
        onGroundingHighlight={() => {}}
        onFigureGrounding={() => {}}
        onRequestSourcePanel={() => {}}
        onGroundingScrollRequest={() => {}}
        onRefresh={() => {}}
        onSchemaRefresh={() => {}}
        onRefreshData={() => {}}
        onOptimisticCellEdit={() => {}}
        onEditFollowUp={() => {}}
        onEditEnd={() => {}}
        onFillEmptyCells={() => {}}
        onRetryWrongCells={() => {}}
        onToggleFormatShortcut={() => {}}
        onUndo={() => {}}
        onRedo={() => {}}
        onRecordEdit={() => {}}
        layoutRevision="rev-1"
        dataView="by_unit"
      />
    );

    act(() => {
      root.render(renderSurface(data1));
    });

    const initialRenderCount = mockHotState.renderCount;
    expect(initialRenderCount).toBeGreaterThanOrEqual(1);
    const initialColumns = mockHotState.latestProps?.columns;
    const initialColHeaders = mockHotState.latestProps?.colHeaders;
    expect(Array.isArray(initialColumns)).toBe(true);
    expect(Array.isArray(initialColHeaders)).toBe(true);

    // Re-render with brand-new parent callback identities while data/schema/size are unchanged.
    act(() => {
      root.render(renderSurface(data1));
    });
    expect(mockHotState.renderCount).toBe(initialRenderCount);

    // Appending a row updates `data` without rebuilding `columns` or `colHeaders`.
    const data2 = makeData(3);
    act(() => {
      root.render(renderSurface(data2));
    });
    expect(mockHotState.renderCount).toBe(initialRenderCount + 1);
    expect(mockHotState.latestProps?.columns).toBe(initialColumns);
    expect(mockHotState.latestProps?.colHeaders).toBe(initialColHeaders);
  });

  it('skips redundant search.query scans across scroll beforeViewRender calls until invalidated or term changes', () => {
    const hotTableRef = { current: null };
    const searchTermRef: { current: string | null } = { current: 'alpha' };
    const data = makeData(2);
    const cellFormats = {};

    act(() => {
      root.render(
        <SpreadsheetSurface
          activeSheet="data"
          data={data}
          schema={baseSchema}
          displayOptions={baseDisplayOptions}
          cellFormats={cellFormats}
          formatVersion={0}
          hotTableRef={hotTableRef}
          searchTermRef={searchTermRef}
          onSelectionChange={jest.fn()}
          onRefresh={jest.fn()}
          onSchemaRefresh={jest.fn()}
          onRefreshData={jest.fn()}
          onOptimisticCellEdit={jest.fn()}
          onEditFollowUp={jest.fn()}
          onEditEnd={jest.fn()}
          layoutRevision="rev-1"
          dataView="by_unit"
        />,
      );
    });

    const props = mockHotState.latestProps!;
    expect(mockHotState.searchQuery).not.toHaveBeenCalled();

    // First paint runs search.query once; repeated scroll frames skip it.
    props.beforeViewRender();
    props.beforeViewRender();
    props.beforeViewRender();
    expect(mockHotState.searchQuery).toHaveBeenCalledTimes(1);
    expect(mockHotState.searchQuery).toHaveBeenLastCalledWith('alpha');

    // Changing the term re-runs once.
    searchTermRef.current = 'beta';
    props.beforeViewRender();
    props.beforeViewRender();
    expect(mockHotState.searchQuery).toHaveBeenCalledTimes(2);
    expect(mockHotState.searchQuery).toHaveBeenLastCalledWith('beta');

    // Settings reset / filter invalidation marks search dirty so next beforeViewRender re-runs once.
    props.afterUpdateSettings();
    props.beforeViewRender();
    props.beforeViewRender();
    expect(mockHotState.searchQuery).toHaveBeenCalledTimes(3);

    props.afterFilter();
    props.beforeViewRender();
    expect(mockHotState.searchQuery).toHaveBeenCalledTimes(4);
  });

  it('runs a single search.query scan for a new Find term, not one more on the following render', () => {
    const hotTableRef = { current: null };
    const searchTermRef: { current: string | null } = { current: null };
    const searchApiRef: { current: SheetSearchApi | null } = { current: null };
    mockHotState.searchQuery.mockReturnValue([{ row: 0, col: 0 }]);

    act(() => {
      root.render(
        <SpreadsheetSurface
          activeSheet="data"
          data={makeData(2)}
          schema={baseSchema}
          displayOptions={baseDisplayOptions}
          cellFormats={{}}
          formatVersion={0}
          hotTableRef={hotTableRef}
          searchTermRef={searchTermRef}
          searchApiRef={searchApiRef}
          onSelectionChange={jest.fn()}
          onRefresh={jest.fn()}
          onSchemaRefresh={jest.fn()}
          onRefreshData={jest.fn()}
          onOptimisticCellEdit={jest.fn()}
          onEditFollowUp={jest.fn()}
          onEditEnd={jest.fn()}
          layoutRevision="rev-1"
          dataView="by_unit"
        />,
      );
    });

    const props = mockHotState.latestProps!;
    mockHotState.searchQuery.mockClear();

    const results = searchApiRef.current!.find('gamma');
    expect(results).toEqual([{ row: 0, col: 0 }]);
    expect(searchTermRef.current).toBe('gamma');
    // The render that find() triggers, plus later scroll frames, reuse the scan.
    props.beforeViewRender();
    props.beforeViewRender();
    expect(mockHotState.searchQuery).toHaveBeenCalledTimes(1);
    expect(mockHotState.searchQuery).toHaveBeenLastCalledWith('gamma');

    // Unmounting releases the entry point.
    act(() => {
      root.render(<div />);
    });
    expect(searchApiRef.current).toBeNull();
  });
});
