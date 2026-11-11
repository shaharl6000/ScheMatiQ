/**
 * Unit tests for selectedCellScope (the "Wrong, try again" selection
 * resolver), emptyCellScope (the "Fill empty cells" resolver, for contrast),
 * and diffPaginatedData (turns a re-extraction's before/after data snapshots
 * into one undoable command).
 *
 * '@/' is not mapped in this project's jest config (see websocket.test.ts),
 * so the one runtime '@/' import helpers.ts pulls in (transitively, via
 * ./constants) is registered as a virtual mock.
 */

jest.mock(
  '@/components/AdvancedSettings/AdvancedSettingsFields',
  () => ({
    observationUnitFromValue: jest.fn(),
    retrieverIsCustomized: jest.fn(),
    DEFAULT_ADVANCED_SETTINGS: {},
  }),
  { virtual: true },
);

import {
  chainDataCommittedCallbacks,
  consumeSchemaSpareRowForCreate,
  createEmptySchemaSpareRowState,
  diffPaginatedData,
  emptyCellScope,
  finishSchemaSpareRowCreate,
  hasSchemaSpareRowDraft,
  recordSchemaSpareRowEdit,
  rollbackSchemaSpareRowCreate,
  selectedCellScope,
} from './helpers';
import type { SheetColumn } from './types';
import type { ColumnInfo, DataRow, PaginatedData } from '@/types';

const sheetColumns: SheetColumn[] = [
  { key: '_row_name' } as SheetColumn,
  { key: 'colA' } as SheetColumn,
  { key: 'colB' } as SheetColumn,
];

const schemaColumns: ColumnInfo[] = [
  { name: 'colA' } as ColumnInfo,
  { name: 'colB' } as ColumnInfo,
];

const dataRows = [
  { _row_name: 'row1', colA: 'filled', colB: '' },
  { _row_name: 'row2', colA: '', colB: 'filled' },
];

describe('selectedCellScope', () => {
  it('returns null for a multi-range (disjoint) selection', () => {
    const selection = [
      [0, 1, 0, 1],
      [1, 2, 1, 2],
    ];
    expect(selectedCellScope(selection, dataRows, sheetColumns, schemaColumns)).toBeNull();
  });

  it('returns the full rectangle for a single range, including already-filled cells', () => {
    const selection = [[0, 1, 1, 2]]; // rows 0-1, cols 1-2 (colA, colB)
    const scope = selectedCellScope(selection, dataRows, sheetColumns, schemaColumns);
    expect(scope).not.toBeNull();
    expect(new Set(scope!.rows)).toEqual(new Set(['row1', 'row2']));
    expect(new Set(scope!.columns)).toEqual(new Set(['colA', 'colB']));
  });

  it('excludes provenance columns like emptyCellScope does', () => {
    const selection = [[0, 0, 1, 2]]; // includes the _row_name column (index 0)
    const scope = selectedCellScope(selection, dataRows, sheetColumns, schemaColumns);
    expect(scope).not.toBeNull();
    expect(scope!.columns).not.toContain('_row_name');
  });

  it('returns null when nothing real is selected', () => {
    const selection: number[][] = [];
    expect(selectedCellScope(selection, dataRows, sheetColumns, schemaColumns)).toBeNull();
  });

  it('includes previousValue for a true single-cell selection', () => {
    const selection = [[0, 1, 0, 1]]; // row1, colA only ("filled")
    const scope = selectedCellScope(selection, dataRows, sheetColumns, schemaColumns);
    expect(scope).toEqual({ rows: ['row1'], columns: ['colA'], previousValue: 'filled' });
  });

  it('leaves previousValue undefined for a multi-cell selection', () => {
    const selection = [[0, 1, 1, 2]]; // rows 0-1, cols 1-2
    const scope = selectedCellScope(selection, dataRows, sheetColumns, schemaColumns);
    expect(scope!.previousValue).toBeUndefined();
  });
});

describe('emptyCellScope (unchanged, for contrast)', () => {
  it('skips already-filled cells within a single range', () => {
    const selection = [[0, 1, 1, 2]];
    const scope = emptyCellScope(selection, dataRows, sheetColumns, schemaColumns);
    expect(scope).not.toBeNull();
    // colA is filled on row1 and colB is filled on row2 -- only the blank
    // (row1, colB) and (row2, colA) pairs contribute.
    expect(new Set(scope!.rows)).toEqual(new Set(['row1', 'row2']));
    expect(new Set(scope!.columns)).toEqual(new Set(['colA', 'colB']));
  });
});

// Diffs a reextraction's before/after data snapshots into one undoable
// command (index.tsx's onReextractionSettled). Also exercises the raw
// (verbatim cell object) field applyCellUpdates uses to route a replay
// through restoreCell instead of updateCell, so undo/redo never stamps
// manually_edited on a value the user never typed.
describe('diffPaginatedData', () => {
  const mkData = (rows: DataRow[]): PaginatedData => ({
    rows, total_count: rows.length, page: 0, page_size: rows.length, has_more: false,
  });

  it('returns no changes for identical snapshots', () => {
    const before = mkData([{ row_name: 'r1', data: { colA: { answer: 'x', excerpts: [] } } }]);
    const after = mkData([{ row_name: 'r1', data: { colA: { answer: 'x', excerpts: [] } } }]);
    expect(diffPaginatedData(before, after)).toEqual({ updates: [], inverseUpdates: [] });
  });

  it('diffs a changed cell into one forward/inverse pair, carrying the raw cell object', () => {
    const beforeCell = { answer: 'old', excerpts: [{ text: 'grounded' }] };
    const afterCell = { answer: 'new', excerpts: [] };
    const before = mkData([{ row_name: 'r1', _source_document: 'doc1', data: { colA: beforeCell } }]);
    const after = mkData([{ row_name: 'r1', _source_document: 'doc1', data: { colA: afterCell } }]);

    const { updates, inverseUpdates } = diffPaginatedData(before, after);

    expect(updates).toEqual([
      { rowName: 'r1', sourceDocument: 'doc1', rowIndexId: undefined, column: 'colA', value: 'new', raw: afterCell },
    ]);
    expect(inverseUpdates).toEqual([
      { rowName: 'r1', sourceDocument: 'doc1', rowIndexId: undefined, column: 'colA', value: 'old', raw: beforeCell },
    ]);
  });

  it('coerces a genuinely absent cell to raw: null, not undefined', () => {
    // colA had no value at all before this reextraction filled it -- the
    // bug this guards: leaving raw undefined here made applyCellUpdates fall
    // through to updateCell (stamping manually_edited=True) instead of
    // restoreCell when undoing back to "no value".
    const before = mkData([{ row_name: 'r1', data: {} }]);
    const after = mkData([{ row_name: 'r1', data: { colA: { answer: 'filled', excerpts: [] } } }]);

    const { inverseUpdates } = diffPaginatedData(before, after);

    expect(inverseUpdates).toHaveLength(1);
    expect(inverseUpdates[0].value).toBe('');
    expect(inverseUpdates[0].raw).toBeNull();
    expect(inverseUpdates[0].raw).not.toBeUndefined();
  });

  it('matches rows by _row_index in preference to row_name', () => {
    // Two rows share a row_name; _row_index disambiguates which one changed.
    const before = mkData([
      { row_name: 'dup', _row_index: 0, data: { colA: { answer: 'a0', excerpts: [] } } },
      { row_name: 'dup', _row_index: 1, data: { colA: { answer: 'a1', excerpts: [] } } },
    ]);
    const after = mkData([
      { row_name: 'dup', _row_index: 0, data: { colA: { answer: 'a0', excerpts: [] } } },
      { row_name: 'dup', _row_index: 1, data: { colA: { answer: 'CHANGED', excerpts: [] } } },
    ]);

    const { updates } = diffPaginatedData(before, after);

    expect(updates).toHaveLength(1);
    expect(updates[0]).toMatchObject({ rowIndexId: 1, value: 'CHANGED' });
  });

  it('skips a row in `after` that has no match in `before` (a new row, not an overwrite)', () => {
    const before = mkData([{ row_name: 'r1', data: { colA: { answer: 'x', excerpts: [] } } }]);
    const after = mkData([
      { row_name: 'r1', data: { colA: { answer: 'x', excerpts: [] } } },
      { row_name: 'r2', data: { colA: { answer: 'brand new row', excerpts: [] } } },
    ]);

    expect(diffPaginatedData(before, after)).toEqual({ updates: [], inverseUpdates: [] });
  });
});

describe('schemaSpareRowState lifecycle', () => {
  it('preserves draft edits typed before name even when live grid cells were cleared by a refresh', () => {
    let state = createEmptySchemaSpareRowState();
    expect(hasSchemaSpareRowDraft(state)).toBe(false);

    state = recordSchemaSpareRowEdit(state, 'definition', 'Patient age at baseline');
    state = recordSchemaSpareRowEdit(state, 'rationale', 'Needed for subgroup analysis');
    state = recordSchemaSpareRowEdit(state, 'allowed_values', 'adult, pediatric');
    state = recordSchemaSpareRowEdit(state, 'auto_expand_threshold', '3');
    expect(hasSchemaSpareRowDraft(state)).toBe(true);

    // Simulate a background refresh having wiped the live DOM row (readLiveCell returns '')
    const consumed = consumeSchemaSpareRowForCreate(state, '  age_group  ', () => '');
    expect(consumed).not.toBeNull();
    expect(consumed!.addColumnPayload).toEqual({
      name: 'age_group',
      definition: 'Patient age at baseline',
      rationale: 'Needed for subgroup analysis',
      allowed_values: ['adult', 'pediatric'],
    });
    expect(consumed!.autoExpandThreshold).toBe(3);
    expect(consumed!.nextState.creatingColumnName).toBe('age_group');
    expect(hasSchemaSpareRowDraft(consumed!.nextState)).toBe(false);
  });

  it('queues follow-up edits made on the spare row while addColumn is still in flight', () => {
    let state = createEmptySchemaSpareRowState();
    const consumed = consumeSchemaSpareRowForCreate(state, 'new_col');
    expect(consumed).not.toBeNull();
    state = consumed!.nextState;

    // User tabs to definition and rationale while addColumn is still in flight
    state = recordSchemaSpareRowEdit(state, 'definition', 'Typed while in flight');
    state = recordSchemaSpareRowEdit(state, 'rationale', 'Also typed while in flight');

    const finished = finishSchemaSpareRowCreate(state);
    expect(finished.columnName).toBe('new_col');
    expect(finished.queuedEdits).toEqual({
      definition: 'Typed while in flight',
      rationale: 'Also typed while in flight',
    });
    expect(finished.nextState).toEqual(createEmptySchemaSpareRowState());
  });

  it('restores spare-row draft (including any queued edits) when addColumn fails', () => {
    let state = createEmptySchemaSpareRowState();
    state = recordSchemaSpareRowEdit(state, 'definition', 'Initial definition');
    state = recordSchemaSpareRowEdit(state, 'rationale', 'Initial rationale');
    state = recordSchemaSpareRowEdit(state, 'allowed_values', 'yes, no');
    state = recordSchemaSpareRowEdit(state, 'auto_expand_threshold', '4');

    const consumed = consumeSchemaSpareRowForCreate(state, 'outcome');
    expect(consumed).not.toBeNull();
    state = consumed!.nextState;

    // User also edited rationale while addColumn was in flight before it failed
    state = recordSchemaSpareRowEdit(state, 'rationale', 'Updated rationale while in flight');

    const rolledBack = rollbackSchemaSpareRowCreate(
      state,
      consumed!.addColumnPayload,
      consumed!.autoExpandThreshold,
    );
    expect(rolledBack.creatingColumnName).toBeNull();
    expect(rolledBack.queuedEdits).toEqual({});
    expect(rolledBack.draft).toEqual({
      definition: 'Initial definition',
      rationale: 'Updated rationale while in flight',
      allowed_values: 'yes, no',
      auto_expand_threshold: '4',
    });
    expect(hasSchemaSpareRowDraft(rolledBack)).toBe(true);
  });
});

describe('chainDataCommittedCallbacks', () => {
  const sampleData: PaginatedData = {
    rows: [{ row_name: 'r1', data: { colA: { answer: 'val', excerpts: [] } } }],
    total_count: 1,
    page: 0,
    page_size: 50,
    has_more: false,
  };

  it('returns undefined when both pending and incoming are undefined', () => {
    expect(chainDataCommittedCallbacks(undefined, undefined)).toBeUndefined();
  });

  it('returns pending when incoming is undefined', () => {
    const pending = jest.fn();
    expect(chainDataCommittedCallbacks(pending, undefined)).toBe(pending);
  });

  it('returns incoming when pending is undefined', () => {
    const incoming = jest.fn();
    expect(chainDataCommittedCallbacks(undefined, incoming)).toBe(incoming);
  });

  it('deduplicates when pending === incoming so it is invoked only once', () => {
    const same = jest.fn();
    const chained = chainDataCommittedCallbacks(same, same);
    expect(chained).toBe(same);
    chained!(sampleData);
    expect(same).toHaveBeenCalledTimes(1);
    expect(same).toHaveBeenCalledWith(sampleData);
  });

  it('invokes two distinct callbacks in order (pending, incoming) with committed PaginatedData', () => {
    const order: string[] = [];
    const pending = jest.fn((data: PaginatedData) => {
      order.push(`pending:${data.total_count}`);
    });
    const incoming = jest.fn((data: PaginatedData) => {
      order.push(`incoming:${data.total_count}`);
    });

    const chained = chainDataCommittedCallbacks(pending, incoming);
    expect(chained).toBeDefined();
    chained!(sampleData);

    expect(pending).toHaveBeenCalledTimes(1);
    expect(pending).toHaveBeenCalledWith(sampleData);
    expect(incoming).toHaveBeenCalledTimes(1);
    expect(incoming).toHaveBeenCalledWith(sampleData);
    expect(order).toEqual(['pending:1', 'incoming:1']);
  });
});

