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

import { diffPaginatedData, emptyCellScope, selectedCellScope } from './helpers';
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
