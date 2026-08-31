/**
 * Unit tests for selectedCellScope (the "Wrong, try again" selection
 * resolver) and, for contrast, emptyCellScope (the existing "Fill empty
 * cells" resolver).
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

import { emptyCellScope, selectedCellScope } from './helpers';
import type { SheetColumn } from './types';
import type { ColumnInfo } from '@/types';

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
