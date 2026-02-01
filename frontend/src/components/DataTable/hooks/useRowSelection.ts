import { useState, useCallback, useMemo } from 'react';

export interface UseRowSelectionOptions {
  /** Total number of rows across all pages (for "select all" functionality) */
  totalRows?: number;
}

export interface UseRowSelectionReturn {
  /** Set of selected row identifiers */
  selectedRows: Set<string>;
  /** Whether all rows on current page are selected */
  isAllPageSelected: boolean;
  /** Whether some (but not all) rows on current page are selected */
  isIndeterminate: boolean;
  /** Toggle selection for a single row */
  toggleRow: (rowId: string) => void;
  /** Toggle selection for all rows on current page */
  toggleAllPage: (pageRowIds: string[]) => void;
  /** Select multiple rows (for shift+click range selection) */
  selectRange: (rowIds: string[]) => void;
  /** Clear all selections */
  clearSelection: () => void;
  /** Check if a specific row is selected */
  isSelected: (rowId: string) => boolean;
  /** Number of selected rows */
  selectedCount: number;
}

export function useRowSelection(
  pageRowIds: string[],
  _options?: UseRowSelectionOptions
): UseRowSelectionReturn {
  const [selectedRows, setSelectedRows] = useState<Set<string>>(new Set());

  const toggleRow = useCallback((rowId: string) => {
    setSelectedRows(prev => {
      const next = new Set(prev);
      if (next.has(rowId)) {
        next.delete(rowId);
      } else {
        next.add(rowId);
      }
      return next;
    });
  }, []);

  const toggleAllPage = useCallback((pageRowIds: string[]) => {
    setSelectedRows(prev => {
      const allSelected = pageRowIds.every(id => prev.has(id));
      const next = new Set(prev);

      if (allSelected) {
        // Deselect all on page
        pageRowIds.forEach(id => next.delete(id));
      } else {
        // Select all on page
        pageRowIds.forEach(id => next.add(id));
      }

      return next;
    });
  }, []);

  const selectRange = useCallback((rowIds: string[]) => {
    setSelectedRows(prev => {
      const next = new Set(prev);
      rowIds.forEach(id => next.add(id));
      return next;
    });
  }, []);

  const clearSelection = useCallback(() => {
    setSelectedRows(new Set());
  }, []);

  const isSelected = useCallback((rowId: string) => {
    return selectedRows.has(rowId);
  }, [selectedRows]);

  const isAllPageSelected = useMemo(() => {
    if (pageRowIds.length === 0) return false;
    return pageRowIds.every(id => selectedRows.has(id));
  }, [pageRowIds, selectedRows]);

  const isIndeterminate = useMemo(() => {
    if (pageRowIds.length === 0) return false;
    const selectedOnPage = pageRowIds.filter(id => selectedRows.has(id)).length;
    return selectedOnPage > 0 && selectedOnPage < pageRowIds.length;
  }, [pageRowIds, selectedRows]);

  const selectedCount = selectedRows.size;

  return {
    selectedRows,
    isAllPageSelected,
    isIndeterminate,
    toggleRow,
    toggleAllPage,
    selectRange,
    clearSelection,
    isSelected,
    selectedCount,
  };
}
