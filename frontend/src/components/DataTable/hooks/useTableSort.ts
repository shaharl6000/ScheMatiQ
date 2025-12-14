import { useState, useCallback, useEffect } from 'react';
import { SortState, SortDirection } from '../types/filters';

interface UseTableSortOptions {
  sessionId: string;
  persistKey?: string;
}

interface UseTableSortReturn {
  sortState: SortState;
  toggleSort: (columnName: string, multiSort?: boolean) => void;
  setSortState: (state: SortState) => void;
  clearSort: () => void;
  getSortDirection: (columnName: string) => SortDirection | null;
  getSortPriority: (columnName: string) => number | null;
  isSorted: (columnName: string) => boolean;
}

const STORAGE_KEY_PREFIX = 'dataTable_sort_';

export function useTableSort({ sessionId, persistKey }: UseTableSortOptions): UseTableSortReturn {
  const storageKey = persistKey || `${STORAGE_KEY_PREFIX}${sessionId}`;

  // Initialize from localStorage if available
  const [sortState, setSortStateInternal] = useState<SortState>(() => {
    try {
      const stored = localStorage.getItem(storageKey);
      if (stored) {
        return JSON.parse(stored);
      }
    } catch {
      // Ignore localStorage errors
    }
    return { columns: [] };
  });

  // Persist to localStorage when state changes
  useEffect(() => {
    try {
      if (sortState.columns.length > 0) {
        localStorage.setItem(storageKey, JSON.stringify(sortState));
      } else {
        localStorage.removeItem(storageKey);
      }
    } catch {
      // Ignore localStorage errors
    }
  }, [sortState, storageKey]);

  // Toggle sort for a column
  const toggleSort = useCallback((columnName: string, multiSort = false) => {
    setSortStateInternal(prev => {
      const existingIndex = prev.columns.findIndex(c => c.column === columnName);
      const existingColumn = prev.columns[existingIndex];

      if (multiSort) {
        // Multi-sort mode (Shift+click)
        if (existingColumn) {
          // Cycle through: asc -> desc -> remove
          if (existingColumn.direction === 'asc') {
            const newColumns = [...prev.columns];
            newColumns[existingIndex] = { ...existingColumn, direction: 'desc' };
            return { columns: newColumns };
          } else {
            // Remove this column from sort
            const newColumns = prev.columns.filter(c => c.column !== columnName);
            // Re-assign priorities
            return {
              columns: newColumns.map((c, i) => ({ ...c, priority: i + 1 }))
            };
          }
        } else {
          // Add new column to sort
          return {
            columns: [
              ...prev.columns,
              {
                column: columnName,
                direction: 'asc',
                priority: prev.columns.length + 1
              }
            ]
          };
        }
      } else {
        // Single sort mode (regular click)
        if (existingColumn) {
          // Cycle through: asc -> desc -> unsorted
          if (existingColumn.direction === 'asc') {
            return {
              columns: [{
                column: columnName,
                direction: 'desc',
                priority: 1
              }]
            };
          } else {
            // Remove sort
            return { columns: [] };
          }
        } else {
          // Start fresh with this column ascending
          return {
            columns: [{
              column: columnName,
              direction: 'asc',
              priority: 1
            }]
          };
        }
      }
    });
  }, []);

  const setSortState = useCallback((state: SortState) => {
    setSortStateInternal(state);
  }, []);

  const clearSort = useCallback(() => {
    setSortStateInternal({ columns: [] });
  }, []);

  const getSortDirection = useCallback((columnName: string): SortDirection | null => {
    const column = sortState.columns.find(c => c.column === columnName);
    return column?.direction ?? null;
  }, [sortState.columns]);

  const getSortPriority = useCallback((columnName: string): number | null => {
    const column = sortState.columns.find(c => c.column === columnName);
    return column?.priority ?? null;
  }, [sortState.columns]);

  const isSorted = useCallback((columnName: string): boolean => {
    return sortState.columns.some(c => c.column === columnName);
  }, [sortState.columns]);

  return {
    sortState,
    toggleSort,
    setSortState,
    clearSort,
    getSortDirection,
    getSortPriority,
    isSorted,
  };
}
