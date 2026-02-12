import { useState, useCallback, useEffect, useMemo } from 'react';
import { ColumnVisibilityState } from '../types/filters';

interface UseColumnVisibilityOptions {
  sessionId: string;
  columns: string[];
  persistKey?: string;
}

interface UseColumnVisibilityReturn {
  visibility: ColumnVisibilityState;
  toggleColumn: (columnName: string) => void;
  showColumn: (columnName: string) => void;
  hideColumn: (columnName: string) => void;
  showAllColumns: () => void;
  hideAllColumns: () => void;
  setVisibility: (state: ColumnVisibilityState) => void;
  isVisible: (columnName: string) => boolean;
  visibleColumns: string[];
  hiddenColumns: string[];
  visibleCount: number;
  hiddenCount: number;
}

const STORAGE_KEY_PREFIX = 'dataTable_visibility_';

// Columns that should be hidden by default
const DEFAULT_HIDDEN_COLUMNS = ['_papers', '_source_document'];

export function useColumnVisibility({
  sessionId,
  columns,
  persistKey
}: UseColumnVisibilityOptions): UseColumnVisibilityReturn {
  const storageKey = persistKey || `${STORAGE_KEY_PREFIX}${sessionId}`;

  // Initialize from localStorage or default visibility
  const [visibility, setVisibilityInternal] = useState<ColumnVisibilityState>(() => {
    try {
      const stored = localStorage.getItem(storageKey);
      if (stored) {
        const parsed = JSON.parse(stored);
        // Merge stored visibility with current columns (new columns use default visibility)
        const merged: ColumnVisibilityState = {};
        columns.forEach(col => {
          if (parsed[col] !== undefined) {
            merged[col] = parsed[col];
          } else {
            // New column - use default visibility (hidden if in DEFAULT_HIDDEN_COLUMNS)
            merged[col] = !DEFAULT_HIDDEN_COLUMNS.includes(col);
          }
        });
        return merged;
      }
    } catch {
      // Ignore localStorage errors
    }
    // Default visibility: all visible except DEFAULT_HIDDEN_COLUMNS
    const initial: ColumnVisibilityState = {};
    columns.forEach(col => {
      initial[col] = !DEFAULT_HIDDEN_COLUMNS.includes(col);
    });
    return initial;
  });

  // Update visibility when columns change (new columns use default visibility)
  useEffect(() => {
    setVisibilityInternal(prev => {
      const updated: ColumnVisibilityState = {};
      columns.forEach(col => {
        if (prev[col] !== undefined) {
          updated[col] = prev[col];
        } else {
          // New column - use default visibility (hidden if in DEFAULT_HIDDEN_COLUMNS)
          updated[col] = !DEFAULT_HIDDEN_COLUMNS.includes(col);
        }
      });
      return updated;
    });
  }, [columns]);

  // Persist to localStorage when state changes
  useEffect(() => {
    try {
      localStorage.setItem(storageKey, JSON.stringify(visibility));
    } catch {
      // Ignore localStorage errors
    }
  }, [visibility, storageKey]);

  const toggleColumn = useCallback((columnName: string) => {
    setVisibilityInternal(prev => ({
      ...prev,
      [columnName]: !prev[columnName]
    }));
  }, []);

  const showColumn = useCallback((columnName: string) => {
    setVisibilityInternal(prev => ({
      ...prev,
      [columnName]: true
    }));
  }, []);

  const hideColumn = useCallback((columnName: string) => {
    setVisibilityInternal(prev => ({
      ...prev,
      [columnName]: false
    }));
  }, []);

  const showAllColumns = useCallback(() => {
    setVisibilityInternal(prev => {
      const allVisible: ColumnVisibilityState = {};
      Object.keys(prev).forEach(col => {
        allVisible[col] = true;
      });
      return allVisible;
    });
  }, []);

  const hideAllColumns = useCallback(() => {
    setVisibilityInternal(prev => {
      const allHidden: ColumnVisibilityState = {};
      Object.keys(prev).forEach(col => {
        allHidden[col] = false;
      });
      return allHidden;
    });
  }, []);

  const setVisibility = useCallback((state: ColumnVisibilityState) => {
    setVisibilityInternal(state);
  }, []);

  const isVisible = useCallback((columnName: string): boolean => {
    return visibility[columnName] !== false;
  }, [visibility]);

  const visibleColumns = useMemo(() => {
    return columns.filter(col => visibility[col] !== false);
  }, [columns, visibility]);

  const hiddenColumns = useMemo(() => {
    return columns.filter(col => visibility[col] === false);
  }, [columns, visibility]);

  const visibleCount = useMemo(() => visibleColumns.length, [visibleColumns]);
  const hiddenCount = useMemo(() => hiddenColumns.length, [hiddenColumns]);

  return {
    visibility,
    toggleColumn,
    showColumn,
    hideColumn,
    showAllColumns,
    hideAllColumns,
    setVisibility,
    isVisible,
    visibleColumns,
    hiddenColumns,
    visibleCount,
    hiddenCount,
  };
}
