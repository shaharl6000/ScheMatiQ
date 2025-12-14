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

export function useColumnVisibility({
  sessionId,
  columns,
  persistKey
}: UseColumnVisibilityOptions): UseColumnVisibilityReturn {
  const storageKey = persistKey || `${STORAGE_KEY_PREFIX}${sessionId}`;

  // Initialize from localStorage or default all to visible
  const [visibility, setVisibilityInternal] = useState<ColumnVisibilityState>(() => {
    try {
      const stored = localStorage.getItem(storageKey);
      if (stored) {
        const parsed = JSON.parse(stored);
        // Merge stored visibility with current columns (new columns default to visible)
        const merged: ColumnVisibilityState = {};
        columns.forEach(col => {
          merged[col] = parsed[col] !== undefined ? parsed[col] : true;
        });
        return merged;
      }
    } catch {
      // Ignore localStorage errors
    }
    // Default all columns to visible
    const initial: ColumnVisibilityState = {};
    columns.forEach(col => {
      initial[col] = true;
    });
    return initial;
  });

  // Update visibility when columns change (new columns default to visible)
  useEffect(() => {
    setVisibilityInternal(prev => {
      const updated: ColumnVisibilityState = {};
      columns.forEach(col => {
        updated[col] = prev[col] !== undefined ? prev[col] : true;
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
