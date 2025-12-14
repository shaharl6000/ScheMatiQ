import { useState, useCallback, useEffect, useMemo } from 'react';
import { FilterRule, FilterState, generateFilterId } from '../types/filters';

interface UseTableFilterOptions {
  sessionId: string;
  persistKey?: string;
}

interface UseTableFilterReturn {
  filterState: FilterState;
  addFilter: (filter: Omit<FilterRule, 'id'>) => void;
  updateFilter: (id: string, updates: Partial<Omit<FilterRule, 'id'>>) => void;
  removeFilter: (id: string) => void;
  clearFilters: () => void;
  setFilterState: (state: FilterState) => void;
  getFiltersForColumn: (columnName: string) => FilterRule[];
  hasFilterForColumn: (columnName: string) => boolean;
  activeFilterCount: number;
}

const STORAGE_KEY_PREFIX = 'dataTable_filter_';

export function useTableFilter({ sessionId, persistKey }: UseTableFilterOptions): UseTableFilterReturn {
  const storageKey = persistKey || `${STORAGE_KEY_PREFIX}${sessionId}`;

  // Initialize from localStorage if available
  const [filterState, setFilterStateInternal] = useState<FilterState>(() => {
    try {
      const stored = localStorage.getItem(storageKey);
      if (stored) {
        return JSON.parse(stored);
      }
    } catch {
      // Ignore localStorage errors
    }
    return { rules: [] };
  });

  // Persist to localStorage when state changes
  useEffect(() => {
    try {
      if (filterState.rules.length > 0) {
        localStorage.setItem(storageKey, JSON.stringify(filterState));
      } else {
        localStorage.removeItem(storageKey);
      }
    } catch {
      // Ignore localStorage errors
    }
  }, [filterState, storageKey]);

  // Add a new filter rule
  const addFilter = useCallback((filter: Omit<FilterRule, 'id'>) => {
    const newRule: FilterRule = {
      ...filter,
      id: generateFilterId(),
    };
    setFilterStateInternal(prev => ({
      rules: [...prev.rules, newRule]
    }));
  }, []);

  // Update an existing filter rule
  const updateFilter = useCallback((id: string, updates: Partial<Omit<FilterRule, 'id'>>) => {
    setFilterStateInternal(prev => ({
      rules: prev.rules.map(rule =>
        rule.id === id ? { ...rule, ...updates } : rule
      )
    }));
  }, []);

  // Remove a filter rule
  const removeFilter = useCallback((id: string) => {
    setFilterStateInternal(prev => ({
      rules: prev.rules.filter(rule => rule.id !== id)
    }));
  }, []);

  // Clear all filters
  const clearFilters = useCallback(() => {
    setFilterStateInternal({ rules: [] });
  }, []);

  // Set entire filter state (for loading presets)
  const setFilterState = useCallback((state: FilterState) => {
    setFilterStateInternal(state);
  }, []);

  // Get filters for a specific column
  const getFiltersForColumn = useCallback((columnName: string): FilterRule[] => {
    return filterState.rules.filter(rule => rule.column === columnName);
  }, [filterState.rules]);

  // Check if column has any filters
  const hasFilterForColumn = useCallback((columnName: string): boolean => {
    return filterState.rules.some(rule => rule.column === columnName);
  }, [filterState.rules]);

  // Count of active filters
  const activeFilterCount = useMemo(() => {
    return filterState.rules.length;
  }, [filterState.rules]);

  return {
    filterState,
    addFilter,
    updateFilter,
    removeFilter,
    clearFilters,
    setFilterState,
    getFiltersForColumn,
    hasFilterForColumn,
    activeFilterCount,
  };
}
