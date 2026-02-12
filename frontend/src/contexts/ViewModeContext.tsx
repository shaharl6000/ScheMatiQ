/**
 * Context for managing table view mode (standard vs. by observation unit).
 */

import React, { createContext, useContext, useState, useCallback, useMemo, ReactNode } from 'react';
import { ViewMode, UnitSummary } from '../types/unit';

interface ViewModeContextValue {
  /** Current view mode */
  viewMode: ViewMode;
  /** Switch view mode */
  setViewMode: (mode: ViewMode) => void;
  /** Currently selected unit for filtering (null = show all) */
  selectedUnit: string | null;
  /** Select a unit to filter by */
  setSelectedUnit: (unit: string | null) => void;
  /** Units selected for merge operation */
  selectedUnitsForMerge: string[];
  /** Toggle unit selection for merge */
  toggleUnitForMerge: (unitName: string) => void;
  /** Clear all merge selections */
  clearMergeSelections: () => void;
  /** Check if a unit is selected for merge */
  isUnitSelectedForMerge: (unitName: string) => boolean;
  /** Set multiple units for merge at once */
  setSelectedUnitsForMerge: (units: string[]) => void;
  /** Expanded unit groups (for collapsible view) */
  expandedUnits: Set<string>;
  /** Toggle unit group expansion */
  toggleUnitExpansion: (unitName: string) => void;
  /** Expand all units */
  expandAllUnits: (units: UnitSummary[]) => void;
  /** Collapse all units */
  collapseAllUnits: () => void;
  /** Check if a unit group is expanded */
  isUnitExpanded: (unitName: string) => boolean;
}

const ViewModeContext = createContext<ViewModeContextValue | undefined>(undefined);

interface ViewModeProviderProps {
  children: ReactNode;
}

export const ViewModeProvider: React.FC<ViewModeProviderProps> = ({ children }) => {
  const [viewMode, setViewMode] = useState<ViewMode>('by_unit');
  const [selectedUnit, setSelectedUnit] = useState<string | null>(null);
  const [selectedUnitsForMerge, setSelectedUnitsForMerge] = useState<string[]>([]);
  const [expandedUnits, setExpandedUnits] = useState<Set<string>>(new Set());

  const toggleUnitForMerge = useCallback((unitName: string) => {
    setSelectedUnitsForMerge(prev => {
      if (prev.includes(unitName)) {
        return prev.filter(u => u !== unitName);
      }
      return [...prev, unitName];
    });
  }, []);

  const clearMergeSelections = useCallback(() => {
    setSelectedUnitsForMerge([]);
  }, []);

  const isUnitSelectedForMerge = useCallback((unitName: string) => {
    return selectedUnitsForMerge.includes(unitName);
  }, [selectedUnitsForMerge]);

  const toggleUnitExpansion = useCallback((unitName: string) => {
    setExpandedUnits(prev => {
      const next = new Set(prev);
      if (next.has(unitName)) {
        next.delete(unitName);
      } else {
        next.add(unitName);
      }
      return next;
    });
  }, []);

  const expandAllUnits = useCallback((units: UnitSummary[]) => {
    setExpandedUnits(new Set(units.map(u => u.name)));
  }, []);

  const collapseAllUnits = useCallback(() => {
    setExpandedUnits(new Set());
  }, []);

  const isUnitExpanded = useCallback((unitName: string) => {
    return expandedUnits.has(unitName);
  }, [expandedUnits]);

  // Reset unit-specific state when switching to standard view
  const handleSetViewMode = useCallback((mode: ViewMode) => {
    setViewMode(mode);
    if (mode === 'standard') {
      setSelectedUnit(null);
      clearMergeSelections();
    }
  }, [clearMergeSelections]);

  const value = useMemo<ViewModeContextValue>(() => ({
    viewMode,
    setViewMode: handleSetViewMode,
    selectedUnit,
    setSelectedUnit,
    selectedUnitsForMerge,
    toggleUnitForMerge,
    clearMergeSelections,
    isUnitSelectedForMerge,
    setSelectedUnitsForMerge,
    expandedUnits,
    toggleUnitExpansion,
    expandAllUnits,
    collapseAllUnits,
    isUnitExpanded,
  }), [
    viewMode,
    handleSetViewMode,
    selectedUnit,
    selectedUnitsForMerge,
    toggleUnitForMerge,
    clearMergeSelections,
    isUnitSelectedForMerge,
    expandedUnits,
    toggleUnitExpansion,
    expandAllUnits,
    collapseAllUnits,
    isUnitExpanded,
  ]);

  return (
    <ViewModeContext.Provider value={value}>
      {children}
    </ViewModeContext.Provider>
  );
};

export const useViewMode = (): ViewModeContextValue => {
  const context = useContext(ViewModeContext);
  if (!context) {
    throw new Error('useViewMode must be used within a ViewModeProvider');
  }
  return context;
};

export default ViewModeContext;
