/**
 * Table component that displays data grouped by observation units.
 * Provides collapsible unit groups with merge functionality.
 */

import React, { useState, useCallback, useMemo } from 'react';
import { Merge, RefreshCw, ChevronDown, ChevronUp, Loader2, Lightbulb } from 'lucide-react';
import { useQuery } from 'react-query';

import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

import { unitsAPI } from '../../services/api';
import { useUnits, useMergeUnits, useUnitSuggestions } from '../../hooks/useUnits';
import { MergeUnitsRequest } from '../../types/unit';
import { UnitFilter } from '../ViewMode/UnitFilter';
import { UnitMergeDialog } from '../ViewMode/UnitMergeDialog';
import { UnitSimilarityCard } from '../Units/UnitSimilarityCard';
import UnitGroupRow from './UnitGroupRow';
import { DataRow, CellValue } from '../../types';
import { cn } from '@/lib/utils';
import { formatColumnName } from '../../utils/formatting';

interface UnitGroupedTableProps {
  /** Session ID */
  sessionId: string;
  /** Session type */
  sessionType: 'load' | 'qbsd';
  /** Columns to display */
  columns: string[];
  /** Column metadata for display */
  columnInfo?: { name: string; allowed_values?: string[] }[];
  /** Callback when data changes */
  onDataChange?: () => void;
}

export const UnitGroupedTable: React.FC<UnitGroupedTableProps> = ({
  sessionId,
  sessionType,
  columns,
  columnInfo,
  onDataChange,
}) => {
  // Unit data hooks
  const { units: unitListResponse, loading: unitsLoading, error: unitsError, refresh: refreshUnits } = useUnits(sessionId);
  const { merge, loading: mergeLoading, error: mergeError, clearError: clearMergeError } = useMergeUnits(sessionId);
  const { suggestions, loading: suggestionsLoading, fetchSuggestions } = useUnitSuggestions(sessionId);

  // Memoize units array to prevent unnecessary re-renders
  const units = useMemo(() => unitListResponse?.units || [], [unitListResponse?.units]);

  // Local state
  const [selectedUnit, setSelectedUnit] = useState<string | null>(null);
  const [expandedUnits, setExpandedUnits] = useState<Set<string>>(new Set());
  const [selectedForMerge, setSelectedForMerge] = useState<Set<string>>(new Set());
  const [mergeDialogOpen, setMergeDialogOpen] = useState(false);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [dismissedSuggestions, setDismissedSuggestions] = useState<Set<string>>(new Set());

  // Pagination state for unit data
  const [page, setPage] = useState(0);
  const [pageSize] = useState(50);

  // Fetch unit-grouped data
  const { data: unitData, isLoading: dataLoading, refetch: refetchData } = useQuery(
    ['unitData', sessionId, selectedUnit, page, pageSize],
    () => unitsAPI.getData(sessionId, {
      unit: selectedUnit || undefined,
      page,
      pageSize,
    }),
    {
      enabled: !!sessionId && unitListResponse !== null,
      keepPreviousData: true,
    }
  );

  // Toggle unit expansion
  const toggleExpansion = useCallback((unitName: string) => {
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

  // Toggle merge selection
  const toggleMergeSelection = useCallback((unitName: string) => {
    setSelectedForMerge(prev => {
      const next = new Set(prev);
      if (next.has(unitName)) {
        next.delete(unitName);
      } else {
        next.add(unitName);
      }
      return next;
    });
  }, []);

  // Clear merge selections
  const clearMergeSelections = useCallback(() => {
    setSelectedForMerge(new Set());
  }, []);

  // Expand/collapse all
  const expandAll = useCallback(() => {
    setExpandedUnits(new Set(units.map(u => u.name)));
  }, [units]);

  const collapseAll = useCallback(() => {
    setExpandedUnits(new Set());
  }, []);

  // Handle merge
  const handleMerge = useCallback(async (request: MergeUnitsRequest) => {
    try {
      await merge(request);
      setMergeDialogOpen(false);
      clearMergeSelections();
      refreshUnits();
      refetchData();
      onDataChange?.();
    } catch (err) {
      // Error is handled by the hook
    }
  }, [merge, clearMergeSelections, refreshUnits, refetchData, onDataChange]);

  // Handle suggestion merge
  const handleSuggestionMerge = useCallback(async (suggestion: any) => {
    try {
      await merge({
        source_units: suggestion.units,
        target_unit: suggestion.suggestedName,
        strategy: 'rename',
      });
      refreshUnits();
      refetchData();
      fetchSuggestions();
      onDataChange?.();
    } catch (err) {
      // Error is handled by the hook
    }
  }, [merge, refreshUnits, refetchData, fetchSuggestions, onDataChange]);

  // Dismiss suggestion
  const handleDismissSuggestion = useCallback((suggestion: any) => {
    const key = suggestion.units.sort().join('|');
    setDismissedSuggestions(prev => new Set(prev).add(key));
  }, []);

  // Get selected units for merge dialog
  const selectedUnitsForMerge = useMemo(() => {
    return units.filter(u => selectedForMerge.has(u.name));
  }, [units, selectedForMerge]);

  // Filter suggestions by dismissed
  const visibleSuggestions = useMemo(() => {
    if (!suggestions?.suggestions) return [];
    return suggestions.suggestions.filter(s => {
      const key = s.units.sort().join('|');
      return !dismissedSuggestions.has(key);
    });
  }, [suggestions, dismissedSuggestions]);

  // Group data rows by unit
  const groupedRows = useMemo(() => {
    if (!unitData?.rows) return new Map<string, DataRow[]>();

    const groups = new Map<string, DataRow[]>();
    unitData.rows.forEach(row => {
      const unitName = row._unit_name || 'Unknown';
      if (!groups.has(unitName)) {
        groups.set(unitName, []);
      }
      groups.get(unitName)!.push(row);
    });

    return groups;
  }, [unitData?.rows]);

  // Determine which columns to show (filter out internal columns)
  const visibleColumns = useMemo(() => {
    return columns.filter(col =>
      !col.startsWith('_') || col === '_unit_name' || col === '_source_document'
    );
  }, [columns]);

  if (unitsLoading) {
    return (
      <Card className="p-8">
        <div className="flex items-center justify-center gap-2 text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin" />
          <span>Loading observation units...</span>
        </div>
      </Card>
    );
  }

  if (unitsError) {
    return (
      <Alert variant="destructive">
        <AlertDescription>
          Failed to load observation units: {unitsError}
        </AlertDescription>
      </Alert>
    );
  }

  if (units.length === 0) {
    return (
      <Alert>
        <AlertDescription>
          No observation units found in this session.
        </AlertDescription>
      </Alert>
    );
  }

  return (
    <Card>
      <div className="p-4">
        {/* Header */}
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="font-semibold text-lg flex items-center gap-2">
              Observation Units
              <Badge variant="secondary">
                {unitListResponse?.totalUnits} units
              </Badge>
              <Badge variant="outline">
                {unitListResponse?.totalRows} rows
              </Badge>
            </h3>
            <p className="text-xs text-muted-foreground">
              Click unit headers to expand/collapse • Select units with checkboxes to merge
            </p>
          </div>

          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => refreshUnits()}
              disabled={unitsLoading}
            >
              <RefreshCw className={cn("h-4 w-4", unitsLoading && "animate-spin")} />
            </Button>
          </div>
        </div>

        {/* Toolbar */}
        <div className="flex flex-wrap items-center gap-2 mb-4 pb-4 border-b">
          {/* Unit filter */}
          <UnitFilter
            units={units}
            selectedUnit={selectedUnit}
            onUnitChange={setSelectedUnit}
            loading={dataLoading}
          />

          {/* Expand/Collapse all */}
          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="sm"
              onClick={expandAll}
              title="Expand all"
            >
              <ChevronDown className="h-4 w-4" />
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={collapseAll}
              title="Collapse all"
            >
              <ChevronUp className="h-4 w-4" />
            </Button>
          </div>

          {/* Merge actions */}
          <div className="flex items-center gap-2 ml-auto">
            {selectedForMerge.size >= 2 && (
              <Button
                variant="default"
                size="sm"
                onClick={() => setMergeDialogOpen(true)}
                className="gap-1"
              >
                <Merge className="h-4 w-4" />
                Merge {selectedForMerge.size} Units
              </Button>
            )}

            {selectedForMerge.size > 0 && (
              <Button
                variant="ghost"
                size="sm"
                onClick={clearMergeSelections}
              >
                Clear Selection
              </Button>
            )}

            {/* Suggestions button */}
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant={showSuggestions ? 'secondary' : 'outline'}
                  size="sm"
                  onClick={() => {
                    if (!showSuggestions && !suggestions) {
                      fetchSuggestions();
                    }
                    setShowSuggestions(!showSuggestions);
                  }}
                  className="gap-1"
                >
                  <Lightbulb className="h-4 w-4" />
                  Suggestions
                  {visibleSuggestions.length > 0 && (
                    <Badge variant="destructive" className="ml-1 h-5 px-1.5">
                      {visibleSuggestions.length}
                    </Badge>
                  )}
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                Find similar units that could be merged
              </TooltipContent>
            </Tooltip>
          </div>
        </div>

        {/* Merge suggestions panel */}
        {showSuggestions && (
          <div className="mb-4 p-4 bg-muted/30 rounded-lg">
            <h4 className="text-sm font-medium mb-3 flex items-center gap-2">
              <Lightbulb className="h-4 w-4 text-yellow-500" />
              Merge Suggestions
            </h4>
            {suggestionsLoading ? (
              <div className="flex items-center gap-2 text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                <span>Finding similar units...</span>
              </div>
            ) : visibleSuggestions.length > 0 ? (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                {visibleSuggestions.map((suggestion, index) => (
                  <UnitSimilarityCard
                    key={index}
                    suggestion={suggestion}
                    onMerge={handleSuggestionMerge}
                    onDismiss={handleDismissSuggestion}
                    loading={mergeLoading}
                  />
                ))}
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">
                No similar units found. All units appear to be unique.
              </p>
            )}
          </div>
        )}

        {/* Table */}
        <div className="overflow-auto max-h-[600px] border rounded-md">
          <table className="w-full border-collapse">
            <thead className="sticky top-0 z-10 bg-background border-b">
              <tr>
                {visibleColumns.map(column => (
                  <th
                    key={column}
                    className="px-4 py-3 text-left font-bold text-base min-w-[120px] bg-background"
                  >
                    {column.startsWith('_') ? (
                      <Badge variant="outline">{formatColumnName(column)}</Badge>
                    ) : (
                      formatColumnName(column)
                    )}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {/* Render units with their rows */}
              {units
                .filter(unit => !selectedUnit || unit.name === selectedUnit)
                .map(unit => {
                  const isExpanded = expandedUnits.has(unit.name);
                  const isSelectedForMerge = selectedForMerge.has(unit.name);
                  const unitRows = groupedRows.get(unit.name) || [];

                  return (
                    <React.Fragment key={unit.name}>
                      {/* Unit group header */}
                      <UnitGroupRow
                        unit={unit}
                        isExpanded={isExpanded}
                        onToggleExpand={() => toggleExpansion(unit.name)}
                        isSelectedForMerge={isSelectedForMerge}
                        onToggleMergeSelection={() => toggleMergeSelection(unit.name)}
                        columnCount={visibleColumns.length}
                      />

                      {/* Unit rows (when expanded) */}
                      {isExpanded && unitRows.map((row, rowIndex) => (
                        <tr
                          key={`${unit.name}-${rowIndex}`}
                          className="border-b hover:bg-muted/30 transition-colors"
                        >
                          {visibleColumns.map(column => {
                            let cellValue: CellValue;
                            if (column === '_unit_name') {
                              cellValue = row._unit_name;
                            } else if (column === '_source_document') {
                              cellValue = row._source_document;
                            } else {
                              cellValue = row.data[column];
                            }

                            return (
                              <td
                                key={column}
                                className="px-4 py-3 text-sm"
                              >
                                {formatCellValue(cellValue)}
                              </td>
                            );
                          })}
                        </tr>
                      ))}

                      {/* Show message if no data rows for expanded unit */}
                      {isExpanded && unitRows.length === 0 && (
                        <tr>
                          <td
                            colSpan={visibleColumns.length}
                            className="px-4 py-8 text-center text-muted-foreground"
                          >
                            No rows loaded for this unit. Data may still be loading.
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
            </tbody>
          </table>
        </div>

        {/* Pagination info */}
        {unitData && (
          <div className="flex items-center justify-between mt-4 text-sm text-muted-foreground">
            <span>
              Showing {unitData.rows.length} of {unitData.total_count} rows
            </span>
            {unitData.has_more && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => setPage(p => p + 1)}
                disabled={dataLoading}
              >
                Load More
              </Button>
            )}
          </div>
        )}
      </div>

      {/* Merge Dialog */}
      <UnitMergeDialog
        open={mergeDialogOpen}
        onClose={() => {
          setMergeDialogOpen(false);
          clearMergeError();
        }}
        selectedUnits={selectedUnitsForMerge}
        onMerge={handleMerge}
        loading={mergeLoading}
        error={mergeError}
      />
    </Card>
  );
};

/**
 * Simple cell value formatter for the grouped table.
 */
function formatCellValue(value: CellValue): React.ReactNode {
  if (value === null || value === undefined) {
    return <Badge variant="outline" className="text-muted-foreground">null</Badge>;
  }

  if (typeof value === 'object') {
    // Handle QBSD format with answer/excerpts
    if ('answer' in value) {
      return String(value.answer);
    }
    return JSON.stringify(value);
  }

  const str = String(value);
  if (str.length > 100) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="cursor-help">{str.slice(0, 100)}...</span>
        </TooltipTrigger>
        <TooltipContent className="max-w-md">
          <p className="whitespace-pre-wrap">{str}</p>
        </TooltipContent>
      </Tooltip>
    );
  }

  return str;
}

export default UnitGroupedTable;
