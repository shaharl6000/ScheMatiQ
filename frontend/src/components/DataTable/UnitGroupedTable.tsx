/**
 * Table component that displays data by observation units in a flat layout.
 * Uses a frozen left column for unit names, similar to DataTable's frozen column pattern.
 */

import React, { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { Loader2, Lightbulb, FileText, AlertCircle, Search, Square } from 'lucide-react';
import { useQuery } from 'react-query';

import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Checkbox } from '@/components/ui/checkbox';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

import { unitsAPI, observationUnitAPI } from '../../services/api';
import { useUnits, useMergeUnits, useUnitSuggestions } from '../../hooks/useUnits';
import { MergeUnitsRequest, UnitSummary } from '../../types/unit';
import { UnitMergeDialog } from '../ViewMode/UnitMergeDialog';
import { UnitSimilarityCard } from '../Units/UnitSimilarityCard';
import { UnitMergePickerDialog } from './UnitMergePickerDialog';
import BulkActionToolbar from './BulkActionToolbar';
import { useRowSelection } from './hooks/useRowSelection';
import ContentModal from '../ContentModal/ContentModal';
import { DataRow, CellValue, ModalContent, QBSDAnswerWithExcerpts } from '../../types';
import { cn } from '@/lib/utils';
import { useToast } from '@/components/ui/use-toast';
import { formatColumnName } from '../../utils/formatting';
import { buildColumnMetadata, applyFilters, applySort, parsePythonString, extractDisplayValue, getDefaultColumnOrder } from './utils';
import { FilterOperator, FilterValue, ColumnMetadata, FilterRule, SortColumn } from './types/filters';
import { useTableSort } from './hooks/useTableSort';
import { useTableFilter } from './hooks/useTableFilter';
import { useColumnVisibility } from './hooks/useColumnVisibility';
import { useColumnStats } from './hooks/useColumnStats';
import FilterBar from './FilterBar';
import FilterDialog from './FilterDialog';
import TableOptionsMenu from './TableOptionsMenu';
import { AVAILABLE_PAGE_SIZES } from '../../constants';
import { Progress } from '@/components/ui/progress';
import { useColumnResize, MIN_COLUMN_WIDTH, DEFAULT_COLUMN_WIDTH } from './hooks/useColumnResize';

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
  /** Columns currently being re-extracted */
  processingColumns?: Set<string>;
  /** Current document/unit extraction progress */
  currentDocumentProgress?: { documentName: string; documentIndex: number; totalDocuments: number } | null;
  /** Callback to stop re-extraction */
  onStopReextraction?: () => void;
  /** Whether re-extraction is being stopped */
  isStoppingReextraction?: boolean;
  /** Whether new documents are being processed */
  isProcessingDocuments?: boolean;
  /** Callback to stop document processing */
  onStopProcessing?: () => void;
  /** Whether document processing is being stopped */
  isStoppingProcessing?: boolean;
  /** External column order (from drag-and-drop in DataTable) */
  columnOrder?: string[];
}

export const UnitGroupedTable: React.FC<UnitGroupedTableProps> = ({
  sessionId,
  sessionType,
  columns,
  columnInfo,
  onDataChange,
  processingColumns,
  currentDocumentProgress,
  onStopReextraction,
  isStoppingReextraction,
  isProcessingDocuments,
  onStopProcessing,
  isStoppingProcessing,
  columnOrder,
}) => {
  // Unit data hooks
  const { units: unitListResponse, loading: unitsLoading, error: unitsError, refresh: refreshUnits } = useUnits(sessionId);
  const { merge, loading: mergeLoading, error: mergeError, clearError: clearMergeError } = useMergeUnits(sessionId);
  const { suggestions, loading: suggestionsLoading, autoMerged, fetchSuggestions } = useUnitSuggestions(sessionId);
  const { toast } = useToast();

  // Column resize hook
  const {
    getColumnWidth,
    handleResizeStart,
  } = useColumnResize({ sessionId });

  // Sort, filter, and visibility hooks (use unit_ prefix to avoid collisions with DataTable)
  const {
    sortState,
    setSortState,
  } = useTableSort({ sessionId, persistKey: `unit_sort_${sessionId}` });

  const {
    filterState,
    addFilter,
    removeFilter,
    clearFilters,
    setFilterState,
  } = useTableFilter({ sessionId, persistKey: `unit_filter_${sessionId}` });

  // Refs for header cells (for resize start width measurement)
  const headerRefs = useRef<Record<string, HTMLTableCellElement | null>>({});
  const frozenThRef = useRef<HTMLTableCellElement | null>(null);

  // Memoize units array to prevent unnecessary re-renders
  const units = useMemo(() => unitListResponse?.units || [], [unitListResponse?.units]);

  // Local state
  const [selectedUnits, setSelectedUnits] = useState<string[]>([]);
  const [mergePickerOpen, setMergePickerOpen] = useState(false);
  const [mergeDialogOpen, setMergeDialogOpen] = useState(false);
  const [unitsToMerge, setUnitsToMerge] = useState<UnitSummary[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [dismissedSuggestions, setDismissedSuggestions] = useState<Set<string>>(new Set());

  // Search and filter UI state
  const [searchTerm, setSearchTerm] = useState('');
  const [fullnessThreshold, setFullnessThreshold] = useState(0);
  const [filterDialogOpen, setFilterDialogOpen] = useState(false);
  const [filterDialogColumn, setFilterDialogColumn] = useState<string | undefined>();

  // Pagination state for unit data
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);

  // Modal state for viewing cell content with excerpts
  const [modalOpen, setModalOpen] = useState(false);
  const [modalContent, setModalContent] = useState<ModalContent>({ title: '', content: null });

  // Row selection state
  const [hoveredRowId, setHoveredRowId] = useState<string | null>(null);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [isBulkDeleting, setIsBulkDeleting] = useState(false);
  const [bulkDeleteError, setBulkDeleteError] = useState<string | null>(null);

  // Fetch unit-grouped data
  // Filter out __none__ sentinel — treat as no filter (show all rows)
  const effectiveUnitFilter = useMemo(() => {
    const filtered = selectedUnits.filter(u => u !== '__none__');
    return filtered.length > 0 ? filtered : undefined;
  }, [selectedUnits]);

  const { data: unitData, isLoading: dataLoading, refetch: refetchData } = useQuery(
    ['unitData', sessionId, selectedUnits, page, pageSize],
    () => unitsAPI.getData(sessionId, {
      units: effectiveUnitFilter,
      page,
      pageSize,
    }),
    {
      enabled: !!sessionId && unitListResponse !== null,
      keepPreviousData: true,
    }
  );

  // Pagination handlers
  const handleChangePage = useCallback((newPage: number) => {
    setPage(newPage);
  }, []);

  const handleChangeRowsPerPage = useCallback((value: string) => {
    setPageSize(parseInt(value, 10));
    setPage(0);
  }, []);

  // Reset to first page when unit filter changes
  useEffect(() => {
    setPage(0);
  }, [selectedUnits]);

  // Total pages calculation — use filtered_count when a unit filter is active
  const displayedRowCount = unitData?.filtered_count ?? unitData?.total_count ?? 0;
  const totalPages = Math.ceil(displayedRowCount / pageSize);

  // Handle merge
  const handleMerge = useCallback(async (request: MergeUnitsRequest) => {
    try {
      await merge(request);
      setMergeDialogOpen(false);
      setUnitsToMerge([]);
      await refreshUnits();
      await refetchData();
      onDataChange?.();
    } catch (err) {
      // Error is handled by the hook
    }
  }, [merge, refreshUnits, refetchData, onDataChange]);

  // Handle backend auto-merge results — refresh table silently
  useEffect(() => {
    if (autoMerged.length > 0) {
      refreshUnits();
      refetchData();
      onDataChange?.();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoMerged]);

  // Handle suggestion merge
  const handleSuggestionMerge = useCallback(async (suggestion: any) => {
    try {
      await merge({
        source_units: suggestion.units,
        target_unit: suggestion.suggestedName,
        strategy: 'rename',
      });
      await refreshUnits();
      await refetchData();
      await fetchSuggestions();
      onDataChange?.();
    } catch (err) {
      // Error is handled by the hook
    }
  }, [merge, refreshUnits, refetchData, fetchSuggestions, onDataChange]);

  // Dismiss suggestion
  const handleDismissSuggestion = useCallback((suggestion: any) => {
    const key = [...suggestion.units].sort().join('|');
    setDismissedSuggestions(prev => new Set(prev).add(key));
  }, []);

  // Handle viewing cell content in modal
  const handleViewContent = useCallback((columnName: string, content: CellValue) => {
    setModalContent({
      title: `${formatColumnName(columnName)} - Full Content`,
      content: content
    });
    setModalOpen(true);
  }, []);

  // Filter suggestions by dismissed
  const visibleSuggestions = useMemo(() => {
    if (!suggestions?.suggestions) return [];
    return suggestions.suggestions.filter(s => {
      const key = [...s.units].sort().join('|');
      return !dismissedSuggestions.has(key);
    });
  }, [suggestions, dismissedSuggestions]);

  // Apply client-side search, filters, and sorting
  const processedRows = useMemo(() => {
    if (!unitData?.rows) return [];

    let rows = unitData.rows;

    // Apply search filter across all column values
    if (searchTerm.trim()) {
      const term = searchTerm.toLowerCase();
      rows = rows.filter(row => {
        // Check unit name
        if (row._unit_name?.toLowerCase().includes(term)) return true;
        if (row.row_name?.toLowerCase().includes(term)) return true;
        if (row._source_document?.toLowerCase().includes(term)) return true;
        // Check all data columns
        return Object.values(row.data).some(val => {
          if (val === null || val === undefined) return false;
          const str = typeof val === 'string' ? val : JSON.stringify(val);
          return str.toLowerCase().includes(term);
        });
      });
    }

    // Apply column filters
    rows = applyFilters(rows, filterState);

    // Apply sorting
    rows = applySort(rows, sortState);

    return rows;
  }, [unitData?.rows, searchTerm, filterState, sortState]);

  // Row selection - compute pageRowIds from filtered/sorted rows
  const pageRowIds = useMemo(() => {
    return processedRows.map(row => row._unit_name || row.row_name || '').filter(Boolean);
  }, [processedRows]);

  const {
    isAllPageSelected,
    isIndeterminate,
    selectedRows,
    toggleRow,
    toggleAllPage,
    clearSelection,
    isSelected,
    selectedCount,
  } = useRowSelection(pageRowIds);

  // Bulk delete handler
  const handleBulkDelete = useCallback(async () => {
    setIsBulkDeleting(true);
    setBulkDeleteError(null);

    try {
      const unitNames = Array.from(selectedRows);
      await observationUnitAPI.removeBulk(sessionId, unitNames);
      setShowDeleteDialog(false);
      clearSelection();
      await refetchData();
      await refreshUnits();
      onDataChange?.();
      toast({
        title: 'Rows deleted',
        description: `${unitNames.length} row${unitNames.length !== 1 ? 's' : ''} removed from the table.`,
      });
    } catch (err: any) {
      setBulkDeleteError(err.response?.data?.detail || err.message || 'Failed to delete rows');
    } finally {
      setIsBulkDeleting(false);
    }
  }, [selectedRows, sessionId, clearSelection, refetchData, refreshUnits, onDataChange, toast]);

  // Clear selection on page/filter/search changes
  useEffect(() => {
    clearSelection();
  }, [page, pageSize, selectedUnits, searchTerm, filterState.rules, sortState.columns, clearSelection]);

  // Check if any row has _source_document
  const hasSourceDocument = useMemo(() => {
    return unitData?.rows?.some(row => row._source_document != null) ?? false;
  }, [unitData?.rows]);

  // All toggleable columns with consistent ordering (matching DataTable's priority-based order)
  const allColumns = useMemo(() => {
    const defaultOrder = getDefaultColumnOrder(unitData?.rows || [], columnInfo);
    if (columnOrder && columnOrder.length > 0) {
      const validOrder = columnOrder.filter(col => defaultOrder.includes(col));
      const newCols = defaultOrder.filter(col => !columnOrder.includes(col));
      return [...validOrder, ...newCols];
    }
    return defaultOrder;
  }, [unitData?.rows, columnInfo, columnOrder]);

  // Column visibility hook
  const {
    visibility,
    toggleColumn,
    showAllColumns,
    hideAllColumns,
    isVisible,
  } = useColumnVisibility({ sessionId, columns: allColumns, persistKey: `unit_visibility_${sessionId}` });

  // Column statistics hook for fullness calculations
  const { getColumnsAboveThreshold } = useColumnStats(unitData?.rows || [], allColumns);

  // Apply visibility and fullness threshold to get displayed columns
  const visibleColumns = useMemo(() => {
    const columnsAboveThreshold = fullnessThreshold > 0
      ? getColumnsAboveThreshold(fullnessThreshold)
      : allColumns;
    return allColumns.filter(col =>
      isVisible(col) && columnsAboveThreshold.includes(col)
    );
  }, [allColumns, isVisible, fullnessThreshold, getColumnsAboveThreshold]);

  // Count columns hidden specifically due to fullness threshold
  const hiddenByFullnessCount = useMemo(() => {
    if (fullnessThreshold === 0) return 0;
    const columnsAboveThreshold = getColumnsAboveThreshold(fullnessThreshold);
    return allColumns.filter(col =>
      isVisible(col) && !columnsAboveThreshold.includes(col)
    ).length;
  }, [allColumns, isVisible, fullnessThreshold, getColumnsAboveThreshold]);

  // Build column metadata for filter dialog
  const columnMetadata = useMemo((): ColumnMetadata[] => {
    return allColumns.map(col => {
      const info = columnInfo?.find(c => c.name === col);
      return buildColumnMetadata(unitData?.rows || [], col, info?.allowed_values);
    });
  }, [allColumns, unitData?.rows, columnInfo]);

  // All table columns including frozen and dedicated columns (used for computing total table width)
  const allTableColumns = useMemo(() => {
    const cols = ['_unit_name'];
    if (hasSourceDocument) cols.push('_source_document');
    return [...cols, ...visibleColumns];
  }, [hasSourceDocument, visibleColumns]);

  // Create mapping of main columns to their corresponding excerpt columns
  const excerptMapping = useMemo(() => {
    const mapping: Record<string, string> = {};

    if (!unitData?.rows) return mapping;

    const allDataColumns = new Set<string>();
    unitData.rows.forEach(row => {
      Object.keys(row.data).forEach(key => allDataColumns.add(key));
    });

    Array.from(allDataColumns).forEach(col => {
      if (col.endsWith('_excerpt')) {
        const baseColumn = col.replace('_excerpt', '');
        if (allDataColumns.has(baseColumn)) {
          mapping[baseColumn] = col;
        }
      }
    });

    return mapping;
  }, [unitData?.rows]);

  // Filter dialog handlers
  const handleOpenFilterDialog = useCallback((column?: string) => {
    setFilterDialogColumn(column);
    setFilterDialogOpen(true);
  }, []);

  const handleApplyFilter = useCallback((
    column: string,
    operator: FilterOperator,
    value: FilterValue,
    caseSensitive?: boolean
  ) => {
    addFilter({ column, operator, value, caseSensitive });
  }, [addFilter]);

  const handleLoadPreset = useCallback((filters: FilterRule[], sort: SortColumn[]) => {
    setFilterState({ rules: filters });
    setSortState({ columns: sort });
  }, [setFilterState, setSortState]);

  if (unitsLoading && !unitListResponse) {
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
              <span className="text-muted-foreground font-normal">
                ({unitListResponse?.totalRows} rows)
              </span>
            </h3>
          </div>

          <div className="flex items-center gap-2">
            <Tooltip>
              <TooltipTrigger asChild>
                <div className="relative w-64">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                  <Input
                    placeholder="Search all cells..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="pl-9"
                    aria-label="Search all columns"
                  />
                </div>
              </TooltipTrigger>
              <TooltipContent>Search across unit names and all column values</TooltipContent>
            </Tooltip>

            <TableOptionsMenu
              onAddFilter={() => handleOpenFilterDialog()}
              onAddRow={() => {}}
              readonly={true}
              sessionId={sessionId}
              currentFilters={filterState.rules}
              currentSort={sortState.columns}
              onLoadPreset={handleLoadPreset}
              fullnessThreshold={fullnessThreshold}
              onFullnessChange={setFullnessThreshold}
              visibleColumnsCount={visibleColumns.length}
              totalColumnsCount={allColumns.length}
              hiddenByFullnessCount={hiddenByFullnessCount}
              columns={allColumns}
              visibility={visibility}
              onToggleColumn={toggleColumn}
              onShowAll={showAllColumns}
              onHideAll={hideAllColumns}
              unitList={units}
              selectedUnits={selectedUnits}
              onUnitChange={setSelectedUnits}
              unitDataLoading={dataLoading}
              onMergeUnits={() => setMergePickerOpen(true)}
              mergeDisabled={units.length < 2}
              onToggleSuggestions={() => setShowSuggestions(prev => !prev)}
              showSuggestions={showSuggestions}
              suggestionsCount={visibleSuggestions.length}
            />

          </div>
        </div>

        {/* Filter toolbar — only render when filters are active */}
        {filterState.rules.length > 0 && (
          <div className="flex flex-wrap items-center gap-2 mb-4 pb-4 border-b">
            <FilterBar
              filters={filterState.rules}
              onRemoveFilter={removeFilter}
              onClearAll={clearFilters}
              onAddFilter={() => handleOpenFilterDialog()}
            />
          </div>
        )}

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

        {/* Re-extraction / Document Processing Progress Bar */}
        {((processingColumns && processingColumns.size > 0) || isProcessingDocuments) && (
          <div className="mb-4 p-4 bg-muted/30 border rounded-lg">
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin text-primary" />
                <span className="font-medium">
                  {processingColumns && processingColumns.size > 0
                    ? `Re-extracting ${processingColumns.size} column${processingColumns.size !== 1 ? 's' : ''}`
                    : 'Extracting data from new documents'}
                </span>
                {currentDocumentProgress && (
                  <span className="text-sm text-muted-foreground">
                    — Observation Unit {currentDocumentProgress.documentIndex} of {currentDocumentProgress.totalDocuments}
                  </span>
                )}
              </div>
              {(processingColumns && processingColumns.size > 0 ? onStopReextraction : onStopProcessing) && (
                <Button
                  variant="destructive"
                  size="sm"
                  onClick={processingColumns && processingColumns.size > 0 ? onStopReextraction : onStopProcessing}
                  disabled={processingColumns && processingColumns.size > 0 ? isStoppingReextraction : isStoppingProcessing}
                  className="gap-1"
                >
                  {(processingColumns && processingColumns.size > 0 ? isStoppingReextraction : isStoppingProcessing) ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Stopping...
                    </>
                  ) : (
                    <>
                      <Square className="h-4 w-4" />
                      Stop
                    </>
                  )}
                </Button>
              )}
            </div>
            {currentDocumentProgress && currentDocumentProgress.totalDocuments > 0 && (
              <div className="space-y-1">
                <Progress
                  value={(currentDocumentProgress.documentIndex / currentDocumentProgress.totalDocuments) * 100}
                  className="h-2"
                />
                <p className="text-xs text-muted-foreground">
                  Processing: {currentDocumentProgress.documentName}
                </p>
              </div>
            )}
          </div>
        )}

        {/* Table */}
        <div className="overflow-auto max-h-[600px] border rounded-md">
          <table
            className="border-collapse"
            style={{
              tableLayout: 'fixed' as const,
              width: `${Math.max(600, 40 + allTableColumns.reduce((sum, col) => sum + (getColumnWidth(col) || DEFAULT_COLUMN_WIDTH), 0))}px`,
            }}
          >
            <thead className="sticky top-0 z-10 bg-background border-b">
              <tr>
                {/* Checkbox column header */}
                <th className="w-[40px] min-w-[40px] px-2 py-3 text-center bg-background">
                  {selectedCount > 0 && (
                    <Checkbox
                      checked={isAllPageSelected ? true : isIndeterminate ? 'indeterminate' : false}
                      onCheckedChange={() => toggleAllPage(pageRowIds)}
                      aria-label="Select all rows on page"
                    />
                  )}
                </th>

                {/* Frozen _unit_name column */}
                <th
                  ref={frozenThRef}
                  className={cn(
                    "px-4 py-3 text-left font-semibold text-sm sticky bg-background z-20 border-r-2 border-primary shadow-[2px_0_4px_rgba(0,0,0,0.1)] relative",
                    !getColumnWidth('_unit_name') && "min-w-[150px] max-w-[250px]",
                    "left-0"
                  )}
                  style={getColumnWidth('_unit_name') ? { width: getColumnWidth('_unit_name'), minWidth: MIN_COLUMN_WIDTH } : { width: DEFAULT_COLUMN_WIDTH }}
                >
                  <div className="flex items-center gap-1">
                    <div
                      className="flex items-center gap-1 flex-1 overflow-hidden"
                    >
                      <Badge variant="outline">{formatColumnName('_unit_name')}</Badge>
                    </div>
                  </div>
                  {/* Resize handle */}
                  <div
                    className="absolute right-0 top-0 bottom-0 w-[6px] cursor-col-resize hover:bg-primary/40 z-10"
                    onMouseDown={(e) => {
                      e.stopPropagation();
                      if (frozenThRef.current) {
                        handleResizeStart(e, '_unit_name', frozenThRef.current.offsetWidth);
                      }
                    }}
                  />
                </th>

                {/* Source Document column - always visible when present */}
                {hasSourceDocument && (
                  <th
                    ref={(el) => { headerRefs.current['_source_document'] = el; }}
                    className={cn(
                      "px-4 py-3 text-left font-semibold text-sm bg-background border-r relative",
                      !getColumnWidth('_source_document') && "min-w-[150px] max-w-[250px]"
                    )}
                    style={getColumnWidth('_source_document') ? { width: getColumnWidth('_source_document'), minWidth: MIN_COLUMN_WIDTH } : { width: DEFAULT_COLUMN_WIDTH }}
                  >
                    <div className="flex items-center gap-1">
                      <div className="flex items-center gap-1.5">
                        <FileText className="h-4 w-4 text-muted-foreground" />
                        Source Document
                      </div>
                    </div>
                    <div
                      className="absolute right-0 top-0 bottom-0 w-[6px] cursor-col-resize hover:bg-primary/40 z-10"
                      onMouseDown={(e) => {
                        e.stopPropagation();
                        const th = headerRefs.current['_source_document'];
                        if (th) handleResizeStart(e, '_source_document', th.offsetWidth);
                      }}
                    />
                  </th>
                )}

                {/* Scrollable data columns */}
                {visibleColumns.map(column => (
                  <th
                    key={column}
                    ref={(el) => { headerRefs.current[column] = el; }}
                    className={cn(
                      "px-4 py-3 text-left font-semibold text-sm bg-background relative",
                      !getColumnWidth(column) && "min-w-[120px] sm:min-w-[150px]"
                    )}
                    style={getColumnWidth(column) ? { width: getColumnWidth(column), minWidth: MIN_COLUMN_WIDTH } : { width: DEFAULT_COLUMN_WIDTH }}
                  >
                    <div className="flex items-center gap-1">
                      {formatColumnName(column)}
                    </div>
                    <div
                      className="absolute right-0 top-0 bottom-0 w-[6px] cursor-col-resize hover:bg-primary/40 z-10"
                      onMouseDown={(e) => {
                        e.stopPropagation();
                        const th = headerRefs.current[column];
                        if (th) handleResizeStart(e, column, th.offsetWidth);
                      }}
                    />
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {/* Flat row rendering */}
              {processedRows.map((row, rowIndex) => {
                const rowId = row._unit_name || row.row_name || '';
                const rowSelected = isSelected(rowId);
                const showCheckbox = hoveredRowId === rowId || selectedCount > 0;

                return (
                  <tr
                    key={rowIndex}
                    className={cn(
                      "border-b hover:bg-muted/50 transition-colors",
                      rowSelected && "bg-blue-50 dark:bg-blue-950/50"
                    )}
                    onMouseEnter={() => setHoveredRowId(rowId)}
                    onMouseLeave={() => setHoveredRowId(null)}
                  >
                    {/* Checkbox cell */}
                    <td className="w-[40px] min-w-[40px] px-2 py-3 text-center" style={{ verticalAlign: 'top' }}>
                      <div className={cn(
                        "transition-opacity duration-100",
                        showCheckbox ? "opacity-100" : "opacity-0"
                      )}>
                        <Checkbox
                          checked={rowSelected}
                          onCheckedChange={() => toggleRow(rowId)}
                          onClick={(e) => e.stopPropagation()}
                          aria-label={`Select row ${rowId}`}
                        />
                      </div>
                    </td>

                    {/* Frozen _unit_name cell */}
                    <td
                      className={cn(
                        "px-4 py-3 sticky border-r",
                        !getColumnWidth('_unit_name') && "min-w-[150px] max-w-[250px]",
                        "left-0",
                        "bg-background"
                      )}
                      style={{
                        zIndex: 5,
                        verticalAlign: 'top',
                        ...(getColumnWidth('_unit_name') ? { width: getColumnWidth('_unit_name'), minWidth: MIN_COLUMN_WIDTH } : {}),
                      }}
                    >
                      <span className="text-sm font-medium">{row._unit_name || 'Unknown'}</span>
                    </td>

                    {/* Source Document cell - always visible when present */}
                    {hasSourceDocument && (
                      <td
                        className="px-4 py-3 text-sm border-r bg-muted/20"
                        style={getColumnWidth('_source_document') ? { width: getColumnWidth('_source_document'), minWidth: MIN_COLUMN_WIDTH } : { width: DEFAULT_COLUMN_WIDTH }}
                      >
                        <div className="flex items-center gap-1.5">
                          <FileText className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="truncate max-w-[160px] font-medium text-foreground/80 cursor-help">
                                {formatSourceDocument(row._source_document)}
                              </span>
                            </TooltipTrigger>
                            <TooltipContent side="right" className="max-w-[400px]">
                              <p className="break-all">{row._source_document || 'Unknown'}</p>
                            </TooltipContent>
                          </Tooltip>
                        </div>
                      </td>
                    )}

                    {/* Scrollable data columns */}
                    {visibleColumns.map(column => (
                      <td
                        key={column}
                        className={cn("px-4 py-3", !getColumnWidth(column) && "min-w-[120px] sm:min-w-[150px]")}
                        style={{
                          verticalAlign: 'top',
                          ...(getColumnWidth(column) ? { width: getColumnWidth(column), minWidth: MIN_COLUMN_WIDTH } : {}),
                        }}
                      >
                        {formatCellValue(row.data[column], column, row, excerptMapping, handleViewContent)}
                      </td>
                    ))}
                  </tr>
                );
              })}
              {/* No results message when all rows are filtered out */}
              {processedRows.length === 0 && unitData?.rows && unitData.rows.length > 0 && (
                <tr>
                  <td
                    colSpan={visibleColumns.length + (hasSourceDocument ? 3 : 2)}
                    className="px-4 py-12 text-center text-muted-foreground"
                  >
                    No rows match the current filters or search.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {unitData && (
          <div className="flex items-center justify-between mt-4">
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Rows per page:</span>
              <Select value={String(pageSize)} onValueChange={handleChangeRowsPerPage}>
                <SelectTrigger className="w-20">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {AVAILABLE_PAGE_SIZES.map(size => (
                    <SelectItem key={size} value={String(size)}>{size}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">
                {displayedRowCount > 0 ? `${page * pageSize + 1}-${Math.min((page + 1) * pageSize, displayedRowCount)} of ${displayedRowCount} rows` : '0 rows'}
              </span>
              <Button
                variant="outline"
                size="sm"
                onClick={() => handleChangePage(page - 1)}
                disabled={page === 0 || dataLoading}
              >
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => handleChangePage(page + 1)}
                disabled={page >= totalPages - 1 || dataLoading}
              >
                Next
              </Button>
            </div>
          </div>
        )}
      </div>

      {/* Merge Unit Picker Dialog */}
      <UnitMergePickerDialog
        open={mergePickerOpen}
        onClose={() => setMergePickerOpen(false)}
        units={units}
        onContinue={(selectedUnits) => {
          setMergePickerOpen(false);
          setUnitsToMerge(selectedUnits);
          setMergeDialogOpen(true);
        }}
      />

      {/* Merge Naming Dialog */}
      <UnitMergeDialog
        open={mergeDialogOpen}
        onClose={() => {
          setMergeDialogOpen(false);
          setUnitsToMerge([]);
          clearMergeError();
        }}
        selectedUnits={unitsToMerge}
        onMerge={handleMerge}
        loading={mergeLoading}
        error={mergeError}
      />

      {/* Content Modal for viewing cell values with excerpts */}
      <ContentModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        title={modalContent.title}
        content={modalContent.content}
      />

      {/* Filter Dialog */}
      <FilterDialog
        open={filterDialogOpen}
        onClose={() => setFilterDialogOpen(false)}
        onApply={handleApplyFilter}
        columns={columnMetadata}
        selectedColumn={filterDialogColumn}
      />

      {/* Floating action bar for bulk selection */}
      <BulkActionToolbar
        selectedCount={selectedCount}
        onDelete={() => {
          setBulkDeleteError(null);
          setShowDeleteDialog(true);
        }}
        onClearSelection={clearSelection}
      />

      {/* Bulk Delete Confirmation Dialog */}
      <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete {selectedCount} Row{selectedCount !== 1 ? 's' : ''}</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete {selectedCount} selected row{selectedCount !== 1 ? 's' : ''}? This will permanently remove {selectedCount !== 1 ? 'them' : 'it'} and all associated data.
            </DialogDescription>
          </DialogHeader>

          {bulkDeleteError && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>{bulkDeleteError}</AlertDescription>
            </Alert>
          )}

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setShowDeleteDialog(false)}
              disabled={isBulkDeleting}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={handleBulkDelete}
              disabled={isBulkDeleting}
            >
              {isBulkDeleting ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Deleting...
                </>
              ) : (
                'Delete'
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </Card>
  );
};

/**
 * Parse pipe-separated excerpt strings like: {'text': '...', 'source': '...'} | {'text': '...'}
 */
function parseExcerpts(excerpts: unknown[]): Array<{text: string; source: string}> {
  const result: Array<{text: string; source: string}> = [];

  for (const exc of excerpts) {
    if (typeof exc === 'string') {
      // Check if it's pipe-separated
      if (exc.includes("'text':") || exc.includes('"text":')) {
        // Split by pipe and parse each part
        const parts = exc.split(/\s*\|\s*/);
        for (const part of parts) {
          const parsed = parsePythonString(part.trim());
          if (typeof parsed === 'object' && parsed !== null && 'text' in parsed) {
            const obj = parsed as Record<string, unknown>;
            result.push({
              text: String(obj.text || ''),
              source: String(obj.source || `Source ${result.length + 1}`)
            });
          } else if (typeof parsed === 'string' && parsed.trim()) {
            result.push({ text: parsed, source: `Source ${result.length + 1}` });
          }
        }
      } else if (exc.trim()) {
        result.push({ text: exc, source: `Source ${result.length + 1}` });
      }
    } else if (typeof exc === 'object' && exc !== null) {
      const obj = exc as Record<string, unknown>;
      if ('text' in obj) {
        result.push({
          text: String(obj.text || ''),
          source: String(obj.source || `Source ${result.length + 1}`)
        });
      }
    }
  }

  return result;
}

/**
 * Normalize value to QBSD format with 'answer' and 'excerpts'.
 */
function normalizeToQBSD(val: unknown): unknown {
  if (!val || typeof val !== 'object') return val;

  // If it's an array with dict items, take first item
  if (Array.isArray(val) && val.length > 0 && typeof val[0] === 'object') {
    return normalizeToQBSD(val[0]);
  }

  const obj = val as Record<string, unknown>;

  // Already in QBSD format
  if ('answer' in obj) {
    let answerVal = obj.answer;
    let excerptsVal = obj.excerpts || [];

    // Parse answer if it's a JSON string
    if (typeof answerVal === 'string') {
      const parsed = parsePythonString(answerVal);
      if (parsed !== answerVal && Array.isArray(parsed) && parsed.length > 0) {
        const firstItem = parsed[0];
        if (typeof firstItem === 'object') {
          const item = firstItem as Record<string, unknown>;
          answerVal = item.value || item.answer || String(firstItem);
          const allExcerpts: unknown[] = [];
          for (const p of parsed) {
            const pObj = p as Record<string, unknown>;
            const exc = pObj.excerpt || pObj.excerpts;
            if (exc) {
              allExcerpts.push(...(Array.isArray(exc) ? exc : [exc]));
            }
          }
          if (allExcerpts.length > 0) {
            excerptsVal = allExcerpts;
          }
        }
      }
    }

    // Parse excerpts using parseExcerpts
    const parsedExcerpts = parseExcerpts(excerptsVal as unknown[]);

    return {
      answer: answerVal,
      excerpts: parsedExcerpts
    };
  }

  // Normalize 'value'/'excerpt'/'citation' format
  if ('value' in obj) {
    const excerptsRaw = obj.citation ? [obj.citation] :
                        obj.excerpt ? [obj.excerpt] :
                        (obj.excerpts || []);
    return {
      answer: obj.value,
      excerpts: parseExcerpts(excerptsRaw as unknown[])
    };
  }

  // ExcerptWithSource format - wrap text as answer with source
  if ('text' in obj) {
    return {
      answer: obj.text,
      excerpts: obj.source ? [{ text: String(obj.text), source: String(obj.source) }] : []
    };
  }

  return val;
}

/** Maximum visible lines in data cells before CSS truncation */
const DATA_CELL_MAX_LINES = 8;

/** CSS line-clamp style for multi-line text truncation */
const lineClampStyle: React.CSSProperties = {
  display: '-webkit-box',
  WebkitBoxOrient: 'vertical' as const,
  WebkitLineClamp: DATA_CELL_MAX_LINES,
  overflow: 'hidden',
};

/**
 * Cell value formatter for the unit table.
 * Handles QBSD answer/excerpts, value/excerpt, and text formats.
 * Renders clickable cells for content with excerpts.
 */
function formatCellValue(
  value: CellValue,
  columnName: string,
  rowData: DataRow | null,
  excerptMapping: Record<string, string>,
  onViewContent: (columnName: string, content: CellValue) => void
): React.ReactNode {
  if (value === null || value === undefined) {
    return <Badge variant="outline" className="text-muted-foreground">null</Badge>;
  }

  // Try to parse string values that look like JSON/Python objects
  let processedValue: unknown = typeof value === 'string' ? parsePythonString(value) : value;

  // Check if parsing resulted in an empty value
  const displayStr = extractDisplayValue(processedValue);
  if (!displayStr || displayStr === 'null' || displayStr === 'None' || displayStr === 'N/A') {
    return <Badge variant="outline" className="text-muted-foreground">null</Badge>;
  }

  // Check for excerpt in separate _excerpt column
  const excerptColumnName = excerptMapping[columnName];
  const hasExcerptColumn = rowData &&
    excerptColumnName &&
    rowData.data[excerptColumnName];

  // Helper to get excerpts from the _excerpt column
  const getExcerptsFromColumn = (): Array<{text: string; source: string}> => {
    if (!hasExcerptColumn) return [];
    const excerptStr = String(rowData!.data[excerptColumnName]);
    return parseExcerpts([excerptStr]);
  };

  // Normalize to QBSD format if it's an object
  if (typeof processedValue === 'object' && processedValue !== null) {
    processedValue = normalizeToQBSD(processedValue);
  }

  // Handle QBSD format objects with answer and excerpts
  if (typeof processedValue === 'object' && processedValue !== null) {
    const obj = processedValue as Record<string, unknown>;
    if ('answer' in obj && typeof obj.answer !== 'undefined') {
      const qbsdValue = processedValue as QBSDAnswerWithExcerpts;
      const answer = qbsdValue.answer;
      let excerpts = qbsdValue.excerpts || [];

      // Check if the answer itself is empty
      const answerStr = extractDisplayValue(answer);
      if (!answerStr || answerStr === 'null' || answerStr === 'None' || answerStr === 'N/A') {
        return <Badge variant="outline" className="text-muted-foreground">null</Badge>;
      }

      // Also check for excerpts in _excerpt column if not already present
      if (excerpts.length === 0 && hasExcerptColumn) {
        excerpts = getExcerptsFromColumn();
      }

      const hasExcerptsData = excerpts.length > 0 || hasExcerptColumn;
      const showExpandIcon = hasExcerptsData || answerStr.length > 30;

      if (showExpandIcon) {
        const tooltip = hasExcerptsData ? "Click to view excerpts" : "Click to view full content";
        // Build content with excerpts from _excerpt column if available
        const modalExcerpts = excerpts.length > 0 ? excerpts : (hasExcerptColumn ? getExcerptsFromColumn() : []);
        return (
          <div
            className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
            onClick={() => onViewContent(columnName, { answer, excerpts: modalExcerpts })}
            title={tooltip}
          >
            <div className="text-xs leading-relaxed break-words" style={lineClampStyle}>
              {answerStr}
            </div>
          </div>
        );
      }

      return (
        <div
          className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
          onClick={() => onViewContent(columnName, { answer, excerpts: [] })}
          title="Click to view content"
        >
          <div className="text-xs leading-relaxed" style={lineClampStyle}>
            {answerStr}
          </div>
        </div>
      );
    }
  }

  // Handle string values - check for excerpt column or long text
  if (hasExcerptColumn || displayStr.length > 100) {
    const modalExcerpts = hasExcerptColumn ? getExcerptsFromColumn() : [];
    const tooltip = hasExcerptColumn ? "Click to view excerpts" : "Click to view full content";

    return (
      <div
        className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
        onClick={() => onViewContent(columnName, {
          answer: displayStr,
          excerpts: modalExcerpts
        })}
        title={tooltip}
      >
        <div className="text-xs leading-relaxed break-words" style={lineClampStyle}>
          {displayStr}
        </div>
      </div>
    );
  }

  return (
    <div
      className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
      onClick={() => onViewContent(columnName, displayStr)}
      title="Click to view content"
    >
      <div className="text-xs leading-relaxed" style={lineClampStyle}>{displayStr}</div>
    </div>
  );
}

/**
 * Format source document path for display.
 * Extracts filename and removes extension for cleaner display.
 */
function formatSourceDocument(source: string | undefined | null): string {
  if (!source) return 'Unknown';
  const parts = source.split('/');
  const filename = parts[parts.length - 1];
  return filename
    .replace(/\.(pdf|txt|md|docx?)$/i, '')
    .replace(/_/g, ' ')
    .trim() || 'Unknown';
}

export default UnitGroupedTable;
