/**
 * Table component that displays data by observation units in a flat layout.
 * Uses a frozen left column for unit names, similar to DataTable's frozen column pattern.
 */

import React, { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { Loader2, Lightbulb, FileText, AlertCircle, Search, GripVertical, ExternalLink } from 'lucide-react';
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  DragEndEvent,
} from '@dnd-kit/core';
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  horizontalListSortingStrategy,
  useSortable,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
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

import { unitsAPI, observationUnitAPI, schematiqAPI } from '../../services/api';
import { useUnits, useMergeUnits, useUnitSuggestions } from '../../hooks/useUnits';
import { MergeUnitsRequest, UnitSummary } from '../../types/unit';
import { UnitMergeDialog } from '../ViewMode/UnitMergeDialog';
import { UnitSimilarityCard } from '../Units/UnitSimilarityCard';
import { UnitMergePickerDialog } from './UnitMergePickerDialog';
import BulkActionToolbar from './BulkActionToolbar';
import { useRowSelection } from './hooks/useRowSelection';
import ContentModal from '../ContentModal/ContentModal';
import { DataRow, CellValue, CellStatus, ModalContent, ScheMatiQAnswerWithExcerpts } from '../../types';
import { cn } from '@/lib/utils';
import { useToast } from '@/components/ui/use-toast';
import { ToastAction } from '@/components/ui/toast';
import { getEditableValue } from './EditableCell';
import { formatColumnName } from '../../utils/formatting';
import {
  buildColumnMetadata,
  parsePythonString,
  extractDisplayValue,
  getDefaultColumnOrder,
  buildExcerptMapping,
  parseExcerpts,
  normalizeToScheMatiQ,
} from './utils';
import { FilterOperator, FilterValue, ColumnMetadata, FilterRule, SortColumn } from './types/filters';
import { useTableSort } from './hooks/useTableSort';
import { useTableFilter } from './hooks/useTableFilter';
import { useColumnVisibility } from './hooks/useColumnVisibility';
import { useColumnStats } from './hooks/useColumnStats';
import FilterBar from './FilterBar';
import FilterDialog from './FilterDialog';
import TableOptionsMenu from './TableOptionsMenu';
import { AVAILABLE_PAGE_SIZES } from '../../constants';
import ExtractionProgressBar from './ExtractionProgressBar';
import { useColumnResize, MIN_COLUMN_WIDTH, DEFAULT_COLUMN_WIDTH } from './hooks/useColumnResize';

// Cell status background colors for enrichment provenance tracking
const CELL_STATUS_STYLES: Record<CellStatus, string> = {
  no_change: '',
  novel_nes: 'border-l-2 border-l-emerald-400 bg-emerald-50/40 dark:bg-emerald-950/20',
  enriched: 'border-l-2 border-l-blue-400 bg-blue-50/40 dark:bg-blue-950/20',
  inferred: 'border-l-2 border-l-amber-400 bg-amber-50/40 dark:bg-amber-950/20',
  external_source: 'border-l-2 border-l-purple-400 bg-purple-50/40 dark:bg-purple-950/20',
  schematiq_original: '',
  schematiq_expanded: 'border-l-2 border-l-emerald-400 bg-emerald-50/40 dark:bg-emerald-950/20',
};

function getCellStatusClass(row: DataRow, column: string): string {
  const status = row._cell_status?.[column];
  if (!status) return '';
  return CELL_STATUS_STYLES[status] || '';
}

// Sortable column header for drag-and-drop reordering
const SortableColumnHeader: React.FC<{
  column: string;
  columnWidth?: number;
  onResizeStart: (e: React.MouseEvent, column: string, startWidth: number) => void;
  headerRefs: React.MutableRefObject<Record<string, HTMLTableCellElement | null>>;
  children: React.ReactNode;
}> = ({ column, columnWidth, onResizeStart, headerRefs, children }) => {
  const thRef = useRef<HTMLTableCellElement>(null);
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: column });

  const setRefs = useCallback((node: HTMLTableCellElement | null) => {
    setNodeRef(node);
    (thRef as React.MutableRefObject<HTMLTableCellElement | null>).current = node;
    headerRefs.current[column] = node;
  }, [setNodeRef, column, headerRefs]);

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    position: 'relative' as const,
    ...(columnWidth ? { width: columnWidth, minWidth: MIN_COLUMN_WIDTH } : { width: 100 }),
  };

  return (
    <th
      ref={setRefs}
      style={style}
      className={cn(
        "group px-2 py-2 text-left font-semibold text-xs bg-background cursor-grab",
        !columnWidth && "min-w-[80px] sm:min-w-[100px]",
        isDragging && "bg-muted opacity-50",
      )}
      {...attributes}
      {...listeners}
    >
      <div className="flex items-center gap-1">
        <GripVertical className="h-3.5 w-3.5 text-muted-foreground shrink-0 opacity-0 group-hover:opacity-50" />
        {children}
      </div>
      <div
        className="absolute right-0 top-0 bottom-0 w-[6px] cursor-col-resize hover:bg-primary/40 z-10"
        onMouseDown={(e) => {
          e.stopPropagation();
          e.preventDefault();
          if (thRef.current) onResizeStart(e, column, thRef.current.offsetWidth);
        }}
      />
    </th>
  );
};

interface UnitGroupedTableProps {
  /** Session ID */
  sessionId: string;
  /** Session type */
  sessionType: 'load' | 'schematiq';
  /** Columns to display */
  columns: string[];
  /** Column metadata for display */
  columnInfo?: { name: string; display_name?: string; definition?: string; allowed_values?: string[] }[];
  /** Callback when data changes */
  onDataChange?: () => void;
  /** Columns currently being re-extracted */
  processingColumns?: Set<string>;
  /** Column currently being extracted (for active chip highlight) */
  currentColumn?: string | null;
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
  /** Callback when columns are reordered via drag-and-drop */
  onColumnReorder?: (newOrder: string[]) => void;
  /** Map of document name to external URL (e.g., DOI link) */
  documentUrlMap?: Map<string, string>;
}

export const UnitGroupedTable: React.FC<UnitGroupedTableProps> = ({
  sessionId,
  sessionType,
  columns,
  columnInfo,
  onDataChange,
  processingColumns,
  currentColumn,
  currentDocumentProgress,
  onStopReextraction,
  isStoppingReextraction,
  isProcessingDocuments,
  onStopProcessing,
  isStoppingProcessing,
  columnOrder,
  onColumnReorder,
  documentUrlMap,
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

  // Drag-and-drop sensors for column reordering
  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: { distance: 8 },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

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
    [
      'unitData',
      sessionId,
      selectedUnits,
      page,
      pageSize,
      searchTerm,
      JSON.stringify(filterState.rules),
      JSON.stringify(sortState.columns),
    ],
    () => unitsAPI.getData(sessionId, {
      units: effectiveUnitFilter,
      page,
      pageSize,
      search: searchTerm.trim() || undefined,
      filters: filterState.rules.length > 0 ? filterState.rules : undefined,
      sort: sortState.columns.length > 0 ? sortState.columns : undefined,
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

  // Reset to first page when any server-side filter/search/sort changes,
  // otherwise we may land on an empty page outside the new filtered range.
  useEffect(() => {
    setPage(0);
  }, [selectedUnits, searchTerm, filterState.rules, sortState.columns]);

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
  const handleViewContent = useCallback((columnName: string, content: CellValue, row?: DataRow) => {
    setModalContent({
      title: `${formatColumnName(columnName)} - Full Content`,
      content: content,
      rowName: row?.row_name || row?._unit_name,
      column: columnName,
      sourceDocument: row?._source_document,
      rowIndex: row?._row_index,
    });
    setModalOpen(true);
  }, []);

  // rowIndex is threaded through so the undo below identifies the row the same
  // way the edit did; identifying it more weakly can revert a different row.
  const handleCellUpdate = useCallback(async (rowName: string, column: string, value: string, sourceDocument?: string, rowIndex?: number) => {
    try {
      const result = await schematiqAPI.updateCell(sessionId, rowName, column, value, sourceDocument);
      const previousValue = result.previous_value;
      refetchData();

      toast({
        title: 'Cell updated',
        description: `Updated "${column}" for "${rowName}"`,
        duration: 8000,
        action: (
          <ToastAction altText="Undo" onClick={async () => {
            try {
              await schematiqAPI.restoreCell(sessionId, rowName, column, previousValue, sourceDocument, rowIndex);
              refetchData();
            } catch {
              toast({ title: 'Undo failed', description: 'Could not revert the cell edit.', variant: 'destructive' });
            }
          }}>
            Undo
          </ToastAction>
        ),
      });
    } catch (error) {
      toast({ title: 'Update failed', description: 'Could not save the cell edit.', variant: 'destructive' });
      throw error;  // Re-throw so EditableCell keeps edit mode open
    }
  }, [sessionId, refetchData, toast]);

  // Filter suggestions by dismissed
  const visibleSuggestions = useMemo(() => {
    if (!suggestions?.suggestions) return [];
    return suggestions.suggestions.filter(s => {
      const key = [...s.units].sort().join('|');
      return !dismissedSuggestions.has(key);
    });
  }, [suggestions, dismissedSuggestions]);

  // Server applies search/filters/sort across the full dataset before paginating
  // by unit. We only re-stable-sort by unit name so visual grouping survives
  // any per-row sort the server applied within a unit.
  const processedRows = useMemo(() => {
    if (!unitData?.rows) return [];

    const rows = unitData.rows;
    const indexMap = new Map(rows.map((r, i) => [r, i]));
    return [...rows].sort((a, b) => {
      const unitA = (a._unit_name || '').toLowerCase();
      const unitB = (b._unit_name || '').toLowerCase();
      const cmp = unitA.localeCompare(unitB);
      if (cmp !== 0) return cmp;
      return (indexMap.get(a) ?? 0) - (indexMap.get(b) ?? 0);
    });
  }, [unitData?.rows]);

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

  // Unit grouping helpers — show unit name only on first row of each group
  const shouldRenderUnitCell = useCallback((rowIndex: number): boolean => {
    if (rowIndex === 0) return true;
    const current = (processedRows[rowIndex]?._unit_name || '').toLowerCase();
    const prev = (processedRows[rowIndex - 1]?._unit_name || '').toLowerCase();
    return current !== prev;
  }, [processedRows]);

  const getUnitRowSpan = useCallback((rowIndex: number): number => {
    const current = (processedRows[rowIndex]?._unit_name || '').toLowerCase();
    let span = 1;
    for (let i = rowIndex + 1; i < processedRows.length; i++) {
      if ((processedRows[i]?._unit_name || '').toLowerCase() === current) span++;
      else break;
    }
    return span;
  }, [processedRows]);

  const unitGroups = useMemo(() => {
    const groups: { startIndex: number; rowCount: number; unitName: string }[] = [];
    processedRows.forEach((row, index) => {
      if (shouldRenderUnitCell(index)) {
        groups.push({ startIndex: index, rowCount: getUnitRowSpan(index), unitName: row._unit_name || '' });
      }
    });
    return groups;
  }, [processedRows, shouldRenderUnitCell, getUnitRowSpan]);

  const getUnitGroupIndex = useCallback((rowIndex: number): number => {
    for (let i = unitGroups.length - 1; i >= 0; i--) {
      if (rowIndex >= unitGroups[i].startIndex) return i;
    }
    return 0;
  }, [unitGroups]);

  // Check if any row has _source_document
  const hasSourceDocument = useMemo(() => {
    return unitData?.rows?.some(row => row._source_document != null) ?? false;
  }, [unitData?.rows]);

  // Set of cell-status values actually present — drives which legend pills
  // render. Per-status (instead of a single boolean) means e.g. a UniProt-only
  // session shows just the "External" pill, not Novel NES / Enriched / Inferred.
  const presentCellStatuses = useMemo(() => {
    const set = new Set<CellStatus>();
    for (const row of unitData?.rows ?? []) {
      if (!row._cell_status) continue;
      for (const status of Object.values(row._cell_status)) {
        if (status) set.add(status);
      }
    }
    return set;
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

  // Handle column drag-and-drop reorder
  const handleDragEnd = useCallback((event: DragEndEvent) => {
    const { active, over } = event;
    if (over && active.id !== over.id && onColumnReorder) {
      const oldIndex = visibleColumns.indexOf(active.id as string);
      const newIndex = visibleColumns.indexOf(over.id as string);
      const newVisibleOrder = arrayMove(visibleColumns, oldIndex, newIndex);
      const newOrder = ['_unit_name', ...(hasSourceDocument ? ['_source_document'] : []), ...newVisibleOrder];
      onColumnReorder(newOrder);
    }
  }, [visibleColumns, hasSourceDocument, onColumnReorder]);

  const excerptMapping = useMemo(
    () => buildExcerptMapping(unitData?.rows || []),
    [unitData?.rows],
  );

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
                ({unitData?.total_count ?? unitListResponse?.totalRows} rows)
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

        {/* Cell status legend — each pill only when that status is present */}
        {(presentCellStatuses.has('novel_nes') ||
          presentCellStatuses.has('enriched') ||
          presentCellStatuses.has('inferred') ||
          presentCellStatuses.has('external_source')) && (
          <div className="flex items-center gap-4 mb-3 text-xs text-muted-foreground">
            <span className="font-medium text-foreground/70">Cell provenance:</span>
            {presentCellStatuses.has('novel_nes') && (
              <span className="flex items-center gap-1.5">
                <span className="inline-block w-2.5 h-2.5 rounded-sm bg-emerald-400" />
                Novel NES
              </span>
            )}
            {presentCellStatuses.has('enriched') && (
              <span className="flex items-center gap-1.5">
                <span className="inline-block w-2.5 h-2.5 rounded-sm bg-blue-400" />
                Enriched
              </span>
            )}
            {presentCellStatuses.has('inferred') && (
              <span className="flex items-center gap-1.5">
                <span className="inline-block w-2.5 h-2.5 rounded-sm bg-amber-400" />
                Inferred
              </span>
            )}
            {presentCellStatuses.has('external_source') && (
              <span className="flex items-center gap-1.5">
                <span className="inline-block w-2.5 h-2.5 rounded-sm bg-purple-400" />
                External
              </span>
            )}
          </div>
        )}

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
          <ExtractionProgressBar
            processingColumns={processingColumns || new Set()}
            currentColumn={currentColumn}
            currentDocumentProgress={currentDocumentProgress}
            onStop={processingColumns && processingColumns.size > 0 ? onStopReextraction : onStopProcessing}
            isStopping={processingColumns && processingColumns.size > 0 ? isStoppingReextraction : isStoppingProcessing}
            isProcessingDocuments={isProcessingDocuments}
            unitLabel="Observation Unit"
            variant="neutral"
          />
        )}

        {/* Table */}
        <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
        <div className="overflow-auto max-h-[600px] border rounded-md">
          <table
            className="border-collapse"
            style={{
              tableLayout: 'fixed' as const,
              width: `${Math.max(600, allTableColumns.reduce((sum, col) => sum + (getColumnWidth(col) || DEFAULT_COLUMN_WIDTH), 0))}px`,
            }}
          >
            <thead className="sticky top-0 z-10 bg-background border-b">
              <tr>
                {/* Frozen _unit_name column */}
                <th
                  ref={frozenThRef}
                  className={cn(
                    "pl-1 pr-2 py-1 text-left font-semibold text-sm sticky bg-background z-20 border-r-2 border-primary shadow-[2px_0_4px_rgba(0,0,0,0.1)] relative",
                    !getColumnWidth('_unit_name') && "min-w-[80px] max-w-[150px]",
                    "left-0"
                  )}
                  style={getColumnWidth('_unit_name') ? { width: getColumnWidth('_unit_name'), minWidth: MIN_COLUMN_WIDTH } : { width: 100 }}
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
                      "px-2 py-1 text-left font-semibold text-sm bg-background border-r relative",
                      !getColumnWidth('_source_document') && "min-w-[150px] max-w-[300px]"
                    )}
                    style={getColumnWidth('_source_document') ? { width: getColumnWidth('_source_document'), minWidth: MIN_COLUMN_WIDTH } : { width: 200 }}
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

                {/* Scrollable data columns (drag-and-drop reorderable) */}
                <SortableContext items={visibleColumns} strategy={horizontalListSortingStrategy}>
                {visibleColumns.map(column => {
                  const colInfo = columnInfo?.find(c => c.name === column);
                  const colDef = colInfo?.definition;
                  const colLabel = colInfo?.display_name || formatColumnName(column);
                  return (
                    <SortableColumnHeader
                      key={column}
                      column={column}
                      columnWidth={getColumnWidth(column)}
                      onResizeStart={(e, col, width) => handleResizeStart(e, col, width)}
                      headerRefs={headerRefs}
                    >
                      {colDef ? (
                        <Tooltip delayDuration={300}>
                          <TooltipTrigger asChild>
                            <span className="cursor-help underline decoration-dashed decoration-muted-foreground/40 underline-offset-4">{colLabel}</span>
                          </TooltipTrigger>
                          <TooltipContent side="bottom" align="start" className="max-w-xs px-3 py-2">
                            <p className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground/70 mb-1.5">Definition</p>
                            <p className="text-[13px] font-normal leading-snug">{colDef}</p>
                          </TooltipContent>
                        </Tooltip>
                      ) : (
                        colLabel
                      )}
                    </SortableColumnHeader>
                  );
                })}
                </SortableContext>
              </tr>
            </thead>
            <tbody>
              {/* Row rendering with unit grouping */}
              {processedRows.map((row, rowIndex) => {
                const rowId = row._unit_name || row.row_name || '';
                const rowSelected = isSelected(rowId);
                const showCheckbox = hoveredRowId === rowId || selectedCount > 0;
                const isFirstOfGroup = shouldRenderUnitCell(rowIndex);
                const isGroupBoundary = isFirstOfGroup && rowIndex > 0;
                const groupIndex = getUnitGroupIndex(rowIndex);
                const isOddGroup = groupIndex % 2 === 1;

                return (
                  <tr
                    key={rowIndex}
                    className={cn(
                      "border-b hover:bg-muted/50 transition-colors",
                      rowSelected && "bg-blue-50 dark:bg-blue-950/50",
                      isGroupBoundary && "border-t-2 border-t-foreground/20",
                      isOddGroup && !rowSelected && "bg-muted/30"
                    )}
                    onMouseEnter={() => setHoveredRowId(rowId)}
                    onMouseLeave={() => setHoveredRowId(null)}
                  >
                    {/* Frozen _unit_name cell — grouped: show name only on first row */}
                    <td
                      className={cn(
                        "pl-1 pr-2 py-1 sticky border-r overflow-hidden",
                        !getColumnWidth('_unit_name') && "min-w-[80px] max-w-[150px]",
                        "left-0",
                        isOddGroup ? "bg-muted/30" : "bg-background",
                        isGroupBoundary && "border-t-2 border-t-foreground/20",
                        !isFirstOfGroup && "border-l-4 border-l-primary/20"
                      )}
                      style={{
                        zIndex: 5,
                        verticalAlign: isFirstOfGroup ? 'top' : 'middle',
                        ...(getColumnWidth('_unit_name') ? { width: getColumnWidth('_unit_name'), minWidth: MIN_COLUMN_WIDTH } : { width: 100 }),
                      }}
                    >
                      {isFirstOfGroup ? (
                        <div className="flex flex-col gap-1">
                          <span className="text-sm font-medium break-words overflow-hidden" style={{ wordBreak: 'break-word' }}>{row._unit_name || 'Unknown'}</span>
                          {getUnitRowSpan(rowIndex) > 1 && (
                            <Badge variant="secondary" className="text-xs w-fit">
                              {getUnitRowSpan(rowIndex)} documents
                            </Badge>
                          )}
                        </div>
                      ) : (
                        <span className="text-xs text-muted-foreground/50 break-words overflow-hidden" style={{ wordBreak: 'break-word' }}>{row._unit_name || 'Unknown'}</span>
                      )}
                    </td>

                    {/* Source Document cell - always visible when present */}
                    {hasSourceDocument && (
                      <td
                        className="pl-1 pr-2 py-1 text-sm border-r bg-muted/20"
                        style={getColumnWidth('_source_document') ? { width: getColumnWidth('_source_document'), minWidth: MIN_COLUMN_WIDTH } : { width: 200 }}
                      >
                        {(() => {
                          const docUrl = documentUrlMap?.get(row._source_document || '');
                          return (
                            <div className="flex items-center gap-1.5">
                              <FileText className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  {docUrl ? (
                                    <a
                                      href={docUrl}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="truncate max-w-[260px] font-medium text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 hover:underline cursor-pointer"
                                      onClick={(e) => e.stopPropagation()}
                                    >
                                      {formatSourceDocument(row._source_document)}
                                    </a>
                                  ) : (
                                    <span className="truncate max-w-[260px] font-medium text-foreground/80 cursor-help">
                                      {formatSourceDocument(row._source_document)}
                                    </span>
                                  )}
                                </TooltipTrigger>
                                <TooltipContent side="right" className="max-w-[400px]">
                                  <p className="break-all">{row._source_document || 'Unknown'}</p>
                                  {docUrl && <p className="text-xs text-muted-foreground mt-1">{docUrl}</p>}
                                </TooltipContent>
                              </Tooltip>
                              {docUrl && <ExternalLink className="h-3 w-3 text-blue-500 shrink-0" />}
                            </div>
                          );
                        })()}
                      </td>
                    )}

                    {/* Scrollable data columns */}
                    {visibleColumns.map(column => (
                      <td
                        key={column}
                        className={cn("px-2 py-1", !getColumnWidth(column) && "min-w-[80px] sm:min-w-[100px]", getCellStatusClass(row, column))}
                        style={{
                          verticalAlign: 'top',
                          ...(getColumnWidth(column) ? { width: getColumnWidth(column), minWidth: MIN_COLUMN_WIDTH } : { width: 100 }),
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
                    colSpan={visibleColumns.length + (hasSourceDocument ? 2 : 1)}
                    className="px-4 py-12 text-center text-muted-foreground"
                  >
                    No rows match the current filters or search.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        </DndContext>

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
        content={(() => {
          // Derive content from live unitData so undo/refetch updates the modal
          if (modalContent.rowName && modalContent.column) {
            const row = unitData?.rows?.find(r => r.row_name === modalContent.rowName || r._unit_name === modalContent.rowName);
            if (row?.data?.[modalContent.column] !== undefined) return row.data[modalContent.column];
          }
          return modalContent.content;
        })()}
        onSave={modalContent.rowName && modalContent.column
          ? async (value: string) => {
              await handleCellUpdate(modalContent.rowName!, modalContent.column!, value, modalContent.sourceDocument, modalContent.rowIndex);
            }
          : undefined
        }
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
 * Handles ScheMatiQ answer/excerpts, value/excerpt, and text formats.
 * Renders clickable cells for content with excerpts.
 */
function formatCellValue(
  value: CellValue,
  columnName: string,
  rowData: DataRow | null,
  excerptMapping: Record<string, string>,
  onViewContent: (columnName: string, content: CellValue, row?: DataRow) => void
): React.ReactNode {
  if (value === null || value === undefined) {
    return (
      <div
        className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
        onClick={() => onViewContent(columnName, value, rowData ?? undefined)}
        title="Click to edit"
      >
        <Badge variant="outline" className="text-muted-foreground">null</Badge>
      </div>
    );
  }

  // Try to parse string values that look like JSON/Python objects
  let processedValue: unknown = typeof value === 'string' ? parsePythonString(value) : value;

  // Check if parsing resulted in an empty value
  const displayStr = extractDisplayValue(processedValue);
  if (!displayStr || displayStr === 'null' || displayStr === 'None' || displayStr === 'N/A') {
    return (
      <div
        className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
        onClick={() => onViewContent(columnName, value, rowData ?? undefined)}
        title="Click to edit"
      >
        <Badge variant="outline" className="text-muted-foreground">null</Badge>
      </div>
    );
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

  // Normalize to ScheMatiQ format if it's an object
  if (typeof processedValue === 'object' && processedValue !== null) {
    processedValue = normalizeToScheMatiQ(processedValue);
  }

  // Handle ScheMatiQ format objects with answer and excerpts
  if (typeof processedValue === 'object' && processedValue !== null) {
    const obj = processedValue as Record<string, unknown>;
    if ('answer' in obj && typeof obj.answer !== 'undefined') {
      const schematiqValue = processedValue as ScheMatiQAnswerWithExcerpts;
      const answer = schematiqValue.answer;
      let excerpts = schematiqValue.excerpts || [];
      const manuallyEdited = schematiqValue.manually_edited;

      // Check if the answer itself is empty
      const answerStr = extractDisplayValue(answer);
      if (!answerStr || answerStr === 'null' || answerStr === 'None' || answerStr === 'N/A') {
        return (
          <div
            className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
            onClick={() => onViewContent(columnName, processedValue as CellValue, rowData ?? undefined)}
            title="Click to edit"
          >
            <Badge variant="outline" className="text-muted-foreground">null</Badge>
          </div>
        );
      }

      // Also check for excerpts in _excerpt column if not already present
      if (excerpts.length === 0 && hasExcerptColumn) {
        excerpts = getExcerptsFromColumn();
      }

      const hasExcerptsData = excerpts.length > 0 || hasExcerptColumn;
      const showExpandIcon = hasExcerptsData || answerStr.length > 30;
      const editedFlag = manuallyEdited ? { manually_edited: true as const } : {};

      if (showExpandIcon) {
        const tooltip = hasExcerptsData ? "Click to view excerpts" : "Click to view full content";
        // Build content with excerpts from _excerpt column if available
        const modalExcerpts = excerpts.length > 0 ? excerpts : (hasExcerptColumn ? getExcerptsFromColumn() : []);
        return (
          <div
            className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
            onClick={() => onViewContent(columnName, { answer, excerpts: modalExcerpts, ...editedFlag }, rowData ?? undefined)}
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
          onClick={() => onViewContent(columnName, { answer, excerpts: [], ...editedFlag }, rowData ?? undefined)}
          title="Click to view content"
        >
          <div className="text-xs leading-relaxed" style={lineClampStyle}>
            {answerStr}
          </div>
        </div>
      );
    }
  }

  // Render URL values as clickable links directly in the cell
  if (/^https?:\/\/\S+$/.test(displayStr)) {
    return (
      <a
        href={displayStr}
        target="_blank"
        rel="noopener noreferrer"
        className="text-xs text-blue-600 dark:text-blue-400 underline hover:text-blue-800 dark:hover:text-blue-300 break-all leading-relaxed"
        title={displayStr}
        onClick={(e) => e.stopPropagation()}
      >
        {displayStr.length > 50 ? displayStr.slice(0, 50) + '...' : displayStr}
      </a>
    );
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
        }, rowData ?? undefined)}
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
      onClick={() => onViewContent(columnName, displayStr, rowData ?? undefined)}
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
