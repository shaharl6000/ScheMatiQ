import React, { useState, useMemo, useCallback, useRef } from 'react';
import { Search, GripVertical, ArrowUp, ArrowDown, Filter, Loader2, Square, Info, AlertCircle, Minus } from 'lucide-react';
import { useQuery } from 'react-query';
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

import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Progress } from '@/components/ui/progress';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { cn } from '@/lib/utils';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { Checkbox } from '@/components/ui/checkbox';

import { PaginatedData, CellValue, DataRow, ModalContent, QBSDAnswerWithExcerpts } from '../../types';
import { sessionAPI, qbsdAPI, observationUnitAPI } from '../../services/api';
import EditableCell from './EditableCell';
import {
  formatColumnName,
  isExcerptContent,
  isVeryLongText,
  hasMultipleLines,
  getPreviewText
} from '../../utils/formatting';
import ContentModal from '../ContentModal/ContentModal';
import ExtractingCell from './ExtractingCell';
import {
  QBSD_REFRESH_INTERVAL,
  AVAILABLE_PAGE_SIZES,
  LONG_TEXT_THRESHOLD,
  MEDIUM_TEXT_THRESHOLD,
  SHORT_TEXT_THRESHOLD,
  MAX_CELL_LINES,
} from '../../constants/index';
import { webSocketService } from '../../services/websocket';

// New filter/sort imports
import { useTableSort } from './hooks/useTableSort';
import { useTableFilter } from './hooks/useTableFilter';
import { useColumnVisibility } from './hooks/useColumnVisibility';
import { useColumnStats } from './hooks/useColumnStats';
import { useColumnResize, MIN_COLUMN_WIDTH } from './hooks/useColumnResize';
import { buildColumnMetadata, isEmpty, parsePythonString } from './utils';
import { FilterOperator, FilterValue, ColumnMetadata, FilterRule, SortColumn } from './types/filters';
import FilterBar from './FilterBar';
import FilterDialog from './FilterDialog';
import TableOptionsMenu from './TableOptionsMenu';
import RowActions from './RowActions';
import BulkActionToolbar from './BulkActionToolbar';
import BulkDeleteDialog from './BulkDeleteDialog';
import { useRowSelection } from './hooks/useRowSelection';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Label } from '@/components/ui/label';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { useToast } from '@/components/ui/use-toast';

interface ColumnInfoProp {
  name: string;
  allowed_values?: string[];
}

interface DataTableProps {
  data?: PaginatedData;
  sessionId: string;
  sessionType: 'load' | 'qbsd';
  newlyAddedRows?: Set<number>;
  columnOrder?: string[];
  onColumnReorder?: (newOrder: string[]) => void;
  streamingCells?: Map<string, Record<string, CellValue>>;
  columnInfo?: ColumnInfoProp[];
  /** Columns currently being re-extracted (for skeleton display) */
  processingColumns?: Set<string>;
  /** Current document being processed for re-extraction */
  currentDocumentProgress?: {
    documentName: string;
    documentIndex: number;
    totalDocuments: number;
  } | null;
  /** Callback to stop re-extraction */
  onStopReextraction?: () => void;
  /** Whether stop is in progress */
  isStoppingReextraction?: boolean;
  /** Whether document processing is in progress (Add More Documents flow) */
  isProcessingDocuments?: boolean;
  /** Callback to stop document processing */
  onStopProcessing?: () => void;
  /** Whether stop processing is in progress */
  isStoppingProcessing?: boolean;
  /** Whether the table is in readonly mode (disables row actions) */
  readonly?: boolean;
  /** Callback when data changes (e.g., row added/deleted) */
  onDataChange?: () => void;
}

// Sortable Header Cell Component
interface SortableHeaderCellProps {
  column: string;
  children: React.ReactNode;
  sortDirection?: 'asc' | 'desc' | null;
  sortPriority?: number | null;
  hasFilter?: boolean;
  onSort?: (column: string, multiSort: boolean) => void;
  onFilter?: (column: string) => void;
  columnWidth?: number;
  onResizeStart?: (e: React.MouseEvent, column: string, currentWidth: number) => void;
}

const SortableHeaderCell: React.FC<SortableHeaderCellProps> = ({
  column,
  children,
  sortDirection,
  sortPriority,
  hasFilter,
  onSort,
  onFilter,
  columnWidth,
  onResizeStart,
}) => {
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
  }, [setNodeRef]);

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    cursor: 'grab',
    position: 'relative' as const,
    ...(columnWidth ? { width: columnWidth, minWidth: MIN_COLUMN_WIDTH } : {}),
  };

  const handleHeaderClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (onSort) {
      onSort(column, e.shiftKey);
    }
  };

  const handleFilterClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (onFilter) {
      onFilter(column);
    }
  };

  const handleResizeMouseDown = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (onResizeStart && thRef.current) {
      onResizeStart(e, column, thRef.current.offsetWidth);
    }
  };

  return (
    <th
      ref={setRefs}
      style={style}
      className={cn(
        "px-4 py-3 text-left font-bold text-base bg-background",
        !columnWidth && "min-w-[120px] sm:min-w-[150px]",
        isDragging && "bg-muted",
        sortDirection && "bg-primary/5"
      )}
      {...attributes}
    >
      <div className="flex items-center gap-1">
        <div {...listeners} className="cursor-grab">
          <GripVertical className="h-4 w-4 text-muted-foreground opacity-50 hover:opacity-100" />
        </div>
        <div
          className="flex items-center gap-1 cursor-pointer hover:text-primary flex-1 overflow-hidden"
          onClick={handleHeaderClick}
        >
          {children}
          {sortDirection && (
            <div className="flex items-center">
              {sortDirection === 'asc' ? (
                <ArrowUp className="h-4 w-4 text-primary" />
              ) : (
                <ArrowDown className="h-4 w-4 text-primary" />
              )}
              {sortPriority && sortPriority > 1 && (
                <span className="text-xs text-primary ml-0.5">{sortPriority}</span>
              )}
            </div>
          )}
        </div>
        <Button
          variant="ghost"
          size="icon"
          className={cn(
            "h-6 w-6 shrink-0",
            hasFilter && "text-primary bg-primary/10"
          )}
          onClick={handleFilterClick}
        >
          <Filter className="h-3 w-3" />
        </Button>
      </div>
      {/* Resize handle */}
      <div
        className="absolute right-0 top-0 bottom-0 w-[6px] cursor-col-resize hover:bg-primary/40 z-10"
        onMouseDown={handleResizeMouseDown}
      />
    </th>
  );
};

// Default empty data for loading state
const EMPTY_DATA: PaginatedData = {
  rows: [],
  total_count: 0,
  page: 0,
  page_size: 50,
  has_more: false
};

const DataTable: React.FC<DataTableProps> = ({
  data: initialData,
  sessionId,
  sessionType,
  newlyAddedRows,
  columnOrder: externalColumnOrder,
  onColumnReorder,
  streamingCells,
  columnInfo,
  processingColumns,
  currentDocumentProgress,
  onStopReextraction,
  isStoppingReextraction,
  isProcessingDocuments,
  onStopProcessing,
  isStoppingProcessing,
  readonly = false,
  onDataChange,
}) => {
  const { toast } = useToast();
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [searchTerm, setSearchTerm] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [modalContent, setModalContent] = useState<ModalContent>({ title: '', content: null });
  const [filterDialogOpen, setFilterDialogOpen] = useState(false);
  const [filterDialogColumn, setFilterDialogColumn] = useState<string | undefined>();
  const [fullnessThreshold, setFullnessThreshold] = useState(0);

  // Add Row dialog state
  const [showAddRowDialog, setShowAddRowDialog] = useState(false);
  const [newRowName, setNewRowName] = useState('');
  const [newRowDocument, setNewRowDocument] = useState('');
  const [isAddingRow, setIsAddingRow] = useState(false);
  const [addRowError, setAddRowError] = useState<string | null>(null);

  // Bulk delete dialog state
  const [showBulkDeleteDialog, setShowBulkDeleteDialog] = useState(false);

  // Refs for table container and frozen column header
  const tableContainerRef = useRef<HTMLDivElement>(null);
  const frozenThRef = useRef<HTMLTableCellElement>(null);

  // Sort, filter, and visibility hooks
  const {
    sortState,
    toggleSort,
    setSortState,
    getSortDirection,
    getSortPriority,
  } = useTableSort({ sessionId });

  const {
    filterState,
    addFilter,
    removeFilter,
    clearFilters,
    setFilterState,
    hasFilterForColumn,
    activeFilterCount,
  } = useTableFilter({ sessionId });

  // Column resize hook
  const {
    columnWidths,
    getColumnWidth,
    handleResizeStart,
  } = useColumnResize({ sessionId });
  const hasCustomWidths = Object.keys(columnWidths).length > 0;

  // Sensors for drag and drop
  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        distance: 8,
      },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  // Fetch data with pagination, filtering, and sorting (server-side)
  const { data: fetchedData, refetch: refetchData } = useQuery(
    [
      'data',
      sessionId,
      sessionType,
      page,
      pageSize,
      JSON.stringify(filterState.rules),
      JSON.stringify(sortState.columns),
      searchTerm
    ],
    () => sessionAPI.getData(
      sessionId,
      sessionType,
      page,
      pageSize,
      filterState.rules.length > 0 ? filterState.rules : undefined,
      sortState.columns.length > 0 ? sortState.columns : undefined,
      searchTerm.trim() || undefined
    ),
    {
      keepPreviousData: true,
      enabled: !!sessionId,
      // Only poll when WebSocket is disconnected (fallback polling)
      // When WebSocket is connected, data updates come via real-time messages
      refetchInterval: () => {
        if (webSocketService.isConnected()) return false;
        return sessionType === 'qbsd' ? QBSD_REFRESH_INTERVAL : false;
      },
    }
  );

  // Cell edit handler
  const handleCellUpdate = useCallback(async (rowName: string, column: string, value: string) => {
    await qbsdAPI.updateCell(sessionId, rowName, column, value);
    refetchData();
  }, [sessionId, refetchData]);

  // Row delete handler
  const handleRowDelete = useCallback(async (rowName: string) => {
    await observationUnitAPI.remove(sessionId, rowName);
    refetchData();
    onDataChange?.();
    toast({
      title: 'Row deleted',
      description: `"${rowName}" has been removed from the table.`,
    });
  }, [sessionId, refetchData, onDataChange, toast]);

  // Row add handler
  const handleAddRow = useCallback(async () => {
    if (!newRowName.trim()) {
      setAddRowError('Row name is required');
      return;
    }

    setIsAddingRow(true);
    setAddRowError(null);

    try {
      await observationUnitAPI.add(sessionId, {
        unit_name: newRowName.trim(),
        document_id: newRowDocument.trim() || undefined,
      });

      setShowAddRowDialog(false);
      setNewRowName('');
      setNewRowDocument('');
      refetchData();
      onDataChange?.();
      toast({
        title: 'Row added',
        description: `"${newRowName.trim()}" has been added to the table.`,
      });
    } catch (err: any) {
      setAddRowError(err.response?.data?.detail || err.message || 'Failed to add row');
    } finally {
      setIsAddingRow(false);
    }
  }, [sessionId, newRowName, newRowDocument, refetchData, onDataChange, toast]);

  const fetchedOrInitialData = fetchedData ?? initialData ?? EMPTY_DATA;

  // Merge streaming cells into the data for display
  const data = useMemo(() => {
    if (!streamingCells || streamingCells.size === 0) {
      return fetchedOrInitialData;
    }

    const mergedRows = [...fetchedOrInitialData.rows];

    // Find the row identifier column using same priority logic as defaultColumns
    // This handles cases where row_name is null but the row identifier is in a data column
    let rowIdentifierColumn: string | null = null;

    if (mergedRows.length > 0 && mergedRows[0].data) {
      const dataColumns = Object.keys(mergedRows[0].data).filter(col =>
        !col.startsWith('_') && !col.endsWith('_excerpt')
      );

      // Helper: normalize column name (same as defaultColumns line 409)
      const normalize = (col: string) => col.toLowerCase().replace(/[_-]/g, ' ');

      // Priority 1: Exact match against row-name-like patterns (same as defaultColumns lines 407-411)
      // These are columns that are SPECIFICALLY row identifiers
      const rowNamePatterns = ['row name', 'row_name', 'rowname', 'name', 'id', 'identifier'];
      rowIdentifierColumn = dataColumns.find(col =>
        rowNamePatterns.includes(normalize(col))
      ) || null;

      // Priority 2: Fuzzy matches (same as defaultColumns lines 432-441)
      // Columns that CONTAIN name/id/title/label
      if (!rowIdentifierColumn) {
        rowIdentifierColumn = dataColumns.find(col => {
          const colLower = col.toLowerCase();
          return colLower.includes('name') || colLower.includes('id') ||
                 colLower.includes('title') || colLower.includes('label');
        }) || null;
      }

      // Priority 3: Fallback to first column
      if (!rowIdentifierColumn && dataColumns.length > 0) {
        rowIdentifierColumn = dataColumns[0];
      }
    }

    // Helper to get row identifier - try row_name first, then row identifier column
    const getRowIdentifier = (row: DataRow): string | null => {
      if (row.row_name) return row.row_name;
      if (rowIdentifierColumn && row.data[rowIdentifierColumn]) {
        const val = row.data[rowIdentifierColumn];
        // Handle both simple values and QBSD format {answer: ...}
        if (typeof val === 'string') return val;
        if (typeof val === 'object' && val !== null) {
          if ('answer' in val) return String(val.answer);
          if ('value' in val) return String(val.value);
        }
        return String(val);
      }
      return null;
    };

    // Build a map of row identifiers to indices for fast lookup
    const rowIdentifierMap = new Map<string, number>();
    mergedRows.forEach((row, index) => {
      const identifier = getRowIdentifier(row);
      if (identifier) {
        rowIdentifierMap.set(identifier, index);
      }
    });

    streamingCells.forEach((cellData, rowName) => {
      const existingRowIndex = rowIdentifierMap.get(rowName) ?? -1;

      if (existingRowIndex >= 0) {
        const existingRow = mergedRows[existingRowIndex];
        mergedRows[existingRowIndex] = {
          ...existingRow,
          data: { ...existingRow.data, ...cellData }
        };
      } else {
        // Row not found on current page - add as new row
        mergedRows.push({
          row_name: rowName,
          papers: [],
          data: cellData
        });
      }
    });

    // Count new streaming rows (not found in original data)
    const existingIdentifiers = new Set(
      fetchedOrInitialData.rows.map(r => getRowIdentifier(r)).filter(Boolean)
    );
    let newStreamingRows = 0;
    streamingCells.forEach((_, rowName) => {
      if (!existingIdentifiers.has(rowName)) {
        newStreamingRows++;
      }
    });

    return {
      ...fetchedOrInitialData,
      rows: mergedRows,
      total_count: fetchedOrInitialData.total_count + newStreamingRows
    };
  }, [fetchedOrInitialData, streamingCells]);

  // Check if observation units are present (enables cell merging for doc_name)
  // Moved BEFORE processedRows so it can be used as a dependency
  const hasObservationUnits = useMemo(() => {
    return data.rows.some(row => row._unit_name != null);
  }, [data.rows]);

  // Helper to normalize document names for comparison (handles case/whitespace differences)
  const normalizeDocName = useCallback((doc: string | undefined): string => {
    return (doc || '').trim().toLowerCase();
  }, []);

  // Data is now filtered and sorted server-side
  // When observation units present, sort by _source_document for visual grouping
  const processedRows = useMemo(() => {
    if (hasObservationUnits) {
      return [...data.rows].sort((a, b) => {
        const docA = normalizeDocName(a._source_document);
        const docB = normalizeDocName(b._source_document);
        const docCompare = docA.localeCompare(docB);
        if (docCompare !== 0) return docCompare;
        // Secondary sort by unit name for consistent ordering within document
        const unitA = (a._unit_name || '').toLowerCase();
        const unitB = (b._unit_name || '').toLowerCase();
        return unitA.localeCompare(unitB);
      });
    }
    return data.rows;
  }, [data.rows, hasObservationUnits, normalizeDocName]);

  // Helper: check if doc_name cell should render (first row of a group)
  // When observation units are present, group by _source_document (the actual document name)
  const shouldRenderDocNameCell = useCallback((rowIndex: number): boolean => {
    if (!hasObservationUnits) return true;
    if (rowIndex === 0) return true;
    const currentDoc = normalizeDocName(processedRows[rowIndex]?._source_document);
    const prevDoc = normalizeDocName(processedRows[rowIndex - 1]?._source_document);
    return currentDoc !== prevDoc;
  }, [hasObservationUnits, processedRows, normalizeDocName]);

  // Helper: get row span for doc_name cell
  // When observation units are present, span based on _source_document grouping
  const getDocNameRowSpan = useCallback((rowIndex: number): number => {
    if (!hasObservationUnits) return 1;
    const currentDoc = normalizeDocName(processedRows[rowIndex]?._source_document);
    let span = 1;
    for (let i = rowIndex + 1; i < processedRows.length; i++) {
      if (normalizeDocName(processedRows[i]?._source_document) === currentDoc) span++;
      else break;
    }
    return span;
  }, [hasObservationUnits, processedRows, normalizeDocName]);

  // Calculate document group boundaries for alternating backgrounds
  const documentGroups = useMemo(() => {
    if (!hasObservationUnits) return [];
    const groups: { startIndex: number; rowCount: number; docName: string }[] = [];

    processedRows.forEach((row, index) => {
      if (shouldRenderDocNameCell(index)) {
        groups.push({
          startIndex: index,
          rowCount: getDocNameRowSpan(index),
          docName: row._source_document || ''
        });
      }
    });
    return groups;
  }, [hasObservationUnits, processedRows, shouldRenderDocNameCell, getDocNameRowSpan]);

  // Get group index for a row (for alternating group backgrounds)
  const getGroupIndex = useCallback((rowIndex: number): number => {
    if (!hasObservationUnits) return 0;
    for (let i = documentGroups.length - 1; i >= 0; i--) {
      if (rowIndex >= documentGroups[i].startIndex) return i;
    }
    return 0;
  }, [hasObservationUnits, documentGroups]);

  // Get row identifiers for the current page (used for selection)
  const pageRowIds = useMemo(() => {
    return processedRows.map(row => row._unit_name || row.row_name || '').filter(Boolean);
  }, [processedRows]);

  // Row selection hook
  const {
    selectedRows,
    isAllPageSelected,
    isIndeterminate,
    toggleRow,
    toggleAllPage,
    clearSelection,
    isSelected,
    selectedCount,
  } = useRowSelection(pageRowIds);

  // Bulk delete handler
  const handleBulkDelete = useCallback(async () => {
    const unitNames = Array.from(selectedRows);
    const result = await observationUnitAPI.removeBulk(sessionId, unitNames);

    clearSelection();
    refetchData();
    onDataChange?.();

    if (result.failed.length === 0) {
      toast({
        title: 'Rows deleted',
        description: `Successfully deleted ${result.deleted_count} row${result.deleted_count !== 1 ? 's' : ''}.`,
      });
    } else {
      toast({
        title: 'Partial deletion',
        description: result.message,
        variant: 'destructive',
      });
    }
  }, [sessionId, selectedRows, clearSelection, refetchData, onDataChange, toast]);

  // Get all column names with proper ordering
  const defaultColumns = useMemo(() => {
    const priorityColumns: string[] = [];
    const regularColumns: string[] = [];

    // First, collect all data columns to check for row-name-like columns
    const allDataColumns = new Set<string>();
    data.rows.forEach(row => {
      Object.keys(row.data).forEach(key => {
        // Skip internal columns starting with _ (these are system columns)
        if (!key.startsWith('_')) {
          allDataColumns.add(key);
        }
      });
    });

    const dataColumnArray = Array.from(allDataColumns).filter(col => !col.endsWith('_excerpt'));

    // Check if there's a row-name-like column in the data (e.g., "Row Name", "Name", etc.)
    const rowNamePatterns = ['row name', 'row_name', 'rowname', 'name', 'id', 'identifier'];
    const hasRowNameColumnInData = dataColumnArray.some(col => {
      const colLower = col.toLowerCase().replace(/[_-]/g, ' ');
      return rowNamePatterns.includes(colLower);
    });

    // Only add _row_name column if:
    // 1. Some rows have row_name at DataRow level, AND
    // 2. There's NO row-name-like column already in the data (to avoid duplicates)
    if (data.rows.some(row => row.row_name) && !hasRowNameColumnInData) {
      priorityColumns.push('_row_name');
    }

    // Add _unit_name column if any row has it (observation unit for multi-row docs)
    if (data.rows.some(row => row._unit_name != null)) {
      priorityColumns.push('_unit_name');
    }

    // Add _source_document column if any row has it (shows actual document name)
    if (data.rows.some(row => row._source_document != null)) {
      regularColumns.push('_source_document');
    }

    if (data.rows.some(row => row.papers?.length)) {
      regularColumns.push('_papers');
    }

    const exactMatches = ['row_name', 'name', 'id', 'title', 'row', 'identifier'];
    exactMatches.forEach(exactName => {
      const found = dataColumnArray.find(col => col.toLowerCase() === exactName);
      if (found && !priorityColumns.includes(found)) {
        priorityColumns.push(found);
      }
    });

    dataColumnArray.forEach(key => {
      const keyLower = key.toLowerCase();
      if (!priorityColumns.includes(key)) {
        if (keyLower.includes('name') || keyLower.includes('id') ||
            keyLower.includes('title') || keyLower.includes('label')) {
          priorityColumns.push(key);
        } else {
          regularColumns.push(key);
        }
      }
    });

    if (priorityColumns.length === 0 && regularColumns.length > 0) {
      const firstColumn = regularColumns.shift();
      if (firstColumn) priorityColumns.push(firstColumn);
    }

    // Include schema columns that don't have data yet (e.g., newly discovered columns)
    // These columns are defined in the schema but may not have extracted values
    const schemaColumns: string[] = [];
    if (columnInfo && columnInfo.length > 0) {
      columnInfo.forEach(col => {
        if (!col.name.startsWith('_') && !col.name.endsWith('_excerpt')) {
          if (!priorityColumns.includes(col.name) && !regularColumns.includes(col.name)) {
            schemaColumns.push(col.name);
          }
        }
      });
    }

    // Combine all columns
    const allCols = [...priorityColumns, ...regularColumns, ...schemaColumns];

    // Move "Document Directory" (and similar patterns) to the end
    const isDocDirectoryColumn = (col: string) => {
      const colLower = col.toLowerCase().replace(/[_-]/g, ' ');
      return colLower.includes('document directory') ||
             colLower.includes('doc directory') ||
             colLower === 'directory';
    };

    const docDirectoryCols = allCols.filter(isDocDirectoryColumn);
    const otherCols = allCols.filter(col => !isDocDirectoryColumn(col));

    return [...otherCols, ...docDirectoryCols];
  }, [data.rows, columnInfo]);

  const allColumns = useMemo(() => {
    let columns = defaultColumns;

    if (externalColumnOrder && externalColumnOrder.length > 0) {
      const validExternalOrder = externalColumnOrder.filter(col => defaultColumns.includes(col));
      const newColumns = defaultColumns.filter(col => !externalColumnOrder.includes(col));
      columns = [...validExternalOrder, ...newColumns];
    }

    // Include processing columns that aren't in data yet (e.g., newly added columns)
    if (processingColumns && processingColumns.size > 0) {
      const missingProcessingCols = Array.from(processingColumns).filter(col => !columns.includes(col));
      if (missingProcessingCols.length > 0) {
        columns = [...columns, ...missingProcessingCols];
      }
    }

    return columns;
  }, [defaultColumns, externalColumnOrder, processingColumns]);

  // Column visibility hook
  const {
    visibility,
    toggleColumn,
    showAllColumns,
    hideAllColumns,
    isVisible,
  } = useColumnVisibility({ sessionId, columns: allColumns });

  // Column statistics hook for fullness calculations
  const { getColumnsAboveThreshold } = useColumnStats(data.rows, allColumns);

  // Apply visibility and fullness threshold to get displayed columns
  const columns = useMemo(() => {
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
    // Count visible columns that would be hidden by fullness filter
    return allColumns.filter(col =>
      isVisible(col) && !columnsAboveThreshold.includes(col)
    ).length;
  }, [allColumns, isVisible, fullnessThreshold, getColumnsAboveThreshold]);

  // Build column metadata for filter dialog
  const columnMetadata = useMemo((): ColumnMetadata[] => {
    return allColumns.map(col => {
      const info = columnInfo?.find(c => c.name === col);
      return buildColumnMetadata(data.rows, col, info?.allowed_values);
    });
  }, [allColumns, data.rows, columnInfo]);

  const frozenColumn = columns[0];
  const scrollableColumns = columns.slice(1);

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;

    if (over && active.id !== over.id) {
      const oldIndex = scrollableColumns.indexOf(active.id as string);
      const newIndex = scrollableColumns.indexOf(over.id as string);

      const newScrollableColumns = arrayMove(scrollableColumns, oldIndex, newIndex);
      const newOrder = [frozenColumn, ...newScrollableColumns];

      if (onColumnReorder) {
        onColumnReorder(newOrder);
      }
    }
  };

  const handleChangePage = (newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (value: string) => {
    setPageSize(parseInt(value, 10));
    setPage(0);
  };

  const handleViewContent = (columnName: string, content: CellValue) => {
    setModalContent({
      title: `${formatColumnName(columnName)} - Full Content`,
      content: content
    });
    setModalOpen(true);
  };

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
    setPage(0); // Reset to first page when filter changes
  }, [addFilter]);

  const handleLoadPreset = useCallback((filters: FilterRule[], sort: SortColumn[]) => {
    setFilterState({ rules: filters });
    setSortState({ columns: sort });
    setPage(0);
  }, [setFilterState, setSortState]);

  // Create mapping of main columns to their corresponding excerpt columns
  const excerptMapping = useMemo(() => {
    const mapping: Record<string, string> = {};

    const allDataColumns = new Set<string>();
    data.rows.forEach(row => {
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
  }, [data.rows]);

  const getExcerptForColumn = (row: DataRow, columnName: string): string | null => {
    const excerptColumnName = excerptMapping[columnName];
    if (excerptColumnName && row.data[excerptColumnName]) {
      return String(row.data[excerptColumnName]);
    }
    return null;
  };

  // Parse pipe-separated excerpt strings like: {'text': '...', 'source': '...'} | {'text': '...'}
  const parseExcerpts = (excerpts: any[]): any[] => {
    const result: any[] = [];

    for (const exc of excerpts) {
      if (typeof exc === 'string') {
        // Check if it's pipe-separated
        if (exc.includes("'text':") || exc.includes('"text":')) {
          // Split by pipe and parse each part
          const parts = exc.split(/\s*\|\s*/);
          for (const part of parts) {
            const parsed = parsePythonString(part.trim());
            if (typeof parsed === 'object' && parsed !== null && 'text' in parsed) {
              result.push(parsed);
            } else if (typeof parsed === 'string' && parsed.trim()) {
              result.push({ text: parsed, source: 'Source' });
            }
          }
        } else if (exc.trim()) {
          result.push({ text: exc, source: 'Source' });
        }
      } else if (typeof exc === 'object' && exc !== null) {
        result.push(exc);
      }
    }

    return result;
  };

  // Normalize value to QBSD format with 'answer' and 'excerpts'
  const normalizeToQBSD = (val: any): any => {
    if (!val || typeof val !== 'object') return val;

    // If it's an array with dict items, take first item
    if (Array.isArray(val) && val.length > 0 && typeof val[0] === 'object') {
      return normalizeToQBSD(val[0]);
    }

    // Already in QBSD format
    if ('answer' in val) {
      let answerVal = val.answer;
      let excerptsVal = val.excerpts || [];

      // Parse answer if it's a JSON string
      if (typeof answerVal === 'string') {
        const parsed = parsePythonString(answerVal);
        if (parsed !== answerVal && Array.isArray(parsed) && parsed.length > 0) {
          const firstItem = parsed[0];
          if (typeof firstItem === 'object') {
            answerVal = firstItem.value || firstItem.answer || String(firstItem);
            const allExcerpts: any[] = [];
            for (const item of parsed) {
              const exc = item.excerpt || item.excerpts;
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

      // Parse excerpts if needed
      const parsedExcerpts = parseExcerpts(excerptsVal);

      return {
        answer: answerVal,
        excerpts: parsedExcerpts
      };
    }

    // Normalize 'value'/'excerpt'/'citation' format
    if ('value' in val) {
      // Check for citation (streaming cells), excerpt, or excerpts
      const excerptsRaw = val.citation ? [val.citation] :
                          val.excerpt ? [val.excerpt] :
                          (val.excerpts || []);
      return {
        answer: val.value,
        excerpts: parseExcerpts(excerptsRaw)
      };
    }

    // ExcerptWithSource format: {text: '...', source: '...'}
    if ('text' in val) {
      return {
        answer: val.text,
        excerpts: val.source ? [{ text: String(val.text), source: String(val.source) }] : []
      };
    }

    return val;
  };

  // Unified clickable cell renderer
  const renderClickableCell = (
    displayText: string,
    onClick: () => void,
    tooltip: string,
    isItalic: boolean = false
  ): React.ReactNode => {
    return (
      <div
        className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
        onClick={onClick}
        title={tooltip}
      >
        <div
          className={cn(
            "text-xs leading-relaxed line-clamp-3 break-words",
            isItalic && "italic text-muted-foreground"
          )}
        >
          {displayText}
        </div>
      </div>
    );
  };

  const formatCellValue = (value: CellValue, columnName: string, rowData?: DataRow): React.ReactNode => {
    // Handle empty values first (null, undefined, "None", "N/A", [], {}, etc.)
    if (isEmpty(value)) {
      // Show skeleton if this column is currently being processed (re-extraction in progress)
      if (processingColumns?.has(columnName)) {
        return <ExtractingCell />;
      }
      return <Badge variant="outline" className="text-muted-foreground bg-muted/50">null</Badge>;
    }

    // Try to parse string values that look like JSON/Python objects
    let processedValue: CellValue = typeof value === 'string' ? parsePythonString(value) as CellValue : value;

    // Check if parsing resulted in an empty value (e.g., parsed "[]" to [])
    if (isEmpty(processedValue)) {
      // Show skeleton if this column is currently being processed
      if (processingColumns?.has(columnName)) {
        return <ExtractingCell />;
      }
      return <Badge variant="outline" className="text-muted-foreground bg-muted/50">null</Badge>;
    }

    // Normalize to QBSD format if it's an object
    if (typeof processedValue === 'object' && processedValue !== null) {
      processedValue = normalizeToQBSD(processedValue);
    }

    // Handle arrays (already checked for empty above)
    if (Array.isArray(processedValue)) {
      if (processedValue.length > 3) {
        return (
          <div
            className="flex items-center gap-1 cursor-pointer"
            onClick={() => handleViewContent(columnName, processedValue)}
            title={`View all ${processedValue.length} items`}
          >
            {processedValue.slice(0, 2).map((item, index) => (
              <Badge key={index} variant="secondary">{String(item)}</Badge>
            ))}
            <span className="text-xs text-muted-foreground">+{processedValue.length - 2} more</span>
          </div>
        );
      }
      return (
        <div className="flex flex-wrap gap-1">
          {processedValue.map((item, index) => (
            <Badge key={index} variant="secondary">{String(item)}</Badge>
          ))}
        </div>
      );
    }

    // Handle QBSD format objects with answer and excerpts
    if (typeof processedValue === 'object' && processedValue !== null) {
      if ('answer' in processedValue && typeof (processedValue as QBSDAnswerWithExcerpts).answer !== 'undefined') {
        const qbsdValue = processedValue as QBSDAnswerWithExcerpts;
        const answer = qbsdValue.answer;
        const excerpts = qbsdValue.excerpts || [];

        // Check if the answer itself is empty (e.g., "None", "", "N/A", null)
        if (isEmpty(answer)) {
          if (processingColumns?.has(columnName)) {
            return <ExtractingCell />;
          }
          return <Badge variant="outline" className="text-muted-foreground bg-muted/50">null</Badge>;
        }

        const answerStr = String(answer);
        const hasExcerptsData = excerpts.length > 0;
        const showExpandIcon = hasExcerptsData || answerStr.length > 40;

        if (showExpandIcon) {
          const tooltip = hasExcerptsData ? "Click to view excerpts" : "Click to view full content";
          return renderClickableCell(
            answerStr,
            () => handleViewContent(columnName, { answer, excerpts }),
            tooltip
          );
        }

        return (
          <div
            className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
            onClick={() => handleViewContent(columnName, { answer, excerpts })}
            title="Click to view content"
          >
            <span className="text-xs leading-relaxed line-clamp-3">
              {answerStr}
            </span>
          </div>
        );
      }

      // Generic object - clickable to view details
      return (
        <div
          className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1 inline-flex items-center gap-1"
          onClick={() => handleViewContent(columnName, processedValue)}
          title="View object details"
        >
          <span className="text-xs text-muted-foreground">[Object]</span>
        </div>
      );
    }

    // Handle string values
    const stringValue = String(processedValue);
    const hasExcerpts = rowData && excerptMapping[columnName] && rowData.data[excerptMapping[columnName]];
    const isExplicitExcerpt = isExcerptContent(columnName, stringValue);
    const isVeryLongContent = isVeryLongText(stringValue, LONG_TEXT_THRESHOLD);
    const hasManyLines = hasMultipleLines(stringValue, MAX_CELL_LINES);

    const needsExpansion = hasExcerpts || isExplicitExcerpt || isVeryLongContent ||
                          (hasManyLines && stringValue.length > MEDIUM_TEXT_THRESHOLD) ||
                          stringValue.length > 40;

    if (needsExpansion) {
      const previewText = isExplicitExcerpt
        ? getPreviewText(stringValue, 50)
        : getPreviewText(stringValue, SHORT_TEXT_THRESHOLD);

      const tooltip = hasExcerpts ? "View content with supporting excerpts" :
                      isExplicitExcerpt ? "View excerpt details" : "View full content";

      const handleClick = () => {
        if (hasExcerpts && rowData) {
          const excerptText = getExcerptForColumn(rowData, columnName);
          // Parse the excerpt text if it contains structured data
          const parsedExcerpts = excerptText ? parseExcerpts([excerptText]) : [];
          handleViewContent(columnName, {
            answer: stringValue,
            excerpts: parsedExcerpts
          });
        } else {
          handleViewContent(columnName, value);
        }
      };

      return renderClickableCell(previewText, handleClick, tooltip, isExplicitExcerpt);
    }

    // Short text - still clickable
    return (
      <div
        className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1"
        onClick={() => handleViewContent(columnName, value)}
        title="Click to view content"
      >
        <span className="text-xs leading-relaxed line-clamp-3">
          {stringValue}
        </span>
      </div>
    );
  };

  // Calculate total pages based on server-side filtered data
  // Use filtered_count from server when available, otherwise total_count
  const displayedRowCount = data.filtered_count ?? data.total_count;
  const totalRowCount = data.total_count;
  const isFiltered = data.filtered_count !== null && data.filtered_count !== undefined;
  const totalPages = Math.ceil(displayedRowCount / pageSize);

  return (
    <Card>
      <div className="p-4">
        {/* Header row with title and search */}
        <div className="flex justify-between items-center mb-4">
          <div>
            <h3 className="font-semibold text-lg flex items-center gap-2">
              Data Table
              {isFiltered ? (
                <span className="text-muted-foreground font-normal">
                  ({displayedRowCount.toLocaleString()} of {totalRowCount.toLocaleString()} rows)
                </span>
              ) : (
                <span className="text-muted-foreground font-normal">
                  ({totalRowCount.toLocaleString()} rows)
                </span>
              )}
              {sessionType === 'qbsd' && (
                <Badge variant="info">Auto-refreshing</Badge>
              )}
            </h3>
          </div>

          <div className="flex items-center gap-2">
            <Tooltip>
              <TooltipTrigger asChild>
                <div className="relative w-64">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                  <Input
                    placeholder="Search data..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="pl-9"
                    aria-label="Search all columns"
                  />
                </div>
              </TooltipTrigger>
              <TooltipContent>Search across row names and all column values</TooltipContent>
            </Tooltip>

            <TableOptionsMenu
              onAddFilter={() => handleOpenFilterDialog()}
              onAddRow={() => {
                setNewRowName('');
                setNewRowDocument('');
                setAddRowError(null);
                setShowAddRowDialog(true);
              }}
              readonly={readonly}
              sessionId={sessionId}
              currentFilters={filterState.rules}
              currentSort={sortState.columns}
              onLoadPreset={handleLoadPreset}
              fullnessThreshold={fullnessThreshold}
              onFullnessChange={setFullnessThreshold}
              visibleColumnsCount={columns.length}
              totalColumnsCount={allColumns.length}
              hiddenByFullnessCount={hiddenByFullnessCount}
              columns={allColumns}
              visibility={visibility}
              onToggleColumn={toggleColumn}
              onShowAll={showAllColumns}
              onHideAll={hideAllColumns}
            />
          </div>
        </div>

        {/* Filter toolbar — only render when filters are active or observation units present */}
        {(filterState.rules.length > 0 || hasObservationUnits) && (
          <div className="flex flex-wrap items-center gap-2 mb-4 pb-4 border-b">
            <FilterBar
              filters={filterState.rules}
              onRemoveFilter={removeFilter}
              onClearAll={clearFilters}
              onAddFilter={() => handleOpenFilterDialog()}
            />

            {/* Observation units indicator */}
            {hasObservationUnits && (
              <Tooltip>
                <TooltipTrigger asChild>
                  <Badge variant="outline" className="gap-1 cursor-help">
                    <Info className="h-3 w-3" />
                    Grouped by Document
                  </Badge>
                </TooltipTrigger>
                <TooltipContent>
                  Multiple observations per document. Rows are grouped by their source document.
                </TooltipContent>
              </Tooltip>
            )}
          </div>
        )}

        {/* Bulk Action Toolbar */}
        {!readonly && selectedCount > 0 && (
          <BulkActionToolbar
            selectedCount={selectedCount}
            onDelete={() => setShowBulkDeleteDialog(true)}
            onClearSelection={clearSelection}
            className="mb-4"
          />
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
                    — {hasObservationUnits ? 'Observation Unit' : 'Document'} {currentDocumentProgress.documentIndex} of {currentDocumentProgress.totalDocuments}
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

        <DndContext
          sensors={sensors}
          collisionDetection={closestCenter}
          onDragEnd={handleDragEnd}
        >
          <div
            ref={tableContainerRef}
            className="overflow-auto max-h-[600px] border rounded-md overscroll-x-contain"
          >
            <table
              className="w-full border-collapse"
              style={{
                minWidth: `${Math.max(600, columns.reduce((sum, col) => sum + (getColumnWidth(col) || 150), 0))}px`,
                ...(hasCustomWidths ? { tableLayout: 'fixed' } : {}),
              }}
            >
              <thead className="sticky top-0 z-10 bg-background border-b">
                <tr>
                  {/* Checkbox column header - only show if not readonly */}
                  {!readonly && (
                    <th className="px-3 py-3 w-[50px] min-w-[50px] sticky left-0 bg-background z-20">
                      <Checkbox
                        checked={isAllPageSelected}
                        onCheckedChange={() => toggleAllPage(pageRowIds)}
                        aria-label="Select all rows on this page"
                        className="data-[state=checked]:bg-primary"
                      />
                      {isIndeterminate && !isAllPageSelected && (
                        <span className="absolute inset-0 flex items-center justify-center pointer-events-none">
                          <Minus className="h-3 w-3 text-primary" />
                        </span>
                      )}
                    </th>
                  )}

                  {/* Frozen first column with sort/filter support */}
                  {frozenColumn && (
                    <th
                      ref={frozenThRef}
                      className={cn(
                        "px-4 py-3 text-left font-bold text-base sticky bg-background z-20 border-r-2 border-primary shadow-[2px_0_4px_rgba(0,0,0,0.1)] relative",
                        !getColumnWidth(frozenColumn) && "min-w-[150px] max-w-[250px]",
                        readonly ? "left-0" : "left-[50px]",
                        getSortDirection(frozenColumn) && "bg-primary/5"
                      )}
                      style={getColumnWidth(frozenColumn) ? { width: getColumnWidth(frozenColumn), minWidth: MIN_COLUMN_WIDTH } : undefined}
                    >
                      <div className="flex items-center gap-1">
                        <div
                          className="flex items-center gap-1 cursor-pointer hover:text-primary flex-1 overflow-hidden"
                          onClick={(e) => toggleSort(frozenColumn, e.shiftKey)}
                        >
                          {frozenColumn.startsWith('_') ? (
                            <Badge variant="outline">{formatColumnName(frozenColumn)}</Badge>
                          ) : (
                            formatColumnName(frozenColumn)
                          )}
                          {getSortDirection(frozenColumn) && (
                            <div className="flex items-center">
                              {getSortDirection(frozenColumn) === 'asc' ? (
                                <ArrowUp className="h-4 w-4 text-primary" />
                              ) : (
                                <ArrowDown className="h-4 w-4 text-primary" />
                              )}
                              {getSortPriority(frozenColumn) && getSortPriority(frozenColumn)! > 1 && (
                                <span className="text-xs text-primary ml-0.5">{getSortPriority(frozenColumn)}</span>
                              )}
                            </div>
                          )}
                        </div>
                        <Button
                          variant="ghost"
                          size="icon"
                          className={cn(
                            "h-6 w-6 shrink-0",
                            hasFilterForColumn(frozenColumn) && "text-primary bg-primary/10"
                          )}
                          onClick={() => handleOpenFilterDialog(frozenColumn)}
                        >
                          <Filter className="h-3 w-3" />
                        </Button>
                      </div>
                      {/* Resize handle */}
                      <div
                        className="absolute right-0 top-0 bottom-0 w-[6px] cursor-col-resize hover:bg-primary/40 z-10"
                        onMouseDown={(e) => {
                          e.stopPropagation();
                          if (frozenThRef.current) {
                            handleResizeStart(e, frozenColumn, frozenThRef.current.offsetWidth);
                          }
                        }}
                      />
                    </th>
                  )}

                  {/* Sortable columns */}
                  <SortableContext
                    items={scrollableColumns}
                    strategy={horizontalListSortingStrategy}
                  >
                    {scrollableColumns.map(column => (
                      <SortableHeaderCell
                        key={column}
                        column={column}
                        sortDirection={getSortDirection(column)}
                        sortPriority={getSortPriority(column)}
                        hasFilter={hasFilterForColumn(column)}
                        onSort={toggleSort}
                        onFilter={handleOpenFilterDialog}
                        columnWidth={getColumnWidth(column)}
                        onResizeStart={handleResizeStart}
                      >
                        {column.startsWith('_') ? (
                          <Badge variant="outline">{formatColumnName(column)}</Badge>
                        ) : (
                          formatColumnName(column)
                        )}
                      </SortableHeaderCell>
                    ))}
                  </SortableContext>

                  {/* Actions column header - only show if not readonly */}
                  {!readonly && (
                    <th className="px-2 py-3 text-left font-bold text-base min-w-[60px] sticky right-0 bg-background border-l">
                      <span className="sr-only">Actions</span>
                    </th>
                  )}
                </tr>
              </thead>
              <tbody>
                {processedRows.map((row, rowIndex) => {
                  const getFrozenCellValue = () => {
                    if (frozenColumn === '_row_name') {
                      // When observation units are present, show document name in merged cells
                      return hasObservationUnits ? row._source_document : row.row_name;
                    } else if (frozenColumn === '_unit_name') {
                      return row._unit_name;
                    } else if (frozenColumn === '_source_document') {
                      return row._source_document;
                    } else if (frozenColumn === '_papers') {
                      return row.papers;
                    } else {
                      return row.data[frozenColumn];
                    }
                  };

                  const actualRowIndex = page * pageSize + rowIndex + 1;
                  const isNewlyAdded = newlyAddedRows?.has(actualRowIndex) || false;
                  const isStreaming = row.row_name ? streamingCells?.has(row.row_name) : false;
                  // Add thicker border between doc groups when observation units present
                  const isFirstRowOfGroup = hasObservationUnits && shouldRenderDocNameCell(rowIndex);
                  const isGroupBoundary = isFirstRowOfGroup && rowIndex > 0;
                  const groupIndex = getGroupIndex(rowIndex);
                  const isOddGroup = groupIndex % 2 === 1;

                  const rowId = row._unit_name || row.row_name || '';
                  const rowIsSelected = isSelected(rowId);

                  return (
                    <tr
                      key={rowIndex}
                      className={cn(
                        "border-b hover:bg-muted/50 transition-colors",
                        isNewlyAdded && "bg-green-100 dark:bg-green-950 animate-pulse",
                        isStreaming && "bg-blue-50 dark:bg-blue-950",
                        isGroupBoundary && "border-t-4 border-t-foreground/40",
                        // Alternating group backgrounds when observation units present
                        hasObservationUnits && isOddGroup && !isNewlyAdded && !isStreaming && "bg-muted/30",
                        // Selected row highlight
                        rowIsSelected && "bg-blue-50 dark:bg-blue-950"
                      )}
                    >
                      {/* Checkbox cell - only show if not readonly */}
                      {!readonly && (
                        <td
                          className={cn(
                            "px-3 py-3 w-[50px] min-w-[50px] sticky left-0 z-10",
                            rowIsSelected ? "bg-blue-50 dark:bg-blue-950" :
                              (isOddGroup && hasObservationUnits ? "bg-muted/30" : "bg-background")
                          )}
                        >
                          <Checkbox
                            checked={rowIsSelected}
                            onCheckedChange={() => toggleRow(rowId)}
                            aria-label={`Select row ${rowId}`}
                          />
                        </td>
                      )}

                      {/* Frozen first column - visual grouping without rowSpan */}
                      {frozenColumn && (
                        hasObservationUnits ? (
                          <td
                            className={cn(
                              "px-4 py-3",
                              !getColumnWidth(frozenColumn) && "min-w-[150px] max-w-[250px]",
                              "sticky border-r",
                              readonly ? "left-0" : "left-[50px]",
                              rowIsSelected ? "bg-blue-50 dark:bg-blue-950" :
                                (isOddGroup ? "bg-muted/30" : "bg-background"),
                              isGroupBoundary && "border-t-4 border-t-foreground/40",
                              // Visual connector for grouped rows
                              !shouldRenderDocNameCell(rowIndex) && "border-l-4 border-l-primary/20"
                            )}
                            style={{
                              verticalAlign: shouldRenderDocNameCell(rowIndex) ? 'top' : 'middle',
                              zIndex: 5,
                              ...(getColumnWidth(frozenColumn) ? { width: getColumnWidth(frozenColumn), minWidth: MIN_COLUMN_WIDTH } : {}),
                            }}
                          >
                            {shouldRenderDocNameCell(rowIndex) ? (
                              // First row of group: show full doc name and observation count
                              <div className="flex flex-col gap-1">
                                {formatCellValue(row._source_document, '_source_document', row)}
                                <Badge variant="secondary" className="text-xs w-fit">
                                  {getDocNameRowSpan(rowIndex)} observation{getDocNameRowSpan(rowIndex) !== 1 ? 's' : ''}
                                </Badge>
                              </div>
                            ) : (
                              // Continuation rows: show subtle indicator
                              <span className="text-muted-foreground/40 text-xs">↑</span>
                            )}
                          </td>
                        ) : (
                          // Regular frozen column (no grouping)
                          <td
                            className={cn(
                              "px-4 py-3 sticky border-r",
                              !getColumnWidth(frozenColumn) && "min-w-[150px] max-w-[250px]",
                              readonly ? "left-0" : "left-[50px]",
                              rowIsSelected ? "bg-blue-50 dark:bg-blue-950" : "bg-background"
                            )}
                            style={getColumnWidth(frozenColumn) ? { width: getColumnWidth(frozenColumn), minWidth: MIN_COLUMN_WIDTH } : undefined}
                          >
                            {!frozenColumn.startsWith('_') && row.row_name ? (
                              <EditableCell
                                value={getFrozenCellValue()}
                                rowName={row.row_name}
                                column={frozenColumn}
                                onSave={handleCellUpdate}
                              >
                                {formatCellValue(getFrozenCellValue(), frozenColumn, row)}
                              </EditableCell>
                            ) : (
                              formatCellValue(getFrozenCellValue(), frozenColumn, row)
                            )}
                          </td>
                        )
                      )}

                      {/* Scrollable columns */}
                      {scrollableColumns.map(column => {
                        let cellValue;

                        if (column === '_row_name') {
                          // When observation units are present, show document name in merged cells
                          cellValue = hasObservationUnits ? row._source_document : row.row_name;
                        } else if (column === '_unit_name') {
                          cellValue = row._unit_name;
                        } else if (column === '_source_document') {
                          cellValue = row._source_document;
                        } else if (column === '_papers') {
                          cellValue = row.papers;
                        } else {
                          cellValue = row.data[column];
                        }

                        // Handle visual grouping for _row_name in scrollable columns (no rowSpan)
                        if (column === '_row_name' && hasObservationUnits) {
                          return (
                            <td
                              key={column}
                              className={cn(
                                "px-4 py-3",
                                !getColumnWidth(column) && "min-w-[120px] sm:min-w-[150px]",
                                isOddGroup ? "bg-muted/30" : "bg-background",
                                isGroupBoundary && "border-t-4 border-t-foreground/40",
                                // Visual connector for grouped rows
                                !shouldRenderDocNameCell(rowIndex) && "border-l-4 border-l-primary/20"
                              )}
                              style={{
                                verticalAlign: shouldRenderDocNameCell(rowIndex) ? 'top' : 'middle',
                                ...(getColumnWidth(column) ? { width: getColumnWidth(column), minWidth: MIN_COLUMN_WIDTH } : {}),
                              }}
                            >
                              {shouldRenderDocNameCell(rowIndex) ? (
                                // First row of group: show full doc name and observation count
                                <div className="flex flex-col gap-1">
                                  {formatCellValue(cellValue, column, row)}
                                  <Badge variant="secondary" className="text-xs w-fit">
                                    {getDocNameRowSpan(rowIndex)} observation{getDocNameRowSpan(rowIndex) !== 1 ? 's' : ''}
                                  </Badge>
                                </div>
                              ) : (
                                // Continuation rows: show subtle indicator
                                <span className="text-muted-foreground/40 text-xs">↑</span>
                              )}
                            </td>
                          );
                        }

                        // Determine if cell is editable (not metadata columns, has row_name)
                        const isEditable = !column.startsWith('_') && !!row.row_name;

                        return (
                          <td
                            key={column}
                            className={cn("px-4 py-3", !getColumnWidth(column) && "min-w-[120px] sm:min-w-[150px]")}
                            style={getColumnWidth(column) ? { width: getColumnWidth(column), minWidth: MIN_COLUMN_WIDTH } : undefined}
                          >
                            {isEditable ? (
                              <EditableCell
                                value={cellValue}
                                rowName={row.row_name || ''}
                                column={column}
                                onSave={handleCellUpdate}
                              >
                                {formatCellValue(cellValue, column, row)}
                              </EditableCell>
                            ) : (
                              formatCellValue(cellValue, column, row)
                            )}
                          </td>
                        );
                      })}

                      {/* Actions column cell - only show if not readonly */}
                      {!readonly && (
                        <td className={cn(
                          "px-2 py-3 min-w-[60px] sticky right-0 border-l",
                          isOddGroup && hasObservationUnits ? "bg-muted/30" : "bg-background"
                        )}>
                          <RowActions
                            rowName={row._unit_name || row.row_name || ''}
                            onDelete={handleRowDelete}
                            disabled={readonly}
                          />
                        </td>
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </DndContext>

        {/* Pagination */}
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
              {displayedRowCount > 0 ? page * pageSize + 1 : 0}-{Math.min((page + 1) * pageSize, displayedRowCount)} of {displayedRowCount}
              {isFiltered && ` (filtered from ${totalRowCount})`}
            </span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => handleChangePage(page - 1)}
              disabled={page === 0}
            >
              Previous
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => handleChangePage(page + 1)}
              disabled={page >= totalPages - 1}
            >
              Next
            </Button>
          </div>
        </div>
      </div>

      {/* Content Modal */}
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

      {/* Add Row Dialog */}
      <Dialog open={showAddRowDialog} onOpenChange={setShowAddRowDialog}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Add New Row</DialogTitle>
            <DialogDescription>
              Add a new row to the table. The row will be created with empty values for all columns.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label htmlFor="row-name">Row Name *</Label>
              <Input
                id="row-name"
                placeholder="e.g., Protein X, Model A, Treatment Group 1"
                value={newRowName}
                onChange={(e) => setNewRowName(e.target.value)}
                disabled={isAddingRow}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="document-id">Source Document (optional)</Label>
              <Input
                id="document-id"
                placeholder="e.g., paper_123.pdf"
                value={newRowDocument}
                onChange={(e) => setNewRowDocument(e.target.value)}
                disabled={isAddingRow}
              />
              <p className="text-xs text-muted-foreground">
                Reference to the document this row is associated with
              </p>
            </div>
          </div>

          {addRowError && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>{addRowError}</AlertDescription>
            </Alert>
          )}

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setShowAddRowDialog(false)}
              disabled={isAddingRow}
            >
              Cancel
            </Button>
            <Button
              onClick={handleAddRow}
              disabled={isAddingRow}
            >
              {isAddingRow ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Adding...
                </>
              ) : (
                'Add Row'
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Bulk Delete Dialog */}
      <BulkDeleteDialog
        open={showBulkDeleteDialog}
        onOpenChange={setShowBulkDeleteDialog}
        selectedRows={Array.from(selectedRows)}
        onConfirm={handleBulkDelete}
      />
    </Card>
  );
};

export default DataTable;
