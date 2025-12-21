import React, { useState, useMemo, useEffect, useCallback } from 'react';
import { Search, Eye, GripVertical, ArrowUp, ArrowDown, Filter } from 'lucide-react';
import { useQuery, useQueryClient } from 'react-query';
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

import { PaginatedData, CellValue, DataRow, ModalContent, QBSDAnswerWithExcerpts } from '../../types';
import { sessionAPI } from '../../services/api';
import {
  formatColumnName,
  needsTruncation,
  truncateText,
  isExcerptContent,
  isVeryLongText,
  hasMultipleLines,
  getPreviewText
} from '../../utils/formatting';
import ContentModal from '../ContentModal/ContentModal';
import {
  QBSD_REFRESH_INTERVAL,
  AVAILABLE_PAGE_SIZES,
  LONG_TEXT_THRESHOLD,
  MEDIUM_TEXT_THRESHOLD,
  SHORT_TEXT_THRESHOLD,
  MAX_CELL_LINES,
} from '../../constants/index';

// New filter/sort imports
import { useTableSort } from './hooks/useTableSort';
import { useTableFilter } from './hooks/useTableFilter';
import { useColumnVisibility } from './hooks/useColumnVisibility';
import { applyFilters, applySort, buildColumnMetadata } from './utils';
import { FilterOperator, FilterValue, ColumnMetadata, FilterRule, SortColumn } from './types/filters';
import FilterBar from './FilterBar';
import FilterDialog from './FilterDialog';
import FilterPresets from './FilterPresets';
import ColumnVisibilityDropdown from './ColumnVisibilityDropdown';

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
}

const SortableHeaderCell: React.FC<SortableHeaderCellProps> = ({
  column,
  children,
  sortDirection,
  sortPriority,
  hasFilter,
  onSort,
  onFilter,
}) => {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: column });

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    cursor: 'grab',
    position: 'relative' as const,
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

  return (
    <th
      ref={setNodeRef}
      style={style}
      className={cn(
        "px-4 py-3 text-left font-bold text-base min-w-[120px] sm:min-w-[150px] bg-background",
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
          className="flex items-center gap-1 cursor-pointer hover:text-primary flex-1"
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
}) => {
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [searchTerm, setSearchTerm] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [modalContent, setModalContent] = useState<ModalContent>({ title: '', content: null });
  const [filterDialogOpen, setFilterDialogOpen] = useState(false);
  const [filterDialogColumn, setFilterDialogColumn] = useState<string | undefined>();
  const queryClient = useQueryClient();

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

  // Fetch data with pagination
  const { data: fetchedData } = useQuery(
    ['data', sessionId, sessionType, page, pageSize],
    () => sessionAPI.getData(sessionId, sessionType, page, pageSize),
    {
      keepPreviousData: true,
      enabled: !!sessionId,
      refetchInterval: sessionType === 'qbsd' ? QBSD_REFRESH_INTERVAL : false,
    }
  );

  const fetchedOrInitialData = fetchedData ?? initialData ?? EMPTY_DATA;

  // Merge streaming cells into the data for display
  const data = useMemo(() => {
    if (!streamingCells || streamingCells.size === 0) {
      return fetchedOrInitialData;
    }

    const mergedRows = [...fetchedOrInitialData.rows];
    const existingRowNames = new Set(mergedRows.map(r => r.row_name));

    streamingCells.forEach((cellData, rowName) => {
      const existingRowIndex = mergedRows.findIndex(r => r.row_name === rowName);

      if (existingRowIndex >= 0) {
        const existingRow = mergedRows[existingRowIndex];
        mergedRows[existingRowIndex] = {
          ...existingRow,
          data: { ...existingRow.data, ...cellData }
        };
      } else {
        mergedRows.push({
          row_name: rowName,
          papers: [],
          data: cellData
        });
      }
    });

    let newStreamingRows = 0;
    streamingCells.forEach((_, rowName) => {
      if (!existingRowNames.has(rowName)) {
        newStreamingRows++;
      }
    });

    return {
      ...fetchedOrInitialData,
      rows: mergedRows,
      total_count: fetchedOrInitialData.total_count + newStreamingRows
    };
  }, [fetchedOrInitialData, streamingCells]);

  useEffect(() => {
    // WebSocket listeners could be added here for more immediate updates
  }, [queryClient, sessionId, sessionType]);

  // Filter data based on search term, column filters, then apply sorting
  const processedRows = useMemo(() => {
    let rows = data.rows;

    // Step 1: Apply search filter
    if (searchTerm.trim()) {
      rows = rows.filter(row => {
        const searchLower = searchTerm.toLowerCase();
        if (row.row_name?.toLowerCase().includes(searchLower)) return true;
        return Object.values(row.data).some(value => {
          if (value === null || value === undefined) return false;
          return String(value).toLowerCase().includes(searchLower);
        });
      });
    }

    // Step 2: Apply column filters
    if (filterState.rules.length > 0) {
      rows = applyFilters(rows, filterState);
    }

    // Step 3: Apply sorting
    if (sortState.columns.length > 0) {
      rows = applySort(rows, sortState);
    }

    return rows;
  }, [data.rows, searchTerm, filterState, sortState]);

  // Keep filteredRows as alias for backward compatibility
  const filteredRows = processedRows;

  // Get all column names with proper ordering
  const defaultColumns = useMemo(() => {
    const priorityColumns: string[] = [];
    const regularColumns: string[] = [];

    // First, collect all data columns to check for row-name-like columns
    const allDataColumns = new Set<string>();
    data.rows.forEach(row => {
      Object.keys(row.data).forEach(key => allDataColumns.add(key));
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

    return [...priorityColumns, ...regularColumns];
  }, [data.rows]);

  const allColumns = useMemo(() => {
    if (externalColumnOrder && externalColumnOrder.length > 0) {
      const validExternalOrder = externalColumnOrder.filter(col => defaultColumns.includes(col));
      const newColumns = defaultColumns.filter(col => !externalColumnOrder.includes(col));
      return [...validExternalOrder, ...newColumns];
    }
    return defaultColumns;
  }, [defaultColumns, externalColumnOrder]);

  // Column visibility hook
  const {
    visibility,
    toggleColumn,
    showAllColumns,
    hideAllColumns,
    isVisible,
  } = useColumnVisibility({ sessionId, columns: allColumns });

  // Apply visibility to get displayed columns
  const columns = useMemo(() => {
    return allColumns.filter(col => isVisible(col));
  }, [allColumns, isVisible]);

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

  // Helper to try parsing string values that look like JSON/Python objects
  const tryParseValue = (val: CellValue): CellValue => {
    if (typeof val !== 'string') return val;
    const trimmed = val.trim();
    if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return val;

    try {
      // Try JSON parse first
      return JSON.parse(trimmed);
    } catch {
      try {
        // Try to handle Python-style single quotes by replacing with double quotes
        // This is a simple heuristic - replace single quotes around keys/values
        const jsonified = trimmed
          .replace(/'/g, '"')
          .replace(/None/g, 'null')
          .replace(/True/g, 'true')
          .replace(/False/g, 'false');
        return JSON.parse(jsonified);
      } catch {
        return val;
      }
    }
  };

  // Helper to normalize a parsed value to QBSD format with 'answer' and 'excerpts'
  const normalizeToQBSD = (val: any): any => {
    if (!val || typeof val !== 'object') return val;

    // If it's an array with dict items, take first item
    if (Array.isArray(val) && val.length > 0 && typeof val[0] === 'object') {
      return normalizeToQBSD(val[0]);
    }

    // Already in QBSD format - but check if 'answer' needs parsing
    if ('answer' in val) {
      let answerVal = val.answer;
      let excerptsVal = val.excerpts || [];

      // Check if 'answer' is a string that looks like JSON/Python object
      // e.g., "[{'value': 'reduction', 'excerpt': '...'}]"
      if (typeof answerVal === 'string') {
        const parsed = tryParseValue(answerVal);
        if (parsed !== answerVal) {
          // Successfully parsed
          if (Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === 'object') {
            const firstItem = parsed[0] as Record<string, any>;
            // Extract answer from first item
            answerVal = firstItem.value || firstItem.answer || String(firstItem);
            // Collect all excerpts
            const allExcerpts: any[] = [];
            for (const item of parsed as Record<string, any>[]) {
              const exc = item.excerpt || item.excerpts;
              if (exc) {
                if (Array.isArray(exc)) {
                  allExcerpts.push(...exc);
                } else {
                  allExcerpts.push(exc);
                }
              }
            }
            if (allExcerpts.length > 0) {
              excerptsVal = allExcerpts;
            }
          } else if (typeof parsed === 'object' && parsed !== null) {
            // Recursively normalize
            return normalizeToQBSD(parsed);
          }
        }
      }

      return {
        answer: answerVal,
        excerpts: excerptsVal
      };
    }

    // Normalize 'value'/'excerpt' to 'answer'/'excerpts'
    if ('value' in val) {
      return {
        answer: val.value,
        excerpts: val.excerpt ? [val.excerpt] : (val.excerpts || [])
      };
    }

    return val;
  };

  const formatCellValue = (value: CellValue, columnName: string, rowData?: DataRow): React.ReactNode => {
    // Try to parse and normalize the value
    let processedValue = tryParseValue(value);
    if (typeof processedValue === 'object' && processedValue !== null) {
      processedValue = normalizeToQBSD(processedValue);
    }

    if (processedValue === null || processedValue === undefined) {
      return <Badge variant="outline">null</Badge>;
    }

    if (Array.isArray(processedValue)) {
      if (processedValue.length > 3) {
        return (
          <div className="flex items-center gap-1">
            {processedValue.slice(0, 2).map((item, index) => (
              <Badge key={index} variant="secondary">{String(item)}</Badge>
            ))}
            <span className="text-xs text-muted-foreground">+{processedValue.length - 2} more</span>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6"
                  onClick={() => handleViewContent(columnName, processedValue)}
                  aria-label="View all items"
                >
                  <Eye className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>View all {processedValue.length} items</TooltipContent>
            </Tooltip>
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

    if (typeof processedValue === 'object' && processedValue !== null) {
      if ('answer' in processedValue && typeof (processedValue as QBSDAnswerWithExcerpts).answer !== 'undefined') {
        const qbsdValue = processedValue as QBSDAnswerWithExcerpts;
        const answer = qbsdValue.answer;
        const excerpts = qbsdValue.excerpts || [];

        return (
          <div className="flex items-center gap-1">
            <span className="text-base leading-relaxed line-clamp-3">
              {String(answer)}
            </span>
            {excerpts.length > 0 && (
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-6 w-6 text-blue-500"
                    onClick={() => handleViewContent(columnName, {
                      answer: answer,
                      excerpts: excerpts
                    })}
                    aria-label="View excerpts"
                  >
                    <Eye className="h-4 w-4" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>View excerpts ({excerpts.length} sources)</TooltipContent>
              </Tooltip>
            )}
          </div>
        );
      }

      return (
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6"
              onClick={() => handleViewContent(columnName, processedValue)}
              aria-label="View object"
            >
              <Eye className="h-4 w-4" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>View object details</TooltipContent>
        </Tooltip>
      );
    }

    const stringValue = String(processedValue);

    const hasExcerpts = rowData && excerptMapping[columnName] && rowData.data[excerptMapping[columnName]];
    const isExplicitExcerpt = isExcerptContent(columnName, stringValue);
    const isVeryLongContent = isVeryLongText(stringValue, LONG_TEXT_THRESHOLD);
    const hasManyLines = hasMultipleLines(stringValue, MAX_CELL_LINES);

    const shouldShowEyeIcon = hasExcerpts || isExplicitExcerpt || isVeryLongContent ||
                             (hasManyLines && stringValue.length > MEDIUM_TEXT_THRESHOLD);

    if (shouldShowEyeIcon) {
      const previewText = isExplicitExcerpt ?
        getPreviewText(stringValue, 50) :
        getPreviewText(stringValue, SHORT_TEXT_THRESHOLD);

      const tooltipText = hasExcerpts ? "View content with supporting excerpts" :
                          isExplicitExcerpt ? "View excerpt details" : "View full content";

      return (
        <div className="flex items-center gap-1">
          <span className={cn(
            "text-base leading-relaxed line-clamp-3",
            isExplicitExcerpt && "italic text-muted-foreground"
          )}>
            {previewText}
          </span>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6 text-blue-500 shrink-0"
                onClick={() => {
                  if (hasExcerpts && rowData) {
                    const excerptText = getExcerptForColumn(rowData, columnName);
                    handleViewContent(columnName, {
                      answer: stringValue,
                      excerpts: excerptText ? [excerptText] : []
                    });
                  } else {
                    handleViewContent(columnName, value);
                  }
                }}
                aria-label={tooltipText}
              >
                <Eye className="h-4 w-4" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>{tooltipText}</TooltipContent>
          </Tooltip>
        </div>
      );
    }

    // Show Eye icon for any text that could be truncated by line-clamp-3
    // Using 40 chars as threshold since column width can cause early truncation
    const couldBeTruncated = stringValue.length > 40;

    if (couldBeTruncated) {
      return (
        <div className="flex items-center gap-1">
          <span className="text-base leading-relaxed line-clamp-3 break-words flex-1">
            {stringValue}
          </span>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6 text-muted-foreground hover:text-blue-500 shrink-0"
                onClick={() => handleViewContent(columnName, value)}
                aria-label="View full content"
              >
                <Eye className="h-4 w-4" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>View full content</TooltipContent>
          </Tooltip>
        </div>
      );
    }

    return (
      <span className="text-base leading-relaxed line-clamp-3">
        {stringValue}
      </span>
    );
  };

  // Calculate total pages based on filtered data
  const displayedRowCount = filteredRows.length;
  const totalRowCount = data.total_count;
  const isFiltered = activeFilterCount > 0 || searchTerm.trim();
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
            <p className="text-xs text-muted-foreground">
              Click headers to sort • Shift+click for multi-sort • Drag to reorder
            </p>
          </div>

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
        </div>

        {/* Filter toolbar */}
        <div className="flex flex-wrap items-center gap-2 mb-4 pb-4 border-b">
          <FilterBar
            filters={filterState.rules}
            onRemoveFilter={removeFilter}
            onClearAll={clearFilters}
            onAddFilter={() => handleOpenFilterDialog()}
          />

          <div className="flex items-center gap-2 ml-auto">
            <FilterPresets
              sessionId={sessionId}
              currentFilters={filterState.rules}
              currentSort={sortState.columns}
              onLoadPreset={handleLoadPreset}
            />
            <ColumnVisibilityDropdown
              columns={allColumns}
              visibility={visibility}
              onToggleColumn={toggleColumn}
              onShowAll={showAllColumns}
              onHideAll={hideAllColumns}
            />
          </div>
        </div>

        <DndContext
          sensors={sensors}
          collisionDetection={closestCenter}
          onDragEnd={handleDragEnd}
        >
          <div className="overflow-auto max-h-[600px] border rounded-md overscroll-x-contain">
            <table className="w-full border-collapse" style={{ minWidth: `${Math.max(600, columns.length * 150)}px` }}>
              <thead className="sticky top-0 z-10 bg-background border-b">
                <tr>
                  {/* Frozen first column with sort/filter support */}
                  {frozenColumn && (
                    <th
                      className={cn(
                        "px-4 py-3 text-left font-bold text-base min-w-[150px] max-w-[250px] sticky left-0 bg-background z-20 border-r-2 border-primary shadow-[2px_0_4px_rgba(0,0,0,0.1)]",
                        getSortDirection(frozenColumn) && "bg-primary/5"
                      )}
                    >
                      <div className="flex items-center gap-1">
                        <div
                          className="flex items-center gap-1 cursor-pointer hover:text-primary flex-1"
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
                      >
                        {column.startsWith('_') ? (
                          <Badge variant="outline">{formatColumnName(column)}</Badge>
                        ) : (
                          formatColumnName(column)
                        )}
                      </SortableHeaderCell>
                    ))}
                  </SortableContext>
                </tr>
              </thead>
              <tbody>
                {filteredRows.slice(page * pageSize, (page + 1) * pageSize).map((row, rowIndex) => {
                  const getFrozenCellValue = () => {
                    if (frozenColumn === '_row_name') {
                      return row.row_name;
                    } else if (frozenColumn === '_papers') {
                      return row.papers;
                    } else {
                      return row.data[frozenColumn];
                    }
                  };

                  const actualRowIndex = page * pageSize + rowIndex + 1;
                  const isNewlyAdded = newlyAddedRows?.has(actualRowIndex) || false;
                  const isStreaming = row.row_name ? streamingCells?.has(row.row_name) : false;

                  return (
                    <tr
                      key={rowIndex}
                      className={cn(
                        "border-b hover:bg-muted/50 transition-colors",
                        isNewlyAdded && "bg-green-100 dark:bg-green-950 animate-pulse",
                        isStreaming && "bg-blue-50 dark:bg-blue-950"
                      )}
                    >
                      {/* Frozen first column */}
                      {frozenColumn && (
                        <td className="px-4 py-3 min-w-[150px] max-w-[250px] sticky left-0 bg-background border-r overflow-hidden">
                          {formatCellValue(getFrozenCellValue(), frozenColumn, row)}
                        </td>
                      )}

                      {/* Scrollable columns */}
                      {scrollableColumns.map(column => {
                        let cellValue;

                        if (column === '_row_name') {
                          cellValue = row.row_name;
                        } else if (column === '_papers') {
                          cellValue = row.papers;
                        } else {
                          cellValue = row.data[column];
                        }

                        return (
                          <td key={column} className="px-4 py-3 min-w-[120px] sm:min-w-[150px] max-h-[100px] overflow-hidden">
                            {formatCellValue(cellValue, column, row)}
                          </td>
                        );
                      })}
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
    </Card>
  );
};

export default DataTable;
