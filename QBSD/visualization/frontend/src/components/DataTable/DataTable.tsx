import React, { useState, useMemo, useEffect } from 'react';
import { Search, Eye, GripVertical } from 'lucide-react';
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

interface DataTableProps {
  data?: PaginatedData;
  sessionId: string;
  sessionType: 'load' | 'qbsd';
  newlyAddedRows?: Set<number>;
  columnOrder?: string[];
  onColumnReorder?: (newOrder: string[]) => void;
  streamingCells?: Map<string, Record<string, CellValue>>;
}

// Sortable Header Cell Component
interface SortableHeaderCellProps {
  column: string;
  children: React.ReactNode;
}

const SortableHeaderCell: React.FC<SortableHeaderCellProps> = ({ column, children }) => {
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

  return (
    <th
      ref={setNodeRef}
      style={style}
      className={cn(
        "px-4 py-3 text-left font-bold text-base min-w-[200px] bg-background",
        isDragging && "bg-muted"
      )}
      {...attributes}
      {...listeners}
    >
      <div className="flex items-center gap-1">
        <GripVertical className="h-4 w-4 text-muted-foreground opacity-50 hover:opacity-100" />
        {children}
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
  streamingCells
}) => {
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [searchTerm, setSearchTerm] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [modalContent, setModalContent] = useState<ModalContent>({ title: '', content: null });
  const queryClient = useQueryClient();

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

  // Filter data based on search term
  const filteredRows = useMemo(() => {
    if (!searchTerm.trim()) return data.rows;

    return data.rows.filter(row => {
      const searchLower = searchTerm.toLowerCase();

      if (row.row_name?.toLowerCase().includes(searchLower)) return true;

      return Object.values(row.data).some(value => {
        if (value === null || value === undefined) return false;
        return String(value).toLowerCase().includes(searchLower);
      });
    });
  }, [data.rows, searchTerm]);

  // Get all column names with proper ordering
  const defaultColumns = useMemo(() => {
    const priorityColumns: string[] = [];
    const regularColumns: string[] = [];

    if (data.rows.some(row => row.row_name)) {
      priorityColumns.push('_row_name');
    }
    if (data.rows.some(row => row.papers?.length)) {
      regularColumns.push('_papers');
    }

    const allDataColumns = new Set<string>();
    data.rows.forEach(row => {
      Object.keys(row.data).forEach(key => allDataColumns.add(key));
    });

    const dataColumnArray = Array.from(allDataColumns).filter(col => !col.endsWith('_excerpt'));

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

  const columns = useMemo(() => {
    if (externalColumnOrder && externalColumnOrder.length > 0) {
      const validExternalOrder = externalColumnOrder.filter(col => defaultColumns.includes(col));
      const newColumns = defaultColumns.filter(col => !externalColumnOrder.includes(col));
      return [...validExternalOrder, ...newColumns];
    }
    return defaultColumns;
  }, [defaultColumns, externalColumnOrder]);

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

  const formatCellValue = (value: CellValue, columnName: string, rowData?: DataRow): React.ReactNode => {
    if (value === null || value === undefined) {
      return <Badge variant="outline">null</Badge>;
    }

    if (Array.isArray(value)) {
      if (value.length > 3) {
        return (
          <div className="flex items-center gap-1">
            {value.slice(0, 2).map((item, index) => (
              <Badge key={index} variant="secondary">{String(item)}</Badge>
            ))}
            <span className="text-xs text-muted-foreground">+{value.length - 2} more</span>
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6"
              onClick={() => handleViewContent(columnName, value)}
            >
              <Eye className="h-4 w-4" />
            </Button>
          </div>
        );
      }
      return (
        <div className="flex flex-wrap gap-1">
          {value.map((item, index) => (
            <Badge key={index} variant="secondary">{String(item)}</Badge>
          ))}
        </div>
      );
    }

    if (typeof value === 'object' && value !== null) {
      if ('answer' in value && typeof (value as QBSDAnswerWithExcerpts).answer !== 'undefined') {
        const qbsdValue = value as QBSDAnswerWithExcerpts;
        const answer = qbsdValue.answer;
        const excerpts = qbsdValue.excerpts || [];

        return (
          <div className="flex items-center gap-1">
            <span className="text-base leading-relaxed line-clamp-3">
              {String(answer)}
            </span>
            {excerpts.length > 0 && (
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6 text-blue-500"
                title={`View excerpts (${excerpts.length} sources)`}
                onClick={() => handleViewContent(columnName, {
                  answer: answer,
                  excerpts: excerpts
                })}
              >
                <Eye className="h-4 w-4" />
              </Button>
            )}
          </div>
        );
      }

      return (
        <Button
          variant="ghost"
          size="icon"
          className="h-6 w-6"
          title="View object"
          onClick={() => handleViewContent(columnName, value)}
        >
          <Eye className="h-4 w-4" />
        </Button>
      );
    }

    const stringValue = String(value);

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

      return (
        <div className="flex items-center gap-1">
          <span className={cn(
            "text-base leading-relaxed line-clamp-3",
            isExplicitExcerpt && "italic text-muted-foreground"
          )}>
            {previewText}
          </span>
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 text-blue-500 shrink-0"
            title={hasExcerpts ? "View content with supporting excerpts" :
                   isExplicitExcerpt ? "View excerpt details" : "View full content"}
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
          >
            <Eye className="h-4 w-4" />
          </Button>
        </div>
      );
    }

    if (needsTruncation(stringValue)) {
      return (
        <span className="text-base leading-relaxed line-clamp-3 break-words">
          {truncateText(stringValue)}
        </span>
      );
    }

    return (
      <span className="text-base leading-relaxed line-clamp-3">
        {stringValue}
      </span>
    );
  };

  const totalPages = Math.ceil((searchTerm ? filteredRows.length : data.total_count) / pageSize);

  return (
    <Card>
      <div className="p-4">
        <div className="flex justify-between items-center mb-4">
          <div>
            <h3 className="font-semibold text-lg flex items-center gap-2">
              Data Table ({data.total_count.toLocaleString()} rows)
              {sessionType === 'qbsd' && (
                <Badge variant="info">Auto-refreshing</Badge>
              )}
            </h3>
            <p className="text-xs text-muted-foreground">
              Drag column headers to reorder
            </p>
          </div>

          <div className="relative w-64">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search data..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-9"
            />
          </div>
        </div>

        <DndContext
          sensors={sensors}
          collisionDetection={closestCenter}
          onDragEnd={handleDragEnd}
        >
          <div className="overflow-auto max-h-[600px] border rounded-md">
            <table className="w-full min-w-[1000px] border-collapse">
              <thead className="sticky top-0 z-10 bg-background border-b">
                <tr>
                  {/* Frozen first column */}
                  {frozenColumn && (
                    <th className="px-4 py-3 text-left font-bold text-base min-w-[200px] max-w-[200px] sticky left-0 bg-background z-20 border-r-2 border-primary shadow-[2px_0_4px_rgba(0,0,0,0.1)]">
                      {frozenColumn.startsWith('_') ? (
                        <Badge variant="outline">{formatColumnName(frozenColumn)}</Badge>
                      ) : (
                        formatColumnName(frozenColumn)
                      )}
                    </th>
                  )}

                  {/* Sortable columns */}
                  <SortableContext
                    items={scrollableColumns}
                    strategy={horizontalListSortingStrategy}
                  >
                    {scrollableColumns.map(column => (
                      <SortableHeaderCell key={column} column={column}>
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
                {(searchTerm ? filteredRows.slice(page * pageSize, (page + 1) * pageSize) : filteredRows).map((row, rowIndex) => {
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
                        <td className="px-4 py-3 min-w-[200px] max-w-[200px] sticky left-0 bg-background border-r overflow-hidden">
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
                          <td key={column} className="px-4 py-3 min-w-[200px] max-h-[100px] overflow-hidden">
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
              {page * pageSize + 1}-{Math.min((page + 1) * pageSize, searchTerm ? filteredRows.length : data.total_count)} of {searchTerm ? filteredRows.length : data.total_count}
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
    </Card>
  );
};

export default DataTable;
