import React, { useState, useMemo, useEffect } from 'react';
import {
  Box,
  Typography,
  Paper,
  TableContainer,
  Table,
  TableHead,
  TableRow,
  TableCell,
  TableBody,
  TablePagination,
  Chip,
  IconButton,
  TextField,
  InputAdornment,
} from '@mui/material';
import { Search, Visibility, DragIndicator } from '@mui/icons-material';
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
  FROZEN_COLUMN_WIDTH,
  REGULAR_COLUMN_WIDTH,
  TABLE_MAX_HEIGHT,
  TABLE_MIN_WIDTH,
  SEARCH_FIELD_WIDTH,
  TABLE_ROW_MAX_HEIGHT
} from '../../constants/index';

interface DataTableProps {
  data?: PaginatedData;  // Optional - DataTable will fetch its own data if not provided
  sessionId: string;
  sessionType: 'load' | 'qbsd';
  newlyAddedRows?: Set<number>;
  columnOrder?: string[];
  onColumnReorder?: (newOrder: string[]) => void;
  streamingCells?: Map<string, Record<string, CellValue>>;  // Real-time cell values as they're extracted
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
    <TableCell
      ref={setNodeRef}
      style={style}
      sx={{
        fontWeight: 'bold',
        fontSize: '1.1rem',
        minWidth: REGULAR_COLUMN_WIDTH,
        backgroundColor: isDragging ? 'action.hover' : 'background.paper',
        '&:hover': {
          backgroundColor: 'action.hover',
        },
      }}
      {...attributes}
      {...listeners}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
        <DragIndicator
          fontSize="small"
          sx={{
            color: 'text.secondary',
            opacity: 0.5,
            '&:hover': { opacity: 1 }
          }}
        />
        {children}
      </Box>
    </TableCell>
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
        distance: 8, // Require 8px of movement before starting drag
      },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  // Fetch data with pagination - DataTable owns its own data fetching
  const { data: fetchedData } = useQuery(
    ['data', sessionId, sessionType, page, pageSize],
    () => sessionAPI.getData(sessionId, sessionType, page, pageSize),
    {
      keepPreviousData: true,
      enabled: !!sessionId,  // Always fetch when sessionId exists
      refetchInterval: sessionType === 'qbsd' ? QBSD_REFRESH_INTERVAL : false, // Auto-refresh for QBSD
    }
  );

  // Use fetched data, fall back to initialData if provided, otherwise use empty data
  const fetchedOrInitialData = fetchedData ?? initialData ?? EMPTY_DATA;

  // Merge streaming cells into the data for display
  // This allows real-time cell values to appear before the row is complete
  const data = useMemo(() => {
    if (!streamingCells || streamingCells.size === 0) {
      return fetchedOrInitialData;
    }

    // Create a copy of rows and merge streaming data
    const mergedRows = [...fetchedOrInitialData.rows];

    // Track which streaming rows are already in the fetched data
    const existingRowNames = new Set(mergedRows.map(r => r.row_name));

    // Process streaming cells
    streamingCells.forEach((cellData, rowName) => {
      const existingRowIndex = mergedRows.findIndex(r => r.row_name === rowName);

      if (existingRowIndex >= 0) {
        // Update existing row with streaming values
        const existingRow = mergedRows[existingRowIndex];
        mergedRows[existingRowIndex] = {
          ...existingRow,
          data: { ...existingRow.data, ...cellData }
        };
      } else {
        // Create new row placeholder for streaming data
        mergedRows.push({
          row_name: rowName,
          papers: [],
          data: cellData
        });
      }
    });

    // Count new streaming rows (not already in fetched data)
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

  // Listen for row completion events via WebSocket (this would be handled by a WebSocket service)
  useEffect(() => {
    // This effect could listen to WebSocket messages and trigger data refresh
    // For now, the refetchInterval above handles the updates

    // Note: WebSocket listeners could be added here for more immediate updates
    // Currently using refetchInterval for auto-refresh
  }, [queryClient, sessionId, sessionType]);

  // Filter data based on search term
  const filteredRows = useMemo(() => {
    if (!searchTerm.trim()) return data.rows;

    return data.rows.filter(row => {
      const searchLower = searchTerm.toLowerCase();

      // Search in row name
      if (row.row_name?.toLowerCase().includes(searchLower)) return true;

      // Search in data values
      return Object.values(row.data).some(value => {
        if (value === null || value === undefined) return false;
        return String(value).toLowerCase().includes(searchLower);
      });
    });
  }, [data.rows, searchTerm]);

  // Get all column names with proper ordering (row name first)
  const defaultColumns = useMemo(() => {
    const priorityColumns: string[] = [];
    const regularColumns: string[] = [];

    // Add standard columns
    if (data.rows.some(row => row.row_name)) {
      priorityColumns.push('_row_name');
    }
    if (data.rows.some(row => row.papers?.length)) {
      regularColumns.push('_papers');
    }

    // Get all data columns and filter out excerpt columns
    const allDataColumns = new Set<string>();
    data.rows.forEach(row => {
      Object.keys(row.data).forEach(key => allDataColumns.add(key));
    });

    // Filter out excerpt columns (_excerpt suffix) from display
    const dataColumnArray = Array.from(allDataColumns).filter(col => !col.endsWith('_excerpt'));

    // First priority: exact matches for common row identifier names
    const exactMatches = ['row_name', 'name', 'id', 'title', 'row', 'identifier'];
    exactMatches.forEach(exactName => {
      const found = dataColumnArray.find(col => col.toLowerCase() === exactName);
      if (found && !priorityColumns.includes(found)) {
        priorityColumns.push(found);
      }
    });

    // Second priority: columns that contain row identifier keywords
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

    // Fallback: if no priority columns found, make the first column a priority
    if (priorityColumns.length === 0 && regularColumns.length > 0) {
      const firstColumn = regularColumns.shift();
      if (firstColumn) priorityColumns.push(firstColumn);
    }

    // Combine: priority columns first, then regular columns
    return [...priorityColumns, ...regularColumns];
  }, [data.rows]);

  // Use external column order if provided, otherwise use default
  const columns = useMemo(() => {
    if (externalColumnOrder && externalColumnOrder.length > 0) {
      // Validate that all columns in externalColumnOrder still exist
      const validExternalOrder = externalColumnOrder.filter(col => defaultColumns.includes(col));
      // Add any new columns that aren't in the external order
      const newColumns = defaultColumns.filter(col => !externalColumnOrder.includes(col));
      return [...validExternalOrder, ...newColumns];
    }
    return defaultColumns;
  }, [defaultColumns, externalColumnOrder]);

  // Determine which columns should be frozen (first column)
  const frozenColumn = columns[0];
  const scrollableColumns = columns.slice(1);

  // Handle drag end for column reordering
  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;

    if (over && active.id !== over.id) {
      const oldIndex = scrollableColumns.indexOf(active.id as string);
      const newIndex = scrollableColumns.indexOf(over.id as string);

      const newScrollableColumns = arrayMove(scrollableColumns, oldIndex, newIndex);
      const newOrder = [frozenColumn, ...newScrollableColumns];

      // Notify parent component of the new order
      if (onColumnReorder) {
        onColumnReorder(newOrder);
      }
    }
  };

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setPageSize(parseInt(event.target.value, 10));
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

    // Get all data columns including excerpt columns
    const allDataColumns = new Set<string>();
    data.rows.forEach(row => {
      Object.keys(row.data).forEach(key => allDataColumns.add(key));
    });

    // Find excerpt columns and map them to their base columns
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

  // Helper function to find excerpt value for a given row and column
  const getExcerptForColumn = (row: DataRow, columnName: string): string | null => {
    const excerptColumnName = excerptMapping[columnName];
    if (excerptColumnName && row.data[excerptColumnName]) {
      return String(row.data[excerptColumnName]);
    }
    return null;
  };

  const formatCellValue = (value: CellValue, columnName: string, rowData?: DataRow): React.ReactNode => {
    if (value === null || value === undefined) {
      return <Chip label="null" size="small" variant="outlined" color="default" />;
    }

    if (Array.isArray(value)) {
      if (value.length > 3) {
        return (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            {value.slice(0, 2).map((item, index) => (
              <Chip key={index} label={String(item)} size="small" sx={{ mr: 0.5 }} />
            ))}
            <Typography variant="caption">
              +{value.length - 2} more
            </Typography>
            <IconButton
              size="small"
              title="View full array"
              onClick={() => handleViewContent(columnName, value)}
            >
              <Visibility fontSize="small" />
            </IconButton>
          </Box>
        );
      }
      return (
        <Box>
          {value.map((item, index) => (
            <Chip key={index} label={String(item)} size="small" sx={{ mr: 0.5, mb: 0.5 }} />
          ))}
        </Box>
      );
    }

    if (typeof value === 'object' && value !== null) {
      // Check if this is QBSD format: {answer: "...", excerpts: [...]}
      if ('answer' in value && typeof (value as QBSDAnswerWithExcerpts).answer !== 'undefined') {
        const qbsdValue = value as QBSDAnswerWithExcerpts;
        const answer = qbsdValue.answer;
        const excerpts = qbsdValue.excerpts || [];

        return (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <Typography variant="body1" component="span" sx={{
              fontSize: '1.1rem',
              lineHeight: 1.5,
              display: '-webkit-box',
              WebkitLineClamp: 3,
              WebkitBoxOrient: 'vertical',
              overflow: 'hidden'
            }}>
              {String(answer)}
            </Typography>
            {excerpts.length > 0 && (
              <IconButton
                size="small"
                title={`View excerpts (${excerpts.length} sources)`}
                onClick={() => handleViewContent(columnName, {
                  answer: answer,
                  excerpts: excerpts
                })}
                sx={{ color: 'info.main' }}
              >
                <Visibility fontSize="small" />
              </IconButton>
            )}
          </Box>
        );
      }

      // Regular object handling
      return (
        <IconButton
          size="small"
          title="View object"
          onClick={() => handleViewContent(columnName, value)}
        >
          <Visibility fontSize="small" />
        </IconButton>
      );
    }

    const stringValue = String(value);

    // Check if this column has associated excerpts in the uploaded data
    const hasExcerpts = rowData && excerptMapping[columnName] && rowData.data[excerptMapping[columnName]];

    // Use utility functions for content detection
    const isExplicitExcerpt = isExcerptContent(columnName, stringValue);
    const isVeryLongContent = isVeryLongText(stringValue, LONG_TEXT_THRESHOLD);
    const hasManyLines = hasMultipleLines(stringValue, MAX_CELL_LINES);

    // Show eye icon for: 1) columns with excerpts, 2) explicit excerpt columns, or 3) very long content
    const shouldShowEyeIcon = hasExcerpts || isExplicitExcerpt || isVeryLongContent ||
                             (hasManyLines && stringValue.length > MEDIUM_TEXT_THRESHOLD);

    if (shouldShowEyeIcon) {
      // For excerpt-like content, show just a short preview with eye icon
      const previewText = isExplicitExcerpt ?
        getPreviewText(stringValue, 50) :
        getPreviewText(stringValue, SHORT_TEXT_THRESHOLD);

      return (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          <Typography
            variant="body1"
            component="span"
            sx={{
              fontSize: '1.1rem',
              lineHeight: 1.5,
              fontStyle: isExplicitExcerpt ? 'italic' : 'normal',
              color: isExplicitExcerpt ? 'text.secondary' : 'text.primary',
              display: '-webkit-box',
              WebkitLineClamp: 3,
              WebkitBoxOrient: 'vertical',
              overflow: 'hidden'
            }}
          >
            {previewText}
          </Typography>
          <IconButton
            size="small"
            title={hasExcerpts ? "View content with supporting excerpts" :
                   isExplicitExcerpt ? "View excerpt details" : "View full content"}
            onClick={() => {
              if (hasExcerpts) {
                // Create combined content with main value and excerpts
                const excerptText = getExcerptForColumn(rowData, columnName);
                handleViewContent(columnName, {
                  answer: stringValue,
                  excerpts: excerptText ? [excerptText] : []
                });
              } else {
                handleViewContent(columnName, value);
              }
            }}
            sx={{ color: 'info.main' }}
          >
            <Visibility fontSize="small" />
          </IconButton>
        </Box>
      );
    }

    // Regular content with truncation for readability (but no eye icon)
    if (needsTruncation(stringValue)) {
      return (
        <Typography
          variant="body1"
          component="span"
          sx={{
            fontSize: '1.1rem',
            lineHeight: 1.5,
            display: '-webkit-box',
            WebkitLineClamp: 3,
            WebkitBoxOrient: 'vertical',
            overflow: 'hidden',
            wordBreak: 'break-word'
          }}
        >
          {truncateText(stringValue)}
        </Typography>
      );
    }

    // Regular content with improved typography
    return (
      <Typography
        variant="body1"
        component="span"
        sx={{
          fontSize: '1.1rem',
          lineHeight: 1.5,
          display: '-webkit-box',
          WebkitLineClamp: 3,
          WebkitBoxOrient: 'vertical',
          overflow: 'hidden'
        }}
      >
        {stringValue}
      </Typography>
    );
  };

  return (
    <Paper>
      <Box sx={{ p: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Box>
            <Typography variant="h6">
              Data Table ({data.total_count.toLocaleString()} rows)
              {sessionType === 'qbsd' && (
                <Chip
                  label="Auto-refreshing"
                  size="small"
                  color="info"
                  sx={{ ml: 1 }}
                />
              )}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Drag column headers to reorder
            </Typography>
          </Box>

          <TextField
            size="small"
            placeholder="Search data..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Search />
                </InputAdornment>
              ),
            }}
            sx={{ width: SEARCH_FIELD_WIDTH }}
          />
        </Box>

        <DndContext
          sensors={sensors}
          collisionDetection={closestCenter}
          onDragEnd={handleDragEnd}
        >
          <TableContainer sx={{ maxHeight: TABLE_MAX_HEIGHT, overflow: 'auto' }}>
            <Table stickyHeader size="small" sx={{ minWidth: TABLE_MIN_WIDTH }}>
              <TableHead>
                <TableRow>
                  {/* Frozen first column (not draggable) */}
                  {frozenColumn && (
                    <TableCell
                      key={frozenColumn}
                      sx={{
                        fontWeight: 'bold',
                        fontSize: '1.1rem',
                        minWidth: FROZEN_COLUMN_WIDTH,
                        maxWidth: FROZEN_COLUMN_WIDTH,
                        position: 'sticky',
                        left: 0,
                        backgroundColor: 'background.paper',
                        zIndex: 3,
                        borderRight: '2px solid',
                        borderRightColor: 'primary.main',
                        boxShadow: '2px 0 4px rgba(0,0,0,0.1)'
                      }}
                    >
                      {frozenColumn.startsWith('_') ? (
                        <Chip
                          label={formatColumnName(frozenColumn)}
                          size="small"
                          color="primary"
                          variant="outlined"
                        />
                      ) : (
                        formatColumnName(frozenColumn)
                      )}
                    </TableCell>
                  )}

                  {/* Sortable (draggable) columns */}
                  <SortableContext
                    items={scrollableColumns}
                    strategy={horizontalListSortingStrategy}
                  >
                    {scrollableColumns.map(column => (
                      <SortableHeaderCell key={column} column={column}>
                        {column.startsWith('_') ? (
                          <Chip
                            label={formatColumnName(column)}
                            size="small"
                            color="primary"
                            variant="outlined"
                          />
                        ) : (
                          formatColumnName(column)
                        )}
                      </SortableHeaderCell>
                    ))}
                  </SortableContext>
                </TableRow>
              </TableHead>
              <TableBody>
                {/* Server handles pagination, so we don't slice here unless filtering locally by search */}
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

                  // Calculate actual row index (accounting for pagination and total rows)
                  const actualRowIndex = page * pageSize + rowIndex + 1; // +1 because row indexes start from 1
                  const isNewlyAdded = newlyAddedRows?.has(actualRowIndex) || false;
                  // Check if this row is currently streaming (has incomplete data being filled in)
                  const isStreaming = row.row_name ? streamingCells?.has(row.row_name) : false;

                  return (
                    <TableRow
                      key={rowIndex}
                      hover
                      sx={{
                        backgroundColor: isNewlyAdded ? 'success.light' : isStreaming ? 'info.light' : 'inherit',
                        animation: isNewlyAdded ? 'pulse 2s ease-in-out' : isStreaming ? 'streamPulse 1.5s ease-in-out infinite' : 'none',
                        '@keyframes pulse': {
                          '0%': { backgroundColor: 'success.light' },
                          '50%': { backgroundColor: 'success.main' },
                          '100%': { backgroundColor: 'success.light' }
                        },
                        '@keyframes streamPulse': {
                          '0%': { backgroundColor: 'rgba(33, 150, 243, 0.1)' },
                          '50%': { backgroundColor: 'rgba(33, 150, 243, 0.2)' },
                          '100%': { backgroundColor: 'rgba(33, 150, 243, 0.1)' }
                        }
                      }}
                    >
                      {/* Frozen first column */}
                      {frozenColumn && (
                        <TableCell
                          key={frozenColumn}
                          sx={{
                            minWidth: REGULAR_COLUMN_WIDTH,
                            maxWidth: REGULAR_COLUMN_WIDTH,
                            position: 'sticky',
                            left: 0,
                            backgroundColor: 'background.paper',
                            zIndex: 2,
                            borderRight: '2px solid',
                            borderRightColor: 'divider',
                            overflow: 'hidden'
                          }}
                        >
                          {formatCellValue(getFrozenCellValue(), frozenColumn, row)}
                        </TableCell>
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
                          <TableCell key={column} sx={{ minWidth: REGULAR_COLUMN_WIDTH, maxHeight: `${TABLE_ROW_MAX_HEIGHT}px`, overflow: 'hidden' }}>
                            {formatCellValue(cellValue, column, row)}
                          </TableCell>
                        );
                      })}
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </TableContainer>
        </DndContext>

        <TablePagination
          rowsPerPageOptions={AVAILABLE_PAGE_SIZES}
          component="div"
          count={searchTerm ? filteredRows.length : data.total_count}
          rowsPerPage={pageSize}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
        />
      </Box>

      {/* Content Modal */}
      <ContentModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        title={modalContent.title}
        content={modalContent.content}
      />
    </Paper>
  );
};

export default DataTable;
