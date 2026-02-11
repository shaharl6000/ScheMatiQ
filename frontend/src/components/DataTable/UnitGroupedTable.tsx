/**
 * Table component that displays data grouped by observation units.
 * Provides collapsible unit groups with merge functionality.
 */

import React, { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { Merge, RefreshCw, ChevronDown, ChevronUp, Loader2, Lightbulb, FileText } from 'lucide-react';
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

import { unitsAPI } from '../../services/api';
import { useUnits, useMergeUnits, useUnitSuggestions } from '../../hooks/useUnits';
import { MergeUnitsRequest } from '../../types/unit';
import { UnitFilter } from '../ViewMode/UnitFilter';
import { UnitMergeDialog } from '../ViewMode/UnitMergeDialog';
import { UnitSimilarityCard } from '../Units/UnitSimilarityCard';
import UnitGroupRow from './UnitGroupRow';
import ContentModal from '../ContentModal/ContentModal';
import { DataRow, CellValue, ModalContent, QBSDAnswerWithExcerpts } from '../../types';
import { cn } from '@/lib/utils';
import { useToast } from '@/components/ui/use-toast';
import { formatColumnName } from '../../utils/formatting';
import { parsePythonString, extractDisplayValue } from './utils';
import { AVAILABLE_PAGE_SIZES } from '../../constants';
import { Eye } from 'lucide-react';
import { useColumnResize, MIN_COLUMN_WIDTH } from './hooks/useColumnResize';

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
  const { suggestions, loading: suggestionsLoading, autoMerged, fetchSuggestions } = useUnitSuggestions(sessionId);
  const { toast } = useToast();

  // Column resize hook
  const {
    columnWidths,
    getColumnWidth,
    handleResizeStart,
  } = useColumnResize({ sessionId });
  const hasCustomWidths = Object.keys(columnWidths).length > 0;

  // Refs for header cells (for resize start width measurement)
  const headerRefs = useRef<Record<string, HTMLTableCellElement | null>>({});

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
  const [pageSize, setPageSize] = useState(50);

  // Modal state for viewing cell content with excerpts
  const [modalOpen, setModalOpen] = useState(false);
  const [modalContent, setModalContent] = useState<ModalContent>({ title: '', content: null });

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

  // Pagination handlers
  const handleChangePage = useCallback((newPage: number) => {
    setPage(newPage);
  }, []);

  const handleChangeRowsPerPage = useCallback((value: string) => {
    setPageSize(parseInt(value, 10));
    setPage(0);
  }, []);

  // Total pages calculation
  const totalPages = unitData ? Math.ceil(unitData.total_count / pageSize) : 0;

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
      await refreshUnits();
      await refetchData();
      onDataChange?.();
    } catch (err) {
      // Error is handled by the hook
    }
  }, [merge, clearMergeSelections, refreshUnits, refetchData, onDataChange]);

  // Handle backend auto-merge results — refresh table and show toast
  useEffect(() => {
    if (autoMerged.length > 0) {
      const totalMerged = autoMerged.reduce((sum, g) => sum + g.mergedUnits.length, 0);
      refreshUnits();
      refetchData();
      onDataChange?.();
      toast({
        title: 'Auto-merged identical units',
        description: `Merged ${totalMerged} units into ${autoMerged.length} ${autoMerged.length === 1 ? 'group' : 'groups'}`,
      });
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

  // Get selected units for merge dialog
  const selectedUnitsForMerge = useMemo(() => {
    return units.filter(u => selectedForMerge.has(u.name));
  }, [units, selectedForMerge]);

  // Filter suggestions by dismissed
  const visibleSuggestions = useMemo(() => {
    if (!suggestions?.suggestions) return [];
    return suggestions.suggestions.filter(s => {
      const key = [...s.units].sort().join('|');
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

  // Check if any row has _source_document
  const hasSourceDocument = useMemo(() => {
    return unitData?.rows?.some(row => row._source_document != null) ?? false;
  }, [unitData?.rows]);

  // Determine which columns to show (filter out internal columns, but keep _unit_name)
  // Note: _source_document is rendered as a dedicated first column, not part of this array
  const visibleColumns = useMemo(() => {
    return columns.filter(col =>
      !col.startsWith('_') || col === '_unit_name'
    );
  }, [columns]);

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
                  onClick={() => setShowSuggestions(!showSuggestions)}
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
          <table
            className="w-full border-collapse"
            style={hasCustomWidths ? { tableLayout: 'fixed' } : undefined}
          >
            <thead className="sticky top-0 z-10 bg-background border-b">
              <tr>
                {/* Source Document column - always first when present */}
                {hasSourceDocument && (
                  <th
                    ref={(el) => { headerRefs.current['_source_document'] = el; }}
                    className={cn(
                      "px-4 py-3 text-left font-bold text-base bg-background border-r relative",
                      !getColumnWidth('_source_document') && "min-w-[180px]"
                    )}
                    style={getColumnWidth('_source_document') ? { width: getColumnWidth('_source_document'), minWidth: MIN_COLUMN_WIDTH } : undefined}
                  >
                    <div className="flex items-center gap-1.5">
                      <FileText className="h-4 w-4 text-muted-foreground" />
                      Source Document
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
                {visibleColumns.map(column => (
                  <th
                    key={column}
                    ref={(el) => { headerRefs.current[column] = el; }}
                    className={cn(
                      "px-4 py-3 text-left font-bold text-base bg-background relative",
                      !getColumnWidth(column) && "min-w-[120px]"
                    )}
                    style={getColumnWidth(column) ? { width: getColumnWidth(column), minWidth: MIN_COLUMN_WIDTH } : undefined}
                  >
                    {column.startsWith('_') ? (
                      <Badge variant="outline">{formatColumnName(column)}</Badge>
                    ) : (
                      formatColumnName(column)
                    )}
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
              {/* Render units with their rows - only show units that have loaded data */}
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
                        columnCount={visibleColumns.length + (hasSourceDocument ? 1 : 0)}
                      />

                      {/* Unit rows (when expanded) */}
                      {isExpanded && unitRows.map((row, rowIndex) => (
                        <tr
                          key={`${unit.name}-${rowIndex}`}
                          className="border-b hover:bg-muted/30 transition-colors"
                        >
                          {/* Source Document cell - always first when present */}
                          {hasSourceDocument && (
                            <td
                              className="px-4 py-3 text-sm border-r bg-muted/20"
                              style={getColumnWidth('_source_document') ? { width: getColumnWidth('_source_document'), minWidth: MIN_COLUMN_WIDTH } : undefined}
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
                          {visibleColumns.map(column => {
                            let cellValue: CellValue;
                            if (column === '_unit_name') {
                              cellValue = row._unit_name;
                            } else {
                              cellValue = row.data[column];
                            }

                            return (
                              <td
                                key={column}
                                className="px-4 py-3 text-sm"
                                style={getColumnWidth(column) ? { width: getColumnWidth(column), minWidth: MIN_COLUMN_WIDTH } : undefined}
                              >
                                {formatCellValue(cellValue, column, row, excerptMapping, handleViewContent)}
                              </td>
                            );
                          })}
                        </tr>
                      ))}

                      {/* Show message if no data rows for expanded unit */}
                      {isExpanded && unitRows.length === 0 && (
                        <tr>
                          <td
                            colSpan={visibleColumns.length + (hasSourceDocument ? 1 : 0)}
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
                {unitData.total_count > 0 ? page * pageSize + 1 : 0}-{Math.min((page + 1) * pageSize, unitData.total_count)} of {unitData.total_count}
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

      {/* Content Modal for viewing cell values with excerpts */}
      <ContentModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        title={modalContent.title}
        content={modalContent.content}
      />
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

/**
 * Cell value formatter for the grouped table.
 * Handles QBSD answer/excerpts, value/excerpt, and text formats.
 * Renders clickable cells with Eye icon for content with excerpts.
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
            className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1 group"
            onClick={() => onViewContent(columnName, { answer, excerpts: modalExcerpts })}
            title={tooltip}
          >
            <div className="relative text-sm leading-relaxed line-clamp-3 break-words pr-6">
              {answerStr.length > 100 ? `${answerStr.slice(0, 100)}...` : answerStr}
              <span className="absolute right-0 top-0 flex items-center h-6 bg-gradient-to-l from-white dark:from-gray-900 from-60% to-transparent pl-2">
                <Eye className="h-4 w-4 text-blue-600 group-hover:text-blue-800" />
              </span>
            </div>
          </div>
        );
      }

      return (
        <span className="text-sm leading-relaxed line-clamp-3">
          {answerStr}
        </span>
      );
    }
  }

  // Handle string values - check for excerpt column or long text
  if (hasExcerptColumn || displayStr.length > 100) {
    const modalExcerpts = hasExcerptColumn ? getExcerptsFromColumn() : [];
    const tooltip = hasExcerptColumn ? "Click to view excerpts" : "Click to view full content";

    return (
      <div
        className="cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-950 rounded p-1 -m-1 group"
        onClick={() => onViewContent(columnName, {
          answer: displayStr,
          excerpts: modalExcerpts
        })}
        title={tooltip}
      >
        <div className="relative text-sm leading-relaxed line-clamp-3 break-words pr-6">
          {displayStr.length > 100 ? `${displayStr.slice(0, 100)}...` : displayStr}
          <span className="absolute right-0 top-0 flex items-center h-6 bg-gradient-to-l from-white dark:from-gray-900 from-60% to-transparent pl-2">
            <Eye className="h-4 w-4 text-blue-600 group-hover:text-blue-800" />
          </span>
        </div>
      </div>
    );
  }

  return <span className="text-sm leading-relaxed">{displayStr}</span>;
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
