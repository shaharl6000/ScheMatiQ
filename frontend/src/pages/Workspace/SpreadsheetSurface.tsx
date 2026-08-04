// Handsontable grid for Data, Schema, and Observation Unit sheets with grounding UI.
// Parent: Workspace (index.tsx). Owns the sole registerAllModules() call.

import { type CSSProperties, type MutableRefObject, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useParams } from 'react-router-dom';
import { HotTable, type HotTableClass } from '@handsontable/react';
import { registerAllModules } from 'handsontable/registry';
import 'handsontable/styles/handsontable.min.css';
import 'handsontable/styles/ht-theme-main.min.css';

import { extractDisplayValue } from '@/components/DataTable/utils/valueUtils';
import {
  buildExcerptMapping,
  resolveCellGrounding,
  type CellGrounding,
} from '@/components/DataTable/utils/excerptUtils';
import ContentModal from '@/components/ContentModal/ContentModal';
import { useToast } from '@/components/ui/use-toast';
import { observationUnitAPI, schemaAPI, schematiqAPI } from '@/services/api';
import type { ColumnInfo, DataRow, PaginatedData, SchemaData } from '@/types';
import { formatColumnName } from '@/utils/formatting';

import {
  EDITABLE_OBSERVATION_UNIT_FIELDS,
  SCHEMA_COLUMN_HEADER_TOOLTIPS,
  cellFormatKey,
} from './constants';
import {
  documentDisplayName,
  formatSheetColHeader,
  getCellFormatClasses,
  parseAllowedValues,
  renderObservationUnitFieldCell,
} from './helpers';
import type {
  CellFormatMap,
  PendingRerunKind,
  SheetColumn,
  SheetId,
  SheetSelection,
  TableDisplayOptions,
} from './types';

registerAllModules();

export function SpreadsheetSurface({
  activeSheet,
  data,
  schema,
  displayOptions,
  cellFormats,
  formatVersion,
  hotTableRef,
  onSelectionChange,
  onGroundingHighlight,
  onGroundingScrollRequest,
  onRefresh,
  onRefreshData,
  onOptimisticCellEdit,
  onEditFollowUp,
  onEditEnd,
  layoutRevision,
  dataView,
}: {
  activeSheet: SheetId;
  data: PaginatedData;
  schema: SchemaData | null;
  displayOptions: TableDisplayOptions;
  cellFormats: CellFormatMap;
  formatVersion: number;
  hotTableRef: MutableRefObject<HotTableClass | null>;
  onSelectionChange: (selection: SheetSelection) => void;
  // Reports all grounding excerpts of the newly selected data cell (or null when
  // the cell has no grounding), so the source panel can highlight them.
  onGroundingHighlight?: (texts: string[] | null) => void;
  // Fires on each mouse click of a grounded data cell, so the source panel can
  // re-scroll to the highlight even when the same cell is clicked again.
  onGroundingScrollRequest?: () => void;
  onRefresh: () => void;
  // Row-data-only refresh after a Data-sheet cell edit (no status/schema churn).
  onRefreshData: () => void;
  // Apply the edited value to React state immediately so the grid does not
  // revert when a background refresh re-renders before the PUT completes.
  onOptimisticCellEdit: (
    identity: { rowName: string; sourceDocument?: string; rowIndex?: number },
    column: string,
    value: string,
  ) => void;
  onEditFollowUp: (kind: PendingRerunKind, columns?: string[]) => void;
  onEditEnd: () => void;
  layoutRevision: string;
  // Current Data-sheet grouping. Drives the visual cell-merge of the leftmost
  // grouping column: 'by_unit' merges unit_name, 'by_document' merges the
  // Source Document column. Ignored on non-data sheets.
  dataView: 'by_unit' | 'by_document';
}) {
  const { sessionId } = useParams();
  const { toast } = useToast();
  const gridContainerRef = useRef<HTMLDivElement | null>(null);
  // Track the grid container as state (not just a ref) so the measurement
  // effect below re-runs the moment the real element mounts. On the
  // project-creation flow the surface first renders a placeholder (no session,
  // no ref) and only attaches the measured element once a session arrives;
  // a plain ref would not re-trigger the effect, leaving the grid stuck at its
  // 320px fallback width until a tab switch forces a remount.
  const [gridContainerEl, setGridContainerEl] = useState<HTMLDivElement | null>(null);
  const setGridContainerRef = useCallback((node: HTMLDivElement | null) => {
    gridContainerRef.current = node;
    setGridContainerEl(node);
  }, []);
  const lastGridSizeRef = useRef({ width: 0, height: 0 });
  const [gridSize, setGridSize] = useState({ width: 0, height: 0 });

  const applyGridSize = useCallback((width: number, height: number) => {
    const nextWidth = Math.max(320, Math.floor(width));
    const nextHeight = Math.max(260, Math.floor(height));
    if (nextWidth < 1 || nextHeight < 1) return;
    lastGridSizeRef.current = { width: nextWidth, height: nextHeight };
    setGridSize((current) => (
      current.width === nextWidth && current.height === nextHeight
        ? current
        : { width: nextWidth, height: nextHeight }
    ));
  }, []);

  const measureGrid = useCallback(() => {
    const element = gridContainerRef.current;
    if (!element) return;
    const rect = element.getBoundingClientRect();
    applyGridSize(rect.width, rect.height);
  }, [applyGridSize]);

  const syncHotTableDimensions = useCallback(() => {
    const element = gridContainerRef.current;
    const hot = hotTableRef.current?.hotInstance;
    if (!element || !hot) return;

    const rect = element.getBoundingClientRect();
    const width = Math.max(320, Math.floor(rect.width));
    const height = Math.max(260, Math.floor(rect.height));
    if (width < 1 || height < 1) return;

    hot.updateSettings({ width, height });
    hot.refreshDimensions();
    applyGridSize(width, height);
  }, [applyGridSize, hotTableRef]);

  useLayoutEffect(() => {
    const element = gridContainerEl;
    if (!element) return undefined;

    lastGridSizeRef.current = { width: 0, height: 0 };
    measureGrid();

    const raf = window.requestAnimationFrame(() => {
      measureGrid();
      window.requestAnimationFrame(measureGrid);
    });
    const retryTimer = window.setTimeout(measureGrid, 120);

    const observedTargets = new Set<Element>([element]);
    if (element.parentElement) {
      observedTargets.add(element.parentElement);
    }

    let observer: ResizeObserver | undefined;
    if (typeof ResizeObserver !== 'undefined') {
      observer = new ResizeObserver(() => {
        window.requestAnimationFrame(measureGrid);
      });
      observedTargets.forEach((target) => observer?.observe(target));
    }

    window.addEventListener('resize', measureGrid);
    return () => {
      window.cancelAnimationFrame(raf);
      window.clearTimeout(retryTimer);
      observer?.disconnect();
      window.removeEventListener('resize', measureGrid);
    };
  }, [activeSheet, layoutRevision, measureGrid, gridContainerEl]);

  useEffect(() => {
    syncHotTableDimensions();
  }, [gridSize, syncHotTableDimensions]);

  // Hover/focus help for Handsontable column-header info icons.
  //
  // Handsontable renders headers as raw HTML inside scroll containers that clip
  // overflow for virtualization, and it has no React tree we can mount a tooltip
  // component into. A CSS ::after tooltip therefore gets cut off (and overflows
  // the viewport on the rightmost column), and a native `title` has an
  // uncontrollable delay. Instead we keep one tooltip element on document.body
  // and position it with JS: it escapes every clipping context, appears
  // instantly, and stays inside the viewport for any column. One element plus
  // delegated listeners means no per-render cost and it survives table remounts.
  useEffect(() => {
    const INFO_SELECTOR = '.workspace-hot .workspace-col-header-info';
    const tooltip = document.createElement('div');
    tooltip.className = 'workspace-hot-tooltip';
    tooltip.setAttribute('role', 'tooltip');
    tooltip.style.display = 'none';
    document.body.appendChild(tooltip);

    let activeIcon: HTMLElement | null = null;

    const place = (icon: HTMLElement) => {
      const margin = 8;
      const gap = 6;
      const anchor = icon.getBoundingClientRect();
      const tip = tooltip.getBoundingClientRect();

      let top = anchor.bottom + gap;
      if (top + tip.height > window.innerHeight - margin) {
        top = anchor.top - tip.height - gap; // flip above
      }
      top = Math.max(margin, Math.min(top, window.innerHeight - margin - tip.height));

      let left = anchor.left;
      if (left + tip.width > window.innerWidth - margin) {
        left = window.innerWidth - margin - tip.width; // clamp to right edge
      }
      left = Math.max(margin, left);

      tooltip.style.top = `${Math.round(top)}px`;
      tooltip.style.left = `${Math.round(left)}px`;
    };

    const show = (icon: HTMLElement) => {
      const text = icon.getAttribute('data-tooltip');
      if (!text) return;
      activeIcon = icon;
      tooltip.textContent = text;
      tooltip.style.display = 'block';
      place(icon); // measure after content + display so dimensions are real
    };

    const hide = () => {
      activeIcon = null;
      tooltip.style.display = 'none';
    };

    const resolveIcon = (event: Event): HTMLElement | null => {
      const target = event.target as HTMLElement | null;
      return (target?.closest?.(INFO_SELECTOR) as HTMLElement | null) ?? null;
    };

    const onOver = (event: Event) => {
      const icon = resolveIcon(event);
      if (icon && icon !== activeIcon) show(icon);
    };
    const onOut = (event: Event) => {
      if (!activeIcon) return;
      const related = (event as MouseEvent).relatedTarget as Node | null;
      if (related && activeIcon.contains(related)) return; // moved onto child svg
      hide();
    };
    const onFocusIn = (event: Event) => {
      const icon = resolveIcon(event);
      if (icon) show(icon);
    };
    const dismiss = () => {
      if (activeIcon) hide();
    };

    document.addEventListener('mouseover', onOver, true);
    document.addEventListener('mouseout', onOut, true);
    document.addEventListener('focusin', onFocusIn, true);
    document.addEventListener('focusout', dismiss, true);
    // Any scroll (the grid scrolls internally) or resize moves the anchor, so
    // drop the tooltip rather than let it drift.
    window.addEventListener('scroll', dismiss, true);
    window.addEventListener('resize', dismiss);

    return () => {
      document.removeEventListener('mouseover', onOver, true);
      document.removeEventListener('mouseout', onOut, true);
      document.removeEventListener('focusin', onFocusIn, true);
      document.removeEventListener('focusout', dismiss, true);
      window.removeEventListener('scroll', dismiss, true);
      window.removeEventListener('resize', dismiss);
      tooltip.remove();
    };
  }, []);

  const schemaColumns = useMemo(() => {
    const cols = (schema?.schema || []) as Array<ColumnInfo & { allowed_values?: string[] }>;
    return cols;
  }, [schema]);

  // Display-only label for a canonical column name. The canonical `name` stays
  // the identity used for every edit/delete/API payload; this only affects what
  // the user sees in the Schema sheet's name cell and the Data tab headers.
  const columnDisplayLabel = useCallback(
    (name: string): string => {
      const col = schemaColumns.find((c) => c.name === name);
      return col?.display_name || formatColumnName(name);
    },
    [schemaColumns]
  );

  const dataColumnNames = useMemo(() => {
    // The schema is the source of truth for which columns the Data tab shows.
    // We intentionally do NOT union in keys from data.rows: a schema edit
    // (rename/delete) sets reprocess=false, so the stored row data keeps the
    // old keys until a re-extract reconciles them. Unioning those keys back in
    // made deleted/renamed columns linger in the Data tab.
    const names = new Set<string>();
    schemaColumns.forEach((column) => names.add(column.name));
    return Array.from(names);
  }, [schemaColumns]);

  const dataRows = useMemo(() => {
    return data.rows.map((row) => {
      const sheetRow: Record<string, string> = {
        _row_name: row.row_name || row._unit_name || '',
        // Provenance: the source document this row was extracted from. Falls back
        // to the parent document or the first referenced paper. File name only.
        _source_document: documentDisplayName(
          row._source_document || row._parent_document || row.papers?.[0],
        ),
      };
      dataColumnNames.forEach((column) => {
        sheetRow[column] = extractDisplayValue(row.data?.[column]);
      });
      return sheetRow;
    });
  }, [data.rows, dataColumnNames]);

  const dataGrounding = useMemo(() => {
    const mapping = buildExcerptMapping(data.rows);
    return data.rows.map((row) => {
      const perColumn: Record<string, CellGrounding> = {};
      dataColumnNames.forEach((canonical) => {
        const grounding = resolveCellGrounding(row, canonical, mapping);
        if (grounding && grounding.excerpts.length > 0) {
          perColumn[canonical] = grounding;
        }
      });
      return perColumn;
    });
  }, [data.rows, dataColumnNames]);

  const [groundingModal, setGroundingModal] = useState<{
    title: string;
    content: { answer: string; excerpts: CellGrounding['excerpts'] };
  } | null>(null);

  // --- Grouped cell merging (By Unit / By Document) ------------------------
  // Mirror the classic flow's grouped views: the leftmost grouping column
  // visually merges consecutive rows that share the same value, with a small
  // count badge on the group's first (top) row.
  //   By Unit      -> group unit_name (col 0); badge "N documents"
  //   By Document  -> group Source Document (col 1); badge "N observations"
  // The parent pre-orders the rows so each group's rows are contiguous, so the
  // merge runs are simple consecutive equal-value spans. All grouping logic is
  // scoped to the Data sheet; other sheets render flat.
  // The grouping (aggregation) column is always rendered leftmost so the user
  // always sees what they're grouping on in the first column: By Unit puts
  // unit_name first, By Document puts the Source Document first.
  const groupColIndex = 0;
  const groupColKey = dataView === 'by_unit' ? '_row_name' : '_source_document';
  const groupBadgeNoun = dataView === 'by_unit' ? 'documents' : 'observations';

  // One normalized group key per data row, in displayed (physical) order. The
  // renderer and merge builder both index this by physical row, so they stay
  // in sync with each other and survive column filtering (which only changes
  // the visual<->physical mapping, not this array).
  //
  // Derived from the raw `data.rows` prop rather than `dataRows` (the sheet
  // rows handed to Handsontable) on purpose: Handsontable's mergeCells plugin
  // writes back into its bound source data, blanking the grouping column on
  // every row it covers with a merge. `dataRows` is that same bound array, so
  // reading it here would pick up those blanks — correct right after the
  // first merge, but wrong on every subsequent recompute (e.g. leaving the
  // Data sheet and coming back), since by then the covered rows' values have
  // already been zeroed out and no longer match their group's key.
  const groupKeys = useMemo<string[]>(() => {
    if (activeSheet !== 'data') return [];
    return data.rows.map((row) => {
      const raw = groupColKey === '_row_name'
        ? (row.row_name || row._unit_name || '')
        : documentDisplayName(row._source_document || row._parent_document || row.papers?.[0]);
      return String(raw ?? '').trim().toLowerCase();
    });
  }, [activeSheet, data.rows, groupColKey]);

  // Whether the current grouped view actually has any group spanning more
  // than one row (i.e. a merge will be drawn). Handsontable's mergeCells
  // plugin is not compatible with columnSorting: sorting while a merge is
  // active reads already-blanked cells as sort keys, scrambles the row order,
  // and applies the resulting (wrong) merge ranges — corrupting the grouping
  // column's values on unrelated rows. See handsontable/handsontable#7509.
  // Since sorting would also scatter a group's rows apart even if that bug
  // didn't exist, the two features are mutually exclusive here: sorting is
  // only offered when there's nothing merged to break.
  const hasMultiRowGroup = useMemo<boolean>(
    () => groupKeys.some((key, i) => i > 0 && key !== '' && key === groupKeys[i - 1]),
    [groupKeys],
  );

  // Resolve the group key shown at a given *visual* row (accounting for sort).
  const groupKeyAtVisual = useCallback(
    (hot: any, visualRow: number): string => {
      const phys = typeof hot?.toPhysicalRow === 'function' ? hot.toPhysicalRow(visualRow) : visualRow;
      if (phys == null || phys < 0) return '';
      return groupKeys[phys] ?? '';
    },
    [groupKeys],
  );

  // Recompute and apply merge ranges from the *current* visual order. Driven
  // imperatively (not via a HotTable prop) so unrelated settings updates — e.g.
  // width/height on resize — never clobber the merges; updateSettings only
  // touches the keys it is given.
  const applyGroupMerges = useCallback(() => {
    const hot = hotTableRef.current?.hotInstance;
    if (!hot) return;
    if (activeSheet !== 'data' || groupKeys.length === 0) {
      try { hot.updateSettings({ mergeCells: false }); } catch { /* plugin may be mid-teardown */ }
      return;
    }
    const rowCount = hot.countRows();
    const ranges: { row: number; col: number; rowspan: number; colspan: number }[] = [];
    let start = 0;
    while (start < rowCount) {
      const key = groupKeyAtVisual(hot, start);
      let end = start + 1;
      if (key) {
        while (end < rowCount && groupKeyAtVisual(hot, end) === key) end++;
      }
      if (end - start > 1) {
        ranges.push({ row: start, col: groupColIndex, rowspan: end - start, colspan: 1 });
      }
      start = end;
    }
    try {
      hot.updateSettings({ mergeCells: ranges.length ? ranges : true });
    } catch { /* plugin may be mid-teardown */ }
  }, [activeSheet, groupColIndex, groupKeyAtVisual, groupKeys.length, hotTableRef]);

  // Keep the column filter alive across React re-renders.
  //
  // The @handsontable/react (v16) wrapper re-pushes every non-init-only prop
  // through updateSettings on every re-render. updateSettings with a `filters`
  // key re-initializes the filters plugin, wiping the active filter -- both the
  // trimmed rows AND the "Filter by value" list, which then collapses to only
  // the currently-visible values so excluded ones can never be re-selected. Any
  // unrelated re-render (selection, grounding, toast, refresh poll) triggers it,
  // so a filter set from the dropdown appears to do nothing / cannot be undone.
  //
  // `filters`/`dropdownMenu` must stay declared as props so they are enabled at
  // construction (enabling them later via updateSettings leaves the value
  // component half-initialized). Instead we register them as init-only after the
  // instance mounts: the wrapper reads `_initOnlySettings` from getSettings() and
  // skips re-pushing any init-only prop whose value is unchanged, so the plugin
  // is configured once and never torn down on subsequent renders. The list is
  // wrapper-facing metadata that Handsontable's core never reads at runtime.
  const markFilterSettingsInitOnly = useCallback(() => {
    const hot = hotTableRef.current?.hotInstance;
    if (!hot) return;
    try {
      const settings = hot.getSettings() as { _initOnlySettings?: string[] };
      if (!Array.isArray(settings._initOnlySettings)) settings._initOnlySettings = [];
      for (const key of ['filters', 'dropdownMenu']) {
        if (!settings._initOnlySettings.includes(key)) settings._initOnlySettings.push(key);
      }
    } catch { /* instance may be mid-teardown */ }
  }, [hotTableRef]);

  // Renderer for the active grouping column: plain text plus a count badge on
  // the group's first row. With cells merged, only the top row of a group is
  // visible for this column, so the badge lands on the spanning cell.
  const renderGroupCell = useCallback(
    (
      instance: unknown,
      td: HTMLTableCellElement,
      visualRow: number,
      _col: number,
      _prop: string | number,
      value: unknown,
    ): void => {
      const hot = instance as any;
      const text = value == null ? '' : String(value);
      td.textContent = text;
      td.style.verticalAlign = 'top';
      if (activeSheet !== 'data') return;

      const key = groupKeyAtVisual(hot, visualRow);
      if (!key) return;
      // Only the first row of a run carries the badge.
      if (visualRow > 0 && groupKeyAtVisual(hot, visualRow - 1) === key) return;

      const rowCount = typeof hot?.countRows === 'function' ? hot.countRows() : 0;
      let span = 1;
      for (let i = visualRow + 1; i < rowCount && groupKeyAtVisual(hot, i) === key; i++) span++;
      if (span <= 1) return;

      td.textContent = '';
      const wrap = document.createElement('div');
      wrap.style.display = 'flex';
      wrap.style.flexDirection = 'column';
      wrap.style.alignItems = 'flex-start';
      wrap.style.gap = '4px';

      const nameEl = document.createElement('span');
      nameEl.textContent = text;
      nameEl.style.fontWeight = '500';
      nameEl.style.wordBreak = 'break-word';

      const badge = document.createElement('span');
      badge.textContent = `${span} ${groupBadgeNoun}`;
      badge.style.alignSelf = 'flex-start';
      badge.style.fontSize = '11px';
      badge.style.lineHeight = '1.4';
      badge.style.padding = '0 6px';
      badge.style.borderRadius = '9999px';
      badge.style.background = 'rgba(100, 116, 139, 0.15)';
      badge.style.color = '#475569';
      badge.style.whiteSpace = 'nowrap';

      wrap.appendChild(nameEl);
      wrap.appendChild(badge);
      td.appendChild(wrap);
    },
    [activeSheet, groupBadgeNoun, groupKeyAtVisual],
  );

  // Re-apply merges whenever the displayed data, grouping column, or grid size
  // changes. gridSize is a dep because Handsontable re-applies width/height via
  // its own settings update on resize, after which the merges must be restored.
  useEffect(() => {
    applyGroupMerges();
  }, [applyGroupMerges, gridSize, formatVersion]);

  const schemaRows = useMemo(() => {
    return schemaColumns.map((column) => ({
      // Display-only: the canonical `column.name` remains the edit identity
      // (handleChanges/handleBeforeRemoveRow read it from schemaColumns, not
      // from this displayed value). No formatColumnName fallback here: this cell
      // is editable, so a plain canonical name must show verbatim (e.g. "status",
      // not "Status") to match what an edit operates on.
      name: column.display_name || column.name || '',
      definition: column.definition || '',
      rationale: column.rationale || '',
      allowed_values: Array.isArray(column.allowed_values) ? column.allowed_values.join(', ') : '',
      auto_expand_threshold: column.auto_expand_threshold ?? '',
    }));
  }, [schemaColumns]);

  const observationUnitRows = useMemo(() => {
    const unit = schema?.observation_unit;
    const exampleNames = Array.isArray(unit?.example_names) ? unit.example_names.join(', ') : '';

    return [
      { field: 'name', value: unit?.name || '' },
      { field: 'definition', value: unit?.definition || '' },
      { field: 'example_names', value: exampleNames },
    ];
  }, [schema?.observation_unit]);

  const sheet = useMemo((): { rows: Record<string, any>[]; columns: SheetColumn[]; minSpareRows?: number } => {
    if (activeSheet === 'schema') {
      return {
        rows: schemaRows,
        minSpareRows: 1,
        columns: [
          { key: 'name', label: 'name', width: 180, headerTooltip: SCHEMA_COLUMN_HEADER_TOOLTIPS.name },
          {
            key: 'definition',
            label: 'definition',
            width: 360,
            headerTooltip: SCHEMA_COLUMN_HEADER_TOOLTIPS.definition,
          },
          {
            key: 'rationale',
            label: 'rationale',
            width: 320,
            headerTooltip: SCHEMA_COLUMN_HEADER_TOOLTIPS.rationale,
          },
          {
            key: 'allowed_values',
            label: 'allowed_values',
            width: 260,
            headerTooltip: SCHEMA_COLUMN_HEADER_TOOLTIPS.allowed_values,
          },
          {
            key: 'auto_expand_threshold',
            label: 'auto_expand_threshold',
            width: 150,
            headerTooltip: SCHEMA_COLUMN_HEADER_TOOLTIPS.auto_expand_threshold,
          },
        ],
      };
    }

    if (activeSheet === 'unit') {
      return {
        rows: observationUnitRows,
        columns: [
          {
            key: 'field',
            label: 'field',
            width: 190,
            readOnly: true,
            renderer: renderObservationUnitFieldCell,
          },
          { key: 'value', label: 'value', width: 680 },
        ],
      };
    }

    // Order the two provenance columns so the active grouping column is
    // leftmost (and carries the merge + badge renderer); the other follows.
    const unitNameCol = { key: '_row_name', label: 'unit_name', width: 220, readOnly: true };
    const sourceDocCol = { key: '_source_document', label: 'Source Document', width: 220, readOnly: true };
    const groupCol = dataView === 'by_unit' ? unitNameCol : sourceDocCol;
    const otherCol = dataView === 'by_unit' ? sourceDocCol : unitNameCol;

    return {
      rows: dataRows,
      columns: [
        { ...groupCol, renderer: renderGroupCell },
        otherCol,
        ...dataColumnNames.map((name) => {
          // Mirror the legacy DataTable header behaviour: the data column's
          // header explains itself with the schema definition on hover.
          const definition = schemaColumns.find((c) => c.name === name)?.definition?.trim();
          return {
            key: name,
            label: columnDisplayLabel(name),
            width: 190,
            headerTooltip: definition || undefined,
          };
        }),
      ],
    };
  }, [activeSheet, dataColumnNames, dataRows, observationUnitRows, schemaRows, columnDisplayLabel, schemaColumns, dataView, renderGroupCell]);

  const handleChanges = useCallback((changes: any[] | null, source: string) => {
    if (!changes || source === 'loadData' || !sessionId) return;

    for (const change of changes) {
      const [rowIndex, prop, oldValue, newValue] = change;
      if (oldValue === newValue || prop == null) continue;
      const key = String(prop);

      if (activeSheet === 'data') {
        if (key.startsWith('_')) continue;
        const sourceRow: DataRow | undefined = data.rows[rowIndex];
        const rowName = sourceRow?.row_name || sourceRow?._unit_name || '';
        const rowIndexId = sourceRow?._row_index;
        const sourceDocument = sourceRow?._source_document || sourceRow?._parent_document;
        if (!rowName && rowIndexId == null) {
          toast({
            title: 'Cell update failed',
            description: 'Could not identify which row to update.',
            variant: 'destructive',
          });
          onRefreshData();
          continue;
        }

        onOptimisticCellEdit(
          { rowName, sourceDocument, rowIndex: rowIndexId },
          key,
          String(newValue ?? ''),
        );

        schematiqAPI.updateCell(
          sessionId,
          rowName,
          key,
          String(newValue ?? ''),
          sourceDocument,
          rowIndexId
        )
          .then(() => {
            const rowLabel = rowName || (rowIndexId != null ? `Row ${rowIndexId + 1}` : 'Row');
            toast({ title: 'Cell updated', description: `${rowLabel} / ${key}` });
            onRefreshData();
          })
          .catch((err: any) => {
            toast({
              title: 'Cell update failed',
              description: err?.response?.data?.detail || err?.message || 'Could not update cell',
              variant: 'destructive',
            });
            onRefreshData();
          });
      }

      if (activeSheet === 'schema') {
        const existing = schemaColumns[rowIndex];
        const editable = ['name', 'definition', 'rationale', 'allowed_values', 'auto_expand_threshold'];
        if (!editable.includes(key)) continue;

        if (!existing && key === 'name' && String(newValue || '').trim()) {
          // A brand-new column is created from the spare schema row. Other cells
          // on that row (definition/rationale/allowed_values) may already hold
          // values the user typed before the name -- e.g. when filling the row
          // right-to-left, where the name is entered last. Those edits landed on
          // a still-non-existing row and were skipped by the `!existing` guard
          // below, so read them back from the live grid and include them in the
          // create request. Otherwise the subsequent onRefresh() re-renders from
          // the backend (name only) and the typed values are lost.
          const hot = hotTableRef.current?.hotInstance;
          const readRowCell = (field: string): string =>
            hot ? String(hot.getDataAtRowProp(rowIndex, field) ?? '') : '';
          const pendingRationale = readRowCell('rationale').trim();
          const pendingAllowedValues = parseAllowedValues(readRowCell('allowed_values'));

          schemaAPI.addColumn(sessionId, {
            name: String(newValue).trim(),
            definition: readRowCell('definition').trim(),
            rationale: pendingRationale || undefined,
            allowed_values:
              pendingAllowedValues && pendingAllowedValues.length > 0
                ? pendingAllowedValues
                : undefined,
          })
            .then(() => {
              toast({ title: 'Schema column added' });
              onEditFollowUp('schema', [String(newValue).trim()]);
              onRefresh();
            })
            .catch((err: any) => {
              toast({
                title: 'Column add failed',
                description: err?.response?.data?.detail || err?.message || 'Could not add column',
                variant: 'destructive',
              });
              onRefresh();
            });
          continue;
        }

        if (!existing) continue;

        if (key === 'auto_expand_threshold') {
          schemaAPI.setAutoExpandThreshold(sessionId, existing.name, Number(newValue) || 0)
            .then(() => {
              toast({ title: 'Schema threshold updated', description: existing.name });
              onEditFollowUp('schema', [existing.name]);
              onRefresh();
            })
            .catch((err: any) => {
              toast({
                title: 'Threshold update failed',
                description: err?.response?.data?.detail || err?.message || 'Could not update threshold',
                variant: 'destructive',
              });
              onRefresh();
            });
          continue;
        }

        const request: any = { old_name: existing.name, reprocess: false };
        if (key === 'name') request.new_name = String(newValue || '').trim();
        if (key === 'definition') request.definition = String(newValue || '');
        if (key === 'rationale') request.rationale = String(newValue || '');
        if (key === 'allowed_values') {
          // Emptying the cell intentionally clears allowed_values; send [] so the
          // backend distinguishes "cleared" from "unchanged" (undefined is dropped by axios).
          request.allowed_values = parseAllowedValues(newValue) ?? [];
        }

        const affectedColumn = key === 'name' ? String(newValue || '').trim() : existing.name;

        schemaAPI.editColumn(sessionId, request)
          .then(() => {
            toast({ title: 'Schema updated', description: existing.name });
            if (affectedColumn) onEditFollowUp('schema', [affectedColumn]);
            onRefresh();
          })
          .catch((err: any) => {
            toast({
              title: 'Schema update failed',
              description: err?.response?.data?.detail || err?.message || 'Could not update schema',
              variant: 'destructive',
            });
            onRefresh();
          });
      }

      if (activeSheet === 'unit') {
        const editedField = observationUnitRows[rowIndex]?.field;
        if (!EDITABLE_OBSERVATION_UNIT_FIELDS.has(editedField)) continue;

        const nextValues = observationUnitRows.reduce<Record<string, unknown>>((values, row) => {
          values[row.field] = row.value;
          return values;
        }, {});
        nextValues[editedField] = newValue;

        const name = String(nextValues.name ?? '').trim();
        const definition = String(nextValues.definition ?? '').trim();

        if (!name || definition.length < 10) {
          toast({
            title: 'Observation unit not saved',
            description: 'The name is required and the definition needs at least 10 characters.',
            variant: 'destructive',
          });
          onRefresh();
          continue;
        }

        observationUnitAPI.updateDefinition(sessionId, {
          name,
          definition,
          example_names: parseAllowedValues(nextValues.example_names),
        })
          .then((result) => {
            onEditFollowUp('unit');
            onRefresh();
          })
          .catch((err: any) => {
            toast({
              title: 'Observation unit update failed',
              description: err?.response?.data?.detail || err?.message || 'Could not update the observation unit',
              variant: 'destructive',
            });
            onRefresh();
          });
      }
    }
  }, [activeSheet, data.rows, observationUnitRows, onEditFollowUp, onOptimisticCellEdit, onRefresh, onRefreshData, schemaColumns, sessionId, toast]);

  const handleBeforeRemoveRow = useCallback(
    (_index: number, _amount: number, physicalRows: number[], _source?: string): boolean | void => {
      // The observation-unit sheet has a fixed set of structural rows
      // (name / definition / example_names). Their values are editable, but the
      // rows themselves must never be removed, so block deletion outright.
      if (activeSheet === 'unit') {
        toast({
          title: 'Cannot delete this row',
          description: 'Observation unit fields are fixed. Edit the value instead of removing the row.',
          variant: 'destructive',
        });
        return false;
      }

      // Row removal only deletes a schema column when done on the Schema sheet.
      // On other sheets, fall through to Handsontable's default behavior.
      if (activeSheet !== 'schema' || !sessionId) return;

      const names = physicalRows
        .map((rowIndex) => schemaColumns[rowIndex]?.name)
        .filter((name): name is string => Boolean(name && name.trim()));

      if (names.length === 0) return;

      Promise.all(names.map((name) => schemaAPI.deleteColumn(sessionId, name)))
        .then(() => {
          toast({
            title: names.length > 1 ? 'Schema columns deleted' : 'Schema column deleted',
            description: names.join(', '),
          });
          // Deleting a column outright does not invalidate the remaining
          // columns' extracted values, so it must not flag a re-extract.
          // The "Schema changed" banner is only for edits/additions that
          // require re-running extraction against the source documents.
          onRefresh();
        })
        .catch((err: any) => {
          toast({
            title: 'Column delete failed',
            description: err?.response?.data?.detail || err?.message || 'Could not delete column',
            variant: 'destructive',
          });
          onRefresh();
        });

      // Cancel Handsontable's local removal; the schema state refresh below
      // re-renders the grid from the server's updated schema, keeping the
      // Schema and Data tabs in sync with a single source of truth.
      return false;
    },
    [activeSheet, onRefresh, schemaColumns, sessionId, toast],
  );

  const handleBeforeRemoveCol = useCallback(
    (_index: number, _amount: number, physicalColumns: number[], _source?: string): boolean | void => {
      // Removing a grid column only maps to a schema-column deletion on the Data
      // sheet. On the Schema / Unit sheets the columns are fixed structural
      // fields (name/definition/... and field/value), so block removal there.
      if (activeSheet !== 'data') {
        toast({
          title: 'Cannot delete this column',
          description: 'Columns can only be removed on the Data sheet. Edit structural fields in place instead.',
          variant: 'destructive',
        });
        return false;
      }

      if (!sessionId) return false;

      const keys = physicalColumns
        .map((colIndex) => sheet.columns[colIndex]?.key)
        .filter((key): key is string => Boolean(key));

      // The leading provenance/grouping columns (_row_name, _source_document)
      // are not schema columns and must never be deleted.
      if (keys.some((key) => key.startsWith('_'))) {
        toast({
          title: 'Cannot delete this column',
          description: 'Source and unit columns are fixed and cannot be removed.',
          variant: 'destructive',
        });
        return false;
      }

      const names = keys.filter((key) => schemaColumns.some((col) => col.name === key));
      if (names.length === 0) return false;

      Promise.all(names.map((name) => schemaAPI.deleteColumn(sessionId, name)))
        .then(() => {
          toast({
            title: names.length > 1 ? 'Schema columns deleted' : 'Schema column deleted',
            description: names.join(', '),
          });
          // Same rationale as row deletion: dropping a column does not
          // invalidate the remaining columns' values, so no re-extract flag.
          onRefresh();
        })
        .catch((err: any) => {
          toast({
            title: 'Column delete failed',
            description: err?.response?.data?.detail || err?.message || 'Could not delete column',
            variant: 'destructive',
          });
          onRefresh();
        });

      // Cancel the local removal; the refresh re-renders the grid from the
      // server's updated schema so Data and Schema tabs stay in sync.
      return false;
    },
    [activeSheet, onRefresh, schemaColumns, sessionId, sheet.columns, toast],
  );

  if (!sessionId) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        Start or open a project to populate the workbook.
      </div>
    );
  }

  return (
    <div
      ref={setGridContainerRef}
      className="workspace-grid-surface h-full w-full min-h-0 min-w-0"
      style={{
        '--workspace-table-font': displayOptions.fontFamily === 'Mono'
          ? 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace'
          : displayOptions.fontFamily,
        '--workspace-table-font-size': `${displayOptions.fontSize}px`,
        '--workspace-table-font-weight': displayOptions.bold ? 600 : 400,
        '--workspace-table-font-style': displayOptions.italic ? 'italic' : 'normal',
        '--workspace-table-text-decoration': [
          displayOptions.underline ? 'underline' : '',
          displayOptions.strikethrough ? 'line-through' : '',
        ].filter(Boolean).join(' ') || 'none',
        '--workspace-table-text-align': displayOptions.align,
      } as CSSProperties}
    >
      <HotTable
        ref={hotTableRef}
        key={`${activeSheet}-${sheet.columns.length}-${formatVersion}-${activeSheet === 'data' ? dataView : 'x'}`}
        className="workspace-hot"
        theme="ht-theme-main"
        data={sheet.rows}
        columns={sheet.columns.map((column) => ({
          data: column.key,
          readOnly: column.readOnly,
          width: column.width,
          ...(column.renderer ? { renderer: column.renderer } : {}),
        }))}
        colHeaders={sheet.columns.map(formatSheetColHeader)}
        rowHeaders
        width={gridSize.width || 320}
        height={gridSize.height || 260}
        stretchH="none"
        manualColumnResize
        manualRowResize
        // Measure each row's height across ALL of its cells, not just the ones
        // currently in the horizontal viewport. Handsontable virtualises columns,
        // so without this the row height tracks only the tallest *visible* cell:
        // scrolling right past a long wrapped cell shrinks the row, scrolling a
        // different long cell into view grows it, and the whole grid shifts
        // vertically as you scroll horizontally (the row you started on drifts
        // out of place). AutoRowSize caches a stable per-row height from the
        // full row, so a row keeps its height at any scroll position. See
        // handsontable/handsontable#493, #1213, #5241.
        autoRowSize={true}
        contextMenu
        filters
        dropdownMenu
        columnSorting={!hasMultiRowGroup}
        copyPaste
        undo
        minSpareRows={sheet.minSpareRows || 0}
        licenseKey="non-commercial-and-evaluation"
        afterInit={() => {
          syncHotTableDimensions();
          applyGroupMerges();
          markFilterSettingsInitOnly();
        }}
        afterColumnSort={applyGroupMerges}
        afterFilter={applyGroupMerges}
        beforeRemoveRow={handleBeforeRemoveRow}
        beforeRemoveCol={handleBeforeRemoveCol}
        afterChange={(changes, source) => {
          handleChanges(changes, source);
          if (source !== 'loadData') onEditEnd();
        }}
        afterDeselect={onEditEnd}
        afterSelectionEnd={(row: number, col: number, row2: number, col2: number) => {
          if (row < 0 || col < 0 || row2 < 0 || col2 < 0) {
            onSelectionChange(null);
            onGroundingHighlight?.(null);
            return;
          }
          const fromRow = Math.min(row, row2);
          const fromCol = Math.min(col, col2);
          onSelectionChange({
            sheet: activeSheet,
            fromRow,
            toRow: Math.max(row, row2),
            fromCol,
            toCol: Math.max(col, col2),
          });

          // Report the top-left cell's grounding excerpts so the source panel
          // can highlight every place the value came from (all marked; the
          // first is scrolled into view).
          if (onGroundingHighlight) {
            let excerptTexts: string[] | null = null;
            if (activeSheet === 'data') {
              const column = sheet.columns[fromCol];
              const hot = hotTableRef.current?.hotInstance;
              const physicalRow = hot ? hot.toPhysicalRow(fromRow) : fromRow;
              const grounding =
                column && physicalRow != null && physicalRow >= 0
                  ? dataGrounding[physicalRow]?.[column.key]
                  : null;
              const texts = (grounding?.excerpts ?? [])
                .map((e) => e.text)
                .filter((t): t is string => Boolean(t && t.trim()));
              excerptTexts = texts.length > 0 ? texts : null;
            }
            onGroundingHighlight(excerptTexts);
          }
        }}
        afterOnCellMouseDown={(event, coords) => {
          if (activeSheet !== 'data' || coords.row < 0 || coords.col < 0) return;

          const column = sheet.columns[coords.col];
          if (!column || column.key === '_row_name') return;

          const hot = hotTableRef.current?.hotInstance;
          const physicalRow = hot ? hot.toPhysicalRow(coords.row) : coords.row;
          if (!dataGrounding[physicalRow]?.[column.key]) return;

          // Any click on a grounded cell re-scrolls the source panel to the
          // highlight (mousedown fires only on real clicks, so this never loops).
          onGroundingScrollRequest?.();

          // Open grounding on the top-right indicator only so single-click
          // selection and keyboard editing on grounded cells stay normal.
          const cellElement = (event.target as HTMLElement | null)?.closest('td');
          if (!cellElement) return;
          const rect = cellElement.getBoundingClientRect();
          const indicatorSize = 14;
          const clickX = event.clientX - rect.left;
          const clickY = event.clientY - rect.top;
          const inIndicatorRegion =
            clickX >= rect.width - indicatorSize && clickY <= indicatorSize;
          if (!inIndicatorRegion) return;

          const grounding = dataGrounding[physicalRow][column.key];
          setGroundingModal({
            title: `${columnDisplayLabel(column.key)} — grounding`,
            content: { answer: grounding.answer, excerpts: grounding.excerpts },
          });
        }}
        cells={(row: number, col: number) => {
          const props: { readOnly?: boolean; className?: string } = {};
          const column = sheet.columns[col];
          if (column?.readOnly) props.readOnly = true;
          if (activeSheet === 'unit' && column?.key === 'value') {
            const field = String(sheet.rows[row]?.field || '');
            props.readOnly = !EDITABLE_OBSERVATION_UNIT_FIELDS.has(field);
          }
          const formatClasses = getCellFormatClasses(cellFormats[cellFormatKey(activeSheet, row, col)]);
          if (formatClasses) props.className = formatClasses;
          if (activeSheet === 'data' && column && dataGrounding[row]?.[column.key]) {
            props.className = [props.className, 'has-grounding'].filter(Boolean).join(' ');
          }
          return props;
        }}
      />
      {groundingModal && (
        <ContentModal
          open
          onClose={() => setGroundingModal(null)}
          title={groundingModal.title}
          content={groundingModal.content}
          evidenceOnly
        />
      )}
    </div>
  );
}
