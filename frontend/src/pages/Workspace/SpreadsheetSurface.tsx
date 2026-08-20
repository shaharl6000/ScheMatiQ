// Handsontable grid for Data, Schema, and Observation Unit sheets with grounding UI.
// Parent: Workspace (index.tsx). Owns the sole registerAllModules() call.

import { type CSSProperties, type MutableRefObject, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useParams } from 'react-router-dom';
import { HotTable, type HotTableClass } from '@handsontable/react';
import { registerAllModules } from 'handsontable/registry';
import 'handsontable/styles/handsontable.min.css';
import 'handsontable/styles/ht-theme-main.min.css';
import DOMPurify from 'dompurify';

import { extractDisplayValue } from '@/components/DataTable/utils/valueUtils';
import {
  buildExcerptMapping,
  resolveCellGrounding,
  type CellGrounding,
} from '@/components/DataTable/utils/excerptUtils';
import ContentModal from '@/components/ContentModal/ContentModal';
import { Button } from '@/components/ui/button';
import { useToast } from '@/components/ui/use-toast';
import { observationUnitAPI, schemaAPI, schematiqAPI } from '@/services/api';
import type { ColumnInfo, DataRow, PaginatedData, SchemaData } from '@/types';
import { formatColumnName } from '@/utils/formatting';
import type { EditCommand } from './hooks/useEditHistory';

import {
  EDITABLE_OBSERVATION_UNIT_FIELDS,
  SCHEMA_COLUMN_HEADER_TOOLTIPS,
  cellFormatKey,
  COMPACT_ROW_HEIGHT,
} from './constants';
import {
  documentDisplayName,
  emptyCellScope,
  formatSheetColHeader,
  getCellFormatClasses,
  parseAllowedValues,
  renderObservationUnitFieldCell,
  schemaColumnKeysForCols,
  selectedColumnIndices,
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

// Cell values come from user-uploaded documents and are written through
// innerHTML, so they have to be stripped of anything executable first.
// Handsontable did this for us by calling DOMPurify internally, but v17
// deprecated that: it now warns on every render and will drop the built-in
// sanitisation entirely in v18, silently leaving the content unfiltered.
//
// Declaring the sanitizer keeps the exact same protection under our own
// dependency (dompurify is now a direct entry in package.json rather than
// something we inherit from Handsontable's tree) and survives the upgrade.
// Handsontable also routes pasted content through this, hence the `source`
// argument in its type -- both paths get the same treatment.
//
// Defined at module scope so its identity is stable; a new function on every
// render would make Handsontable re-apply settings continuously.
const sanitizeCellHtml = (content: string): string => DOMPurify.sanitize(content);

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
  onSchemaRefresh,
  onRefreshData,
  onOptimisticCellEdit,
  onEditFollowUp,
  onEditEnd,
  onFillEmptyCells,
  onToggleFormatShortcut,
  onUndo,
  onRedo,
  onRecordEdit,
  onNewProject,
  onImportProject,
  sessionMissing,
  dataMissing,
  compactRows,
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
  onRefresh: () => void | Promise<void>;
  // Schema-only refresh (no data reload) used after a column delete so the
  // grid's data prop keeps its identity and horizontal scroll is preserved.
  onSchemaRefresh: () => void | Promise<void>;
  // Row-data-only refresh after a Data-sheet cell edit (no status/schema churn).
  onRefreshData: () => void;
  // Apply the edited values to React state immediately so the grid does not
  // revert when a background refresh re-renders before the PUT completes. Takes
  // the whole batch so a multi-cell change is one atomic state update.
  onOptimisticCellEdit: (
    edits: {
      identity: { rowName: string; sourceDocument?: string; rowIndex?: number };
      column: string;
      value: string;
    }[],
  ) => void;
  onEditFollowUp: (kind: PendingRerunKind, columns?: string[]) => void;
  onEditEnd: () => void;
  // "Fill empty cells": re-run extraction scoped to just the blank cells
  // covered by the current selection (unit row names + schema column keys),
  // without touching cells that already hold a value. Optional so the menu
  // item can still render (disabled when there's nothing blank to fill)
  // wherever this component is used without wiring the handler.
  onFillEmptyCells?: (scope: { rows: string[]; columns: string[] }) => void;
  // Toggle a text format (bold/italic/underline) on the current selection,
  // invoked by the Ctrl/Cmd+B/I/U keyboard shortcuts registered below.
  onToggleFormatShortcut?: (key: 'bold' | 'italic' | 'underline') => void;
  // Reverse / re-apply the last reversible edit (Ctrl/Cmd+Z, Ctrl/Cmd+Y or
  // Ctrl/Cmd+Shift+Z), wired to the workspace undo/redo stack. Native
  // Handsontable undo is disabled (undo={false}) because it only tracks in-grid
  // changes and its stack is cleared whenever the controlled data prop reloads.
  onUndo?: () => void;
  onRedo?: () => void;
  // Record an undoable command after a cell-value edit is applied.
  onRecordEdit?: (command: EditCommand) => void;
  // Empty-state actions. Closing the New Project dialog previously left the
  // workbook as a dead end whose only way forward was the File menu, which is
  // itself unreachable on narrow viewports.
  onNewProject?: () => void;
  onImportProject?: () => void;
  // Fixed row height instead of measured. AutoRowSize keeps a stable per-row
  // height so rows do not drift while scrolling horizontally, but it sizes each
  // row to its tallest cell across all columns, which on wide schemas left ~5
  // of 194 rows on screen. Uniform heights avoid the drift the same way.
  // Session id is present in the URL but the backend cannot resolve it.
  sessionMissing?: boolean;
  // Session resolves, but its extracted data file does not exist in storage.
  dataMissing?: boolean;
  compactRows?: boolean;
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

  // Keep the column headers lined up with their columns at the right end of the
  // horizontal scroll range.
  //
  // Handsontable draws the headers in a separate overlay table (.ht_clone_top)
  // and mirrors the master table's scroll position onto it with a plain
  // `headerHolder.scrollLeft = master.scrollLeft` assignment
  // (Overlays.syncScrollPositions). The browser clamps that assignment to the
  // overlay's own maximum scrollLeft, and the overlay sizes its viewport itself
  // in TopOverlay.adjustRootElementSize as
  // `min(workspaceWidth - (hasVerticalScroll() ? scrollbarWidth : 0), rootScrollWidth)`.
  // When that vertical-scroll check disagrees with the master's real viewport the
  // overlay viewport ends up one scrollbar wider than the master's, so it runs out
  // of scroll range one scrollbar early: alignment is exact everywhere until the
  // last ~15px of the range, and once scrolled fully right every header sits
  // ~15px to the right of the column it belongs to.
  //
  // Give the overlay the same maximum by extending its hider (the scrollable
  // content) by exactly that overhang, then re-apply the master's scroll
  // position. Both numbers are recomputed from the live DOM on every call, so
  // this is a no-op whenever Handsontable sizes the overlay correctly (overhang
  // 0 means the target width is the width Handsontable itself assigns).
  //
  // The row-header numbers (.ht_clone_inline_start) have the exact vertical
  // twin of this bug. InlineStartOverlay.adjustRootElementSize sizes that
  // overlay's viewport height as
  // `min(workspaceHeight - (hasHorizontalScroll() ? scrollbarWidth : 0), rootScrollHeight)`,
  // so when the horizontal-scroll check disagrees with the master's real
  // viewport the row-header overlay ends up one scrollbar taller and runs out
  // of vertical scroll range one scrollbar early: once scrolled to the bottom,
  // every row number sits ~15px above the row it labels (its baseline no longer
  // matches the adjacent cell content). The same hider-extend + scroll re-apply,
  // on the height axis and driven by afterScrollVertically, realigns it.
  const syncHeaderOverlayScroll = useCallback(() => {
    const root = hotTableRef.current?.hotInstance?.rootElement;
    if (!root) return;

    const masterHolder = root.querySelector<HTMLElement>('.ht_master .wtHolder');
    const masterHider = root.querySelector<HTMLElement>('.ht_master .wtHider');
    if (!masterHolder || !masterHider) return;

    // Top overlay (column headers) -- horizontal axis.
    const headerHolder = root.querySelector<HTMLElement>('.ht_clone_top .wtHolder');
    const headerHider = root.querySelector<HTMLElement>('.ht_clone_top .wtHider');
    if (headerHolder && headerHider) {
      const overhang = Math.max(0, headerHolder.clientWidth - masterHolder.clientWidth);
      const targetHiderWidth = masterHider.offsetWidth + overhang;
      if (Math.abs(headerHider.offsetWidth - targetHiderWidth) >= 1) {
        headerHider.style.width = `${targetHiderWidth}px`;
      }
      if (headerHolder.scrollLeft !== masterHolder.scrollLeft) {
        headerHolder.scrollLeft = masterHolder.scrollLeft;
      }
    }

    // Inline-start overlay (row-header numbers) -- vertical axis.
    const rowHeaderHolder = root.querySelector<HTMLElement>('.ht_clone_inline_start .wtHolder');
    const rowHeaderHider = root.querySelector<HTMLElement>('.ht_clone_inline_start .wtHider');
    if (rowHeaderHolder && rowHeaderHider) {
      const overhang = Math.max(0, rowHeaderHolder.clientHeight - masterHolder.clientHeight);
      const targetHiderHeight = masterHider.offsetHeight + overhang;
      if (Math.abs(rowHeaderHider.offsetHeight - targetHiderHeight) >= 1) {
        rowHeaderHider.style.height = `${targetHiderHeight}px`;
      }
      if (rowHeaderHolder.scrollTop !== masterHolder.scrollTop) {
        rowHeaderHolder.scrollTop = masterHolder.scrollTop;
      }
    }
  }, [hotTableRef]);

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
      // Include every extracted value on the row, not just the current schema
      // columns, so this array keeps a STABLE reference when a column is
      // added/removed — the `columns` prop selects what to show. If this array
      // were rebuilt on a column change, the `data` prop would change identity
      // and Handsontable would reload the data (which resets horizontal scroll).
      const rowData = row.data || {};
      for (const key of Object.keys(rowData)) {
        sheetRow[key] = extractDisplayValue(rowData[key]);
      }
      return sheetRow;
    });
  }, [data.rows]);

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

  // Per-cell "confirmed empty" flags: the model looked and explicitly found
  // nothing (vs. a cell that was never extracted). Both display blank, so this
  // is a separate lookup from dataRows (whose flattened string values lose the
  // marker) -- same physical-row indexing as dataGrounding above.
  const dataConfirmedEmpty = useMemo(() => {
    return data.rows.map((row) => {
      const perColumn: Record<string, boolean> = {};
      const rowData = row.data || {};
      for (const key of Object.keys(rowData)) {
        const value = rowData[key] as { _confirmed_empty?: boolean } | undefined;
        if (value && typeof value === 'object' && value._confirmed_empty) {
          perColumn[key] = true;
        }
      }
      return perColumn;
    });
  }, [data.rows]);

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
  // Latest-value ref so the shortcut callback (registered once at afterInit) is
  // never stale, without re-registering on every render. Mirrors menuActionsRef.
  const formatShortcutRef = useRef(onToggleFormatShortcut);
  formatShortcutRef.current = onToggleFormatShortcut;
  const undoRef = useRef(onUndo);
  undoRef.current = onUndo;
  const redoRef = useRef(onRedo);
  redoRef.current = onRedo;
  const fillEmptyCellsRef = useRef(onFillEmptyCells);
  fillEmptyCellsRef.current = onFillEmptyCells;

  // Register Ctrl/Cmd+B/I/U through Handsontable's built-in ShortcutManager
  // rather than a hand-rolled keydown handler. The shortcuts live in the 'grid'
  // context, so they fire only when the grid is focused and NOT while a cell
  // editor is open (typing "b" into a cell must not bold it). Ctrl+B/I/U are not
  // Handsontable defaults, so this adds no conflict with copy/undo/select-all.
  // Registered per instance; the grid remounts on sheet/view changes, giving
  // each instance its own fresh context (no accumulation).
  //
  // Registration is driven by the effect below rather than by `afterInit`.
  // `afterInit` runs from inside Handsontable's constructor, before
  // @handsontable/react has assigned `hotInstance` onto the ref, so reading the
  // ref there yields undefined and the registration silently no-ops -- which is
  // why Ctrl/Cmd+B/I/U stopped firing while Handsontable's own grid shortcuts
  // (arrows, Ctrl+A) kept working. The other afterInit callees survived this
  // because they are also invoked from later hooks and effects; this one had
  // afterInit as its only caller.
  const registerFormatShortcuts = useCallback((instance?: HotTableClass['hotInstance']) => {
    const hot = instance ?? hotTableRef.current?.hotInstance;
    if (!hot) return;
    try {
      const context = hot.getShortcutManager().getContext('grid');
      if (!context) return;
      const bindings: [string, 'bold' | 'italic' | 'underline'][] = [
        ['b', 'bold'],
        ['i', 'italic'],
        ['u', 'underline'],
      ];
      for (const [letter, format] of bindings) {
        context.addShortcut({
          keys: [['control', letter], ['meta', letter]],
          callback: () => { formatShortcutRef.current?.(format); },
          preventDefault: true,
          stopPropagation: true,
          group: 'schematiq:formatting',
        });
      }
      // Undo/redo drive the workspace edit-history stack (formats + cell-value
      // edits). Live in the 'grid' context, so they do not fire while a cell
      // editor is open, leaving the editor's own text undo intact. Native HT
      // undo/redo is disabled (undo={false}) to avoid a double handler.
      context.addShortcut({
        keys: [['control', 'z'], ['meta', 'z']],
        callback: () => { undoRef.current?.(); },
        preventDefault: true,
        stopPropagation: true,
        group: 'schematiq:formatting',
      });
      context.addShortcut({
        keys: [['control', 'y'], ['meta', 'y'], ['control', 'shift', 'z'], ['meta', 'shift', 'z']],
        callback: () => { redoRef.current?.(); },
        preventDefault: true,
        stopPropagation: true,
        group: 'schematiq:formatting',
      });
    } catch { /* instance may be mid-teardown or context unavailable */ }
  }, [hotTableRef]);

  // Runs after every render, but the identity guard makes it a no-op except on
  // the first render after a new grid instance appears. Deliberately without a
  // dependency array: the grid remounts on `key` changes (sheet/view) and the
  // ref is populated by the wrapper after mount, so there is no dependency that
  // reliably marks "a new instance now exists".
  const shortcutInstanceRef = useRef<HotTableClass['hotInstance'] | null>(null);
  useEffect(() => {
    const hot = hotTableRef.current?.hotInstance;
    if (!hot || shortcutInstanceRef.current === hot) return;
    shortcutInstanceRef.current = hot;
    registerFormatShortcuts(hot);
  });

  const markFilterSettingsInitOnly = useCallback((instance?: HotTableClass['hotInstance']) => {
    const hot = instance ?? hotTableRef.current?.hotInstance;
    if (!hot) return;
    try {
      const settings = hot.getSettings() as { _initOnlySettings?: string[] };
      if (!Array.isArray(settings._initOnlySettings)) settings._initOnlySettings = [];
      // `contextMenu` is included for the same reason: it is now an object
      // config (custom Data-sheet items), and letting the wrapper re-push it on
      // every re-render re-initializes the context-menu plugin. A clear (which
      // re-renders without changing the grid `key`, so the grid is not
      // remounted) then re-inits the menu on each optimistic-edit render, which
      // cascades into a "Maximum update depth exceeded" loop. Marking it
      // init-only configures the menu once at construction; the config is a
      // stable reference and the grid remounts on sheet/column changes anyway.
      for (const key of ['filters', 'dropdownMenu', 'contextMenu']) {
        if (!settings._initOnlySettings.includes(key)) settings._initOnlySettings.push(key);
      }
    } catch { /* instance may be mid-teardown */ }
  }, [hotTableRef]);

  // Drive `markFilterSettingsInitOnly` from an effect rather than `afterInit`.
  // `afterInit` fires from inside Handsontable's constructor, before
  // @handsontable/react has assigned the instance onto the ref, so the
  // afterInit call read `hotTableRef.current?.hotInstance` as undefined and
  // silently no-opped -- leaving `filters`/`dropdownMenu`/`contextMenu` OUT of
  // `_initOnlySettings`. The wrapper then re-pushed `filters` through
  // updateSettings on the next unrelated re-render, re-initializing the filters
  // plugin and wiping the active filter, so a filter set from the dropdown
  // appeared to do nothing. This is the same ref-timing gotcha already handled
  // for `registerFormatShortcuts` above; the identity guard makes it a no-op
  // except on the first render after a new grid instance appears (the grid
  // remounts on sheet/view `key` changes, resetting `_initOnlySettings`).
  const filterInitInstanceRef = useRef<HotTableClass['hotInstance'] | null>(null);
  useEffect(() => {
    const hot = hotTableRef.current?.hotInstance;
    if (!hot || filterInitInstanceRef.current === hot) return;
    filterInitInstanceRef.current = hot;
    markFilterSettingsInitOnly(hot);
  });

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

  const cellsCallback = useCallback((row: number, col: number) => {
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
    if (activeSheet === 'data' && column && dataConfirmedEmpty[row]?.[column.key]) {
      props.className = [props.className, 'cell-confirmed-empty'].filter(Boolean).join(' ');
    }
    return props;
  }, [activeSheet, cellFormats, dataConfirmedEmpty, dataGrounding, sheet]);

  const prevFormatVersionRef = useRef(formatVersion);

  // Re-apply merges whenever the displayed data, grouping column, or grid size
  // changes. gridSize is a dep because Handsontable re-applies width/height via
  // its own settings update on resize, after which the merges must be restored.
  // When cell formats change (formatVersion bump), invalidate Handsontable's
  // cached cell meta and re-render in place so the grid keeps selection, scroll,
  // and undo — instead of remounting via a React key change.
  useEffect(() => {
    applyGroupMerges();

    const formatChanged = prevFormatVersionRef.current !== formatVersion;
    prevFormatVersionRef.current = formatVersion;
    if (!formatChanged) return;

    const hot = hotTableRef.current?.hotInstance;
    if (!hot) return;

    const selection = hot.getSelected();
    try {
      hot.updateSettings({ cells: cellsCallback });
      hot.render();
    } catch { /* instance may be mid-teardown */ }

    if (selection && selection.length > 0) {
      try {
        hot.selectCells(selection);
      } catch { /* selection may be invalid mid-teardown */ }
    }
  }, [applyGroupMerges, cellsCallback, formatVersion, gridSize, hotTableRef]);

  // Recompute row heights in place when the compact-rows toggle flips.
  //
  // Compact mode swaps the row sizing strategy: `autoRowSize` (measure each
  // row from its content) off and a fixed `rowHeights` on, and back again.
  // The React wrapper pushes those two settings via updateSettings, but the
  // AutoRowSize plugin keeps a per-row height cache that it does NOT clear when
  // it is disabled, and Handsontable's row-header overlay keeps drawing from
  // the stale cached heights. The result is the header gutter numbering one
  // entry per *visual line* of a tall row instead of one per logical row, so
  // the row numbers run far past the real row count and no longer line up with
  // their rows (a grouped/merged view makes this most visible). Clearing the
  // cache and forcing a full recompute + render realigns the header overlay
  // with the actual row heights. Skip the initial mount: a freshly mounted
  // table already sizes cleanly, and the key-based remount on sheet/view change
  // covers those transitions.
  const prevCompactRef = useRef(compactRows);
  useEffect(() => {
    if (prevCompactRef.current === compactRows) return;
    prevCompactRef.current = compactRows;

    const hot = hotTableRef.current?.hotInstance;
    if (!hot) return;

    try {
      const autoRowSize = hot.getPlugin('autoRowSize');
      if (autoRowSize?.clearCache) {
        autoRowSize.clearCache();
        autoRowSize.calculateAllRowsHeight?.();
      }
      hot.render();
      applyGroupMerges();
    } catch { /* instance may be mid-teardown */ }
  }, [applyGroupMerges, compactRows, hotTableRef]);

  // Coalesce cell edit/clear toasts. Handsontable can split one visual clear
  // into several afterChange batches (e.g. a 1-cell batch plus the rest), which
  // otherwise produces both a per-cell "Cell cleared" toast and a "N cells
  // cleared" summary. Accumulate across a short window and show a single toast:
  // the detailed one when exactly one cell changed in total, the summary
  // otherwise.
  const editToastRef = useRef<{
    cleared: number;
    updated: number;
    failed: number;
    total: number;
    single: { cleared: boolean; label: string; column: string } | null;
    timer: ReturnType<typeof setTimeout> | null;
  }>({ cleared: 0, updated: 0, failed: 0, total: 0, single: null, timer: null });

  const flushEditToast = useCallback(() => {
    const a = editToastRef.current;
    if (a.timer) { clearTimeout(a.timer); a.timer = null; }
    const { cleared, updated, failed, total, single } = a;
    editToastRef.current = { cleared: 0, updated: 0, failed: 0, total: 0, single: null, timer: null };
    if (total === 0) return;
    if (failed > 0) {
      toast({
        title: 'Some cells could not be saved',
        description: `${failed} of ${total} update${total > 1 ? 's' : ''} failed.`,
        variant: 'destructive',
      });
    } else if (total === 1 && single) {
      toast({
        title: single.cleared ? 'Cell cleared' : 'Cell updated',
        description: `${single.label} / ${single.column}`,
      });
    } else {
      const allCleared = updated === 0;
      toast({ title: allCleared ? `${cleared} cells cleared` : `${cleared + updated} cells updated` });
    }
  }, [toast]);

  const queueEditToast = useCallback(
    (batch: {
      succeeded: number;
      failed: number;
      allCleared: boolean;
      single: { cleared: boolean; label: string; column: string } | null;
    }) => {
      const a = editToastRef.current;
      a.total += batch.succeeded + batch.failed;
      a.failed += batch.failed;
      if (batch.allCleared) a.cleared += batch.succeeded;
      else a.updated += batch.succeeded;
      // Keep the detailed label only while the whole interaction is a single cell.
      if (a.total === 1 && batch.single) a.single = batch.single;
      else a.single = null;
      if (a.timer) clearTimeout(a.timer);
      a.timer = setTimeout(flushEditToast, 250);
    },
    [flushEditToast],
  );

  type CellUpdate = {
    rowName: string;
    sourceDocument?: string;
    rowIndexId?: number;
    column: string;
    value: string;
  };

  // Apply a batch of cell writes through the standard edit pipeline: optimistic
  // React update up front, the persisting PUTs, one summary toast, then a
  // refresh. Shared by direct edits and by undo/redo (which replay the inverse
  // or forward batch), so the paths can never drift.
  const applyCellUpdates = useCallback((updates: CellUpdate[]) => {
    if (!sessionId || updates.length === 0) return;

    onOptimisticCellEdit(
      updates.map((u) => ({
        identity: { rowName: u.rowName, sourceDocument: u.sourceDocument, rowIndex: u.rowIndexId },
        column: u.column,
        value: u.value,
      })),
    );

    const allCleared = updates.every((u) => u.value.trim() === '');

    Promise.allSettled(
      updates.map((u) =>
        schematiqAPI.updateCell(sessionId, u.rowName, u.column, u.value, u.sourceDocument, u.rowIndexId),
      ),
    ).then((results) => {
      const failed = results.filter((r) => r.status === 'rejected').length;
      const succeeded = updates.length - failed;

      let single: { cleared: boolean; label: string; column: string } | null = null;
      if (updates.length === 1 && failed === 0) {
        const u = updates[0];
        const rowLabel = u.rowName || (u.rowIndexId != null ? `Row ${u.rowIndexId + 1}` : 'Row');
        single = { cleared: allCleared, label: rowLabel, column: u.column };
      }
      queueEditToast({ succeeded, failed, allCleared, single });

      onRefreshData();
    });
  }, [onOptimisticCellEdit, onRefreshData, queueEditToast, sessionId]);

  const handleChanges = useCallback((changes: any[] | null, source: string) => {
    if (!changes || source === 'loadData' || !sessionId) return;

    // --- Data sheet: batch every cell edit/clear in this change set into a
    // single request group and a single summary toast. Handsontable delivers a
    // whole column-clear or multi-cell delete as one `afterChange` call with N
    // entries; firing a toast + refresh per entry produced the "Cell updated"
    // spam and the flicker from N racing refreshes.
    //
    // It also reports *visual* row indices, but `data.rows` is in physical
    // order. Grouping applies mergeCells and filters/sorting may be active, so
    // visual != physical; resolving the row without converting targeted the
    // wrong record (or an empty spare row) and made clears fail with
    // "Row ... not found" for some cells. Convert once via the live instance.
    if (activeSheet === 'data') {
      const hot = hotTableRef.current?.hotInstance;
      const toPhysicalRow = (visualRow: number): number =>
        hot && typeof hot.toPhysicalRow === 'function' ? hot.toPhysicalRow(visualRow) : visualRow;

      const updates: CellUpdate[] = [];
      const inverseUpdates: CellUpdate[] = [];
      let unidentified = 0;

      for (const change of changes) {
        const [visualRowIndex, prop, oldValue, newValue] = change;
        if (oldValue === newValue || prop == null) continue;
        const key = String(prop);
        if (key.startsWith('_')) continue;

        const sourceRow: DataRow | undefined = data.rows[toPhysicalRow(visualRowIndex)];
        const rowName = sourceRow?.row_name || sourceRow?._unit_name || '';
        const rowIndexId = sourceRow?._row_index;
        const sourceDocument = sourceRow?._source_document || sourceRow?._parent_document;

        if (!rowName && rowIndexId == null) {
          unidentified += 1;
          continue;
        }

        updates.push({ rowName, sourceDocument, rowIndexId, column: key, value: String(newValue ?? '') });
        inverseUpdates.push({ rowName, sourceDocument, rowIndexId, column: key, value: String(oldValue ?? '') });
      }

      if (updates.length === 0) {
        if (unidentified > 0) {
          toast({
            title: 'Cell update failed',
            description: 'Could not identify which row to update.',
            variant: 'destructive',
          });
          onRefreshData();
        }
        return;
      }

      // Apply the whole batch to React state up front, in one atomic update, so
      // the grid does not revert while the writes are in flight. A single call
      // (rather than one per cell) keeps a multi-cell clear from triggering a
      // render cascade inside Handsontable's synchronous afterChange.
      applyCellUpdates(updates);

      // Record the batch as one history entry: Ctrl/Cmd+Z restores the previous
      // values, Ctrl/Cmd+Y (or Shift+Z) reapplies them, both through the same
      // pipeline. Undo/redo replay via applyCellUpdates (which does not record),
      // so they never stack a command.
      onRecordEdit?.({
        undo: () => applyCellUpdates(inverseUpdates),
        redo: () => applyCellUpdates(updates),
      });

      return;
    }

    for (const change of changes) {
      const [rowIndex, prop, oldValue, newValue] = change;
      if (oldValue === newValue || prop == null) continue;
      const key = String(prop);

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
  }, [activeSheet, applyCellUpdates, data.rows, hotTableRef, observationUnitRows, onEditFollowUp, onRecordEdit, onRefresh, onRefreshData, schemaColumns, sessionId, toast]);

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
          //
          // Schema-only refresh (no data reload), matching the Data-sheet
          // "Delete column" path: deleting a column changes only the column
          // list, so reloading the rows here is wasted work and would reset the
          // Data grid's scroll when the user switches back.
          onSchemaRefresh();
        })
        .catch((err: any) => {
          toast({
            title: 'Column delete failed',
            description: err?.response?.data?.detail || err?.message || 'Could not delete column',
            variant: 'destructive',
          });
          onSchemaRefresh();
        });

      // Cancel Handsontable's local removal; the schema state refresh below
      // re-renders the grid from the server's updated schema, keeping the
      // Schema and Data tabs in sync with a single source of truth.
      return false;
    },
    [activeSheet, onSchemaRefresh, schemaColumns, sessionId, toast],
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
          // invalidate the remaining columns' values, so no re-extract flag and
          // a schema-only refresh is enough (no data reload / scroll reset).
          onSchemaRefresh();
        })
        .catch((err: any) => {
          toast({
            title: 'Column delete failed',
            description: err?.response?.data?.detail || err?.message || 'Could not delete column',
            variant: 'destructive',
          });
          onSchemaRefresh();
        });

      // Cancel the local removal; the refresh re-renders the grid from the
      // server's updated schema so Data and Schema tabs stay in sync.
      return false;
    },
    [activeSheet, onSchemaRefresh, schemaColumns, sessionId, sheet.columns, toast],
  );

  // Resolve the schema-column names covered by the current grid selection,
  // excluding the fixed provenance columns (_row_name / _source_document).
  // Shared by the "Delete column" menu item and the Delete-key shortcut.
  const selectedSchemaColumnNames = useCallback(
    (hot: any): string[] => {
      const selection: number[][] = typeof hot?.getSelected === 'function' ? hot.getSelected() || [] : [];
      const colIndices = selectedColumnIndices(selection);
      return schemaColumnKeysForCols(colIndices, sheet.columns, schemaColumns);
    },
    [schemaColumns, sheet.columns],
  );

  // Resolve the "Fill empty cells" scope for the current selection: the unit
  // row names and schema column keys that are covered AND still blank. Cells
  // that already hold a value are excluded from both sets, so a row/column
  // that is entirely filled in never appears in the resulting scope (and a
  // selection with nothing blank resolves to null, disabling the menu item).
  // Provenance columns (_row_name / _source_document) never count as targets.
  const selectedEmptyCellScope = useCallback(
    (hot: any): { rows: string[]; columns: string[] } | null => {
      if (activeSheet !== 'data') return null;
      const selection: number[][] = typeof hot?.getSelected === 'function' ? hot.getSelected() || [] : [];
      if (selection.length === 0) return null;

      const toPhysicalRow = typeof hot.toPhysicalRow === 'function' ? hot.toPhysicalRow.bind(hot) : undefined;
      return emptyCellScope(selection, dataRows, sheet.columns, schemaColumns, toPhysicalRow);
    },
    [activeSheet, dataRows, schemaColumns, sheet.columns],
  );

  // Delete one or more schema columns (header + values, from both the Schema
  // and Data views) via the schema API, then refresh. Shared by the context
  // menu item and the Delete-key shortcut.
  // Guards against a double-fire of the same delete action. Without it the
  // first call deletes the column, the second fails "column not found" (a
  // spurious "Column delete failed" toast), and the second call's write races
  // the refresh's getData so the grid momentarily blanks. The guard is held
  // across the request AND the follow-up refresh so a delayed second fire
  // during the refresh window is also dropped.
  const deletingColumnsRef = useRef(false);

  const deleteSchemaColumns = useCallback(
    (names: string[]) => {
      if (names.length === 0 || !sessionId || deletingColumnsRef.current) return;
      deletingColumnsRef.current = true;
      Promise.allSettled(names.map((name) => schemaAPI.deleteColumn(sessionId, name)))
        .then((results) => {
          const failed = results.filter((r) => r.status === 'rejected').length;
          if (failed > 0) {
            toast({
              title: 'Column delete failed',
              description: `${failed} of ${names.length} column${names.length > 1 ? 's' : ''} could not be deleted.`,
              variant: 'destructive',
            });
          } else {
            toast({
              title: names.length > 1 ? 'Columns deleted' : 'Column deleted',
              description: names.join(', '),
            });
          }
          // Deleting a column does not change the row data we display, so do a
          // schema-only refresh (no data reload). Combined with the
          // column-independent dataRows above, the grid's `data` prop keeps its
          // identity, so only the `columns` prop changes and Handsontable drops
          // the column in place without resetting horizontal scroll.
          return onSchemaRefresh();
        })
        .finally(() => {
          deletingColumnsRef.current = false;
        });
    },
    [onSchemaRefresh, sessionId, toast],
  );

  // Excel-style: when one or more whole columns are selected (by clicking the
  // column header) and the user presses Delete/Backspace, remove the column(s)
  // entirely rather than clearing cell contents. A partial (cell-level)
  // selection keeps the default behaviour, which clears contents and routes
  // through handleChanges.
  const handleBeforeKeyDown = useCallback(
    (event: KeyboardEvent) => {
      if (activeSheet !== 'data') return;
      if (event.key !== 'Delete' && event.key !== 'Backspace') return;
      const hot = hotTableRef.current?.hotInstance as any;
      if (!hot?.selection?.isSelectedByColumnHeader?.()) return;
      const names = selectedSchemaColumnNames(hot);
      if (names.length === 0) return;
      event.stopImmediatePropagation();
      event.preventDefault();
      deleteSchemaColumns(names);
    },
    [activeSheet, deleteSchemaColumns, hotTableRef, selectedSchemaColumnNames],
  );

  // Latest-value ref so the menu configs below can be built once (stable
  // references) while still acting on current data. Rebuilding the dropdown
  // config whenever `sheet`/`schemaColumns` re-memo (every data refresh) would
  // change its reference and make the @handsontable/react wrapper re-push — and
  // thus re-initialize — the filters plugin, wiping the active filter that
  // `markFilterSettingsInitOnly` works to preserve.
  const menuActionsRef = useRef<{
    selectedNames: (hot: any) => string[];
    deleteColumns: (names: string[]) => void;
    emptyScope: (hot: any) => { rows: string[]; columns: string[] } | null;
    fillEmptyCells: (scope: { rows: string[]; columns: string[] }) => void;
  }>({ selectedNames: () => [], deleteColumns: () => {}, emptyScope: () => null, fillEmptyCells: () => {} });
  menuActionsRef.current = {
    selectedNames: selectedSchemaColumnNames,
    deleteColumns: deleteSchemaColumns,
    emptyScope: selectedEmptyCellScope,
    fillEmptyCells: (scope: { rows: string[]; columns: string[] }) => fillEmptyCellsRef.current?.(scope),
  };

  // Custom Data-sheet menu items shared by the right-click context menu and the
  // column-header dropdown. Handsontable disables its native "Remove column"
  // whenever the grid uses an object data source *or* a `columns` option (both
  // true here), so column deletion was unreachable from either menu. These add a
  // working "Delete column" (removes the schema column, header + values, via the
  // schema API) and a "Clear cell(s)" item that is not header-gated like the
  // native "Clear column". Built with a stable identity (see menuActionsRef).
  const customColumnMenuItems = useMemo(
    () => ({
      delete_schema_column: {
        name(this: any): string {
          return menuActionsRef.current.selectedNames(this).length > 1 ? 'Delete columns' : 'Delete column';
        },
        disabled(this: any): boolean {
          return menuActionsRef.current.selectedNames(this).length === 0;
        },
        callback(this: any): void {
          menuActionsRef.current.deleteColumns(menuActionsRef.current.selectedNames(this));
        },
      },
      clear_selected_cells: {
        name: 'Clear cell(s)',
        disabled(this: any): boolean {
          const selection = typeof this?.getSelected === 'function' ? this.getSelected() : null;
          return !selection || selection.length === 0;
        },
        callback(this: any): void {
          // Routes through afterChange -> handleChanges, which persists the
          // empties and shows a single summary toast.
          if (typeof this?.emptySelectedCells === 'function') this.emptySelectedCells('edit');
        },
      },
      fill_empty_cells: {
        name: 'Fill empty cells',
        disabled(this: any): boolean {
          return menuActionsRef.current.emptyScope(this) === null;
        },
        callback(this: any): void {
          const scope = menuActionsRef.current.emptyScope(this);
          if (scope) menuActionsRef.current.fillEmptyCells(scope);
        },
      },
    }),
    [],
  );

  // Right-click context menu (Data sheet only; other sheets keep the default).
  const contextMenuConfig = useMemo(() => {
    if (activeSheet !== 'data') return true;
    return {
      items: {
        ...customColumnMenuItems,
        sep_1: { name: '---------' },
        copy: {},
        cut: {},
      },
    };
  }, [activeSheet, customColumnMenuItems]);

  // Column-header dropdown (the ▼ button). This is a *separate* menu from the
  // context menu; the default one is what showed the greyed-out "Remove column",
  // so the header dropdown also needs the custom items. The predefined
  // `filter_*` keys are re-included so the "Filter by condition/value" UI is
  // preserved.
  const dropdownMenuConfig = useMemo(() => {
    if (activeSheet !== 'data') return true;
    return {
      items: {
        ...customColumnMenuItems,
        sep_1: { name: '---------' },
        filter_by_condition: {},
        filter_operators: {},
        filter_by_condition2: {},
        filter_by_value: {},
        filter_action_bar: {},
      },
    };
  }, [activeSheet, customColumnMenuItems]);

  if (!sessionId || sessionMissing || dataMissing) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-4 p-6 text-center">
        <p className="text-sm text-muted-foreground">
          {sessionMissing
            ? 'This project could not be found. It may have been removed, or the link may be out of date.'
            : dataMissing
              ? 'This project exists but its extracted data is no longer in storage. The schema is intact, so re-running extraction will rebuild the table.'
              : 'Start or open a project to populate the workbook.'}
        </p>
        {(onNewProject || onImportProject) && (
          <div className="flex flex-wrap items-center justify-center gap-2">
            {onNewProject && (
              <Button size="sm" onClick={onNewProject}>
                New project
              </Button>
            )}
            {onImportProject && (
              <Button size="sm" variant="outline" onClick={onImportProject}>
                Import project
              </Button>
            )}
          </div>
        )}
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
        key={`${activeSheet}-${activeSheet === 'data' ? dataView : 'x'}`}
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
        autoRowSize={!compactRows}
        {...(compactRows ? { rowHeights: COMPACT_ROW_HEIGHT } : {})}
        contextMenu={contextMenuConfig}
        filters
        dropdownMenu={dropdownMenuConfig}
        columnSorting={!hasMultiRowGroup}
        copyPaste
        sanitizer={sanitizeCellHtml}
        // Required for the highlight, not for the query. getPlugin('search')
        // .query() sets `isSearchResult` on cell meta regardless, but the hook
        // that turns that into the htSearchResult class is guarded by the
        // plugin's isEnabled(), which reads this setting -- so without it, Find
        // jumped to the match and reported the count while highlighting nothing.
        search
        undo={false}
        minSpareRows={sheet.minSpareRows || 0}
        licenseKey="non-commercial-and-evaluation"
        afterInit={() => {
          syncHotTableDimensions();
          applyGroupMerges();
          // markFilterSettingsInitOnly is driven by a post-mount effect instead:
          // at afterInit the wrapper has not yet assigned the instance onto the
          // ref, so calling it here would read undefined and silently no-op.
          syncHeaderOverlayScroll();
        }}
        // Runs after Handsontable has drawn and (mis)clamped the header overlay's
        // scroll position, so the compensation above lands on final values.
        afterScrollHorizontally={syncHeaderOverlayScroll}
        afterScrollVertically={syncHeaderOverlayScroll}
        afterViewRender={syncHeaderOverlayScroll}
        afterColumnSort={applyGroupMerges}
        afterFilter={applyGroupMerges}
        beforeRemoveRow={handleBeforeRemoveRow}
        beforeRemoveCol={handleBeforeRemoveCol}
        beforeKeyDown={handleBeforeKeyDown}
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
        cells={cellsCallback}
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
