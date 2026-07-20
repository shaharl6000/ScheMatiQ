import { type ChangeEvent, Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { type HotTableClass } from '@handsontable/react';

import {
  Bot,
  Check,
  ChevronDown,
  FileUp,
  Loader2,
  MoreVertical,
  PanelLeft,
  Plus,
  Sparkles,
  Table2,
  X,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Progress } from '@/components/ui/progress';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { useToast } from '@/components/ui/use-toast';
import StatsDashboard from '@/components/StatsDashboard/StatsDashboard';
import ScheMatiQMonitor from '@/components/ScheMatiQMonitor/ScheMatiQMonitor';
import DocumentViewer from '@/components/DocumentViewer/DocumentViewer';
import DocumentPreview from '@/components/DocumentViewer/DocumentPreview';
import DocumentUpload from '@/components/DocumentUpload/DocumentUpload';
import { ViewModeToggle } from '@/components/ViewMode/ViewModeToggle';
import MissingDocumentsSection from '@/components/SchemaEditor/MissingDocumentsSection';
import TableFeedbackWidget from '@/components/TableFeedbackWidget/TableFeedbackWidget';
import {
  WS_DISCONNECTED_REFRESH_INTERVAL,
  getAvailableProviders,
  getDefaultModelForProvider,
  type LLMProviderKey,
} from '@/constants';
import api, { configAPI, loadAPI, schemaAPI, schematiqAPI, unitsAPI } from '@/services/api';
import webSocketService from '@/services/websocket';
import type {
  CostEstimate,
  DataRow,
  DocumentAvailabilityResponse,
  PaginatedData,
  ReextractionCompletedData,
  ReextractionFailedData,
  ReextractionProgressData,
  ReextractionRequest,
  ReextractionStartedData,
  ScheMatiQConfig,
  ScheMatiQStatus,
  SchemaData,
  VisualizationSession,
  WebSocketMessage,
} from '@/types';
import type { DocumentListResponse } from '@/types/unit';
import { getApiKeyForProvider, getConfiguredProviders } from '@/utils/apiKeyStorage';

import { ChatPanel } from './chat/ChatPanel';
import { emptyData, SHEETS, cellFormatKey } from './constants';
import {
  buildExportFilename,
  dataEquals,
  documentDisplayName,
  schemaFromLoadSession,
  selectionArea,
  selectionsEqual,
  statusFromLoadSession,
} from './helpers';
import { NewProjectDialog } from './NewProjectDialog';
import { PendingRerunBanner } from './PendingRerunBanner';
import { ProjectDetailsDialog } from './ProjectDetailsDialog';
import { SpreadsheetChrome } from './SpreadsheetChrome';
import { useAddDocuments } from './hooks/useAddDocuments';
import { SpreadsheetSurface } from './SpreadsheetSurface';
import type {
  CellFormatMap,
  PendingRerunKind,
  SheetId,
  SheetSelection,
  TableDisplayOptions,
  WorkspaceReextractionState,
  WorkspaceSessionMode,
} from './types';

import './Workspace.css';

async function downloadAs(path: string, filename: string): Promise<void> {
  const response = await api.get(path, { responseType: 'blob' });
  const blob = new Blob([response.data]);
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.click();
  window.URL.revokeObjectURL(url);
}

function Workspace() {
  const { sessionId } = useParams();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const requestedMode: WorkspaceSessionMode = searchParams.get('mode') === 'load' ? 'load' : 'schematiq';
  const { toast } = useToast();
  const importInputRef = useRef<HTMLInputElement | null>(null);
  const hotTableRef = useRef<HotTableClass | null>(null);
  const [activeSheet, setActiveSheet] = useState<SheetId>('data');
  const [sessionMode, setSessionMode] = useState<WorkspaceSessionMode>(requestedMode);
  const [projectDialogOpen, setProjectDialogOpen] = useState(!sessionId);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [pendingRerunKind, setPendingRerunKind] = useState<PendingRerunKind | null>(null);
  const [pendingSchemaColumns, setPendingSchemaColumns] = useState<string[]>([]);
  const [rerunStarting, setRerunStarting] = useState(false);
  const [status, setStatus] = useState<ScheMatiQStatus | null>(null);
  const [session, setSession] = useState<VisualizationSession | null>(null);
  const [schema, setSchema] = useState<SchemaData | null>(null);
  const [data, setData] = useState<PaginatedData>(emptyData);
  const [dataView, setDataView] = useState<'by_document' | 'by_unit'>('by_document');
  const [unitData, setUnitData] = useState<PaginatedData>(emptyData);
  const [documents, setDocuments] = useState<DocumentListResponse | null>(null);
  const [config, setConfig] = useState<ScheMatiQConfig | null>(null);
  const [developerMode, setDeveloperMode] = useState(false);
  const [costEstimate, setCostEstimate] = useState<CostEstimate | null>(null);
  const [loading, setLoading] = useState(false);
  // Separate from `loading`: structure (status/schema) is ready and the grid
  // chrome is shown, but the heavy row payload is still streaming in. Lets the
  // full-screen overlay clear early so the user sees the project has opened.
  const [dataLoading, setDataLoading] = useState(false);
  const [importingProject, setImportingProject] = useState(false);
  const [tableDisplay, setTableDisplay] = useState<TableDisplayOptions>(() => {
    try {
      const saved = localStorage.getItem('workspace.tableDisplay');
      if (saved) return JSON.parse(saved);
    } catch {
      // Ignore malformed local display preferences.
    }
    return {
      fontFamily: 'Inter',
      fontSize: 12,
      bold: false,
      italic: false,
      underline: false,
      strikethrough: false,
      align: 'left',
    };
  });
  const [cellFormats, setCellFormats] = useState<CellFormatMap>(() => {
    try {
      const saved = localStorage.getItem('workspace.cellFormats');
      if (saved) return JSON.parse(saved);
    } catch {
      // Ignore malformed local cell formatting preferences.
    }
    return {};
  });
  const [formatVersion, setFormatVersion] = useState(0);
  const [sheetSelection, setSheetSelection] = useState<SheetSelection>(null);
  const [showSourcePanel, setShowSourcePanel] = useState(false);
  // Grounding excerpts of the currently selected data cell, highlighted in the
  // source panel so the user can see every place the value came from.
  const [groundingHighlights, setGroundingHighlights] = useState<string[] | null>(null);
  // Bumped on each grounded-cell click so the source panel re-scrolls to the
  // highlight even when the excerpt set is unchanged (same cell clicked again).
  const [groundingScrollNonce, setGroundingScrollNonce] = useState(0);
  // View-only re-attach of source files for the data-sheet source panel. Bumping
  // the token re-probes the preview so a freshly uploaded file resolves.
  const [sourceDocReloadToken, setSourceDocReloadToken] = useState(0);
  const [attachingSourceDocs, setAttachingSourceDocs] = useState(false);
  const sourceDocInputRef = useRef<HTMLInputElement | null>(null);
  const [chatWidth, setChatWidth] = useState(() => {
    const saved = Number(localStorage.getItem('workspace.chatWidth'));
    return Number.isFinite(saved) ? saved : 380;
  });
  const [isDraggingDivider, setIsDraggingDivider] = useState(false);
  const [reextraction, setReextraction] = useState<WorkspaceReextractionState | null>(null);
  const [wsConnected, setWsConnected] = useState(false);
  const [reextractConfirm, setReextractConfirm] = useState<{ columns: string[] } | null>(null);
  const [reextractAvailability, setReextractAvailability] = useState<DocumentAvailabilityResponse | null>(null);
  const [reextractAvailabilityLoading, setReextractAvailabilityLoading] = useState(false);
  const [stoppingReextraction, setStoppingReextraction] = useState(false);

  const deferredDataRef = useRef<PaginatedData | null>(null);
  const cancelChatPendingRef = useRef<(() => Promise<boolean>) | null>(null);

  const cancelChatPendingIfAny = useCallback(async (): Promise<boolean> => {
    if (!cancelChatPendingRef.current) return true;
    return cancelChatPendingRef.current();
  }, []);

  const isCellEditorOpen = useCallback(() => {
    const editor = hotTableRef.current?.hotInstance?.getActiveEditor?.();
    return Boolean(editor?.isOpened?.());
  }, []);

  const applyData = useCallback(
    (nextData: PaginatedData, opts?: { silent?: boolean }) => {
      if (opts?.silent && isCellEditorOpen()) {
        deferredDataRef.current = nextData;
        return;
      }
      deferredDataRef.current = null;
      setData((current) => (dataEquals(current, nextData) ? current : nextData));
    },
    [isCellEditorOpen],
  );

  const flushDeferredData = useCallback(() => {
    const pending = deferredDataRef.current;
    if (!pending) return;
    deferredDataRef.current = null;
    setData((current) => (dataEquals(current, pending) ? current : pending));
  }, []);

  const refresh = useCallback(async (options?: { silent?: boolean }) => {
    if (!sessionId) return;
    const silent = Boolean(options?.silent);
    if (!silent) {
      setLoading(true);
    }
    // Fetch the lightweight structure (status/schema/config/session) first and
    // render it before the heavy row payload, so the full-screen overlay clears
    // as soon as the project's shell is on screen. The row/document payload is
    // fetched in a second phase tracked by `dataLoading`, which drives a subtle
    // inline indicator instead of blocking the whole view. `silent` refreshes
    // (WebSocket-driven background polls) keep the original single-phase load so
    // their behaviour is unchanged.
    const loadHeavy = async (
      fetchData: () => Promise<PaginatedData>,
      fetchDocuments: () => Promise<DocumentListResponse | null>,
    ) => {
      if (!silent) setDataLoading(true);
      try {
        const [nextData, nextDocuments] = await Promise.all([
          fetchData().catch(() => emptyData),
          fetchDocuments().catch(() => null),
        ]);
        applyData(nextData, options);
        setDocuments(nextDocuments);
      } finally {
        if (!silent) setDataLoading(false);
      }
    };

    try {
      if (sessionMode === 'load') {
        const loadSession = await loadAPI.getSession(sessionId).catch(() => null);
        setStatus(statusFromLoadSession(loadSession));
        setSchema(schemaFromLoadSession(loadSession));
        setSession(loadSession);
        setConfig(null);
        if (!silent) setLoading(false);
        await loadHeavy(
          () => loadAPI.getData(sessionId, 0, 500),
          () => unitsAPI.getDocuments(sessionId),
        );
        return;
      }

      try {
        const [nextStatus, nextSchema, nextConfig, statsSession] = await Promise.all([
          schematiqAPI.getStatus(sessionId),
          schematiqAPI.getSchema(sessionId).catch(() => null),
          schematiqAPI.getConfig(sessionId).catch(() => null),
          loadAPI.getSession(sessionId).catch(() => null),
        ]);
        setStatus(nextStatus);
        setSchema(nextSchema);
        setSession(statsSession);
        setConfig(nextConfig);
        if (!silent) setLoading(false);
        await loadHeavy(
          () => schematiqAPI.getData(sessionId, 0, 500),
          () => unitsAPI.getDocuments(sessionId),
        );
      } catch (err) {
        const loadSession = await loadAPI.getSession(sessionId).catch(() => null);
        if (!loadSession) throw err;
        setSessionMode('load');
        setStatus(statusFromLoadSession(loadSession));
        setSchema(schemaFromLoadSession(loadSession));
        setSession(loadSession);
        setConfig(null);
        if (!silent) setLoading(false);
        await loadHeavy(
          () => loadAPI.getData(sessionId, 0, 500),
          () => unitsAPI.getDocuments(sessionId),
        );
      }
    } finally {
      if (!silent) {
        setLoading(false);
      }
    }
  }, [applyData, sessionId, sessionMode]);

  const inFlightSilentRef = useRef<Promise<void> | null>(null);
  const refreshSilent = useCallback(() => {
    if (inFlightSilentRef.current) return inFlightSilentRef.current;
    const run = refresh({ silent: true }).finally(() => {
      inFlightSilentRef.current = null;
    });
    inFlightSilentRef.current = run;
    return run;
  }, [refresh]);

  const {
    addDocsFiles,
    addDocsUploading,
    addDocsProcessing,
    addDocsResult,
    addDocsError,
    addDocsNotice,
    addDocsPending,
    handleAddDocsFilesChange,
    uploadAddDocuments,
    processAddDocuments,
  } = useAddDocuments({
    sessionId,
    documents,
    session,
    refresh,
    setActiveSheet,
    toast,
  });

  useEffect(() => {
    setProjectDialogOpen(!sessionId);
    setSessionMode(requestedMode);
  }, [requestedMode, sessionId]);

  // Mark the body while the Workspace is mounted so global toasts can be lifted
  // above the fixed bottombar (see Workspace.css). Scoped to this route only so
  // toast positioning elsewhere in the app is unaffected.
  useEffect(() => {
    document.body.setAttribute('data-workspace-active', 'true');
    return () => {
      document.body.removeAttribute('data-workspace-active');
    };
  }, []);

  // Fetch app-level developer mode so the table feedback widget can be hidden
  // for developers (parity with the classic flow).
  useEffect(() => {
    configAPI.getConfig()
      .then((cfg) => setDeveloperMode(Boolean(cfg?.developer_mode)))
      .catch(() => {});
  }, []);

  useEffect(() => {
    setPendingRerunKind(null);
    setPendingSchemaColumns([]);
    setRerunStarting(false);
    setReextraction(null);
    setWsConnected(false);
    // Reset session-scoped view/data so a freshly imported/loaded session
    // never renders the previous session's sheet, schema, or rows.
    setActiveSheet('data');
    setData(emptyData);
    setSchema(null);
    setStatus(null);
    setSession(null);
    setDataView('by_document');
    setUnitData(emptyData);
  }, [sessionId]);

  // Lazily fetch the observation-unit-grouped data when the Data sheet is in
  // "By Unit" mode. Same schema columns as the by-document view; only the row
  // grouping differs (one row per observation unit instead of per document).
  useEffect(() => {
    if (activeSheet !== 'data' || dataView !== 'by_unit' || !sessionId) return;
    let cancelled = false;
    unitsAPI
      .getData(sessionId, { page: 0, pageSize: 500 })
      .then((res) => { if (!cancelled) setUnitData(res); })
      .catch(() => { if (!cancelled) setUnitData(emptyData); });
    return () => {
      cancelled = true;
    };
  }, [activeSheet, dataView, sessionId, data]);

  // Order the By Unit rows so their unit groups appear in the same sequence as
  // the By Document view (the units' first appearance in the by-document data),
  // instead of the backend's alphabetical order. This keeps the two views
  // aligned — identical when each document maps to a single unit — while still
  // grouping each unit's rows together. Ordering only; rows are unchanged.
  const alignedUnitData = useMemo<PaginatedData>(() => {
    const rows = unitData.rows || [];
    if (rows.length === 0) return unitData;
    const unitKey = (row: DataRow) => String(row.row_name || row._unit_name || '');
    const firstSeen = new Map<string, number>();
    (data.rows || []).forEach((row, i) => {
      const k = unitKey(row);
      if (!firstSeen.has(k)) firstSeen.set(k, i);
    });
    const groups = new Map<string, DataRow[]>();
    rows.forEach((row) => {
      const k = unitKey(row);
      const g = groups.get(k);
      if (g) g.push(row);
      else groups.set(k, [row]);
    });
    const orderedKeys = Array.from(groups.keys()).sort((a, b) => {
      const ia = firstSeen.has(a) ? (firstSeen.get(a) as number) : Number.MAX_SAFE_INTEGER;
      const ib = firstSeen.has(b) ? (firstSeen.get(b) as number) : Number.MAX_SAFE_INTEGER;
      return ia !== ib ? ia - ib : a.localeCompare(b);
    });
    const orderedRows = orderedKeys.flatMap((k) => groups.get(k) as DataRow[]);
    return { ...unitData, rows: orderedRows };
  }, [unitData, data]);

  // By Document counterpart of alignedUnitData: order the by-document rows so
  // every document's rows are contiguous (preserving each document's first
  // appearance), then stable-sort within a document by unit name. This mirrors
  // the classic By Document view and lets the grid visually merge the Source
  // Document column. Ordering only; rows are unchanged.
  const alignedDocData = useMemo<PaginatedData>(() => {
    const rows = data.rows || [];
    if (rows.length === 0) return data;
    const docKey = (row: DataRow) =>
      documentDisplayName(
        row._source_document ||
          row._parent_document ||
          (Array.isArray(row.papers) ? row.papers[0] : ''),
      ).toLowerCase();
    const unitKey = (row: DataRow) => String(row.row_name || row._unit_name || '').toLowerCase();
    const firstSeen = new Map<string, number>();
    rows.forEach((row, i) => {
      const k = docKey(row);
      if (!firstSeen.has(k)) firstSeen.set(k, i);
    });
    const groups = new Map<string, DataRow[]>();
    rows.forEach((row) => {
      const k = docKey(row);
      const g = groups.get(k);
      if (g) g.push(row);
      else groups.set(k, [row]);
    });
    const orderedKeys = Array.from(groups.keys()).sort((a, b) => {
      const ia = firstSeen.has(a) ? (firstSeen.get(a) as number) : Number.MAX_SAFE_INTEGER;
      const ib = firstSeen.has(b) ? (firstSeen.get(b) as number) : Number.MAX_SAFE_INTEGER;
      return ia !== ib ? ia - ib : a.localeCompare(b);
    });
    const orderedRows = orderedKeys.flatMap((k) =>
      [...(groups.get(k) as DataRow[])].sort((x, y) => unitKey(x).localeCompare(unitKey(y))),
    );
    return { ...data, rows: orderedRows };
  }, [data]);

  useEffect(() => {
    if (activeSheet === 'monitor' && sessionMode !== 'schematiq') {
      setActiveSheet('data');
    }
  }, [activeSheet, sessionMode]);

  useEffect(() => {
    setSheetSelection(null);
  }, [activeSheet, sessionId]);

  useEffect(() => {
    refresh();
  }, [refresh, sessionId]);

  useEffect(() => {
    if (!sessionId || wsConnected) return undefined;
    const interval = window.setInterval(() => refreshSilent(), WS_DISCONNECTED_REFRESH_INTERVAL);
    return () => window.clearInterval(interval);
  }, [refreshSilent, sessionId, wsConnected]);

  useEffect(() => {
    if (!sessionId) return undefined;

    const handler = (message: WebSocketMessage) => {
      if (message.type === 'connected') {
        setWsConnected(true);
        void refreshSilent();
        return;
      }

      if (message.type === 'disconnected' || message.type === 'reconnecting') {
        setWsConnected(false);
        return;
      }

      if (message.type === 'heartbeat' || message.type === 'pong') {
        return;
      }

      if (message.type === 'reextraction_started' && message.data) {
        const payload = message.data as ReextractionStartedData;
        setReextraction({
          operationId: payload.operation_id,
          columns: payload.columns || [],
          progress: 0,
          processedDocuments: 0,
          totalDocuments: payload.total_documents || 0,
        });
        setActiveSheet('data');
        void refresh({ silent: true });
        return;
      }

      if (message.type === 'reextraction_progress' && message.data) {
        const payload = message.data as ReextractionProgressData;
        setReextraction((current) => ({
          operationId: payload.operation_id,
          columns: current?.columns || (payload.column ? [payload.column] : []),
          progress: payload.progress ?? current?.progress ?? 0,
          processedDocuments: payload.processed_documents ?? current?.processedDocuments ?? 0,
          totalDocuments: payload.total_documents ?? current?.totalDocuments ?? 0,
          currentColumn: payload.column || current?.currentColumn,
        }));
        void refresh({ silent: true });
        return;
      }

      if (message.type === 'reextraction_completed' && message.data) {
        const payload = message.data as ReextractionCompletedData;
        setReextraction(null);
        void refresh({ silent: true });
        toast({
          title: 'Re-extraction completed',
          description: payload.columns?.length
            ? `Updated ${payload.columns.length} column(s) from source documents.`
            : 'Table values were refreshed from source documents.',
        });
        return;
      }

      if (message.type === 'reextraction_failed' && message.data) {
        const payload = message.data as ReextractionFailedData;
        setReextraction(null);
        toast({
          title: 'Re-extraction failed',
          description: payload.error || 'Could not re-extract values from source documents.',
          variant: 'destructive',
        });
        return;
      }

      if (message.type === 'reextraction_stopped') {
        setReextraction(null);
        void refresh({ silent: true });
        return;
      }

      if (
        message.type === 'progress' ||
        message.type === 'completed' ||
        message.type === 'schema_completed' ||
        message.type === 'schema_updated' ||
        message.type === 'cell_extracted' ||
        message.type === 'row_completed' ||
        message.type === 'schema_progress' ||
        message.type === 'reprocessing_progress' ||
        message.type === 'reprocessing_completed' ||
        message.type === 'observation_unit_definition_updated'
      ) {
        void refresh({ silent: true });
      }
    };

    webSocketService.addMessageHandler(handler);
    webSocketService.connect(sessionId, 'progress');

    return () => {
      webSocketService.removeMessageHandler(handler);
      webSocketService.disconnect();
      setWsConnected(false);
    };
  }, [refresh, refreshSilent, sessionId, toast]);

  const estimateCurrentCost = useCallback(async () => {
    if (!sessionId) return;
    if (sessionMode === 'load') {
      toast({
        title: 'No pending extraction cost',
        description: 'This imported project is already loaded. Cost estimates apply to ScheMatiQ runs.',
      });
      return;
    }
    const estimate = await schematiqAPI.estimateCost(sessionId);
    setCostEstimate(estimate);
  }, [sessionId, sessionMode, toast]);

  const updateTableDisplay = useCallback((next: TableDisplayOptions) => {
    setTableDisplay(next);
    localStorage.setItem('workspace.tableDisplay', JSON.stringify(next));
  }, []);

  const updateSheetSelection = useCallback((nextSelection: SheetSelection) => {
    setSheetSelection((current) => (
      selectionsEqual(current, nextSelection) ? current : nextSelection
    ));
  }, []);

  // Dedupe like updateSheetSelection: HotTable re-emits afterSelectionEnd on
  // every re-render, so returning the previous value for an unchanged excerpt
  // set is required to avoid an infinite render loop.
  const handleGroundingHighlight = useCallback((texts: string[] | null) => {
    setGroundingHighlights((current) => {
      if (current === texts) return current;
      if (!current || !texts) return texts;
      if (current.length !== texts.length) return texts;
      for (let i = 0; i < current.length; i += 1) {
        if (current[i] !== texts[i]) return texts;
      }
      return current;
    });
  }, []);

  const applyTableFormat = useCallback((patch: Partial<TableDisplayOptions>) => {
    const hotSelection = hotTableRef.current?.hotInstance?.getSelectedLast?.();
    const liveSelection: SheetSelection = hotSelection
      ? {
        sheet: activeSheet,
        fromRow: Math.min(hotSelection[0], hotSelection[2]),
        toRow: Math.max(hotSelection[0], hotSelection[2]),
        fromCol: Math.min(hotSelection[1], hotSelection[3]),
        toCol: Math.max(hotSelection[1], hotSelection[3]),
      }
      : null;
    const activeSelection = selectionArea(sheetSelection) > selectionArea(liveSelection)
      ? sheetSelection
      : liveSelection;

    if (!activeSelection || activeSelection.sheet !== activeSheet) {
      const nextDisplay = { ...tableDisplay, ...patch };
      updateTableDisplay(nextDisplay);
      return;
    }

    setCellFormats((current) => {
      const next = { ...current };
      for (let row = activeSelection.fromRow; row <= activeSelection.toRow; row += 1) {
        for (let col = activeSelection.fromCol; col <= activeSelection.toCol; col += 1) {
          const key = cellFormatKey(activeSheet, row, col);
          next[key] = { ...next[key], ...patch };
        }
      }
      localStorage.setItem('workspace.cellFormats', JSON.stringify(next));
      return next;
    });
    setFormatVersion((current) => current + 1);
  }, [activeSheet, sheetSelection, tableDisplay, updateTableDisplay]);

  const selectedDisplayOptions = useMemo(() => {
    if (!sheetSelection || sheetSelection.sheet !== activeSheet) return tableDisplay;
    const selectedFormat = cellFormats[cellFormatKey(activeSheet, sheetSelection.fromRow, sheetSelection.fromCol)];
    return { ...tableDisplay, ...selectedFormat };
  }, [activeSheet, cellFormats, sheetSelection, tableDisplay]);

  const printWorkspace = useCallback(() => {
    window.print();
  }, []);

  const exportCurrentProject = useCallback(async () => {
    if (!sessionId) return;
    const question = schema?.query || config?.query || '';
    const filename = buildExportFilename(question, 'csv', sessionId);
    try {
      const tzOffset = new Date().getTimezoneOffset();
      const path = sessionMode === 'schematiq'
        ? `/schematiq/export/${sessionId}?tz_offset=${tzOffset}`
        : `/load/export/${sessionId}?tz_offset=${tzOffset}`;
      await downloadAs(path, filename);
    } catch (err: any) {
      toast({
        title: 'Export failed',
        description: err?.response?.data?.detail || err?.message || 'Could not export this project',
        variant: 'destructive',
      });
    }
  }, [sessionId, sessionMode, schema, config, toast]);

  const saveCurrentProject = useCallback(async () => {
    if (!sessionId) return;
    const question = schema?.query || config?.query || '';
    const filename = buildExportFilename(question, 'schematiq.json', sessionId);
    try {
      const tzOffset = new Date().getTimezoneOffset();
      const path = sessionMode === 'schematiq'
        ? `/schematiq/export-complete/${sessionId}?format=json&tz_offset=${tzOffset}`
        : `/load/export-complete/${sessionId}?format=json`;
      await downloadAs(path, filename);
    } catch (err: any) {
      toast({
        title: 'Save failed',
        description: err?.response?.data?.detail || err?.message || 'Could not save this project',
        variant: 'destructive',
      });
    }
  }, [sessionId, sessionMode, schema, config, toast]);

  const saveProjectWithDocuments = useCallback(async () => {
    if (!sessionId) return;
    const question = schema?.query || config?.query || '';
    const filename = buildExportFilename(question, 'bundle.zip', sessionId);
    try {
      const tzOffset = new Date().getTimezoneOffset();
      const path = sessionMode === 'schematiq'
        ? `/schematiq/export-complete/${sessionId}?format=bundle&tz_offset=${tzOffset}`
        : `/load/export-complete/${sessionId}?format=bundle`;
      await downloadAs(path, filename);
    } catch (err: any) {
      toast({
        title: 'Save failed',
        description: err?.response?.data?.detail || err?.message || 'Could not save this project bundle',
        variant: 'destructive',
      });
    }
  }, [sessionId, sessionMode, schema, config, toast]);

  const searchPage = useCallback(() => {
    const term = window.prompt('Find in visible workspace');
    const findInPage = (window as Window & { find?: (text: string) => boolean }).find;
    if (term && findInPage) findInPage(term);
  }, []);

  const importExistingProject = useCallback(async (file: File) => {
    setImportingProject(true);
    try {
      const upload = await loadAPI.uploadFile(file);
      await loadAPI.parseFile(upload.session_id);
      setSessionMode('load');
      setActiveSheet('data');
      toast({
        title: 'Project imported',
        description: `${file.name} is open in the workspace.`,
      });
      navigate(`/workspace/${upload.session_id}?mode=load`, { replace: true });
    } catch (err: any) {
      toast({
        title: 'Import failed',
        description: err?.response?.data?.detail || err?.message || 'Could not import this project file',
        variant: 'destructive',
      });
    } finally {
      setImportingProject(false);
      if (importInputRef.current) importInputRef.current.value = '';
    }
  }, [navigate, toast]);

  const clearPendingRerun = useCallback(() => {
    setPendingRerunKind(null);
    setPendingSchemaColumns([]);
  }, []);

  const markRerunNeeded = useCallback((kind: PendingRerunKind, columns: string[] = []) => {
    if (kind === 'unit') {
      setPendingRerunKind('unit');
      setPendingSchemaColumns([]);
      return;
    }

    setPendingRerunKind((current) => current === 'unit' ? 'unit' : 'schema');
    setPendingSchemaColumns((current) => {
      const merged = new Set(current);
      columns
        .map((column) => column.trim())
        .filter(Boolean)
        .forEach((column) => merged.add(column));
      return Array.from(merged);
    });
  }, []);

  // Pre-check source-document availability for the current session. Used by the
  // re-extract confirm card so the user can upload missing originals (e.g. after
  // loading a project from JSON, where documents are not bundled) before running
  // the extraction model. Failures leave availability null and fall back to the
  // backend gate in start_gated_reextraction, which still blocks and reports.
  const runReextractPrecheck = useCallback(async () => {
    if (!sessionId) return;
    setReextractAvailabilityLoading(true);
    try {
      const availability = await schemaAPI.precheckDocuments(sessionId, {
        operation_type: 'reextraction',
      });
      setReextractAvailability(availability);
    } catch {
      setReextractAvailability(null);
    } finally {
      setReextractAvailabilityLoading(false);
    }
  }, [sessionId]);

  // Resolve the requested columns to a concrete, non-empty target set and open
  // the confirm card. Both routes (manual button + chat tool) re-extract only
  // after an explicit confirm; the backend gated action owns baseline capture
  // and document precheck so this stays purely presentational.
  const requestReextraction = useCallback((columns?: string[]) => {
    if (!sessionId || rerunStarting) return;

    const schemaColumnNames = new Set(
      (schema?.schema || [])
        .map((column) => column.name)
        .filter((name): name is string => Boolean(name)),
    );
    const requestedColumns = (columns && columns.length > 0 ? columns : pendingSchemaColumns)
      .map((name) => name.trim())
      .filter((name) => Boolean(name) && !name.toLowerCase().endsWith('_excerpt'));
    const targetColumns = (requestedColumns.length > 0 ? requestedColumns : Array.from(schemaColumnNames))
      .filter((name) => schemaColumnNames.has(name));

    if (targetColumns.length === 0) {
      toast({
        title: 'No columns to re-extract',
        description: 'Add schema columns first, then try again.',
        variant: 'destructive',
      });
      return;
    }

    setReextractConfirm({ columns: targetColumns });
    setReextractAvailability(null);
    void runReextractPrecheck();
  }, [pendingSchemaColumns, rerunStarting, runReextractPrecheck, schema?.schema, sessionId, toast]);

  const startReextraction = useCallback(async (targetColumns: string[]) => {
    if (!sessionId || rerunStarting || targetColumns.length === 0) return;

    const chatPendingCleared = await cancelChatPendingIfAny();
    if (!chatPendingCleared) {
      toast({
        title: 'Chat confirmation still pending',
        description: 'Cancel the chat confirmation card first, then try again.',
        variant: 'destructive',
      });
      return;
    }

    setRerunStarting(true);
    try {
      const cfg = await configAPI.getConfig().catch(() => ({ allow_llm_config: true }));
      const configured = await getConfiguredProviders();
      const available = getAvailableProviders(configured);
      const provider: LLMProviderKey = !cfg.allow_llm_config
        ? 'gemini'
        : (available[0] ?? 'gemini');
      const model = getDefaultModelForProvider(provider);
      const apiKey = await getApiKeyForProvider(provider);
      const request: ReextractionRequest = { columns: targetColumns };
      if (apiKey) {
        request.llm_config = { provider, model, api_key: apiKey, temperature: 0 };
      }

      const response = await schemaAPI.startReextraction(sessionId, request);
      const docCount = response.rows_to_process || response.estimated_papers || 0;
      if (docCount === 0) {
        toast({
          title: 'No source documents',
          description: 'Upload documents or open a ScheMatiQ project with source files, then try again.',
          variant: 'destructive',
        });
        return;
      }

      clearPendingRerun();
      setActiveSheet('data');
      setReextraction({
        operationId: response.operation_id,
        columns: response.columns,
        progress: 0,
        processedDocuments: 0,
        totalDocuments: docCount,
        currentColumn: response.columns[0],
      });
      toast({
        title: 'Re-extraction started',
        description: `Re-extracting ${response.columns.join(', ')} across ${docCount} document(s). Other columns stay unchanged.`,
        duration: 4000,
      });
    } catch (err: any) {
      toast({
        title: 'Re-extraction failed to start',
        description: err?.response?.data?.detail || err?.message || 'Could not start re-extraction',
        variant: 'destructive',
      });
    } finally {
      setRerunStarting(false);
    }
  }, [cancelChatPendingIfAny, clearPendingRerun, rerunStarting, sessionId, toast]);

  const confirmReextraction = useCallback(async () => {
    if (!reextractConfirm) return;
    // Belt-and-suspenders: the Confirm button is disabled when no documents are
    // available, but guard here too so a stale click can't bypass the gate.
    if (reextractAvailability && !reextractAvailability.can_proceed) return;
    const { columns } = reextractConfirm;
    setReextractConfirm(null);
    setReextractAvailability(null);
    await startReextraction(columns);
  }, [reextractAvailability, reextractConfirm, startReextraction]);

  // Cancel a running re-extraction. Reuses the same backend stop mechanism as
  // the classic Visualizer (POST /schema/stop-reextraction); the WebSocket
  // 'reextraction_stopped' handler clears the spinner.
  const stopReextraction = useCallback(async () => {
    if (!sessionId || !reextraction?.operationId || stoppingReextraction) return;
    setStoppingReextraction(true);
    try {
      await schemaAPI.stopReextraction(sessionId, reextraction.operationId);
      toast({
        title: 'Stopping re-extraction',
        description: 'Finishing the current document, then stopping. Partial results are kept.',
      });
    } catch (err: any) {
      toast({
        title: 'Could not stop re-extraction',
        description: err?.response?.data?.detail || err?.message || 'Stop request failed.',
        variant: 'destructive',
      });
      setStoppingReextraction(false);
    }
  }, [reextraction?.operationId, sessionId, stoppingReextraction, toast]);

  useEffect(() => {
    if (!reextraction) setStoppingReextraction(false);
  }, [reextraction]);

  const startSchemaRediscovery = useCallback(async () => {
    if (!sessionId || rerunStarting) return;

    if (sessionMode !== 'schematiq') {
      toast({
        title: 'Rediscovery needs a ScheMatiQ run',
        description: 'Imported static projects can edit the observation unit, but rediscovering schema requires a ScheMatiQ project with source documents.',
        variant: 'destructive',
      });
      return;
    }

    setRerunStarting(true);
    try {
      const chatPendingCleared = await cancelChatPendingIfAny();
      if (!chatPendingCleared) {
        toast({
          title: 'Chat confirmation still pending',
          description: 'Cancel the chat confirmation card first, then try again.',
          variant: 'destructive',
        });
        return;
      }
      await schematiqAPI.resume(sessionId);
      clearPendingRerun();
      toast({
        title: 'Schema rediscovery started',
        description: 'Rediscovering schema from the updated observation unit.',
        duration: 4000,
      });
      await refresh({ silent: true });
    } catch (err: any) {
      toast({
        title: 'Schema rediscovery failed',
        description: err?.response?.data?.detail || err?.message || 'Could not start schema rediscovery',
        variant: 'destructive',
      });
    } finally {
      setRerunStarting(false);
    }
  }, [cancelChatPendingIfAny, clearPendingRerun, refresh, rerunStarting, sessionId, sessionMode, toast]);

  const notifyEditFollowUp = useCallback((kind: PendingRerunKind, columns: string[] = []) => {
    markRerunNeeded(kind, columns);

    if (kind === 'unit') {
      // The persistent top banner (driven by markRerunNeeded above) already
      // surfaces the "Rediscover schema & re-extract" action, so we do not fire a
      // competing toast. Exception: in an imported (non-ScheMatiQ) project
      // rediscovery is impossible and the banner's action only errors, so we
      // surface a proactive, action-less explanation instead.
      if (sessionMode !== 'schematiq') {
        toast({
          title: 'Observation unit updated',
          description: 'Imported static projects can edit the unit, but rediscovery needs a ScheMatiQ project with source documents.',
        });
      }
      return;
    }

    // Schema edits: the persistent "Schema changed" banner is the single,
    // always-fresh entry point for re-extraction, so we raise no duplicate toast
    // here. The old toast was also broken — its action captured a stale
    // `requestReextraction` closure (schema not yet refreshed when the toast was
    // created), producing a false "No columns to re-extract" error on click.
  }, [markRerunNeeded, sessionMode, toast]);

  const runPendingEdits = useCallback(async () => {
    if (!sessionId || !pendingRerunKind || rerunStarting) return;
    if (pendingRerunKind === 'unit') {
      await startSchemaRediscovery();
      return;
    }
    requestReextraction(pendingSchemaColumns);
  }, [pendingRerunKind, pendingSchemaColumns, rerunStarting, requestReextraction, sessionId, startSchemaRediscovery]);

  const progressPercent = Math.round((status?.progress || 0) * 100);
  const topbarQuestion = schema?.query || config?.query || '';
  const projectTitle = topbarQuestion || (sessionId ? `ScheMatiQ ${sessionId.slice(0, 8)}` : 'Untitled workspace');
  const chromeStatus = useMemo(() => {
    if (!sessionId) return 'No project open';
    if (loading) return 'Loading…';

    const rawStatus = status?.status || '';
    const isDone = rawStatus === 'completed' || rawStatus === 'schema_extracted' || rawStatus === 'stopped';
    const isFailed = rawStatus === 'error' || rawStatus === 'failed';

    if (isFailed) return 'Extraction failed';

    // Still working: surface a live, human label instead of the raw status key.
    if (!isDone) {
      return sessionMode === 'load' ? 'Importing…' : 'Extracting…';
    }

    // Done: replace the status word (which just repeats what the visible table
    // already shows) with useful context — where the data came from + its size.
    const source = sessionMode === 'load' ? 'Imported file' : 'Extracted from documents';
    const rowCount = data.total_count;
    const colCount = status?.columns_discovered ?? schema?.schema.length ?? 0;
    const parts = [source];
    if (rowCount > 0) parts.push(`${rowCount} ${rowCount === 1 ? 'row' : 'rows'}`);
    if (colCount > 0) parts.push(`${colCount} ${colCount === 1 ? 'column' : 'columns'}`);
    return parts.join(' · ');
  }, [sessionId, loading, status, sessionMode, data.total_count, schema]);
  const isSheetHidden = chatWidth >= window.innerWidth - 80;
  const isChatHidden = chatWidth <= 24;
  const bodyGridColumns = isSheetHidden
    ? '0px 8px minmax(0, 1fr)'
    : isChatHidden
      ? 'minmax(0, 1fr) 8px 0px'
      : `minmax(0, 1fr) 8px ${chatWidth}px`;
  const gridLayoutRevision = String(chatWidth);
  const reextractionPercent = Math.round((reextraction?.progress || 0) * 100);
  const bottombarStatus = reextraction
    ? `Re-extracting ${reextraction.columns.join(', ')} (${reextraction.processedDocuments}/${reextraction.totalDocuments || '?'} docs)`
    : (status?.current_step || status?.status || 'No project status');
  const bottombarProgress = reextraction ? reextractionPercent : progressPercent;

  const startDividerDrag = useCallback((event: React.PointerEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDraggingDivider(true);

    const handleMove = (moveEvent: PointerEvent) => {
      const nextWidth = window.innerWidth - moveEvent.clientX;
      const maxWidth = Math.max(24, window.innerWidth - 24);
      const clamped = Math.min(maxWidth, Math.max(0, nextWidth));
      const snapped = clamped < 56 ? 0 : clamped > window.innerWidth - 80 ? window.innerWidth : clamped;
      setChatWidth(snapped);
      localStorage.setItem('workspace.chatWidth', String(snapped));
    };

    const handleUp = () => {
      setIsDraggingDivider(false);
      window.removeEventListener('pointermove', handleMove);
      window.removeEventListener('pointerup', handleUp);
    };

    window.addEventListener('pointermove', handleMove);
    window.addEventListener('pointerup', handleUp);
  }, []);

  // Source document of the currently selected data row, used by the optional
  // source panel on the Data sheet. Uses the raw _source_document value so it
  // matches the document names the content endpoint allowlists.
  const selectedSourceDoc = useMemo<string | null>(() => {
    if (activeSheet !== 'data' || !sheetSelection || sheetSelection.sheet !== 'data') return null;
    const rows = (dataView === 'by_unit' ? alignedUnitData : alignedDocData).rows || [];
    if (rows.length === 0) return null;
    const visualRow = sheetSelection.fromRow;
    if (visualRow == null || visualRow < 0) return null;

    // Map the visually selected row to its data row, accounting for column
    // sorting. toPhysicalRow can return null/-1 for out-of-range rows, so guard
    // and fall back to the visual index, then bound-check before reading.
    const hot = hotTableRef.current?.hotInstance;
    let physical = visualRow;
    if (hot && typeof hot.toPhysicalRow === 'function') {
      const mapped = hot.toPhysicalRow(visualRow);
      if (mapped != null && mapped >= 0) physical = mapped;
    }
    const row = rows[physical] ?? rows[visualRow];
    if (!row) return null;

    const raw =
      row._source_document ||
      row._parent_document ||
      (Array.isArray(row.papers) ? row.papers[0] : undefined);
    return raw ? String(raw).trim() : null;
  }, [activeSheet, sheetSelection, alignedDocData, alignedUnitData, dataView]);

  const handleAttachSourceDocs = useCallback(
    async (e: ChangeEvent<HTMLInputElement>) => {
      const files = Array.from(e.target.files || []);
      e.target.value = '';
      if (!sessionId || files.length === 0) return;
      setAttachingSourceDocs(true);
      try {
        await unitsAPI.attachSourceDocuments(sessionId, files);
        setSourceDocReloadToken((t) => t + 1);
      } catch (err) {
        toast({
          title: 'Upload failed',
          description: 'Could not attach the source documents. Please try again.',
          variant: 'destructive',
        });
      } finally {
        setAttachingSourceDocs(false);
      }
    },
    [sessionId, toast],
  );

  const dataGridNode = (
    <div className="workspace-grid-wrap">
      <SpreadsheetSurface
        activeSheet={activeSheet}
        data={activeSheet === 'data' && dataView === 'by_unit' ? alignedUnitData : alignedDocData}
        schema={schema}
        displayOptions={tableDisplay}
        cellFormats={cellFormats}
        formatVersion={formatVersion}
        hotTableRef={hotTableRef}
        onSelectionChange={updateSheetSelection}
        onGroundingHighlight={handleGroundingHighlight}
        onGroundingScrollRequest={() => setGroundingScrollNonce((n) => n + 1)}
        onRefresh={refreshSilent}
        onEditFollowUp={notifyEditFollowUp}
        onEditEnd={flushDeferredData}
        layoutRevision={gridLayoutRevision}
        dataView={dataView}
      />
      {(loading || importingProject) && sessionId && (
        <div className="workspace-loading-overlay" role="status" aria-live="polite">
          <div className="workspace-loading-card">
            <Loader2 className="h-5 w-5 animate-spin" />
            <span>Loading project…</span>
          </div>
        </div>
      )}
      {/*
        Structure is on screen (overlay cleared) but the row payload is still
        streaming in. Show a small non-blocking badge instead of covering the
        whole grid, so the project reads as "open, loading rows" rather than
        "stuck". Suppressed while the full overlay or an import is showing.
      */}
      {dataLoading && !loading && !importingProject && sessionId && (
        <div className="workspace-data-loading-badge" role="status" aria-live="polite">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          <span>Loading rows…</span>
        </div>
      )}
    </div>
  );

  return (
    <div className="workspace-root h-full w-full">
      <SpreadsheetChrome
        projectTitle={projectTitle}
        sessionStatus={chromeStatus}
        canUseProjectActions={Boolean(sessionId)}
        displayOptions={selectedDisplayOptions}
        onNewProject={() => setProjectDialogOpen(true)}
        onImportProject={() => importInputRef.current?.click()}
        onOpenClassic={() => {
          if (sessionId) navigate(`/visualize/${sessionId}?mode=${sessionMode}`);
        }}
        onProjectDetails={() => setDetailsDialogOpen(true)}
        onRefresh={refresh}
        onPrint={printWorkspace}
        onExport={exportCurrentProject}
        onSaveProject={saveCurrentProject}
        onSaveProjectWithDocs={saveProjectWithDocuments}
        onHome={() => navigate('/workspace')}
        onSearch={searchPage}
        onEstimateCost={estimateCurrentCost}
        onShowSheet={() => setChatWidth(0)}
        onShowChat={() => setChatWidth(window.innerWidth)}
        onSplitView={() => setChatWidth(380)}
        onRunPendingEdits={runPendingEdits}
        onAddDocuments={() => setActiveSheet('documents')}
        onApplyFormat={applyTableFormat}
        rerunDisabled={!sessionId || !pendingRerunKind || rerunStarting}
      />

      {pendingRerunKind && (
        <PendingRerunBanner
          kind={pendingRerunKind}
          columns={pendingSchemaColumns}
          sessionMode={sessionMode}
          busy={rerunStarting}
          onReextract={() => requestReextraction(pendingRerunKind === 'schema' ? pendingSchemaColumns : undefined)}
          onRediscover={startSchemaRediscovery}
          onDismiss={clearPendingRerun}
        />
      )}

      <div
        className="workspace-body"
        data-dragging={isDraggingDivider}
        style={{ gridTemplateColumns: bodyGridColumns }}
      >
        <section className="workspace-sheet-pane" data-hidden={isSheetHidden}>
          {activeSheet === 'data' && (
            <div
              className="workspace-data-toolbar"
              style={{ flex: '0 0 auto', display: 'flex', alignItems: 'center', gap: 8, padding: '6px 8px', borderBottom: '1px solid #e2e8e2' }}
            >
              <ViewModeToggle
                viewMode={dataView === 'by_unit' ? 'by_unit' : 'standard'}
                onViewModeChange={(mode) => setDataView(mode === 'by_unit' ? 'by_unit' : 'by_document')}
              />
              <span style={{ width: 1, alignSelf: 'stretch', background: '#e2e8e2', margin: '2px 4px' }} />
              <Button
                size="sm"
                variant={showSourcePanel ? 'secondary' : 'outline'}
                onClick={() => setShowSourcePanel((v) => !v)}
                className="gap-1.5"
                aria-pressed={showSourcePanel}
                title="Show the source document for the selected row, side by side with the data"
              >
                <PanelLeft className="h-4 w-4" />
                {showSourcePanel ? 'Hide source' : 'Show source document'}
              </Button>
              <span style={{ flex: 1 }} />
            </div>
          )}
          {activeSheet === 'stats' ? (
            <div className="workspace-dashboard-wrap" style={{ height: '100%', overflow: 'auto', padding: '16px' }}>
              {session?.statistics ? (
                <StatsDashboard
                  statistics={session.statistics}
                  session={session}
                  creationMetadata={session.creation_metadata}
                  modificationHistory={session.modification_history}
                />
              ) : (
                <div className="workspace-dashboard-empty" style={{ color: 'var(--muted-foreground, #6b7280)', fontSize: 14 }}>
                  Statistics will appear once processing completes.
                </div>
              )}
            </div>
          ) : activeSheet === 'documents' ? (
            <div style={{ height: '100%', minHeight: 0, display: 'flex', flexDirection: 'column' }}>
              <div style={{ flex: 1, minHeight: 0, overflow: 'hidden' }}>
                <DocumentViewer sessionId={sessionId} refreshKey={data.total_count} />
              </div>
              {sessionId && (
                <div
                  className="border-t border-border"
                  style={{ flexShrink: 0, maxHeight: '48%', overflowY: 'auto', padding: '16px' }}
                >
                  <h3 className="text-base font-semibold mb-1">Add more documents</h3>
                  <p className="text-sm text-muted-foreground mb-3">
                    Upload additional documents and extract them with this project&apos;s existing schema. New rows are appended to the table.
                  </p>
                  {addDocsError && (
                    <Alert variant="destructive" className="mb-3">
                      <AlertDescription className="whitespace-pre-line">{addDocsError}</AlertDescription>
                    </Alert>
                  )}
                  {addDocsNotice && (
                    <Alert className="mb-3">
                      <AlertDescription className="whitespace-pre-line">{addDocsNotice}</AlertDescription>
                    </Alert>
                  )}
                  {addDocsFiles.length > 0 && (
                    <div className="mb-3 flex items-center gap-2 rounded-md border border-primary/30 bg-primary/5 px-3 py-2 text-sm text-foreground">
                      <Check className="h-4 w-4 shrink-0 text-primary" />
                      <span>
                        {addDocsFiles.length} file{addDocsFiles.length !== 1 ? 's' : ''} ready. Review the list below, then click &ldquo;Upload&rdquo; to add {addDocsFiles.length !== 1 ? 'them' : 'it'} to the project.
                      </span>
                    </div>
                  )}
                  <DocumentUpload
                    onFilesChange={handleAddDocsFilesChange}
                    uploadedFiles={addDocsFiles}
                    loading={addDocsUploading}
                    onUpload={uploadAddDocuments}
                    canUpload={Boolean(sessionId) && !addDocsProcessing}
                    uploadResult={addDocsResult}
                    sessionId={sessionId}
                    existingDocumentCount={documents?.documents?.length ?? 0}
                    hideHeader
                  />
                  {addDocsPending && (
                    <Button
                      type="button"
                      className="w-full mt-3"
                      onClick={processAddDocuments}
                      disabled={addDocsProcessing}
                    >
                      {addDocsProcessing
                        ? 'Starting extraction\u2026'
                        : `Process ${addDocsResult?.uploaded_files?.length ?? 0} new document(s)`}
                    </Button>
                  )}
                </div>
              )}
            </div>
          ) : activeSheet !== 'monitor' ? (
            activeSheet === 'data' && showSourcePanel ? (
              <div className="workspace-data-split">
                <div className="workspace-source-panel">
                  <input
                    ref={sourceDocInputRef}
                    type="file"
                    multiple
                    accept=".txt,.md,.pdf,.doc,.docx,.rtf,.json"
                    onChange={handleAttachSourceDocs}
                    className="hidden"
                  />
                  <DocumentPreview
                    sessionId={sessionId}
                    documentName={selectedSourceDoc}
                    emptyHint="Select a row to see its source document."
                    reloadToken={sourceDocReloadToken}
                    highlightTexts={groundingHighlights}
                    scrollNonce={groundingScrollNonce}
                    onRequestUpload={attachingSourceDocs ? undefined : () => sourceDocInputRef.current?.click()}
                  />
                </div>
                {dataGridNode}
              </div>
            ) : dataGridNode
          ) : null}

          {/*
            ScheMatiQ Monitor stays mounted (hidden, not unmounted) while another
            sheet is active, so its log history survives tab switches. It is only
            rendered in schematiq mode (the monitor tab is hidden otherwise).
          */}
          {sessionMode === 'schematiq' && sessionId && (
            <div
              className="workspace-dashboard-wrap"
              style={{
                height: '100%',
                overflow: 'auto',
                padding: '16px',
                display: activeSheet === 'monitor' ? 'block' : 'none',
              }}
            >
              <ScheMatiQMonitor
                sessionId={sessionId}
                onResumeStarted={refreshSilent}
                onExtractionStarted={(columns, operationId) => {
                  // Optimistically surface the bottom-bar re-extraction banner
                  // when a run is started from the monitor tab. The subsequent
                  // reextraction_started/progress WebSocket events fill in
                  // totalDocuments and switch to the data sheet.
                  if (!operationId) return;
                  setReextraction({
                    operationId,
                    columns,
                    progress: 0,
                    processedDocuments: 0,
                    totalDocuments: 0,
                    currentColumn: columns[0],
                  });
                }}
              />
            </div>
          )}
        </section>

        <div
          className="workspace-resizer"
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize sheet and chat panes"
          onPointerDown={startDividerDrag}
          data-dragging={isDraggingDivider}
        />

        <div className="workspace-chat-pane" data-hidden={isChatHidden}>
          <ChatPanel
            sessionId={sessionId}
            sessionMode={sessionMode}
            status={status}
            schema={schema}
            data={data}
            onRefresh={refreshSilent}
            onEditFollowUp={notifyEditFollowUp}
            onRegisterCancelPending={(cancel) => {
              cancelChatPendingRef.current = cancel;
            }}
          />
        </div>
      </div>

      <div className="workspace-bottombar">
        <div className="workspace-bottombar-tabs">
          {SHEETS.filter((sheet) => sheet.id !== 'monitor' || sessionMode === 'schematiq').map((sheet, index, sheets) => (
            <Fragment key={sheet.id}>
              {index > 0 && sheets[index - 1].group !== sheet.group && (
                <span className="workspace-sheet-tab-divider" aria-hidden="true" />
              )}
              <button
                className="workspace-sheet-tab"
                data-active={activeSheet === sheet.id}
                data-group={sheet.group}
                onClick={() => setActiveSheet(sheet.id)}
              >
                {sheet.label}
              </button>
            </Fragment>
          ))}
        </div>

        <Tooltip>
          <TooltipTrigger asChild>
            <div className="workspace-topbar-question" tabIndex={0}>
              {topbarQuestion || (sessionId ? `Session ${sessionId.slice(0, 8)}` : 'No project open')}
            </div>
          </TooltipTrigger>
          <TooltipContent
            side="top"
            align="start"
            className="workspace-topbar-question-tooltip"
          >
            {topbarQuestion || (sessionId ? `Session ${sessionId}` : 'No project open')}
          </TooltipContent>
        </Tooltip>

        <div className="workspace-topbar-status" title={bottombarStatus}>
          {status || reextraction ? (
            <>
              <Progress value={bottombarProgress} className="h-2.5" />
              <span className="workspace-topbar-percent">{bottombarProgress}%</span>
              {reextraction && (
                <Button
                  size="sm"
                  variant="outline"
                  onClick={stopReextraction}
                  disabled={stoppingReextraction}
                  aria-label="Stop re-extraction"
                >
                  {stoppingReextraction ? <Loader2 className="h-4 w-4 animate-spin" /> : <X className="h-4 w-4" />}
                  {stoppingReextraction ? 'Stopping…' : 'Stop'}
                </Button>
              )}
            </>
          ) : (
            <span className="workspace-topbar-status-empty">No status</span>
          )}
        </div>

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button size="icon" variant="ghost" aria-label="Workspace menu">
              <MoreVertical className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-64">
            <DropdownMenuLabel>Project</DropdownMenuLabel>
            <DropdownMenuItem onClick={() => setProjectDialogOpen(true)}>
              <Plus className="h-4 w-4" />
              New Project
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => importInputRef.current?.click()} disabled={importingProject}>
              {importingProject ? <Loader2 className="h-4 w-4 animate-spin" /> : <FileUp className="h-4 w-4" />}
              Import Existing Project
            </DropdownMenuItem>
            {sessionId && (
              <DropdownMenuItem onClick={() => navigate(`/visualize/${sessionId}?mode=${sessionMode}`)}>
                <Table2 className="h-4 w-4" />
                Classic Visualizer
              </DropdownMenuItem>
            )}
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => navigate('/classic')}>
              <FileUp className="h-4 w-4" />
              Existing Start Page
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuLabel>Workspace</DropdownMenuLabel>
            <DropdownMenuItem onClick={() => setDetailsDialogOpen(true)} disabled={!sessionId}>
              <Table2 className="h-4 w-4" />
              Project Details
            </DropdownMenuItem>
            <DropdownMenuItem onClick={estimateCurrentCost} disabled={!sessionId}>
              <Sparkles className="h-4 w-4" />
              Estimate Cost
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => setChatWidth(0)}>
              <Table2 className="h-4 w-4" />
              Show Sheet Full Screen
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => setChatWidth(window.innerWidth)}>
              <Bot className="h-4 w-4" />
              Show Chat Full Screen
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => setChatWidth(380)}>
              <ChevronDown className="h-4 w-4 rotate-90" />
              Split View
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuLabel>Status</DropdownMenuLabel>
            <div className="px-2 py-1.5 text-xs text-muted-foreground">
              <div>{sessionMode} / {status?.status || 'no project'}</div>
              {sessionId && <div>Session {sessionId.slice(0, 8)}</div>}
            </div>
          </DropdownMenuContent>
        </DropdownMenu>

        <input
          ref={importInputRef}
          type="file"
          className="hidden"
          accept=".json,.jsonl,.csv,.zip,.schematiq.json,application/json,text/csv,application/zip"
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (file) importExistingProject(file);
          }}
        />
      </div>

      <NewProjectDialog
        open={projectDialogOpen}
        onOpenChange={setProjectDialogOpen}
        onCreated={() => {
          setActiveSheet('data');
          refresh();
        }}
      />

      <ProjectDetailsDialog
        open={detailsDialogOpen}
        onOpenChange={setDetailsDialogOpen}
        sessionId={sessionId}
        sessionMode={sessionMode}
        status={status}
        schema={schema}
        documents={documents}
        config={config}
        costEstimate={costEstimate}
      />

      <Dialog
        open={Boolean(reextractConfirm)}
        onOpenChange={(open) => {
          if (!open) {
            setReextractConfirm(null);
            setReextractAvailability(null);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Re-extract values?</DialogTitle>
            <DialogDescription>
              {reextractConfirm
                ? `Re-extract ${reextractConfirm.columns.length} column(s): ${reextractConfirm.columns.join(', ')}. This runs the extraction model over your source documents. Other columns stay unchanged.`
                : ''}
            </DialogDescription>
          </DialogHeader>

          {sessionId && (reextractAvailabilityLoading || reextractAvailability) && (
            <div className="py-1">
              <MissingDocumentsSection
                sessionId={sessionId}
                availability={reextractAvailability}
                loading={reextractAvailabilityLoading}
                onRefresh={runReextractPrecheck}
              />
              {reextractAvailability && !reextractAvailability.can_proceed && (
                <p className="text-xs text-muted-foreground mt-2">
                  Re-extraction needs at least one source document. Upload the
                  files listed above — names must match the originals — then Confirm.
                </p>
              )}
            </div>
          )}

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setReextractConfirm(null);
                setReextractAvailability(null);
              }}
              disabled={rerunStarting}
            >
              Cancel
            </Button>
            <Button
              onClick={confirmReextraction}
              disabled={
                rerunStarting ||
                reextractAvailabilityLoading ||
                Boolean(reextractAvailability && !reextractAvailability.can_proceed)
              }
            >
              {rerunStarting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Check className="h-4 w-4" />}
              Confirm
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Table feedback widget (release mode only) — parity with the classic flow */}
      {!developerMode && sessionId && (
        <TableFeedbackWidget
          sessionId={sessionId}
          sessionStatus={session?.status || ''}
          activeTab={activeSheet}
          tableRowCount={data?.total_count || 0}
          tableColumnCount={session?.columns?.length || 0}
        />
      )}
    </div>
  );
}

export default Workspace;
