import { type CSSProperties, type MutableRefObject, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { HotTable, type HotTableClass } from '@handsontable/react';
import { registerAllModules } from 'handsontable/registry';
import 'handsontable/styles/handsontable.min.css';
import 'handsontable/styles/ht-theme-main.min.css';

import {
  AlignLeft,
  Bold,
  Bot,
  Check,
  ChevronDown,
  Download,
  FileUp,
  FolderOpen,
  Italic,
  Loader2,
  Play,
  Plus,
  Printer,
  RotateCw,
  Search,
  Sparkles,
  Strikethrough,
  Sigma,
  Table2,
  Type,
  Underline,
  X,
  MoreVertical,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
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
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Progress } from '@/components/ui/progress';
import { Textarea } from '@/components/ui/textarea';
import { Badge } from '@/components/ui/badge';
import { ToastAction } from '@/components/ui/toast';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { useToast } from '@/components/ui/use-toast';
import { extractDisplayValue } from '@/components/DataTable/utils/valueUtils';
import {
  DEFAULT_DOCUMENT_RANDOMIZATION_SEED,
  DEFAULT_DOCUMENTS_BATCH_SIZE,
  DEFAULT_MAX_KEYS_SCHEMA,
  getAvailableProviders,
  getDefaultModelForProvider,
  type LLMProviderKey,
} from '@/constants';
import { chatAPI, configAPI, loadAPI, observationUnitAPI, schemaAPI, schematiqAPI, unitsAPI } from '@/services/api';
import webSocketService from '@/services/websocket';
import {
  ChatToolInfo,
  ChatTurnMessage,
  ColumnInfo,
  CostEstimate,
  DataRow,
  PaginatedData,
  SchemaData,
  ReextractionCompletedData,
  ReextractionFailedData,
  ReextractionProgressData,
  ReextractionRequest,
  ReextractionStartedData,
  ScheMatiQConfig,
  ScheMatiQStatus,
  VisualizationSession,
  WebSocketMessage,
} from '@/types';
import { DocumentListResponse } from '@/types/unit';
import { getApiKeyForProvider, getConfiguredProviders } from '@/utils/apiKeyStorage';

import './Workspace.css';

registerAllModules();

type SheetId = 'data' | 'unit' | 'schema';
type WorkspaceSessionMode = 'schematiq' | 'load';
type PendingRerunKind = 'schema' | 'unit';

type WorkspaceReextractionState = {
  operationId: string;
  columns: string[];
  progress: number;
  processedDocuments: number;
  totalDocuments: number;
  currentColumn?: string;
};

type SheetColumn = {
  key: string;
  label: string;
  width?: number;
  readOnly?: boolean;
};

type WorkspaceMessage = {
  id: string;
  role: 'assistant' | 'user' | 'tool';
  content: string;
  kind?: 'text' | 'tool_log';
  toolName?: string;
  toolStatus?: 'running' | 'done' | 'error';
};

type PendingChatAction = {
  id: string;
  label: string;
  description: string;
  chatId: string;
};

type TableFontFamily = 'Inter' | 'Arial' | 'Georgia' | 'Mono';
type TableTextAlign = 'left' | 'center' | 'right';

type TableDisplayOptions = {
  fontFamily: TableFontFamily;
  fontSize: number;
  bold: boolean;
  italic: boolean;
  underline: boolean;
  strikethrough: boolean;
  align: TableTextAlign;
};

type CellFormat = Partial<TableDisplayOptions>;
type CellFormatMap = Record<string, CellFormat>;

type SheetSelection = {
  sheet: SheetId;
  fromRow: number;
  toRow: number;
  fromCol: number;
  toCol: number;
} | null;

const cellFormatKey = (sheet: SheetId, row: number, col: number) => `${sheet}:${row}:${col}`;

const selectionsEqual = (a: SheetSelection, b: SheetSelection) => {
  if (a === b) return true;
  if (!a || !b) return false;
  return a.sheet === b.sheet
    && a.fromRow === b.fromRow
    && a.toRow === b.toRow
    && a.fromCol === b.fromCol
    && a.toCol === b.toCol;
};

const selectionArea = (selection: SheetSelection) => {
  if (!selection) return 0;
  return (selection.toRow - selection.fromRow + 1) * (selection.toCol - selection.fromCol + 1);
};

const getCellFormatClasses = (format?: CellFormat) => {
  if (!format) return '';
  return [
    format.fontFamily ? `workspace-cell-font-${format.fontFamily.toLowerCase()}` : '',
    format.fontSize ? `workspace-cell-size-${format.fontSize}` : '',
    format.bold === true ? 'workspace-cell-bold' : '',
    format.bold === false ? 'workspace-cell-weight-normal' : '',
    format.italic === true ? 'workspace-cell-italic' : '',
    format.italic === false ? 'workspace-cell-style-normal' : '',
    format.underline === true ? 'workspace-cell-underline' : '',
    format.underline === false ? 'workspace-cell-underline-off' : '',
    format.strikethrough === true ? 'workspace-cell-strike' : '',
    format.strikethrough === false ? 'workspace-cell-strike-off' : '',
    format.align ? `workspace-cell-align-${format.align}` : '',
  ].filter(Boolean).join(' ');
};

type NewProjectDialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreated: (sessionId: string) => void;
};

const SHEETS: Array<{ id: SheetId; label: string }> = [
  { id: 'data', label: 'Data' },
  { id: 'unit', label: 'Observation Unit' },
  { id: 'schema', label: 'Schema' },
];

const WORKSPACE_MENUS = [
  {
    label: 'File',
    items: ['New project', 'Import project', 'Open classic visualizer', 'Export table'],
  },
  {
    label: 'Edit',
    items: ['Undo', 'Redo', 'Find and replace', 'Delete values'],
  },
  {
    label: 'View',
    items: ['Show sheet full screen', 'Show chat full screen', 'Split view', 'Project details'],
  },
  {
    label: 'Insert',
    items: ['Column', 'Observation unit', 'Schema field', 'Comment'],
  },
  {
    label: 'Format',
    items: ['Text wrapping', 'Bold headers', 'Alternating colors', 'Clear formatting'],
  },
  {
    label: 'Data',
    items: ['Sort range', 'Create filter', 'Re-extract table', 'Validate schema'],
  },
  {
    label: 'Tools',
    items: ['Estimate cost', 'Refresh project', 'Schema suggestions', 'Merge units'],
  },
  {
    label: 'Help',
    items: ['Keyboard shortcuts', 'About ScheMatiQ workspace'],
  },
];

const DEFAULT_PROVIDER = 'gemini';
const DEFAULT_SCHEMA_MODEL = 'gemini-2.5-flash';
const DEFAULT_VALUE_MODEL = 'gemini-3.1-flash-lite-preview';
const EDITABLE_OBSERVATION_UNIT_FIELDS = new Set(['name', 'definition', 'example_names']);
const TABLE_FONT_OPTIONS: TableFontFamily[] = ['Inter', 'Arial', 'Georgia', 'Mono'];
const TABLE_FONT_SIZE_OPTIONS = [10, 11, 12, 13, 14, 16, 18];

const emptyData: PaginatedData = {
  rows: [],
  total_count: 0,
  page: 0,
  page_size: 500,
  has_more: false,
};

function dataEquals(a: PaginatedData, b: PaginatedData): boolean {
  if (a === b) return true;
  if (
    a.total_count !== b.total_count
    || a.filtered_count !== b.filtered_count
    || a.rows.length !== b.rows.length
  ) {
    return false;
  }
  return JSON.stringify(a.rows) === JSON.stringify(b.rows);
}

function formatFileSize(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / Math.pow(1024, index)).toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function formatCost(estimate?: CostEstimate | null): string {
  if (!estimate) return 'No estimate available';
  const cost = estimate.total_cost_usd ?? 0;
  const calls = estimate.total_api_calls ?? 0;
  const tokens = (estimate.total_input_tokens ?? 0) + (estimate.total_output_tokens ?? 0);
  return `$${cost.toFixed(4)} estimated, ${calls} API calls, ${tokens.toLocaleString()} tokens`;
}

function parseAllowedValues(value: unknown): string[] | undefined {
  if (Array.isArray(value)) return value.map(String).filter(Boolean);
  const text = String(value ?? '').trim();
  if (!text) return undefined;
  return text
    .split(/[,\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function buildConfig(query: string, apiKey: string): ScheMatiQConfig {
  const backend = {
    provider: DEFAULT_PROVIDER,
    model: DEFAULT_SCHEMA_MODEL,
    temperature: 0,
    api_key: apiKey || undefined,
  };

  return {
    query,
    docs_path: null,
    upload_pending: true,
    max_keys_schema: DEFAULT_MAX_KEYS_SCHEMA,
    documents_batch_size: DEFAULT_DOCUMENTS_BATCH_SIZE,
    schema_creation_backend: backend,
    value_extraction_backend: {
      ...backend,
      model: DEFAULT_VALUE_MODEL,
    },
    output_path: 'outputs/workspace_output.json',
    document_randomization_seed: DEFAULT_DOCUMENT_RANDOMIZATION_SEED,
    skip_value_extraction: false,
  };
}

function schemaFromLoadSession(session: VisualizationSession | null): SchemaData | null {
  if (!session) return null;
  return {
    query: session.schema_query || '',
    schema: session.columns || [],
    metadata: {
      imported_from_csv: session.metadata?.extracted_schema?.metadata?.imported_from_csv,
      original_session_id: session.metadata?.extracted_schema?.metadata?.original_session_id,
      generated_timestamp: session.metadata?.extracted_schema?.metadata?.generated_timestamp,
      import_timestamp: session.metadata?.extracted_schema?.metadata?.import_timestamp,
    },
    observation_unit: session.observation_unit,
    llm_configuration: session.metadata?.extracted_schema?.llm_configuration,
  };
}

function statusFromLoadSession(session: VisualizationSession | null): ScheMatiQStatus | null {
  if (!session) return null;
  const completed = session.status === 'completed' || session.status === 'schema_extracted';
  return {
    session_id: session.id,
    status: session.status,
    progress: completed ? 1 : 0,
    current_step: completed ? 'Imported project loaded' : session.status,
    steps_completed: completed ? 1 : 0,
    total_steps: 1,
    schema_completed: Boolean(session.columns?.length),
    columns_discovered: session.columns?.length || 0,
    total_documents: session.statistics?.total_documents || session.metadata?.uploaded_documents?.length || 0,
    processed_documents: session.metadata?.processed_documents || session.statistics?.total_documents || 0,
  };
}

function NewProjectDialog({ open, onOpenChange, onCreated }: NewProjectDialogProps) {
  const navigate = useNavigate();
  const { toast } = useToast();
  const folderInputRef = useRef<HTMLInputElement | null>(null);
  const [query, setQuery] = useState('');
  const [apiKey, setApiKey] = useState('');
  const [files, setFiles] = useState<File[]>([]);
  const [serverHasKeys, setServerHasKeys] = useState(false);
  const [estimate, setEstimate] = useState<CostEstimate | null>(null);
  const [estimating, setEstimating] = useState(false);
  const [creating, setCreating] = useState(false);
  const [startConfirmed, setStartConfirmed] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    configAPI.getConfig()
      .then((config) => setServerHasKeys(Boolean(config.server_has_api_keys)))
      .catch(() => setServerHasKeys(false));
  }, []);

  useEffect(() => {
    if (folderInputRef.current) {
      folderInputRef.current.setAttribute('webkitdirectory', '');
      folderInputRef.current.setAttribute('directory', '');
    }
  }, []);

  const selectedBytes = useMemo(
    () => files.reduce((sum, file) => sum + file.size, 0),
    [files]
  );

  const canEstimate = query.trim().length > 0 && files.length > 0 && (serverHasKeys || apiKey.trim().length > 0);

  const estimateProject = useCallback(async () => {
    setError(null);
    setEstimating(true);
    try {
      const config = buildConfig(query.trim(), apiKey.trim());
      const result = await schematiqAPI.estimateCostPreview(
        config,
        files.map((file) => ({ name: file.webkitRelativePath || file.name, size: file.size }))
      );
      setEstimate(result);
      setStartConfirmed(false);
      return result;
    } catch (err: any) {
      const detail = err?.response?.data?.detail || err?.message || 'Failed to estimate this project';
      setError(detail);
      throw err;
    } finally {
      setEstimating(false);
    }
  }, [apiKey, files, query]);

  const startProject = useCallback(async () => {
    if (!query.trim() || files.length === 0) {
      setError('Choose a folder of documents and enter a research question first.');
      return;
    }
    if (!serverHasKeys && !apiKey.trim()) {
      setError('Add an API key or configure server-side API keys before starting.');
      return;
    }
    if (!estimate && !startConfirmed) {
      await estimateProject();
      setStartConfirmed(true);
      return;
    }

    setCreating(true);
    setError(null);
    try {
      const config = buildConfig(query.trim(), apiKey.trim());
      const result = await schematiqAPI.configure(config);
      await loadAPI.addDocuments(result.session_id, files);
      await schematiqAPI.run(result.session_id);
      toast({
        title: 'Project started',
        description: 'The workspace will update as schema and data arrive.',
      });
      onCreated(result.session_id);
      onOpenChange(false);
      navigate(`/workspace/${result.session_id}`, { replace: true });
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to start project');
    } finally {
      setCreating(false);
    }
  }, [apiKey, estimate, estimateProject, files, navigate, onCreated, onOpenChange, query, serverHasKeys, startConfirmed, toast]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>New Project</DialogTitle>
          <DialogDescription>
            Import a local folder, describe the research question, estimate cost, then start extraction.
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-4">
          <div className="grid gap-2">
            <Label htmlFor="workspace-query">Research question</Label>
            <Textarea
              id="workspace-query"
              value={query}
              onChange={(event) => {
                setQuery(event.target.value);
                setEstimate(null);
                setStartConfirmed(false);
              }}
              placeholder="What database should ScheMatiQ build from these documents?"
              rows={4}
            />
          </div>

          <div className="grid gap-2">
            <Label>Local document folder</Label>
            <div className="flex items-center gap-2">
              <Button type="button" variant="outline" onClick={() => folderInputRef.current?.click()}>
                <FolderOpen className="h-4 w-4" />
                Choose Folder
              </Button>
              <span className="text-sm text-muted-foreground">
                {files.length > 0
                  ? `${files.length} files, ${formatFileSize(selectedBytes)}`
                  : 'No folder selected'}
              </span>
            </div>
            <input
              ref={folderInputRef}
              type="file"
              multiple
              className="hidden"
              onChange={(event) => {
                setFiles(Array.from(event.target.files || []));
                setEstimate(null);
                setStartConfirmed(false);
              }}
            />
          </div>

          <div className="grid gap-2">
            <Label htmlFor="workspace-api-key">API key</Label>
            <Input
              id="workspace-api-key"
              value={apiKey}
              type="password"
              onChange={(event) => {
                setApiKey(event.target.value);
                setEstimate(null);
                setStartConfirmed(false);
              }}
              placeholder={serverHasKeys ? 'Optional: server keys are configured' : 'Required unless server keys are configured'}
            />
          </div>

          {estimate && (
            <div className="rounded-md border bg-muted/30 p-3 text-sm">
              <div className="font-medium">Estimated cost</div>
              <div className="mt-1 text-muted-foreground">{formatCost(estimate)}</div>
              {estimate.warnings?.length > 0 && (
                <div className="mt-2 text-amber-700 dark:text-amber-300">
                  {estimate.warnings.join(' ')}
                </div>
              )}
            </div>
          )}

          {error && (
            <div className="rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">
              {error}
            </div>
          )}
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={estimateProject} disabled={!canEstimate || estimating || creating}>
            {estimating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
            Estimate
          </Button>
          <Button type="button" onClick={startProject} disabled={!canEstimate || estimating || creating}>
            {creating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
            {estimate || startConfirmed ? 'Start Project' : 'Estimate & Start'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ProjectDetailsDialog({
  open,
  onOpenChange,
  sessionId,
  sessionMode,
  status,
  schema,
  documents,
  config,
  costEstimate,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sessionId?: string;
  sessionMode: WorkspaceSessionMode;
  status: ScheMatiQStatus | null;
  schema: SchemaData | null;
  documents: DocumentListResponse | null;
  config: ScheMatiQConfig | null;
  costEstimate: CostEstimate | null;
}) {
  const runRows = [
    { label: 'Session ID', value: sessionId || '' },
    { label: 'Mode', value: sessionMode },
    { label: 'Status', value: status?.status || '' },
    { label: 'Current step', value: status?.current_step || '' },
    { label: 'Progress', value: `${Math.round((status?.progress || 0) * 100)}%` },
    { label: 'Documents', value: `${status?.processed_documents || 0}/${status?.total_documents || 0}` },
    { label: 'Columns discovered', value: status?.columns_discovered ?? schema?.schema.length ?? '' },
    { label: 'Cost estimate', value: formatCost(costEstimate) },
  ];

  const settingsRows = [
    { label: 'Research question', value: schema?.query || config?.query || '' },
    { label: 'Schema provider', value: config?.schema_creation_backend?.provider || '' },
    { label: 'Schema model', value: config?.schema_creation_backend?.model || '' },
    { label: 'Value provider', value: config?.value_extraction_backend?.provider || '' },
    { label: 'Value model', value: config?.value_extraction_backend?.model || '' },
    { label: 'Documents batch size', value: config?.documents_batch_size ?? '' },
    { label: 'Max schema columns', value: config?.max_keys_schema ?? '' },
  ];

  const provenanceRows = [
    { label: 'Observation source document', value: schema?.observation_unit?.source_document || '' },
    { label: 'Observation discovery iteration', value: schema?.observation_unit?.discovery_iteration ?? '' },
    { label: 'Original session', value: schema?.metadata?.original_session_id || '' },
    { label: 'Generated at', value: schema?.metadata?.generated_timestamp || '' },
    { label: 'Imported at', value: schema?.metadata?.import_timestamp || '' },
  ];

  const documentRows = documents?.documents || [];

  const renderRows = (rows: Array<{ label: string; value: unknown }>) => (
    <div className="workspace-detail-grid">
      {rows.map((row) => (
        <div key={row.label} className="contents">
          <div className="workspace-detail-label">{row.label}</div>
          <div className="workspace-detail-value">{String(row.value ?? '') || '-'}</div>
        </div>
      ))}
    </div>
  );

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle>Project Details</DialogTitle>
          <DialogDescription>
            Read-only context kept out of the editable workbook.
          </DialogDescription>
        </DialogHeader>

        <div className="workspace-detail-scroll">
          <section className="workspace-detail-section">
            <h3>Run</h3>
            {renderRows(runRows)}
          </section>

          <section className="workspace-detail-section">
            <h3>Settings</h3>
            {renderRows(settingsRows)}
          </section>

          <section className="workspace-detail-section">
            <h3>Provenance</h3>
            {renderRows(provenanceRows)}
          </section>

          <section className="workspace-detail-section">
            <h3>Documents</h3>
            {documentRows.length > 0 ? (
              <div className="workspace-detail-docs">
                {documentRows.map((document) => (
                  <div key={document.name} className="workspace-detail-doc">
                    <div className="workspace-detail-doc-name">{document.name}</div>
                    <div className="workspace-detail-doc-meta">
                      {document.rowCount} rows{document.url ? ` / ${document.url}` : ''}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">No document details available.</div>
            )}
          </section>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function SpreadsheetSurface({
  activeSheet,
  data,
  schema,
  displayOptions,
  cellFormats,
  formatVersion,
  hotTableRef,
  onSelectionChange,
  onRefresh,
  onEditFollowUp,
  onEditEnd,
  layoutRevision,
}: {
  activeSheet: SheetId;
  data: PaginatedData;
  schema: SchemaData | null;
  displayOptions: TableDisplayOptions;
  cellFormats: CellFormatMap;
  formatVersion: number;
  hotTableRef: MutableRefObject<HotTableClass | null>;
  onSelectionChange: (selection: SheetSelection) => void;
  onRefresh: () => void;
  onEditFollowUp: (kind: PendingRerunKind, columns?: string[]) => void;
  onEditEnd: () => void;
  layoutRevision: string;
}) {
  const { sessionId } = useParams();
  const { toast } = useToast();
  const gridContainerRef = useRef<HTMLDivElement | null>(null);
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
    const element = gridContainerRef.current;
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
  }, [activeSheet, layoutRevision, measureGrid]);

  useEffect(() => {
    syncHotTableDimensions();
  }, [gridSize, syncHotTableDimensions]);

  const schemaColumns = useMemo(() => {
    const cols = (schema?.schema || []) as Array<ColumnInfo & { allowed_values?: string[] }>;
    return cols;
  }, [schema]);

  const dataColumnNames = useMemo(() => {
    const names = new Set<string>();
    schemaColumns.forEach((column) => names.add(column.name));
    data.rows.forEach((row) => {
      Object.keys(row.data || {}).forEach((name) => names.add(name));
    });
    return Array.from(names);
  }, [data.rows, schemaColumns]);

  const dataRows = useMemo(() => {
    return data.rows.map((row) => {
      const sheetRow: Record<string, string> = {
        _row_name: row.row_name || row._unit_name || '',
      };
      dataColumnNames.forEach((column) => {
        sheetRow[column] = extractDisplayValue(row.data?.[column]);
      });
      return sheetRow;
    });
  }, [data.rows, dataColumnNames]);

  const schemaRows = useMemo(() => {
    return schemaColumns.map((column) => ({
      name: column.name || '',
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
          { key: 'name', label: 'name', width: 180 },
          { key: 'definition', label: 'definition', width: 360 },
          { key: 'rationale', label: 'rationale', width: 320 },
          { key: 'allowed_values', label: 'allowed_values', width: 260 },
          { key: 'auto_expand_threshold', label: 'auto_expand_threshold', width: 150 },
        ],
      };
    }

    if (activeSheet === 'unit') {
      return {
        rows: observationUnitRows,
        columns: [
          { key: 'field', label: 'field', width: 190, readOnly: true },
          { key: 'value', label: 'value', width: 680 },
        ],
      };
    }

    return {
      rows: dataRows,
      columns: [
        { key: '_row_name', label: 'unit_name', width: 220, readOnly: true },
        ...dataColumnNames.map((name) => ({ key: name, label: name, width: 190 })),
      ],
    };
  }, [activeSheet, dataColumnNames, dataRows, observationUnitRows, schemaRows]);

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
        if (!rowName && rowIndexId == null) {
          toast({
            title: 'Cell update failed',
            description: 'Could not identify which row to update.',
            variant: 'destructive',
          });
          onRefresh();
          continue;
        }

        schematiqAPI.updateCell(
          sessionId,
          rowName,
          key,
          String(newValue ?? ''),
          sourceRow?._source_document || sourceRow?._parent_document,
          rowIndexId
        )
          .then(() => {
            const rowLabel = rowName || (rowIndexId != null ? `Row ${rowIndexId + 1}` : 'Row');
            toast({ title: 'Cell updated', description: `${rowLabel} / ${key}` });
            onRefresh();
          })
          .catch((err: any) => {
            toast({
              title: 'Cell update failed',
              description: err?.response?.data?.detail || err?.message || 'Could not update cell',
              variant: 'destructive',
            });
            onRefresh();
          });
      }

      if (activeSheet === 'schema') {
        const existing = schemaColumns[rowIndex];
        const editable = ['name', 'definition', 'rationale', 'allowed_values', 'auto_expand_threshold'];
        if (!editable.includes(key)) continue;

        if (!existing && key === 'name' && String(newValue || '').trim()) {
          schemaAPI.addColumn(sessionId, {
            name: String(newValue).trim(),
            definition: '',
            rationale: '',
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
        if (key === 'allowed_values') request.allowed_values = parseAllowedValues(newValue);

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
  }, [activeSheet, data.rows, observationUnitRows, onEditFollowUp, onRefresh, schemaColumns, sessionId, toast]);

  if (!sessionId) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        Start or open a project to populate the workbook.
      </div>
    );
  }

  return (
    <div
      ref={gridContainerRef}
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
      {gridSize.width > 0 && gridSize.height > 0 && <HotTable
        ref={hotTableRef}
        key={`${activeSheet}-${sheet.columns.length}-${formatVersion}`}
        className="workspace-hot"
        theme="ht-theme-main"
        data={sheet.rows}
        columns={sheet.columns.map((column) => ({
          data: column.key,
          readOnly: column.readOnly,
          width: column.width,
        }))}
        colHeaders={sheet.columns.map((column) => column.label)}
        rowHeaders
        width={gridSize.width}
        height={gridSize.height}
        stretchH="none"
        manualColumnResize
        manualRowResize
        contextMenu
        filters
        dropdownMenu
        columnSorting
        copyPaste
        undo
        minSpareRows={sheet.minSpareRows || 0}
        licenseKey="non-commercial-and-evaluation"
        afterInit={syncHotTableDimensions}
        afterChange={(changes, source) => {
          handleChanges(changes, source);
          if (source !== 'loadData') onEditEnd();
        }}
        afterDeselect={onEditEnd}
        afterSelectionEnd={(row: number, col: number, row2: number, col2: number) => {
          if (row < 0 || col < 0 || row2 < 0 || col2 < 0) {
            onSelectionChange(null);
            return;
          }
          onSelectionChange({
            sheet: activeSheet,
            fromRow: Math.min(row, row2),
            toRow: Math.max(row, row2),
            fromCol: Math.min(col, col2),
            toCol: Math.max(col, col2),
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
          return props;
        }}
      />}
    </div>
  );
}

const CHAT_MUTATION_TOOLS = new Set([
  'add_column',
  'edit_column',
  'delete_column',
  'merge_columns',
  'update_cell',
  'add_unit',
  'remove_unit',
  'edit_observation_unit',
  'run_schematiq',
  'reextract',
  'continue_discovery',
  'reprocess',
]);

const CHAT_SCHEMA_FOLLOWUP_TOOLS = new Set([
  'add_column',
  'edit_column',
  'delete_column',
  'merge_columns',
]);

function mapChatTurnMessage(message: ChatTurnMessage): WorkspaceMessage {
  return {
    id: message.id,
    role: message.role,
    content: message.content,
    kind: message.kind,
    toolName: message.tool_name,
    toolStatus: message.tool_status,
  };
}

function formatToolsList(tools: ChatToolInfo[]): string {
  if (!tools.length) {
    return 'No tools are available in the current context.';
  }
  return tools
    .map((tool) => {
      const badge = tool.cost_class === 'expensive' ? ' [cost]' : '';
      const status = tool.available ? '' : ' (planned)';
      return `• ${tool.name}${badge}${status}: ${tool.description}`;
    })
    .join('\n');
}

function ChatPanel({
  sessionId,
  sessionMode,
  onRefresh,
  onEditFollowUp,
}: {
  sessionId?: string;
  sessionMode: WorkspaceSessionMode;
  status: ScheMatiQStatus | null;
  schema: SchemaData | null;
  data: PaginatedData;
  onRefresh: () => void;
  onEditFollowUp: (kind: PendingRerunKind, columns?: string[]) => void;
}) {
  const [messages, setMessages] = useState<WorkspaceMessage[]>([
    {
      id: 'hello',
      role: 'assistant',
      content:
        'Ask me to inspect or edit this project. I use workspace tools to read schema and data before making changes. Type /tools to list available tools.',
    },
  ]);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const [chatId, setChatId] = useState<string | null>(null);
  const [pinnedTool, setPinnedTool] = useState<string | null>(null);
  const [availableTools, setAvailableTools] = useState<ChatToolInfo[]>([]);
  const [pendingAction, setPendingAction] = useState<PendingChatAction | null>(null);
  const messagesRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const container = messagesRef.current;
    if (!container) return;

    container.scrollTo({
      top: container.scrollHeight,
      behavior: 'smooth',
    });
  }, [messages, pendingAction]);

  const loadTools = useCallback(async () => {
    const tools = await chatAPI.getTools(sessionId, sessionMode);
    setAvailableTools(tools.filter((tool) => tool.available));
    return tools;
  }, [sessionId, sessionMode]);

  useEffect(() => {
    loadTools().catch(() => {
      setAvailableTools([]);
    });
  }, [loadTools]);

  const appendMessages = useCallback((next: WorkspaceMessage[]) => {
    setMessages((current) => [...current, ...next]);
  }, []);

  const applyChatResponse = useCallback((response: Awaited<ReturnType<typeof chatAPI.sendMessage>>) => {
    setChatId(response.chat_id);
    appendMessages(response.messages.map(mapChatTurnMessage));
    if (response.pending_action) {
      setPendingAction({
        id: response.pending_action.tool_name,
        label: response.pending_action.label,
        description: response.pending_action.description,
        chatId: response.chat_id,
      });
    } else {
      setPendingAction(null);
    }
    const completedTools = response.messages.filter(
      (message) =>
        message.kind === 'tool_log'
        && message.tool_status === 'done'
        && message.tool_name,
    );

    if (completedTools.some((message) => CHAT_MUTATION_TOOLS.has(message.tool_name!))) {
      onRefresh();
    }

    const alreadyFollowedUp = completedTools.some((message) =>
      message.tool_name === 'reextract'
      || message.tool_name === 'reprocess'
      || message.tool_name === 'run_schematiq'
      || message.tool_name === 'continue_discovery',
    );

    if (!alreadyFollowedUp && completedTools.some((message) => message.tool_name === 'edit_observation_unit')) {
      onEditFollowUp('unit');
    } else if (!alreadyFollowedUp && completedTools.some((message) => CHAT_SCHEMA_FOLLOWUP_TOOLS.has(message.tool_name!))) {
      const editedColumns = completedTools
        .filter((message) => CHAT_SCHEMA_FOLLOWUP_TOOLS.has(message.tool_name!))
        .flatMap((message) => message.columns ?? []);
      // Only prompt a re-extract when a column was added/edited/merged; a
      // delete-only change yields no columns and needs no re-extraction.
      if (editedColumns.length > 0) {
        onEditFollowUp('schema', editedColumns);
      }
    }
  }, [appendMessages, onEditFollowUp, onRefresh]);

  const showToolsList = useCallback(async () => {
    setBusy(true);
    try {
      const tools = await loadTools();
      appendMessages([
        {
          id: `${Date.now()}-tools`,
          role: 'assistant',
          content: `Available tools:\n\n${formatToolsList(tools)}`,
        },
      ]);
    } catch (err: any) {
      appendMessages([
        {
          id: `${Date.now()}-tools-error`,
          role: 'assistant',
          content: err?.response?.data?.detail || err?.message || 'Could not load tools.',
        },
      ]);
    } finally {
      setBusy(false);
    }
  }, [appendMessages, loadTools]);

  const ask = useCallback(async () => {
    const text = input.trim();
    if (!text || busy) return;
    setInput('');
    appendMessages([{ id: `${Date.now()}-user`, role: 'user', content: text }]);

    if (text.toLowerCase().startsWith('/tools')) {
      await showToolsList();
      return;
    }

    if (!sessionId) {
      appendMessages([
        {
          id: `${Date.now()}-no-session`,
          role: 'assistant',
          content: 'Open > New Project or Import Project to get started. Once a project exists I can inspect and edit it.',
        },
      ]);
      return;
    }

    setBusy(true);
    setPendingAction(null);
    try {
      const response = await chatAPI.sendMessage(sessionId, {
        message: text,
        chat_id: chatId || undefined,
        session_mode: sessionMode,
        pinned_tool: pinnedTool || undefined,
      });
      applyChatResponse(response);
    } catch (err: any) {
      appendMessages([
        {
          id: `${Date.now()}-error`,
          role: 'assistant',
          content: err?.response?.data?.detail || err?.message || 'That workspace action failed.',
        },
      ]);
    } finally {
      setBusy(false);
    }
  }, [
    appendMessages,
    applyChatResponse,
    busy,
    chatId,
    input,
    pinnedTool,
    sessionId,
    sessionMode,
    showToolsList,
  ]);

  const confirmPendingAction = useCallback(async () => {
    if (!pendingAction || !sessionId) return;
    setBusy(true);
    try {
      const response = await chatAPI.confirmAction(sessionId, pendingAction.chatId);
      applyChatResponse(response);
    } catch (err: any) {
      appendMessages([
        {
          id: `${Date.now()}-confirm-error`,
          role: 'assistant',
          content: err?.response?.data?.detail || err?.message || 'The confirmed action failed.',
        },
      ]);
    } finally {
      setBusy(false);
    }
  }, [appendMessages, applyChatResponse, pendingAction, sessionId]);

  const cancelPendingAction = useCallback(async () => {
    const action = pendingAction;
    setPendingAction(null);
    if (!action || !sessionId) return;
    try {
      const response = await chatAPI.cancelAction(sessionId, action.chatId);
      applyChatResponse(response);
    } catch {
      // Best-effort: the next expensive call overwrites the server's pending
      // slot regardless, so a failed cancel is non-fatal.
    }
  }, [applyChatResponse, pendingAction, sessionId]);

  return (
    <aside className="workspace-chat">
      <div className="workspace-chat-header">
        <div className="flex items-center gap-2 text-sm font-medium">
          <Bot className="h-4 w-4" />
          Chat
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="outline">gemini-3.1-flash-lite</Badge>
          <Button size="sm" variant="outline" onClick={showToolsList} disabled={busy}>
            Tools
          </Button>
        </div>
      </div>

      {availableTools.length > 0 && (
        <div className="workspace-chat-tools px-3 pb-2">
          <select
            className="w-full rounded-md border bg-background px-2 py-1 text-xs"
            value={pinnedTool || ''}
            onChange={(event) => setPinnedTool(event.target.value || null)}
          >
            <option value="">Pin a tool (optional)</option>
            {availableTools.map((tool) => (
              <option key={tool.name} value={tool.name}>
                {tool.name}
                {tool.cost_class === 'expensive' ? ' [cost]' : ''}
              </option>
            ))}
          </select>
        </div>
      )}

      <div className="workspace-chat-messages" ref={messagesRef}>
        {messages.map((message) => (
          <div
            key={message.id}
            className={`workspace-chat-message${message.kind === 'tool_log' ? ' workspace-chat-tool-log' : ''}`}
            data-role={message.role}
            data-tool-status={message.toolStatus}
          >
            {message.content}
          </div>
        ))}

        {pendingAction && (
          <div className="rounded-md border bg-muted/30 p-3 text-sm">
            <div className="font-medium">{pendingAction.label}</div>
            <div className="mt-1 text-muted-foreground">{pendingAction.description}</div>
            <div className="mt-3 flex gap-2">
              <Button size="sm" onClick={confirmPendingAction} disabled={busy}>
                <Check className="h-4 w-4" />
                Confirm
              </Button>
              <Button size="sm" variant="outline" onClick={cancelPendingAction} disabled={busy}>
                <X className="h-4 w-4" />
                Cancel
              </Button>
            </div>
          </div>
        )}
      </div>

      <div className="workspace-chat-input">
        <Textarea
          value={input}
          onChange={(event) => setInput(event.target.value)}
          placeholder="Ask ScheMatiQ or type /tools"
          rows={3}
          disabled={busy}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
              event.preventDefault();
              ask();
            }
          }}
        />
      </div>
    </aside>
  );
}

function PendingRerunBanner({
  kind,
  columns,
  sessionMode,
  busy,
  onReextract,
  onRediscover,
  onDismiss,
}: {
  kind: PendingRerunKind;
  columns: string[];
  sessionMode: WorkspaceSessionMode;
  busy: boolean;
  onReextract: () => void;
  onRediscover: () => void;
  onDismiss: () => void;
}) {
  const columnSummary = columns.length > 0
    ? columns.slice(0, 3).join(', ') + (columns.length > 3 ? ` +${columns.length - 3} more` : '')
    : 'all columns';

  return (
    <div className="workspace-followup-banner" role="status">
      <div className="workspace-followup-banner-copy">
        <strong>
          {kind === 'unit' ? 'Observation unit changed' : 'Schema changed'}
        </strong>
        <span>
          {kind === 'unit'
            ? 'Changing the unit changes row granularity: rediscover the schema, then re-extract all data.'
            : `Re-extract to refresh values from source documents (${columnSummary}).`}
        </span>
      </div>
      <div className="workspace-followup-banner-actions">
        {kind === 'unit' ? (
          <button
            className="workspace-followup-action workspace-followup-action-primary"
            type="button"
            onClick={onRediscover}
            disabled={busy || sessionMode !== 'schematiq'}
            title={sessionMode !== 'schematiq' ? 'Schema rediscovery requires a ScheMatiQ project with source documents' : undefined}
          >
            {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Sparkles className="h-3.5 w-3.5" />}
            Rediscover schema &amp; re-extract
          </button>
        ) : (
          <button
            className="workspace-followup-action workspace-followup-action-primary"
            type="button"
            onClick={onReextract}
            disabled={busy}
          >
            {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RotateCw className="h-3.5 w-3.5" />}
            Re-extract table
          </button>
        )}
        <button className="workspace-followup-action workspace-followup-action-ghost" type="button" onClick={onDismiss} disabled={busy}>
          Dismiss
        </button>
      </div>
    </div>
  );
}

function SpreadsheetChrome({
  projectTitle,
  sessionStatus,
  canUseProjectActions,
  displayOptions,
  onNewProject,
  onImportProject,
  onOpenClassic,
  onProjectDetails,
  onRefresh,
  onPrint,
  onExport,
  onSearch,
  onEstimateCost,
  onShowSheet,
  onShowChat,
  onSplitView,
  onRunPendingEdits,
  onApplyFormat,
  rerunDisabled,
}: {
  projectTitle: string;
  sessionStatus: string;
  canUseProjectActions: boolean;
  displayOptions: TableDisplayOptions;
  onNewProject: () => void;
  onImportProject: () => void;
  onOpenClassic: () => void;
  onProjectDetails: () => void;
  onRefresh: () => void;
  onPrint: () => void;
  onExport: () => void;
  onSearch: () => void;
  onEstimateCost: () => void;
  onShowSheet: () => void;
  onShowChat: () => void;
  onSplitView: () => void;
  onRunPendingEdits: () => void;
  onApplyFormat: (patch: Partial<TableDisplayOptions>) => void;
  rerunDisabled: boolean;
}) {
  const runMenuItem = (label: string) => {
    if (label === 'New project') onNewProject();
    if (label === 'Import project') onImportProject();
    if (label === 'Open classic visualizer') onOpenClassic();
    if (label === 'Export table') onExport();
    if (label === 'Project details') onProjectDetails();
    if (label === 'Refresh project') onRefresh();
    if (label === 'Estimate cost') onEstimateCost();
    if (label === 'Show sheet full screen') onShowSheet();
    if (label === 'Show chat full screen') onShowChat();
    if (label === 'Split view') onSplitView();
    if (label === 'Re-extract table') onRunPendingEdits();
  };

  const isDisabled = (label: string) => {
    if (label === 'New project' || label === 'Import project') return false;
    if (label === 'Re-extract table') return rerunDisabled;
    return !canUseProjectActions && [
      'Open classic visualizer',
      'Export table',
      'Project details',
      'Refresh project',
      'Estimate cost',
      'Show sheet full screen',
      'Show chat full screen',
      'Split view',
      'Re-extract table',
    ].includes(label);
  };

  return (
    <div className="workspace-chrome" role="toolbar" aria-label="Spreadsheet menu and formatting toolbar">
      <div className="workspace-chrome-titlebar">
        <div className="workspace-file-mark">
          <Table2 className="h-4 w-4" />
        </div>
        <div className="workspace-file-title">
          <div className="workspace-file-name">{projectTitle}</div>
          <div className="workspace-file-status">{sessionStatus}</div>
        </div>
        <div className="workspace-menu-row">
          {WORKSPACE_MENUS.map((menu) => (
            <DropdownMenu key={menu.label}>
              <DropdownMenuTrigger asChild>
                <button className="workspace-menu-button" type="button">
                  {menu.label}
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="start" className="workspace-menu-content w-56">
                {menu.items.map((item) => (
                  <DropdownMenuItem
                    key={item}
                    disabled={isDisabled(item)}
                    onClick={() => runMenuItem(item)}
                  >
                    {item}
                  </DropdownMenuItem>
                ))}
              </DropdownMenuContent>
            </DropdownMenu>
          ))}
        </div>
      </div>

      <div className="workspace-toolbar-row">
        <button className="workspace-toolbar-icon" type="button" onClick={onPrint} title="Print">
          <Printer className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" onClick={onExport} disabled={!canUseProjectActions} title="Export">
          <Download className="h-3.5 w-3.5" />
        </button>

        <span className="workspace-toolbar-separator" />

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="workspace-toolbar-select workspace-toolbar-font" type="button">
              {displayOptions.fontFamily}
              <ChevronDown className="h-3.5 w-3.5" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="workspace-menu-content w-40">
            {TABLE_FONT_OPTIONS.map((font) => (
              <DropdownMenuItem key={font} onClick={() => onApplyFormat({ fontFamily: font })}>
                {font}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="workspace-toolbar-select workspace-toolbar-size" type="button">
              {displayOptions.fontSize}
              <ChevronDown className="h-3.5 w-3.5" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="workspace-menu-content w-28">
            {TABLE_FONT_SIZE_OPTIONS.map((size) => (
              <DropdownMenuItem key={size} onClick={() => onApplyFormat({ fontSize: size })}>
                {size}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>

        <span className="workspace-toolbar-separator" />

        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.bold} onClick={() => onApplyFormat({ bold: !displayOptions.bold })} title="Bold">
          <Bold className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.italic} onClick={() => onApplyFormat({ italic: !displayOptions.italic })} title="Italic">
          <Italic className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.underline} onClick={() => onApplyFormat({ underline: !displayOptions.underline })} title="Underline">
          <Underline className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.strikethrough} onClick={() => onApplyFormat({ strikethrough: !displayOptions.strikethrough })} title="Strikethrough">
          <Strikethrough className="h-3.5 w-3.5" />
        </button>

        <span className="workspace-toolbar-separator" />

        <button className="workspace-toolbar-icon" type="button" title="Text color">
          <Type className="h-3.5 w-3.5" />
        </button>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="workspace-toolbar-icon" type="button" title="Align">
              <AlignLeft className="h-3.5 w-3.5" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="workspace-menu-content w-32">
            {(['left', 'center', 'right'] as TableTextAlign[]).map((align) => (
              <DropdownMenuItem key={align} onClick={() => onApplyFormat({ align })}>
                {align[0].toUpperCase() + align.slice(1)}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
        <button className="workspace-toolbar-icon" type="button" title="Functions">
          <Sigma className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" onClick={onSearch} title="Search">
          <Search className="h-3.5 w-3.5" />
        </button>

        <span className="workspace-toolbar-spacer" />

        <button className="workspace-toolbar-action" type="button" onClick={onEstimateCost} disabled={!canUseProjectActions}>
          <Sparkles className="h-3.5 w-3.5" />
          Estimate
        </button>
        <button
          className="workspace-toolbar-action"
          type="button"
          onClick={onRunPendingEdits}
          disabled={rerunDisabled}
          title="Re-extract values from source documents after schema or observation-unit edits"
        >
          <RotateCw className="h-3.5 w-3.5" />
          Re-extract
        </button>
      </div>
    </div>
  );
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
  const [schema, setSchema] = useState<SchemaData | null>(null);
  const [data, setData] = useState<PaginatedData>(emptyData);
  const [documents, setDocuments] = useState<DocumentListResponse | null>(null);
  const [config, setConfig] = useState<ScheMatiQConfig | null>(null);
  const [costEstimate, setCostEstimate] = useState<CostEstimate | null>(null);
  const [loading, setLoading] = useState(false);
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
  const [chatWidth, setChatWidth] = useState(() => {
    const saved = Number(localStorage.getItem('workspace.chatWidth'));
    return Number.isFinite(saved) ? saved : 380;
  });
  const [isDraggingDivider, setIsDraggingDivider] = useState(false);
  const [reextraction, setReextraction] = useState<WorkspaceReextractionState | null>(null);
  const [reextractConfirm, setReextractConfirm] = useState<{ columns: string[] } | null>(null);
  const [stoppingReextraction, setStoppingReextraction] = useState(false);

  const deferredDataRef = useRef<PaginatedData | null>(null);

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
    if (!options?.silent) {
      setLoading(true);
    }
    try {
      if (sessionMode === 'load') {
        const [loadSession, nextData, nextDocuments] = await Promise.all([
          loadAPI.getSession(sessionId).catch(() => null),
          loadAPI.getData(sessionId, 0, 500).catch(() => emptyData),
          unitsAPI.getDocuments(sessionId).catch(() => null),
        ]);
        setStatus(statusFromLoadSession(loadSession));
        setSchema(schemaFromLoadSession(loadSession));
        applyData(nextData, options);
        setDocuments(nextDocuments);
        setConfig(null);
        return;
      }

      try {
        const [nextStatus, nextSchema, nextData, nextDocuments, nextConfig] = await Promise.all([
          schematiqAPI.getStatus(sessionId),
          schematiqAPI.getSchema(sessionId).catch(() => null),
          schematiqAPI.getData(sessionId, 0, 500).catch(() => emptyData),
          unitsAPI.getDocuments(sessionId).catch(() => null),
          schematiqAPI.getConfig(sessionId).catch(() => null),
        ]);
        setStatus(nextStatus);
        setSchema(nextSchema);
        applyData(nextData, options);
        setDocuments(nextDocuments);
        setConfig(nextConfig);
      } catch (err) {
        const loadSession = await loadAPI.getSession(sessionId).catch(() => null);
        if (!loadSession) throw err;
        setSessionMode('load');
        const [nextData, nextDocuments] = await Promise.all([
          loadAPI.getData(sessionId, 0, 500).catch(() => emptyData),
          unitsAPI.getDocuments(sessionId).catch(() => null),
        ]);
        setStatus(statusFromLoadSession(loadSession));
        setSchema(schemaFromLoadSession(loadSession));
        applyData(nextData, options);
        setDocuments(nextDocuments);
        setConfig(null);
      }
    } finally {
      if (!options?.silent) {
        setLoading(false);
      }
    }
  }, [sessionId, sessionMode]);

  const inFlightSilentRef = useRef<Promise<void> | null>(null);
  const refreshSilent = useCallback(() => {
    if (inFlightSilentRef.current) return inFlightSilentRef.current;
    const run = refresh({ silent: true }).finally(() => {
      inFlightSilentRef.current = null;
    });
    inFlightSilentRef.current = run;
    return run;
  }, [refresh]);

  useEffect(() => {
    setProjectDialogOpen(!sessionId);
    setSessionMode(requestedMode);
  }, [requestedMode, sessionId]);

  useEffect(() => {
    setPendingRerunKind(null);
    setPendingSchemaColumns([]);
    setRerunStarting(false);
    setReextraction(null);
  }, [sessionId]);

  useEffect(() => {
    setSheetSelection(null);
  }, [activeSheet, sessionId]);

  useEffect(() => {
    refresh();
    if (!sessionId) return undefined;
    const interval = window.setInterval(() => refresh({ silent: true }), 5000);
    return () => window.clearInterval(interval);
  }, [refresh, sessionId]);

  useEffect(() => {
    if (!sessionId) return undefined;

    const handler = (message: WebSocketMessage) => {
      if (
        message.type === 'connected' ||
        message.type === 'heartbeat' ||
        message.type === 'pong'
      ) {
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
    };
  }, [refresh, sessionId, toast]);

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
    try {
      if (sessionMode === 'schematiq') {
        await schematiqAPI.export(sessionId);
      } else {
        const blob = await loadAPI.exportData(sessionId);
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `schematiq_import_${sessionId.slice(0, 8)}.csv`;
        link.click();
        window.URL.revokeObjectURL(url);
      }
    } catch (err: any) {
      toast({
        title: 'Export failed',
        description: err?.response?.data?.detail || err?.message || 'Could not export this project',
        variant: 'destructive',
      });
    }
  }, [sessionId, sessionMode, toast]);

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
  }, [pendingSchemaColumns, rerunStarting, schema?.schema, sessionId, toast]);

  const startReextraction = useCallback(async (targetColumns: string[]) => {
    if (!sessionId || rerunStarting || targetColumns.length === 0) return;

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
  }, [clearPendingRerun, rerunStarting, sessionId, toast]);

  const confirmReextraction = useCallback(async () => {
    if (!reextractConfirm) return;
    const { columns } = reextractConfirm;
    setReextractConfirm(null);
    await startReextraction(columns);
  }, [reextractConfirm, startReextraction]);

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
      await schematiqAPI.resume(sessionId);
      clearPendingRerun();
      toast({
        title: 'Schema rediscovery started',
        description: 'Rediscovering schema from the updated observation unit.',
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
  }, [clearPendingRerun, refresh, rerunStarting, sessionId, sessionMode, toast]);

  const notifyEditFollowUp = useCallback((kind: PendingRerunKind, columns: string[] = []) => {
    markRerunNeeded(kind, columns);

    if (kind === 'unit') {
      toast({
        title: 'Observation unit updated',
        description: sessionMode === 'schematiq'
          ? 'Changing the unit changes row granularity. Rediscover the schema, then re-extract all data.'
          : 'Imported static projects can edit the unit, but rediscovery needs a ScheMatiQ project with source documents.',
        action: sessionMode === 'schematiq' ? (
          <ToastAction altText="Rediscover schema and re-extract" onClick={() => startSchemaRediscovery()}>
            Rediscover &amp; re-extract
          </ToastAction>
        ) : undefined,
      });
      return;
    }

    toast({
      title: 'Schema updated',
      description: columns.length > 0
        ? `Re-extract ${columns.join(', ')} to refresh values from source documents.`
        : 'Re-extract to refresh values from source documents.',
      action: (
        <ToastAction altText="Re-extract columns" onClick={() => requestReextraction(columns)}>
          Re-extract
        </ToastAction>
      ),
    });
  }, [markRerunNeeded, requestReextraction, sessionMode, startSchemaRediscovery, toast]);

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
  const chromeStatus = sessionId
    ? `${sessionMode} / ${loading ? 'loading' : status?.status || 'loading'}`
    : 'No project open';
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
        onSearch={searchPage}
        onEstimateCost={estimateCurrentCost}
        onShowSheet={() => setChatWidth(0)}
        onShowChat={() => setChatWidth(window.innerWidth)}
        onSplitView={() => setChatWidth(380)}
        onRunPendingEdits={runPendingEdits}
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
          <div className="workspace-grid-wrap">
            <SpreadsheetSurface
              activeSheet={activeSheet}
              data={data}
              schema={schema}
              displayOptions={tableDisplay}
              cellFormats={cellFormats}
              formatVersion={formatVersion}
              hotTableRef={hotTableRef}
              onSelectionChange={updateSheetSelection}
              onRefresh={refreshSilent}
              onEditFollowUp={notifyEditFollowUp}
              onEditEnd={flushDeferredData}
              layoutRevision={gridLayoutRevision}
            />
            {loading && sessionId && (
              <div className="workspace-loading-overlay" role="status" aria-live="polite">
                <div className="workspace-loading-card">
                  <Loader2 className="h-5 w-5 animate-spin" />
                  <span>Loading project…</span>
                </div>
              </div>
            )}
          </div>
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
          />
        </div>
      </div>

      <div className="workspace-bottombar">
        <div className="workspace-bottombar-tabs">
          {SHEETS.map((sheet) => (
            <button
              key={sheet.id}
              className="workspace-sheet-tab"
              data-active={activeSheet === sheet.id}
              onClick={() => setActiveSheet(sheet.id)}
            >
              {sheet.label}
            </button>
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
              <Progress value={bottombarProgress} className="h-1.5" />
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
            <DropdownMenuItem onClick={() => navigate('/')}>
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
          accept=".json,.jsonl,.csv,.schematiq.json,application/json,text/csv"
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
        onOpenChange={(open) => { if (!open) setReextractConfirm(null); }}
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
          <DialogFooter>
            <Button variant="outline" onClick={() => setReextractConfirm(null)} disabled={rerunStarting}>
              Cancel
            </Button>
            <Button onClick={confirmReextraction} disabled={rerunStarting}>
              {rerunStarting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Check className="h-4 w-4" />}
              Confirm
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

export default Workspace;
