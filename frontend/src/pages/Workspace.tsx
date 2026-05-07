import { type CSSProperties, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { HotTable } from '@handsontable/react';
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
  RefreshCw,
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
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { useToast } from '@/components/ui/use-toast';
import { extractDisplayValue } from '@/components/DataTable/utils/valueUtils';
import { DEFAULT_DOCUMENT_RANDOMIZATION_SEED, DEFAULT_DOCUMENTS_BATCH_SIZE, DEFAULT_MAX_KEYS_SCHEMA } from '@/constants';
import { configAPI, loadAPI, observationUnitAPI, schemaAPI, schematiqAPI, unitsAPI } from '@/services/api';
import webSocketService from '@/services/websocket';
import {
  ColumnInfo,
  CostEstimate,
  DataRow,
  PaginatedData,
  SchemaData,
  ScheMatiQConfig,
  ScheMatiQStatus,
  VisualizationSession,
  WebSocketMessage,
} from '@/types';
import { DocumentListResponse } from '@/types/unit';

import './Workspace.css';

registerAllModules();

type SheetId = 'data' | 'unit' | 'schema';
type WorkspaceSessionMode = 'schematiq' | 'load';
type PendingRerunKind = 'schema' | 'unit';

type SheetColumn = {
  key: string;
  label: string;
  width?: number;
  readOnly?: boolean;
};

type WorkspaceMessage = {
  id: string;
  role: 'assistant' | 'user';
  content: string;
};

type PendingChatAction = {
  id: string;
  label: string;
  description: string;
  run: () => Promise<void>;
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
    items: ['Sort range', 'Create filter', 'Repopulate edited fields', 'Validate schema'],
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
  onRefresh,
  onRerunNeeded,
}: {
  activeSheet: SheetId;
  data: PaginatedData;
  schema: SchemaData | null;
  displayOptions: TableDisplayOptions;
  onRefresh: () => void;
  onRerunNeeded: (kind: PendingRerunKind, columns?: string[]) => void;
}) {
  const { sessionId } = useParams();
  const { toast } = useToast();
  const gridContainerRef = useRef<HTMLDivElement | null>(null);
  const [gridSize, setGridSize] = useState({ width: 900, height: 520 });

  useEffect(() => {
    const element = gridContainerRef.current;
    if (!element) return undefined;

    const updateSize = () => {
      const rect = element.getBoundingClientRect();
      setGridSize({
        width: Math.max(320, Math.floor(rect.width)),
        height: Math.max(260, Math.floor(rect.height)),
      });
    };

    updateSize();

    if (typeof ResizeObserver !== 'undefined') {
      const observer = new ResizeObserver(updateSize);
      observer.observe(element);
      return () => observer.disconnect();
    }

    window.addEventListener('resize', updateSize);
    return () => window.removeEventListener('resize', updateSize);
  }, []);

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
        const rowName = sourceRow?.row_name || sourceRow?._unit_name;
        if (!rowName) continue;

        schematiqAPI.updateCell(
          sessionId,
          rowName,
          key,
          String(newValue ?? ''),
          sourceRow?._source_document || sourceRow?._parent_document
        )
          .then(() => {
            toast({ title: 'Cell updated', description: `${rowName} / ${key}` });
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
              onRerunNeeded('schema', [String(newValue).trim()]);
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
              onRerunNeeded('schema', [existing.name]);
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
            if (affectedColumn) onRerunNeeded('schema', [affectedColumn]);
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
            toast({
              title: 'Observation unit updated',
              description: result.warning || `${name} saved`,
            });
            onRerunNeeded('unit');
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
  }, [activeSheet, data.rows, observationUnitRows, onRefresh, onRerunNeeded, schemaColumns, sessionId, toast]);

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
      className="h-full w-full min-h-0 min-w-0"
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
        key={`${activeSheet}-${sheet.columns.length}`}
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
        afterChange={handleChanges}
        cells={(row, col) => {
          const props: { readOnly?: boolean } = {};
          const column = sheet.columns[col];
          if (column?.readOnly) props.readOnly = true;
          if (activeSheet === 'unit' && column?.key === 'value') {
            const field = String(sheet.rows[row]?.field || '');
            props.readOnly = !EDITABLE_OBSERVATION_UNIT_FIELDS.has(field);
          }
          return props;
        }}
      />
    </div>
  );
}

function ChatPanel({
  sessionId,
  sessionMode,
  status,
  schema,
  data,
  onRefresh,
}: {
  sessionId?: string;
  sessionMode: WorkspaceSessionMode;
  status: ScheMatiQStatus | null;
  schema: SchemaData | null;
  data: PaginatedData;
  onRefresh: () => void;
}) {
  const [messages, setMessages] = useState<WorkspaceMessage[]>([
    {
      id: 'hello',
      role: 'assistant',
      content:
        'I can inspect this project, estimate expensive actions, and route simple workspace commands. The model-backed conversational agent is the next layer on top of this tool surface.',
    },
  ]);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
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

  const addMessage = useCallback((role: WorkspaceMessage['role'], content: string) => {
    setMessages((current) => [
      ...current,
      { id: `${Date.now()}-${Math.random()}`, role, content },
    ]);
  }, []);

  const ask = useCallback(async () => {
    const text = input.trim();
    if (!text) return;
    setInput('');
    setPendingAction(null);
    addMessage('user', text);

    const normalized = text.toLowerCase();
    if (!sessionId) {
      addMessage('assistant', 'Open > New Project is the starting point. Once a project exists I can inspect schema, data, status, and estimates.');
      return;
    }

    setBusy(true);
    try {
      const mentionsObservationUnit = /\bobservation\b|\bunit\b/.test(normalized);

      if (normalized.includes('status') || normalized.includes('progress')) {
        const latest = sessionMode === 'schematiq'
          ? await schematiqAPI.getStatus(sessionId)
          : status;
        addMessage('assistant', `Status: ${latest?.status || 'unknown'}\nStep: ${latest?.current_step || 'unknown'}\nProgress: ${Math.round((latest?.progress || 0) * 100)}%`);
        onRefresh();
        return;
      }

      if (mentionsObservationUnit) {
        const latest = sessionMode === 'schematiq'
          ? await schematiqAPI.getSchema(sessionId)
          : schema;
        const unit = latest?.observation_unit;
        if (!unit?.name && !unit?.definition) {
          addMessage('assistant', 'I could not find an observation unit for this project yet.');
          return;
        }
        addMessage(
          'assistant',
          `Observation unit: ${unit.name || 'unnamed'}${unit.definition ? `\n\nDefinition: ${unit.definition}` : ''}`
        );
        onRefresh();
        return;
      }

      if (normalized.includes('schema') || normalized.includes('columns')) {
        const latest = sessionMode === 'schematiq'
          ? await schematiqAPI.getSchema(sessionId)
          : schema;
        if (!latest) {
          addMessage('assistant', 'I could not find schema information for this imported project yet.');
          return;
        }
        const names = latest.schema.map((column) => column.name).slice(0, 16).join(', ');
        addMessage('assistant', `The schema currently has ${latest.schema.length} columns.${names ? `\n\nColumns: ${names}` : ''}`);
        onRefresh();
        return;
      }

      if (normalized.includes('data') || normalized.includes('table') || normalized.includes('rows')) {
        const latest = sessionMode === 'schematiq'
          ? await schematiqAPI.getData(sessionId, 0, 10)
          : await loadAPI.getData(sessionId, 0, 10);
        addMessage('assistant', `The data sheet has ${latest.total_count} rows available. I loaded a preview of ${latest.rows.length} rows into the workbook.`);
        onRefresh();
        return;
      }

      if (normalized.includes('cost') || normalized.includes('estimate')) {
        if (sessionMode === 'load') {
          addMessage('assistant', 'This imported project is already loaded, so there is no pending extraction cost to estimate. Cost estimates apply when starting or re-running LLM-backed ScheMatiQ operations.');
          return;
        }
        const estimate = await schematiqAPI.estimateCost(sessionId);
        addMessage('assistant', `Estimated current full run cost:\n${formatCost(estimate)}`);
        return;
      }

      if (normalized.includes('run') || normalized.includes('start extraction') || normalized.includes('start schematiq')) {
        if (sessionMode === 'load') {
          addMessage('assistant', 'This is an imported static project. Starting extraction from here needs a follow-up tool that connects imported data to source documents; for now, use New Project for a fresh ScheMatiQ run.');
          return;
        }
        const estimate = await schematiqAPI.estimateCost(sessionId);
        addMessage('assistant', `This is an expensive tool call. Estimated full run cost:\n${formatCost(estimate)}\n\nConfirm to start ScheMatiQ execution.`);
        setPendingAction({
          id: 'run-schematiq',
          label: 'Start ScheMatiQ',
          description: formatCost(estimate),
          run: async () => {
            await schematiqAPI.run(sessionId);
            addMessage('assistant', 'Confirmed. ScheMatiQ execution has started, and workbook progress will update here.');
            onRefresh();
          },
        });
        return;
      }

      if (normalized.includes('web') || normalized.includes('internet')) {
        addMessage('assistant', 'Web access is not wired into the app chat yet. I would add it as an explicit tool with a confirmation step before leaving local project context.');
        return;
      }

      const columnCount = schema?.schema.length || 0;
      addMessage(
        'assistant',
        `I can help through project tools right now. Try "status", "show schema", "estimate cost", or "show data".\n\nCurrent context: ${status?.status || 'unknown'} status, ${columnCount} schema columns, ${data.total_count || 0} data rows.`
      );
    } catch (err: any) {
      addMessage('assistant', err?.response?.data?.detail || err?.message || 'That workspace action failed.');
    } finally {
      setBusy(false);
    }
  }, [addMessage, data.total_count, input, onRefresh, schema, sessionId, sessionMode, status]);

  const confirmPendingAction = useCallback(async () => {
    if (!pendingAction) return;
    setBusy(true);
    try {
      await pendingAction.run();
      setPendingAction(null);
    } catch (err: any) {
      addMessage('assistant', err?.response?.data?.detail || err?.message || 'The confirmed action failed.');
    } finally {
      setBusy(false);
    }
  }, [addMessage, pendingAction]);

  return (
    <aside className="workspace-chat">
      <div className="workspace-chat-header">
        <div className="flex items-center gap-2 text-sm font-medium">
          <Bot className="h-4 w-4" />
          Chat
        </div>
        <Badge variant="outline">tool scaffold</Badge>
      </div>

      <div className="workspace-chat-messages" ref={messagesRef}>
        {messages.map((message) => (
          <div key={message.id} className="workspace-chat-message" data-role={message.role}>
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
              <Button size="sm" variant="outline" onClick={() => setPendingAction(null)} disabled={busy}>
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
          placeholder="Ask ScheMatiQ"
          rows={3}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
              ask();
            }
          }}
        />
      </div>
    </aside>
  );
}

function SpreadsheetChrome({
  projectTitle,
  sessionStatus,
  loading,
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
  onDisplayOptionsChange,
  rerunDisabled,
}: {
  projectTitle: string;
  sessionStatus: string;
  loading: boolean;
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
  onDisplayOptionsChange: (next: TableDisplayOptions) => void;
  rerunDisabled: boolean;
}) {
  const updateDisplay = (patch: Partial<TableDisplayOptions>) => {
    onDisplayOptionsChange({ ...displayOptions, ...patch });
  };

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
    if (label === 'Repopulate edited fields') onRunPendingEdits();
  };

  const isDisabled = (label: string) => {
    if (label === 'New project' || label === 'Import project') return false;
    if (label === 'Repopulate edited fields') return rerunDisabled;
    return !canUseProjectActions && [
      'Open classic visualizer',
      'Export table',
      'Project details',
      'Refresh project',
      'Estimate cost',
      'Show sheet full screen',
      'Show chat full screen',
      'Split view',
      'Repopulate edited fields',
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
        <button className="workspace-toolbar-icon" type="button" onClick={onRefresh} disabled={!canUseProjectActions || loading} title="Refresh">
          {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
        </button>
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
              <DropdownMenuItem key={font} onClick={() => updateDisplay({ fontFamily: font })}>
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
              <DropdownMenuItem key={size} onClick={() => updateDisplay({ fontSize: size })}>
                {size}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>

        <span className="workspace-toolbar-separator" />

        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.bold} onClick={() => updateDisplay({ bold: !displayOptions.bold })} title="Bold">
          <Bold className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.italic} onClick={() => updateDisplay({ italic: !displayOptions.italic })} title="Italic">
          <Italic className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.underline} onClick={() => updateDisplay({ underline: !displayOptions.underline })} title="Underline">
          <Underline className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.strikethrough} onClick={() => updateDisplay({ strikethrough: !displayOptions.strikethrough })} title="Strikethrough">
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
              <DropdownMenuItem key={align} onClick={() => updateDisplay({ align })}>
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
        <button className="workspace-toolbar-action" type="button" onClick={onRunPendingEdits} disabled={rerunDisabled}>
          <RotateCw className="h-3.5 w-3.5" />
          Repopulate
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
  const [chatWidth, setChatWidth] = useState(() => {
    const saved = Number(localStorage.getItem('workspace.chatWidth'));
    return Number.isFinite(saved) ? saved : 380;
  });
  const [isDraggingDivider, setIsDraggingDivider] = useState(false);

  const refresh = useCallback(async () => {
    if (!sessionId) return;
    setLoading(true);
    try {
      if (sessionMode === 'load') {
        const [loadSession, nextData, nextDocuments] = await Promise.all([
          loadAPI.getSession(sessionId).catch(() => null),
          loadAPI.getData(sessionId, 0, 500).catch(() => emptyData),
          unitsAPI.getDocuments(sessionId).catch(() => null),
        ]);
        setStatus(statusFromLoadSession(loadSession));
        setSchema(schemaFromLoadSession(loadSession));
        setData(nextData);
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
        setData(nextData);
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
        setData(nextData);
        setDocuments(nextDocuments);
        setConfig(null);
      }
    } finally {
      setLoading(false);
    }
  }, [sessionId, sessionMode]);

  useEffect(() => {
    setProjectDialogOpen(!sessionId);
    setSessionMode(requestedMode);
  }, [requestedMode, sessionId]);

  useEffect(() => {
    setPendingRerunKind(null);
    setPendingSchemaColumns([]);
    setRerunStarting(false);
  }, [sessionId]);

  useEffect(() => {
    refresh();
    if (!sessionId) return undefined;
    const interval = window.setInterval(refresh, 5000);
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
        message.type === 'reextraction_started' ||
        message.type === 'reextraction_progress' ||
        message.type === 'reextraction_completed' ||
        message.type === 'observation_unit_definition_updated'
      ) {
        refresh();
      }
    };

    webSocketService.addMessageHandler(handler);
    webSocketService.connect(sessionId, 'progress');

    return () => {
      webSocketService.removeMessageHandler(handler);
      webSocketService.disconnect();
    };
  }, [refresh, sessionId]);

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

  const runPendingEdits = useCallback(async () => {
    if (!sessionId || !pendingRerunKind || rerunStarting) return;

    setRerunStarting(true);
    try {
      if (pendingRerunKind === 'unit') {
        if (sessionMode !== 'schematiq') {
          toast({
            title: 'Rediscovery needs a ScheMatiQ run',
            description: 'Imported static projects can edit the observation unit, but rediscovering schema requires a ScheMatiQ project with source documents.',
            variant: 'destructive',
          });
          return;
        }

        await schematiqAPI.resume(sessionId);
        toast({
          title: 'Rerun started',
          description: 'Schema rediscovery started from the updated observation unit.',
        });
      } else {
        const columns = pendingSchemaColumns.length > 0
          ? pendingSchemaColumns
          : (schema?.schema || []).map((column) => column.name).filter(Boolean);

        await schemaAPI.reprocessDocuments(sessionId, {
          columns,
          incremental: true,
          force_reprocess: true,
        });
        toast({
          title: 'Rerun started',
          description: columns.length > 0
            ? `Repopulating data for ${columns.length} schema column(s).`
            : 'Repopulating data from the current schema.',
        });
      }

      setPendingRerunKind(null);
      setPendingSchemaColumns([]);
      await refresh();
    } catch (err: any) {
      toast({
        title: 'Rerun failed to start',
        description: err?.response?.data?.detail || err?.message || 'Could not start the rerun',
        variant: 'destructive',
      });
    } finally {
      setRerunStarting(false);
    }
  }, [pendingRerunKind, pendingSchemaColumns, refresh, rerunStarting, schema?.schema, sessionId, sessionMode, toast]);

  const progressPercent = Math.round((status?.progress || 0) * 100);
  const topbarQuestion = schema?.query || config?.query || '';
  const projectTitle = topbarQuestion || (sessionId ? `ScheMatiQ ${sessionId.slice(0, 8)}` : 'Untitled workspace');
  const chromeStatus = sessionId
    ? `${sessionMode} / ${status?.status || 'loading'}`
    : 'No project open';
  const rerunTitle = pendingRerunKind === 'unit'
    ? 'Rediscover schema and repopulate data'
    : pendingRerunKind === 'schema'
      ? 'Repopulate data from schema edits'
      : 'No rerun needed';
  const isSheetHidden = chatWidth >= window.innerWidth - 80;
  const isChatHidden = chatWidth <= 24;
  const bodyGridColumns = isSheetHidden
    ? '0px 8px minmax(0, 1fr)'
    : isChatHidden
      ? 'minmax(0, 1fr) 8px 0px'
      : `minmax(0, 1fr) 8px ${chatWidth}px`;

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
    <div className="workspace-root">
      <SpreadsheetChrome
        projectTitle={projectTitle}
        sessionStatus={chromeStatus}
        loading={loading}
        canUseProjectActions={Boolean(sessionId)}
        displayOptions={tableDisplay}
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
        onDisplayOptionsChange={updateTableDisplay}
        rerunDisabled={!sessionId || !pendingRerunKind || rerunStarting}
      />

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
              onRefresh={refresh}
              onRerunNeeded={markRerunNeeded}
            />
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
            onRefresh={refresh}
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

        <div className="workspace-topbar-status" title={status?.current_step || status?.status || 'No project status'}>
          {status ? (
            <>
              <Progress value={progressPercent} className="h-1.5" />
              <span className="workspace-topbar-percent">{progressPercent}%</span>
            </>
          ) : (
            <span className="workspace-topbar-status-empty">No status</span>
          )}
        </div>

        <Button
          size="icon"
          variant="ghost"
          className="workspace-rerun-button"
          data-pending={Boolean(pendingRerunKind)}
          data-kind={pendingRerunKind || 'none'}
          onClick={runPendingEdits}
          disabled={!sessionId || !pendingRerunKind || rerunStarting}
          title={rerunTitle}
          aria-label={rerunTitle}
        >
          {rerunStarting ? <Loader2 className="h-4 w-4 animate-spin" /> : <RotateCw className="h-4 w-4" />}
        </Button>

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
            <DropdownMenuItem onClick={refresh} disabled={!sessionId || loading}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
              Refresh
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
    </div>
  );
}

export default Workspace;
