import { type CSSProperties, type MutableRefObject, Fragment, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
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
  Cloud,
  Italic,
  Loader2,
  Play,
  Plus,
  Printer,
  RotateCw,
  Save,
  Search,
  Sparkles,
  Strikethrough,
  Sigma,
  Table2,
  Type,
  Underline,
  X,
  MoreVertical,
  PanelLeft,
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
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { useToast } from '@/components/ui/use-toast';
import { extractDisplayValue } from '@/components/DataTable/utils/valueUtils';
import StatsDashboard from '@/components/StatsDashboard/StatsDashboard';
import ScheMatiQMonitor from '@/components/ScheMatiQMonitor/ScheMatiQMonitor';
import DocumentViewer from '@/components/DocumentViewer/DocumentViewer';
import DocumentPreview from '@/components/DocumentViewer/DocumentPreview';
import { ViewModeToggle } from '@/components/ViewMode/ViewModeToggle';
import MissingDocumentsSection from '@/components/SchemaEditor/MissingDocumentsSection';
import {
  buildExcerptMapping,
  resolveCellGrounding,
  type CellGrounding,
} from '@/components/DataTable/utils/excerptUtils';
import ContentModal from '@/components/ContentModal/ContentModal';
import { CostBreakdown } from '@/components/CostBreakdown/CostBreakdown';
import { CloudDatasetPicker, type CloudDataset } from '@/components/CloudDatasetPicker/CloudDatasetPicker';
import { ConsentDialog, getSavedConsent } from '@/components/ConsentDialog/ConsentDialog';
import {
  AdvancedSettingsFields,
  DEFAULT_ADVANCED_SETTINGS,
  observationUnitFromValue,
  retrieverIsCustomized,
  type AdvancedSettingsValue,
} from '@/components/AdvancedSettings/AdvancedSettingsFields';
import {
  getAvailableProviders,
  getDefaultModelForProvider,
  WS_DISCONNECTED_REFRESH_INTERVAL,
  type LLMProviderKey,
} from '@/constants';
import api, { chatAPI, cloudAPI, configAPI, loadAPI, observationUnitAPI, schemaAPI, schematiqAPI, unitsAPI } from '@/services/api';
import webSocketService from '@/services/websocket';
import {
  ChatToolInfo,
  ChatTurnMessage,
  ColumnInfo,
  CostEstimate,
  DataRow,
  DocumentAvailabilityResponse,
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
import { formatColumnName } from '@/utils/formatting';

import './Workspace.css';

registerAllModules();

// Source-document provenance shown in the Data sheet. We display the file name
// only — never a full path — so loaded projects and ScheMatiQ runs read the
// same way. Handles both POSIX and Windows separators.
const documentDisplayName = (value?: string | null): string => {
  if (!value) return '';
  const parts = String(value).split(/[\\/]/);
  return (parts[parts.length - 1] || String(value)).trim();
};

type SheetId = 'data' | 'unit' | 'schema' | 'stats' | 'monitor' | 'documents';
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
  headerTooltip?: string;
  // Optional Handsontable cell renderer (used for the observation-unit `field`
  // column, where the meaningful concepts live in the rows rather than the
  // headers). Typed loosely to avoid importing Handsontable's renderer types.
  renderer?: (
    instance: unknown,
    td: HTMLTableCellElement,
    row: number,
    col: number,
    prop: string | number,
    value: unknown,
  ) => void;
};

const SCHEMA_COLUMN_HEADER_TOOLTIPS = {
  name:
    "The column's canonical name. This is the identity used for every edit, rename, re-extraction, and export, so the data tab keys off it.",
  definition:
    'What this column captures. Sent to the model as the extraction instruction, so keep it precise and unambiguous.',
  rationale:
    'Why this column exists. Optional context for collaborators; it is not used during extraction.',
  allowed_values:
    'Optional limits: categories (yes/no), numbers, ranges, or one saved date style per column. Leave empty for plain text.',
  auto_expand_threshold:
    'Automatically add new values to allowed_values when they appear in at least this many documents. Set to -1 to disable auto-expansion.',
} as const;

const SCHEMA_COLUMN_HEADER_INFO_ICON =
  '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>';

// The observation-unit sheet is a key/value table: the concepts (name,
// definition, example_names) are row labels in the read-only `field` column,
// not headers. So the help attaches per row, mirroring the per-field help in
// the Edit Observation Unit dialog.
const OBSERVATION_UNIT_FIELD_TOOLTIPS: Record<string, string> = {
  name: 'Short label shown for each extracted row (e.g. "Judge"). It names what a single row represents.',
  definition:
    'What counts as one row. Sent to the model to split documents into rows, so be specific (the dialog suggests 10–500 chars).',
  example_names:
    'Optional sample row names that illustrate the unit and guide extraction. They are not stored as data.',
};

function escapeHtmlAttribute(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;');
}

function escapeHtmlText(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// Shared markup for a "label + hover-help icon" pair. Used both in
// Handsontable column headers and in the observation-unit `field` cells; the
// label truncates with an ellipsis while the icon stays pinned, and the
// tooltip text rides on data-tooltip for the body-level tooltip to render.
function infoLabelMarkup(rawLabel: string, tooltip: string): string {
  const label = escapeHtmlText(rawLabel);
  const escaped = escapeHtmlAttribute(tooltip);
  return (
    `<span class="workspace-col-header-wrap">` +
    `<span class="workspace-col-header-label">${label}</span>` +
    `<span class="workspace-col-header-info" data-tooltip="${escaped}" aria-label="${escaped}" tabindex="0" role="img">${SCHEMA_COLUMN_HEADER_INFO_ICON}</span>` +
    `</span>`
  );
}

function formatSheetColHeader(column: SheetColumn): string {
  if (!column.headerTooltip) return escapeHtmlText(column.label);
  return infoLabelMarkup(column.label, column.headerTooltip);
}

// Handsontable renderer for the observation-unit `field` column: renders the
// field name plus a hover-help icon when a description exists, otherwise plain
// text. The column is read-only, so taking over the cell content is safe.
function renderObservationUnitFieldCell(
  _instance: unknown,
  td: HTMLTableCellElement,
  _row: number,
  _col: number,
  _prop: string | number,
  value: unknown,
): void {
  const field = value == null ? '' : String(value);
  const tooltip = OBSERVATION_UNIT_FIELD_TOOLTIPS[field];
  if (!tooltip) {
    td.textContent = field;
    return;
  }
  td.innerHTML = infoLabelMarkup(field, tooltip);
}

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

const SHEETS: Array<{ id: SheetId; label: string; group: 'structure' | 'analysis' }> = [
  { id: 'data', label: 'Data', group: 'structure' },
  { id: 'unit', label: 'Observation Unit', group: 'structure' },
  { id: 'schema', label: 'Schema', group: 'structure' },
  { id: 'stats', label: 'Statistics', group: 'analysis' },
  { id: 'documents', label: 'Documents', group: 'analysis' },
  { id: 'monitor', label: 'Monitor', group: 'analysis' },
];

const WORKSPACE_MENUS = [
  {
    label: 'File',
    items: ['New project', 'Import project', 'Open classic visualizer', 'Download table (.csv)', 'Save project (.schematiq.json)'],
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
const EDITABLE_OBSERVATION_UNIT_FIELDS = new Set(['name', 'definition', 'example_names']);
const TABLE_FONT_OPTIONS: TableFontFamily[] = ['Inter', 'Arial', 'Georgia', 'Mono'];
const TABLE_FONT_SIZE_OPTIONS = [10, 11, 12, 13, 14, 16, 18];

/**
 * Build a human-readable export filename from the research question.
 *
 * Shape: `schematiq_<up to 3 slugged words from the question>_<YYYY-MM-DD>.<ext>`
 * Falls back to `schematiq_<8-char session id>_<date>` when no question is set.
 * e.g. "How do firms respond to tariffs?" -> schematiq_how-do-firms_2026-06-28.csv
 */
function buildExportFilename(question: string, ext: string, sessionId?: string): string {
  const date = new Date().toISOString().slice(0, 10); // YYYY-MM-DD, local-agnostic
  const slug = (question || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ') // drop punctuation
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 3)
    .join('-');
  const stem = slug || (sessionId ? sessionId.slice(0, 8) : 'project');
  return `schematiq_${stem}_${date}.${ext}`;
}

/** Download a blob from an API path under a caller-controlled filename. */
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

const WORKSPACE_DEFAULT_ADVANCED: AdvancedSettingsValue = {
  ...DEFAULT_ADVANCED_SETTINGS,
  schemaProvider: DEFAULT_PROVIDER,
  valueProvider: DEFAULT_PROVIDER,
};

type DocumentSourceInput =
  | { mode: 'upload' }
  | { mode: 'cloud'; datasets: string[] };

function buildConfig(
  query: string,
  apiKey: string,
  advanced: AdvancedSettingsValue = WORKSPACE_DEFAULT_ADVANCED,
  docs: DocumentSourceInput = { mode: 'upload' },
  optOut = false,
): ScheMatiQConfig {
  const config: ScheMatiQConfig = {
    query,
    docs_path: docs.mode === 'cloud' ? docs.datasets : null,
    upload_pending: docs.mode === 'upload',
    opt_out_data_collection: optOut,
    max_keys_schema: advanced.maxKeysSchema,
    documents_batch_size: advanced.documentsBatchSize,
    schema_creation_backend: {
      provider: advanced.schemaProvider,
      model: advanced.schemaModel,
      temperature: advanced.schemaTemperature,
      api_key: apiKey || undefined,
    },
    value_extraction_backend: {
      provider: advanced.valueProvider,
      model: advanced.valueModel,
      temperature: advanced.valueTemperature,
      api_key: apiKey || undefined,
    },
    output_path: 'outputs/workspace_output.json',
    document_randomization_seed: advanced.seed,
    skip_value_extraction: advanced.skipValueExtraction,
    initial_observation_unit: observationUnitFromValue(advanced),
    review_observation_unit: advanced.observationUnitMode === 'auto' ? advanced.reviewObservationUnit : undefined,
    initial_schema: advanced.initialSchemaData ?? undefined,
    initial_schema_path: !advanced.initialSchemaData ? advanced.initialSchemaPath : undefined,
  };

  if (advanced.convergenceThreshold != null) {
    config.convergence_threshold = advanced.convergenceThreshold;
  }

  if (retrieverIsCustomized(advanced)) {
    config.retriever = {
      type: 'embedding',
      model_name: advanced.retrieverModelName,
      passage_chars: advanced.retrieverPassageChars,
      overlap: advanced.retrieverOverlap,
      k: advanced.retrieverK,
      enable_dynamic_k: true,
      dynamic_k_threshold: advanced.retrieverDynamicK,
      dynamic_k_minimum: 3,
    };
  }

  return config;
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
  const [documentSource, setDocumentSource] = useState<'upload' | 'cloud'>('upload');
  const [datasets, setDatasets] = useState<CloudDataset[]>([]);
  const [datasetsLoading, setDatasetsLoading] = useState(true);
  const [selectedDatasets, setSelectedDatasets] = useState<string[]>([]);
  const [serverHasKeys, setServerHasKeys] = useState(false);
  const [estimate, setEstimate] = useState<CostEstimate | null>(null);
  const [estimating, setEstimating] = useState(false);
  const [creating, setCreating] = useState(false);
  const [startConfirmed, setStartConfirmed] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [advanced, setAdvanced] = useState<AdvancedSettingsValue>(WORKSPACE_DEFAULT_ADVANCED);
  const [developerMode, setDeveloperMode] = useState(false);
  const [allowLlmConfig, setAllowLlmConfig] = useState(false);
  const [dataCollectionEnabled, setDataCollectionEnabled] = useState(false);
  const [consentOpen, setConsentOpen] = useState(false);
  const [maxDocuments, setMaxDocuments] = useState<number | undefined>(undefined);
  const [providers, setProviders] = useState<LLMProviderKey[]>([DEFAULT_PROVIDER as LLMProviderKey]);

  const updateAdvanced = useCallback((patch: Partial<AdvancedSettingsValue>) => {
    setAdvanced((prev) => ({ ...prev, ...patch }));
    setEstimate(null);
    setStartConfirmed(false);
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const config = await configAPI.getConfig().catch(() => null);
      const configured = await getConfiguredProviders().catch(() => []);
      if (cancelled) return;
      const available = getAvailableProviders(configured) as LLMProviderKey[];
      setServerHasKeys(Boolean(config?.server_has_api_keys));
      setDeveloperMode(Boolean(config?.developer_mode));
      setAllowLlmConfig(Boolean(config?.allow_llm_config));
      setDataCollectionEnabled(Boolean(config?.data_collection_enabled));
      setMaxDocuments(typeof config?.max_documents === 'number' ? config.max_documents : undefined);
      setProviders(available.length > 0 ? available : [DEFAULT_PROVIDER as LLMProviderKey]);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await cloudAPI.getDatasets();
        if (!cancelled) setDatasets(Array.isArray(data) ? data : []);
      } catch {
        if (!cancelled) setDatasets([]);
      } finally {
        if (!cancelled) setDatasetsLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
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

  const hasDocuments = documentSource === 'cloud' ? selectedDatasets.length > 0 : files.length > 0;
  const canEstimate = query.trim().length > 0 && hasDocuments && (serverHasKeys || apiKey.trim().length > 0);

  const estimateProject = useCallback(async () => {
    setError(null);
    setEstimating(true);
    try {
      const docs: DocumentSourceInput = documentSource === 'cloud'
        ? { mode: 'cloud', datasets: selectedDatasets }
        : { mode: 'upload' };
      const config = buildConfig(query.trim(), apiKey.trim(), advanced, docs);
      const result = await schematiqAPI.estimateCostPreview(
        config,
        documentSource === 'cloud'
          ? []
          : files.map((file) => ({ name: file.webkitRelativePath || file.name, size: file.size })),
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
  }, [apiKey, files, query, advanced, documentSource, selectedDatasets]);

  const runCreate = useCallback(async (optOut: boolean) => {
    setCreating(true);
    setError(null);
    try {
      const docs: DocumentSourceInput = documentSource === 'cloud'
        ? { mode: 'cloud', datasets: selectedDatasets }
        : { mode: 'upload' };
      const config = buildConfig(query.trim(), apiKey.trim(), advanced, docs, optOut);
      const result = await schematiqAPI.configure(config);
      if (documentSource === 'upload') {
        await loadAPI.addDocuments(result.session_id, files, advanced.bypassLimit);
      }
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
  }, [apiKey, files, navigate, onCreated, onOpenChange, query, toast, advanced, documentSource, selectedDatasets]);

  const startProject = useCallback(async () => {
    if (!query.trim() || !hasDocuments) {
      setError(
        documentSource === 'cloud'
          ? 'Select at least one cloud dataset and enter a research question first.'
          : 'Choose a folder of documents and enter a research question first.',
      );
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

    // Consent gate: skip when data collection is off or in developer mode;
    // otherwise honor saved consent, or prompt for it.
    if (!dataCollectionEnabled || developerMode) {
      await runCreate(false);
      return;
    }
    const { consentGiven, savedOptOut } = getSavedConsent();
    if (consentGiven) {
      await runCreate(savedOptOut);
      return;
    }
    setConsentOpen(true);
  }, [apiKey, dataCollectionEnabled, developerMode, documentSource, estimate, estimateProject, hasDocuments, query, runCreate, serverHasKeys, startConfirmed]);

  return (
    <>
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>New Project</DialogTitle>
          <DialogDescription>
            Pick a local folder or a cloud dataset, describe the research question, estimate cost, then start extraction.
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
            <Label>Documents</Label>
            <div className="flex gap-2">
              <Button
                type="button"
                size="sm"
                variant={documentSource === 'upload' ? 'default' : 'outline'}
                onClick={() => {
                  setDocumentSource('upload');
                  setEstimate(null);
                  setStartConfirmed(false);
                  folderInputRef.current?.click();
                }}
              >
                <FolderOpen className="h-4 w-4" />
                Choose Folder
              </Button>
              <Button
                type="button"
                size="sm"
                variant={documentSource === 'cloud' ? 'default' : 'outline'}
                onClick={() => { setDocumentSource('cloud'); setEstimate(null); setStartConfirmed(false); }}
              >
                <Cloud className="h-4 w-4" />
                Cloud dataset
              </Button>
            </div>

            {documentSource === 'upload' ? (
              <span className="text-sm text-muted-foreground">
                {files.length > 0
                  ? `${files.length} files, ${formatFileSize(selectedBytes)}`
                  : 'No folder selected'}
              </span>
            ) : (
              <CloudDatasetPicker
                datasets={datasets}
                loading={datasetsLoading}
                selected={selectedDatasets}
                onChange={(names) => { setSelectedDatasets(names); setEstimate(null); setStartConfirmed(false); }}
                maxDocuments={maxDocuments}
                bypassLimit={advanced.bypassLimit}
              />
            )}

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

          <Collapsible>
            <CollapsibleTrigger className="group flex items-center gap-2 text-sm font-medium hover:text-foreground transition-colors">
              <ChevronDown className="h-4 w-4 transition-transform duration-200 group-data-[state=open]:rotate-180" />
              <span>Advanced settings</span>
              <span className="text-xs font-normal text-muted-foreground">(optional)</span>
            </CollapsibleTrigger>
            <CollapsibleContent className="mt-3">
              <AdvancedSettingsFields
                value={advanced}
                onChange={updateAdvanced}
                developerMode={developerMode}
                allowLlmConfig={allowLlmConfig}
                providers={providers}
                maxDocuments={maxDocuments}
              />
            </CollapsibleContent>
          </Collapsible>

          {estimate && (
            <div className="rounded-md border bg-muted/30 p-3 text-sm">
              <div className="font-medium">Estimated cost</div>
              <div className="mt-1 text-muted-foreground">{formatCost(estimate)}</div>
              <CostBreakdown
                estimate={estimate}
                skipValueExtraction={advanced.skipValueExtraction}
                className="mt-2"
              />
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
    <ConsentDialog open={consentOpen} onOpenChange={setConsentOpen} onConfirm={runCreate} />
    </>
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
  onRefresh: () => void;
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
  // in sync with each other and survive column sorting/filtering (which only
  // changes the visual<->physical mapping, not this array).
  const groupKeys = useMemo<string[]>(() => {
    if (activeSheet !== 'data') return [];
    return dataRows.map((row) => String((row as Record<string, string>)[groupColKey] ?? '').trim().toLowerCase());
  }, [activeSheet, dataRows, groupColKey]);

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
  }, [activeSheet, data.rows, observationUnitRows, onEditFollowUp, onRefresh, schemaColumns, sessionId, toast]);

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
        contextMenu
        filters
        dropdownMenu
        columnSorting
        copyPaste
        undo
        minSpareRows={sheet.minSpareRows || 0}
        licenseKey="non-commercial-and-evaluation"
        afterInit={() => {
          syncHotTableDimensions();
          applyGroupMerges();
        }}
        afterColumnSort={applyGroupMerges}
        afterFilter={applyGroupMerges}
        beforeRemoveRow={handleBeforeRemoveRow}
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
        afterOnCellMouseDown={(event, coords) => {
          if (activeSheet !== 'data' || coords.row < 0 || coords.col < 0) return;

          const column = sheet.columns[coords.col];
          if (!column || column.key === '_row_name') return;

          const hot = hotTableRef.current?.hotInstance;
          const physicalRow = hot ? hot.toPhysicalRow(coords.row) : coords.row;
          if (!dataGrounding[physicalRow]?.[column.key]) return;

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

// Expensive chat tools that pause for server-side confirmation (pending_action)
// or, once completed, already imply a re-run prompt — skip the top banner then.
const CHAT_RERUN_FOLLOWUP_TOOLS = new Set([
  'reextract',
  'reprocess',
  'run_schematiq',
  'continue_discovery',
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

// Render assistant text replies as Markdown (bullets, bold, code, tables, etc.).
// User messages and tool logs stay as plain text to preserve their exact formatting.
function ChatMessageBody({ message }: { message: WorkspaceMessage }) {
  const isMarkdown = message.role === 'assistant' && message.kind !== 'tool_log';
  if (!isMarkdown) {
    return <>{message.content}</>;
  }
  return (
    <div className="workspace-chat-markdown">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          a: ({ node, ...props }) => (
            <a {...props} target="_blank" rel="noopener noreferrer" />
          ),
        }}
      >
        {message.content}
      </ReactMarkdown>
    </div>
  );
}

function ChatPanel({
  sessionId,
  sessionMode,
  onRefresh,
  onEditFollowUp,
  onRegisterCancelPending,
}: {
  sessionId?: string;
  sessionMode: WorkspaceSessionMode;
  status: ScheMatiQStatus | null;
  schema: SchemaData | null;
  data: PaginatedData;
  onRefresh: () => void;
  onEditFollowUp: (kind: PendingRerunKind, columns?: string[]) => void;
  onRegisterCancelPending?: (cancel: (() => Promise<boolean>) | null) => void;
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

    const alreadyFollowedUp =
      (response.pending_action != null
        && CHAT_RERUN_FOLLOWUP_TOOLS.has(response.pending_action.tool_name))
      || completedTools.some((message) =>
        CHAT_RERUN_FOLLOWUP_TOOLS.has(message.tool_name!),
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

  const cancelPendingAction = useCallback(async (): Promise<boolean> => {
    if (!pendingAction || !sessionId) return true;
    setBusy(true);
    try {
      const response = await chatAPI.cancelAction(sessionId, pendingAction.chatId);
      setPendingAction(null);
      applyChatResponse(response);
      return true;
    } catch (err: any) {
      appendMessages([
        {
          id: `${Date.now()}-cancel-error`,
          role: 'assistant',
          content:
            err?.response?.data?.detail
            || err?.message
            || 'Could not cancel the pending action. Try again.',
        },
      ]);
      return false;
    } finally {
      setBusy(false);
    }
  }, [appendMessages, applyChatResponse, pendingAction, sessionId]);

  useEffect(() => {
    if (!onRegisterCancelPending) return;
    if (pendingAction) {
      onRegisterCancelPending(cancelPendingAction);
    } else {
      onRegisterCancelPending(null);
    }
    return () => onRegisterCancelPending(null);
  }, [cancelPendingAction, onRegisterCancelPending, pendingAction]);

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
            <ChatMessageBody message={message} />
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
  onSaveProject,
  onHome,
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
  onSaveProject: () => void;
  onHome: () => void;
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
    if (label === 'Download table (.csv)') onExport();
    if (label === 'Save project (.schematiq.json)') onSaveProject();
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
      'Download table (.csv)',
      'Save project (.schematiq.json)',
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
        <button
          type="button"
          className="workspace-file-mark"
          onClick={onHome}
          title="ScheMatiQ home"
          aria-label="ScheMatiQ home"
        >
          <img src="/icon.png" alt="" className="workspace-file-mark-logo" />
          <span className="workspace-file-mark-name">ScheMatiQ</span>
        </button>
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
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="workspace-toolbar-icon" type="button" disabled={!canUseProjectActions} title="Export">
              <Download className="h-3.5 w-3.5" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="workspace-menu-content w-72">
            <DropdownMenuItem onClick={onExport} disabled={!canUseProjectActions}>
              <Download className="h-4 w-4 mr-2 shrink-0" />
              <div>
                <div>Download Table (.csv)</div>
                <div className="text-xs text-muted-foreground">Clean data for Excel, no metadata</div>
              </div>
            </DropdownMenuItem>
            <DropdownMenuItem onClick={onSaveProject} disabled={!canUseProjectActions}>
              <Save className="h-4 w-4 mr-2 shrink-0" />
              <div>
                <div>Save Project (.schematiq.json)</div>
                <div className="text-xs text-muted-foreground">Full project with schema and history, for reloading</div>
              </div>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>

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
  const [session, setSession] = useState<VisualizationSession | null>(null);
  const [schema, setSchema] = useState<SchemaData | null>(null);
  const [data, setData] = useState<PaginatedData>(emptyData);
  const [dataView, setDataView] = useState<'by_document' | 'by_unit'>('by_document');
  const [unitData, setUnitData] = useState<PaginatedData>(emptyData);
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
  const [showSourcePanel, setShowSourcePanel] = useState(false);
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
        setSession(loadSession);
        applyData(nextData, options);
        setDocuments(nextDocuments);
        setConfig(null);
        return;
      }

      try {
        const [nextStatus, nextSchema, nextData, nextDocuments, nextConfig, statsSession] = await Promise.all([
          schematiqAPI.getStatus(sessionId),
          schematiqAPI.getSchema(sessionId).catch(() => null),
          schematiqAPI.getData(sessionId, 0, 500).catch(() => emptyData),
          unitsAPI.getDocuments(sessionId).catch(() => null),
          schematiqAPI.getConfig(sessionId).catch(() => null),
          loadAPI.getSession(sessionId).catch(() => null),
        ]);
        setStatus(nextStatus);
        setSchema(nextSchema);
        setSession(statsSession);
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
        setSession(loadSession);
        applyData(nextData, options);
        setDocuments(nextDocuments);
        setConfig(null);
      }
    } finally {
      if (!options?.silent) {
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
        onHome={() => navigate('/')}
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
            <div style={{ height: '100%', minHeight: 0, overflow: 'hidden' }}>
              <DocumentViewer sessionId={sessionId} refreshKey={data.total_count} />
            </div>
          ) : activeSheet !== 'monitor' ? (
            activeSheet === 'data' && showSourcePanel ? (
              <div className="workspace-data-split">
                <div className="workspace-source-panel">
                  <DocumentPreview
                    sessionId={sessionId}
                    documentName={selectedSourceDoc}
                    emptyHint="Select a row to see its source document."
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
    </div>
  );
}

export default Workspace;
