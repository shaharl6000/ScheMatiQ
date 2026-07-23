import {
  observationUnitFromValue,
  retrieverIsCustomized,
  type AdvancedSettingsValue,
} from '@/components/AdvancedSettings/AdvancedSettingsFields';
import type {
  ChatToolInfo,
  ChatTurnMessage,
  CostEstimate,
  PaginatedData,
  ScheMatiQConfig,
  ScheMatiQStatus,
  SchemaData,
  VisualizationSession,
} from '@/types';

import {
  OBSERVATION_UNIT_FIELD_TOOLTIPS,
  SCHEMA_COLUMN_HEADER_INFO_ICON,
  WORKSPACE_DEFAULT_ADVANCED,
} from './constants';
import type {
  CellFormat,
  DocumentSourceInput,
  SheetColumn,
  SheetSelection,
  WorkspaceMessage,
} from './types';

// Shared pure helper functions for the Workspace page and its sub-components.

// Source-document provenance shown in the Data sheet. We display the file name
// only — never a full path — so loaded projects and ScheMatiQ runs read the
// same way. Handles both POSIX and Windows separators.
export const documentDisplayName = (value?: string | null): string => {
  if (!value) return '';
  const parts = String(value).split(/[\\/]/);
  return (parts[parts.length - 1] || String(value)).trim();
};
export function escapeHtmlAttribute(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;');
}

export function escapeHtmlText(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// Shared markup for a "label + hover-help icon" pair. Used both in
// Handsontable column headers and in the observation-unit `field` cells; the
// label truncates with an ellipsis while the icon stays pinned, and the
// tooltip text rides on data-tooltip for the body-level tooltip to render.
export function infoLabelMarkup(rawLabel: string, tooltip: string): string {
  const label = escapeHtmlText(rawLabel);
  const escaped = escapeHtmlAttribute(tooltip);
  return (
    `<span class="workspace-col-header-wrap">` +
    `<span class="workspace-col-header-label">${label}</span>` +
    `<span class="workspace-col-header-info" data-tooltip="${escaped}" aria-label="${escaped}" tabindex="0" role="img">${SCHEMA_COLUMN_HEADER_INFO_ICON}</span>` +
    `</span>`
  );
}

export function formatSheetColHeader(column: SheetColumn): string {
  if (!column.headerTooltip) return escapeHtmlText(column.label);
  return infoLabelMarkup(column.label, column.headerTooltip);
}

// Handsontable renderer for the observation-unit `field` column: renders the
// field name plus a hover-help icon when a description exists, otherwise plain
// text. The column is read-only, so taking over the cell content is safe.
export function renderObservationUnitFieldCell(
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
export const selectionsEqual = (a: SheetSelection, b: SheetSelection) => {
  if (a === b) return true;
  if (!a || !b) return false;
  return a.sheet === b.sheet
    && a.fromRow === b.fromRow
    && a.toRow === b.toRow
    && a.fromCol === b.fromCol
    && a.toCol === b.toCol;
};

export const selectionArea = (selection: SheetSelection) => {
  if (!selection) return 0;
  return (selection.toRow - selection.fromRow + 1) * (selection.toCol - selection.fromCol + 1);
};

export const getCellFormatClasses = (format?: CellFormat) => {
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
export function buildExportFilename(question: string, ext: string, sessionId?: string): string {
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
export function dataEquals(a: PaginatedData, b: PaginatedData): boolean {
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

export function formatFileSize(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / Math.pow(1024, index)).toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

export function formatCost(estimate?: CostEstimate | null): string {
  if (!estimate) return 'No estimate available';
  const cost = estimate.total_cost_usd ?? 0;
  const calls = estimate.total_api_calls ?? 0;
  const tokens = (estimate.total_input_tokens ?? 0) + (estimate.total_output_tokens ?? 0);
  return `$${cost.toFixed(4)} estimated, ${calls} API calls, ${tokens.toLocaleString()} tokens`;
}

export function parseAllowedValues(value: unknown): string[] | undefined {
  if (Array.isArray(value)) return value.map(String).filter(Boolean);
  const text = String(value ?? '').trim();
  if (!text) return undefined;
  return text
    .split(/[,\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}
export function buildConfig(
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
    batch_strategy: advanced.batchStrategy,
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

export function schemaFromLoadSession(session: VisualizationSession | null): SchemaData | null {
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

export function statusFromLoadSession(session: VisualizationSession | null): ScheMatiQStatus | null {
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
export function mapChatTurnMessage(message: ChatTurnMessage): WorkspaceMessage {
  return {
    id: message.id,
    role: message.role,
    content: message.content,
    kind: message.kind,
    toolName: message.tool_name,
    toolStatus: message.tool_status,
  };
}

export function formatToolsList(tools: ChatToolInfo[]): string {
  if (!tools.length) {
    return 'No tools are available in the current context.';
  }
  return tools
    .map((tool) => {
      const badge = tool.cost_class === 'expensive' ? ' [cost]' : '';
      const status = tool.available ? '' : ' (planned)';
      // Markdown list item; the name is inline code so underscores in tool
      // names (e.g. edit_observation_unit) are not parsed as emphasis.
      return `- \`${tool.name}\`${badge}${status}: ${tool.description}`;
    })
    .join('\n');
}
