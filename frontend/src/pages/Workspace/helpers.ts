import {
  observationUnitFromValue,
  retrieverIsCustomized,
  type AdvancedSettingsValue,
} from '@/components/AdvancedSettings/AdvancedSettingsFields';
import type {
  ChatToolInfo,
  ChatTurnMessage,
  ColumnInfo,
  CostEstimate,
  DataRow,
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
  WrongCellScope,
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

// Union of column indices covered by Handsontable's `getSelected()` ranges
// (each `[r1, c1, r2, c2]`). Row bounds are ignored -- this is only safe to
// use when the caller doesn't need row/column pairing (e.g. whole-column
// operations like column delete). Do not use it to derive a row index union
// too: flattening multi-range selections into separate row and column unions
// and then crossing them back together produces (row, col) pairs that were
// never actually selected -- see emptyCellScope below, which instead walks
// each range's own row x col rectangle.
export function selectedColumnIndices(selection: number[][]): number[] {
  const cols = new Set<number>();
  for (const range of selection) {
    const [, c1, , c2] = range;
    const fromCol = Math.min(c1, c2);
    const toCol = Math.max(c1, c2);
    for (let c = fromCol; c <= toCol; c += 1) if (c >= 0) cols.add(c);
  }
  return Array.from(cols);
}

// Resolve the real schema-column keys (never provenance/grouping columns like
// _row_name / _source_document) covered by a set of grid column indices.
export function schemaColumnKeysForCols(
  colIndices: number[],
  sheetColumns: SheetColumn[],
  schemaColumns: ColumnInfo[],
): string[] {
  const keys = colIndices
    .map((c) => sheetColumns[c]?.key)
    .filter((key): key is string => Boolean(key) && !key.startsWith('_'));
  return keys.filter((key) => schemaColumns.some((col) => col.name === key));
}

// Resolve the "Fill empty cells" scope for a Handsontable selection: the
// unit-row names and schema-column keys covered AND still blank. Cells that
// already hold a value are excluded from both sets, so a row/column that is
// entirely filled in never appears in the result, and a selection with
// nothing blank resolves to null (used to disable the menu item).
// Walks each selection range's own row x col rectangle (rather than a
// flattened row-index union crossed with a column-index union) so a
// multi-range (ctrl+click) selection never pulls in a (row, col) pair that
// wasn't actually part of any selected range.
// `toPhysicalRow` translates visual -> physical row (pass the grid instance's
// own mapper so sorting/filtering don't shift which rows are meant); omit it
// when visual and physical rows are already the same (no active sort/filter).
export function emptyCellScope(
  selection: number[][],
  dataRows: Array<Record<string, string>>,
  sheetColumns: SheetColumn[],
  schemaColumns: ColumnInfo[],
  toPhysicalRow?: (visualRow: number) => number,
): { rows: string[]; columns: string[] } | null {
  const rowNames = new Set<string>();
  const columnKeys = new Set<string>();

  // A column's key and whether it's a real (non-provenance) schema column
  // depend only on its index -- never on the row or which range it's in --
  // so resolve that once per column here instead of re-deriving it (including
  // the schemaColumns scan) for every row that happens to cover it.
  const columnKeyByIndex: Array<string | undefined> = sheetColumns.map((column) => {
    const key = column.key;
    if (!key || key.startsWith('_')) return undefined;
    return schemaColumns.some((c) => c.name === key) ? key : undefined;
  });

  for (const range of selection) {
    const [r1, c1, r2, c2] = range;
    const fromRow = Math.min(r1, r2);
    const toRow = Math.max(r1, r2);
    const fromCol = Math.min(c1, c2);
    const toCol = Math.max(c1, c2);

    for (let visualRow = fromRow; visualRow <= toRow; visualRow += 1) {
      const physicalRow = toPhysicalRow ? toPhysicalRow(visualRow) : visualRow;
      if (physicalRow == null || physicalRow < 0) continue;
      const rowData = dataRows[physicalRow];
      const rowName = String(rowData?._row_name || '').trim();
      if (!rowData || !rowName) continue;

      for (let col = fromCol; col <= toCol; col += 1) {
        const key = columnKeyByIndex[col];
        if (!key) continue;
        const value = rowData[key];
        if (value != null && String(value).trim() !== '') continue; // already filled

        rowNames.add(rowName);
        columnKeys.add(key);
      }
    }
  }

  if (rowNames.size === 0 || columnKeys.size === 0) return null;
  return { rows: Array.from(rowNames), columns: Array.from(columnKeys) };
}

// Resolve the "Wrong, try again" scope: every cell covered by the current
// selection, filled or not (unlike emptyCellScope, values are never
// inspected). Only safe for a single, contiguous drag-rectangle -- for that
// one range, rows x columns IS exactly the selected cell set. A multi-range
// (ctrl+click) selection is refused (returns null) because unioning multiple
// ranges' rows and columns together would sweep in cells that were never
// actually selected (see emptyCellScope's comment above), and this resolver
// has no only_empty guard downstream to protect them from being overwritten.
// When the selection resolves to exactly one (row, column) pair, its current
// value is returned as `previousValue` so the caller can tell the model
// specifically what it got wrong, instead of only a generic "try again" note
// -- left undefined for a multi-cell selection, where a single prior value
// wouldn't apply to every cell in scope.
export function selectedCellScope(
  selection: number[][],
  dataRows: Array<Record<string, string>>,
  sheetColumns: SheetColumn[],
  schemaColumns: ColumnInfo[],
  toPhysicalRow?: (visualRow: number) => number,
): WrongCellScope | null {
  if (selection.length !== 1) return null;

  const rowNames = new Set<string>();
  const columnKeys = new Set<string>();
  let cellCount = 0;
  let lastValue: string | undefined;

  const columnKeyByIndex: Array<string | undefined> = sheetColumns.map((column) => {
    const key = column.key;
    if (!key || key.startsWith('_')) return undefined;
    return schemaColumns.some((c) => c.name === key) ? key : undefined;
  });

  const [r1, c1, r2, c2] = selection[0];
  const fromRow = Math.min(r1, r2);
  const toRow = Math.max(r1, r2);
  const fromCol = Math.min(c1, c2);
  const toCol = Math.max(c1, c2);

  for (let visualRow = fromRow; visualRow <= toRow; visualRow += 1) {
    const physicalRow = toPhysicalRow ? toPhysicalRow(visualRow) : visualRow;
    if (physicalRow == null || physicalRow < 0) continue;
    const rowData = dataRows[physicalRow];
    const rowName = String(rowData?._row_name || '').trim();
    if (!rowData || !rowName) continue;

    for (let col = fromCol; col <= toCol; col += 1) {
      const key = columnKeyByIndex[col];
      if (!key) continue;
      rowNames.add(rowName);
      columnKeys.add(key);
      cellCount += 1;
      lastValue = rowData[key];
    }
  }

  if (rowNames.size === 0 || columnKeys.size === 0) return null;
  return {
    rows: Array.from(rowNames),
    columns: Array.from(columnKeys),
    previousValue: cellCount === 1 ? String(lastValue ?? '').trim() : undefined,
  };
}

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
/** Apply a single cell edit to a copy of the paginated data (optimistic UI). */
export function patchDataCell(
  data: PaginatedData,
  identity: { rowName: string; sourceDocument?: string; rowIndex?: number },
  column: string,
  value: string,
): PaginatedData {
  let changed = false;
  const rows = data.rows.map((row, idx) => {
    if (!rowMatchesEditIdentity(row, identity, idx)) return row;
    changed = true;
    return patchRowCellValue(row, column, value);
  });
  if (!changed) return data;
  return { ...data, rows };
}

function rowMatchesEditIdentity(
  row: DataRow,
  identity: { rowName: string; sourceDocument?: string; rowIndex?: number },
  _index: number,
): boolean {
  if (!identity.rowName && identity.rowIndex != null) {
    return row._row_index === identity.rowIndex;
  }
  const name = row.row_name || row._unit_name || '';
  if (name !== identity.rowName) return false;
  if (identity.sourceDocument) {
    const src = row._source_document || row._parent_document || '';
    if (src && src !== identity.sourceDocument) return false;
  }
  return true;
}

function patchRowCellValue(row: DataRow, column: string, value: string): DataRow {
  const next = { ...row };
  const cell = { answer: value, excerpts: [] as unknown[], manually_edited: true };
  if (next.data && typeof next.data === 'object') {
    next.data = { ...next.data, [column]: cell };
  } else {
    next.data = { [column]: cell };
  }
  return next;
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
  // status='processing' on an UPLOAD-type session only happens mid schema
  // rediscovery (see /load/rediscover) — a plain import never reaches it (it
  // uses 'processing_documents'). The raw status string otherwise reads as an
  // internal state name, not something meant for the bottom-bar step label or
  // the project-details "Current step" row, both of which read current_step
  // directly.
  const currentStep = completed
    ? 'Imported project loaded'
    : session.status === 'processing'
      ? 'Rediscovering schema…'
      : session.status;
  return {
    session_id: session.id,
    status: session.status,
    progress: completed ? 1 : 0,
    current_step: currentStep,
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

// The concurrency limiter answers 503 when the server is at capacity. That is a
// "come back shortly" condition, not a failure of the thing the user asked for,
// and it was only being distinguished in one of the three Workspace entry points
// that can provoke it -- so re-extraction and project start reported it as
// though the operation itself had broken.
export const SERVER_BUSY_MESSAGE =
  'The server is at capacity right now. Nothing was lost -- try again in a few minutes.';

export function describeRequestError(
  err: unknown,
  fallback: string,
): { message: string; isBusy: boolean; code?: string } {
  const response = (err as { response?: { status?: number; data?: { detail?: unknown } } })?.response;
  const rawDetail = response?.data?.detail;
  // `detail` is usually a plain string, but a few backend errors (e.g.
  // ConfirmedEmptyScopeError on /schema/reextract) send {message, code} so the
  // caller can react to the specific error, not just display text.
  const detailObj = rawDetail && typeof rawDetail === 'object' ? (rawDetail as Record<string, unknown>) : null;
  let detail = '';
  if (typeof rawDetail === 'string') {
    detail = rawDetail;
  } else if (typeof detailObj?.message === 'string') {
    detail = detailObj.message;
  }
  const code = typeof detailObj?.code === 'string' ? detailObj.code : undefined;
  if (response?.status === 503) {
    // Prefer the server's wording when it sent any; it names the limit.
    return { message: detail || SERVER_BUSY_MESSAGE, isBusy: true, code };
  }
  return {
    message: detail || (err as { message?: string })?.message || fallback,
    isBusy: false,
    code,
  };
}
