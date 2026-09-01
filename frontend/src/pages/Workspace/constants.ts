import {
  DEFAULT_ADVANCED_SETTINGS,
  type AdvancedSettingsValue,
} from '@/components/AdvancedSettings/AdvancedSettingsFields';
import type { PaginatedData } from '@/types';

import type { SheetId, TableFontFamily, WorkspaceMenu } from './types';

// Shared constants for the Workspace page and its sub-components.

export const SCHEMA_COLUMN_HEADER_TOOLTIPS = {
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

export const SCHEMA_COLUMN_HEADER_INFO_ICON =
  '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>';

// The observation-unit sheet is a key/value table: the concepts (name,
// definition, example_names) are row labels in the read-only `field` column,
// not headers. So the help attaches per row, mirroring the per-field help in
// the Edit Observation Unit dialog.
export const OBSERVATION_UNIT_FIELD_TOOLTIPS: Record<string, string> = {
  name: 'Short label shown for each extracted row (e.g. "Judge"). It names what a single row represents.',
  definition:
    'What counts as one row. Sent to the model to split documents into rows, so be specific (the dialog suggests 10–500 chars).',
  example_names:
    'Optional sample row names that illustrate the unit and guide extraction. They are not stored as data.',
};
export const cellFormatKey = (sheet: SheetId, row: number, col: number) => `${sheet}:${row}:${col}`;
export const SHEETS: Array<{ id: SheetId; label: string; group: 'structure' | 'analysis' }> = [
  { id: 'data', label: 'Data', group: 'structure' },
  { id: 'unit', label: 'Observation Unit', group: 'structure' },
  { id: 'schema', label: 'Schema', group: 'structure' },
  { id: 'stats', label: 'Statistics', group: 'analysis' },
  { id: 'documents', label: 'Documents', group: 'analysis' },
  { id: 'monitor', label: 'Monitor', group: 'analysis' },
];

// Top menu bar. Every item carries the action it performs, and
// SpreadsheetChrome resolves those actions through an exhaustive Record, so a
// menu entry cannot exist without a handler behind it. Items that were purely
// decorative (Insert, Format, Undo/Redo, Sort range, Validate schema, and so
// on) are omitted rather than rendered as clickable no-ops; the formatting
// toolbar below covers font/bold/align, and the column header dropdown covers
// sorting and filtering.
// Row height used when compact rows are on. Matches the 12px table font with
// single-line padding.
export const COMPACT_ROW_HEIGHT = 28;

export const WORKSPACE_MENUS: WorkspaceMenu[] = [
  {
    label: 'File',
    items: [
      { label: 'New project', action: 'newProject' },
      { label: 'Import project', action: 'importProject' },
      { label: 'Open classic visualizer', action: 'openClassic', requiresProject: true },
      { label: 'Download table (.csv)', action: 'exportCsv', requiresProject: true },
      { label: 'Save project (.schematiq.json)', action: 'saveProject', requiresProject: true },
      { label: 'Save project with documents (.zip)', action: 'saveProjectWithDocs', requiresProject: true },
    ],
  },
  {
    label: 'Edit',
    items: [
      // Handsontable's UndoRedo plugin is enabled on the grid, and edits made
      // by it flow through the same afterChange -> updateCell path as typed
      // ones, so these persist rather than only reverting the display.
      { label: 'Undo', action: 'undo', requiresProject: true },
      { label: 'Redo', action: 'redo', requiresProject: true },
      // Browser find over the table. Not a replace, so it is not labelled one.
      { label: 'Find in workspace', action: 'find' },
    ],
  },
  {
    label: 'View',
    items: [
      { label: 'Open chat', action: 'openChat', requiresProject: true },
      { label: 'Split view', action: 'splitView', requiresProject: true },
      { label: 'Toggle compact rows', action: 'toggleCompactRows', requiresProject: true },
      { label: 'Project details', action: 'projectDetails', requiresProject: true },
    ],
  },
  {
    label: 'Data',
    items: [
      { label: 'Re-extract table', action: 'reextract', requiresProject: true },
      { label: 'Add documents', action: 'addDocuments', requiresProject: true },
    ],
  },
  {
    label: 'Tools',
    items: [
      { label: 'Estimate cost', action: 'estimateCost', requiresProject: true },
      { label: 'Refresh project', action: 'refreshProject', requiresProject: true },
    ],
  },
  {
    label: 'Help',
    items: [
      { label: 'Keyboard shortcuts', action: 'keyboardShortcuts' },
      { label: 'Report an issue', action: 'reportIssue' },
      { label: 'Cite ScheMatiQ', action: 'cite' },
    ],
  },
];

export const DEFAULT_PROVIDER = 'gemini';
export const EDITABLE_OBSERVATION_UNIT_FIELDS = new Set(['name', 'definition', 'example_names']);
export const TABLE_FONT_OPTIONS: TableFontFamily[] = ['Inter', 'Arial', 'Georgia', 'Mono'];
export const TABLE_FONT_SIZE_OPTIONS = [10, 11, 12, 13, 14, 16, 18];
export const emptyData: PaginatedData = {
  rows: [],
  total_count: 0,
  page: 0,
  page_size: 500,
  has_more: false,
};
// Delay before re-checking a zero-row data response that arrived while the grid
// was showing rows. Long enough for an in-flight backend data-file rewrite to
// finish, short enough that a genuinely emptied table clears without a visible
// wait. See applyData in index.tsx.
export const EMPTY_DATA_RECHECK_MS = 900;
// Automatic retry schedule for a table-data fetch that failed with 503: the
// backend answers 503 specifically when the session's rows exist in storage
// but could not be hydrated locally, so a retry is expected to succeed once
// hydration recovers. The delay doubles per consecutive failure and is capped,
// so a backend that recovers quickly is retried quickly without hammering one
// that does not. See noteDataFetchError in index.tsx.
export const DATA_LOAD_RETRY_BASE_MS = 4000;
export const DATA_LOAD_RETRY_MAX_MS = 30000;
export const WORKSPACE_DEFAULT_ADVANCED: AdvancedSettingsValue = {
  ...DEFAULT_ADVANCED_SETTINGS,
  schemaProvider: DEFAULT_PROVIDER,
  valueProvider: DEFAULT_PROVIDER,
};
export const SHOW_API_KEY_FIELD = false;
export const CHAT_MUTATION_TOOLS = new Set([
  'add_column',
  'edit_column',
  'delete_column',
  'merge_columns',
  'update_cell',
  'add_unit',
  'remove_unit',
  'merge_units',
  'rename_unit',
  'edit_observation_unit',
  'run_schematiq',
  'reextract',
  'extract_cells',
  'continue_discovery',
  'reprocess',
]);

export const CHAT_SCHEMA_FOLLOWUP_TOOLS = new Set([
  'add_column',
  'edit_column',
  'delete_column',
  'merge_columns',
]);

// Expensive chat tools that pause for server-side confirmation (pending_action)
// or, once completed, already imply a re-run prompt — skip the top banner then.
export const CHAT_RERUN_FOLLOWUP_TOOLS = new Set([
  'reextract',
  'extract_cells',
  'reprocess',
  'run_schematiq',
  'continue_discovery',
]);
export const SHOW_TOOL_SUGGESTION: boolean = false;
