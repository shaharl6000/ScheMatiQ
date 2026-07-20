import {
  DEFAULT_ADVANCED_SETTINGS,
  type AdvancedSettingsValue,
} from '@/components/AdvancedSettings/AdvancedSettingsFields';
import type { PaginatedData } from '@/types';

import type { SheetId, TableFontFamily } from './types';

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

export const WORKSPACE_MENUS = [
  {
    label: 'File',
    items: ['New project', 'Import project', 'Open classic visualizer', 'Download table (.csv)', 'Save project (.schematiq.json)', 'Save project with documents (.zip)'],
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
    items: ['Sort range', 'Create filter', 'Re-extract table', 'Add documents', 'Validate schema'],
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
  'edit_observation_unit',
  'run_schematiq',
  'reextract',
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
  'reprocess',
  'run_schematiq',
  'continue_discovery',
]);
export const SHOW_TOOL_SUGGESTION: boolean = false;
