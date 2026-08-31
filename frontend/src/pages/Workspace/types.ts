// Shared TypeScript types for the Workspace page and its sub-components.

export type SheetId = 'data' | 'unit' | 'schema' | 'stats' | 'monitor' | 'documents';

// Every action the top menu bar can perform. SpreadsheetChrome maps this union
// to handlers via a Record, so adding a menu item without wiring it is a type
// error rather than a menu entry that silently does nothing when clicked.
export type WorkspaceMenuAction =
  | 'newProject'
  | 'importProject'
  | 'openClassic'
  | 'exportCsv'
  | 'saveProject'
  | 'saveProjectWithDocs'
  | 'undo'
  | 'redo'
  | 'find'
  | 'showSheet'
  | 'showChat'
  | 'splitView'
  | 'toggleCompactRows'
  | 'projectDetails'
  | 'reextract'
  | 'addDocuments'
  | 'estimateCost'
  | 'refreshProject'
  | 'keyboardShortcuts'
  | 'reportIssue';

export type WorkspaceMenuItem = {
  label: string;
  action: WorkspaceMenuAction;
  // Acts on an open project; rendered disabled while none is open.
  requiresProject?: boolean;
};

export type WorkspaceMenu = {
  label: string;
  items: WorkspaceMenuItem[];
};
export type WorkspaceSessionMode = 'schematiq' | 'load';
export type PendingRerunKind = 'schema' | 'unit';

export type WorkspaceReextractionState = {
  operationId: string;
  columns: string[];
  progress: number;
  processedDocuments: number;
  totalDocuments: number;
  currentColumn?: string;
};

export type SheetColumn = {
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
export type WorkspaceMessage = {
  id: string;
  role: 'assistant' | 'user' | 'tool';
  content: string;
  kind?: 'text' | 'tool_log';
  toolName?: string;
  toolStatus?: 'running' | 'done' | 'error';
};

export type PendingChatAction = {
  id: string;
  label: string;
  description: string;
  chatId: string;
};

export type TableFontFamily = 'Inter' | 'Arial' | 'Georgia' | 'Mono';
export type TableTextAlign = 'left' | 'center' | 'right';

export type TableDisplayOptions = {
  fontFamily: TableFontFamily;
  fontSize: number;
  bold: boolean;
  italic: boolean;
  underline: boolean;
  strikethrough: boolean;
  align: TableTextAlign;
};

export type CellFormat = Partial<TableDisplayOptions>;
export type CellFormatMap = Record<string, CellFormat>;

export type SheetSelection = {
  sheet: SheetId;
  fromRow: number;
  toRow: number;
  fromCol: number;
  toCol: number;
} | null;
// Resolved scope for the "Wrong, try again" menu item (see
// selectedCellScope in helpers.ts): unit row names + schema column keys
// covered by a single-cell-or-rectangle selection, plus the current value of
// the one flagged cell when the selection is exactly one cell.
export type WrongCellScope = {
  rows: string[];
  columns: string[];
  previousValue?: string;
};
export type NewProjectDialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreated: (sessionId: string) => void;
};
export type DocumentSourceInput =
  | { mode: 'upload' }
  | { mode: 'cloud'; datasets: string[] };
