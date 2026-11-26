// Contextual floating pill bar surfaced at the bottom-center of the Data grid
// whenever cells, rows, or columns are selected. Promotes AI-native actions
// ("Fill empty", "Wrong, try again", "Ask AI", "Merge columns/units", "Verify",
// "Evidence") out of right-click menus into immediate, discoverable controls.
// Parent: Workspace (index.tsx).

import {
  CheckCircle2,
  Columns3,
  FileSearch,
  GitMerge,
  MessageSquarePlus,
  RotateCw,
  SlidersHorizontal,
  Sparkles,
  X,
} from 'lucide-react';

import type { WrongCellScope } from './types';

export interface SelectionActionContext {
  cellCount: number;
  rowCount: number;
  colCount: number;
  rowNames: string[];
  columnKeys: string[];
  columnLabels: string[];
  emptyScope: { rows: string[]; columns: string[] } | null;
  wrongScope: WrongCellScope | null;
  singleCell?: {
    rowName: string;
    columnKey: string;
    columnLabel: string;
    value: string;
    sourceDoc?: string;
    verifyKey: string;
    isVerified: boolean;
    hasGrounding: boolean;
  };
}

export function FloatingSelectionBar({
  context,
  busy,
  canInspectColumn,
  onOpenEvidence,
  onToggleVerify,
  onFillEmpty,
  onRetryWrong,
  onAskAI,
  onInspectColumn,
  onMergeColumns,
  onMergeUnits,
  onDismiss,
}: {
  context: SelectionActionContext | null;
  busy?: boolean;
  canInspectColumn?: boolean;
  onOpenEvidence: () => void;
  onToggleVerify: (verifyKey: string) => void;
  onFillEmpty: (scope: { rows: string[]; columns: string[] }) => void;
  onRetryWrong: (scope: WrongCellScope) => void;
  onAskAI: (ctx: SelectionActionContext) => void;
  onInspectColumn?: (columnKey: string) => void;
  onMergeColumns?: (columns: string[]) => void;
  onMergeUnits?: (units: string[]) => void;
  onDismiss: () => void;
}) {
  if (!context || context.cellCount === 0) return null;

  const summary =
    context.cellCount === 1 && context.singleCell
      ? context.singleCell.columnLabel
      : `${context.cellCount} cell${context.cellCount === 1 ? '' : 's'}${
          context.colCount > 1 ? ` · ${context.colCount} cols` : ''
        }`;

  const canMergeCols = context.columnKeys.length >= 2 && Boolean(onMergeColumns);
  const canMergeRows = context.rowNames.length >= 2 && context.colCount === 1 && Boolean(onMergeUnits);

  return (
    <div
      className="workspace-floating-selection-bar"
      role="toolbar"
      aria-label="Selection actions"
    >
      <span className="workspace-fsb-summary" title={summary}>
        {summary}
      </span>

      <span className="workspace-fsb-sep" aria-hidden="true" />

      {context.singleCell && (
        <button
          type="button"
          className="workspace-fsb-btn"
          onClick={onOpenEvidence}
          title="Inspect source document evidence for this cell"
        >
          <FileSearch className="h-3.5 w-3.5" />
          <span>{context.singleCell.hasGrounding ? 'Evidence' : 'Source'}</span>
        </button>
      )}

      {context.singleCell && (
        <button
          type="button"
          className={`workspace-fsb-btn${context.singleCell.isVerified ? ' is-verified' : ''}`}
          onClick={() => onToggleVerify(context.singleCell!.verifyKey)}
          title={context.singleCell.isVerified ? 'Unmark verified' : 'Mark this cell as human-verified'}
        >
          <CheckCircle2 className="h-3.5 w-3.5" />
          <span>{context.singleCell.isVerified ? 'Verified' : 'Verify'}</span>
        </button>
      )}

      {context.emptyScope && (
        <button
          type="button"
          className="workspace-fsb-btn is-ai"
          disabled={busy}
          onClick={() => onFillEmpty(context.emptyScope!)}
          title="Run extraction only on blank cells in this selection"
        >
          <Sparkles className="h-3.5 w-3.5" />
          <span>Fill empty</span>
        </button>
      )}

      {context.wrongScope && (
        <button
          type="button"
          className="workspace-fsb-btn"
          disabled={busy}
          onClick={() => onRetryWrong(context.wrongScope!)}
          title="Tell the model this answer is wrong and re-extract from the document"
        >
          <RotateCw className="h-3.5 w-3.5" />
          <span>Wrong, try again</span>
        </button>
      )}

      <button
        type="button"
        className="workspace-fsb-btn is-ai"
        onClick={() => onAskAI(context)}
        title="Attach this selection as context in the AI assistant"
      >
        <MessageSquarePlus className="h-3.5 w-3.5" />
        <span>Ask AI</span>
      </button>

      {canInspectColumn && context.columnKeys.length === 1 && onInspectColumn && (
        <button
          type="button"
          className="workspace-fsb-btn"
          onClick={() => onInspectColumn(context.columnKeys[0])}
          title="Edit this column's prompt, type, and allowed values"
        >
          <SlidersHorizontal className="h-3.5 w-3.5" />
          <span>Column</span>
        </button>
      )}

      {canMergeCols && (
        <button
          type="button"
          className="workspace-fsb-btn"
          disabled={busy}
          onClick={() => onMergeColumns?.(context.columnKeys)}
          title={`Merge ${context.columnKeys.length} selected columns`}
        >
          <Columns3 className="h-3.5 w-3.5" />
          <span>Merge cols</span>
        </button>
      )}

      {canMergeRows && (
        <button
          type="button"
          className="workspace-fsb-btn"
          disabled={busy}
          onClick={() => onMergeUnits?.(context.rowNames)}
          title={`Merge ${context.rowNames.length} selected rows into one unit`}
        >
          <GitMerge className="h-3.5 w-3.5" />
          <span>Merge units</span>
        </button>
      )}

      <button
        type="button"
        className="workspace-fsb-close"
        onClick={onDismiss}
        title="Dismiss selection bar"
        aria-label="Dismiss selection bar"
      >
        <X className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}
