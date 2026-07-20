// Banner prompting re-extract or schema rediscovery after schema/unit edits.
// Parent: Workspace (index.tsx).

import { Loader2, RotateCw, Sparkles } from 'lucide-react';

import type { PendingRerunKind, WorkspaceSessionMode } from './types';

export function PendingRerunBanner({
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
