// Banner shown over the Data sheet when the session resolves (its schema is
// intact) but its extracted table rows are no longer in storage. Keeps the
// workbook usable (including the Schema and Observation Unit sheets) and offers
// a direct action to re-run extraction with the existing schema.
// Parent: Workspace (index.tsx).

import { Loader2, RotateCw } from 'lucide-react';

export function DataMissingBanner({
  busy,
  canRerun,
  onRerun,
}: {
  busy: boolean;
  canRerun: boolean;
  onRerun: () => void;
}) {
  return (
    <div
      className="workspace-followup-banner"
      role="status"
      data-testid="data-missing-banner"
    >
      <div className="workspace-followup-banner-copy">
        <strong>Extracted data is no longer in storage</strong>
        <span>
          This project exists but its extracted data is no longer in storage. The schema is intact, so re-running extraction will rebuild the table.
        </span>
      </div>
      <div className="workspace-followup-banner-actions">
        <button
          className="workspace-followup-action workspace-followup-action-primary"
          type="button"
          onClick={onRerun}
          disabled={busy || !canRerun}
          title={!canRerun ? 'Add at least one schema column before running extraction' : undefined}
        >
          {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RotateCw className="h-3.5 w-3.5" />}
          Re-run extraction
        </button>
      </div>
    </div>
  );
}
