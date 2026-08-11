// Banner shown when the table-data endpoint answers 503 ("data read failed"):
// the session's rows exist in storage but could not be hydrated to the server's
// local disk, so the grid would otherwise render schema headers over an empty
// table with no explanation. Retries run automatically with backoff (see
// noteDataFetchError in index.tsx); the button retries immediately.
// Parent: Workspace (index.tsx).

import { Loader2, RotateCw } from 'lucide-react';

export function DataLoadErrorBanner({
  message,
  retrying,
  onRetry,
}: {
  message: string;
  retrying: boolean;
  onRetry: () => void;
}) {
  return (
    <div className="workspace-followup-banner workspace-data-error-banner" role="alert">
      <div className="workspace-followup-banner-copy">
        <strong>Table data could not be loaded</strong>
        <span>{message} Retrying automatically — the rows are safe in storage.</span>
      </div>
      <div className="workspace-followup-banner-actions">
        <button
          className="workspace-followup-action workspace-followup-action-primary"
          type="button"
          onClick={onRetry}
          disabled={retrying}
        >
          {retrying ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RotateCw className="h-3.5 w-3.5" />}
          Retry now
        </button>
      </div>
    </div>
  );
}
