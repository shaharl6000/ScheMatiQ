// Read-only dialog showing project run settings, provenance, and document list.
// Parent: Workspace (index.tsx).

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import type { CostEstimate, ScheMatiQConfig, ScheMatiQStatus, SchemaData } from '@/types';
import type { DocumentListResponse } from '@/types/unit';

import { formatCost } from './helpers';
import type { WorkspaceSessionMode } from './types';

export function ProjectDetailsDialog({
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
    { label: 'Batching', value: (config?.batch_strategy ?? 'smart') === 'fixed' ? 'Fixed size' : 'Smart (automatic)' },
    ...((config?.batch_strategy ?? 'smart') === 'fixed'
      ? [{ label: 'Documents per batch', value: config?.documents_batch_size ?? '' }]
      : []),
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
