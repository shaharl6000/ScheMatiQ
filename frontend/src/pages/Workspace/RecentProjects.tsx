// Recent projects list shown at the top of the New Project dialog so returning
// users can reopen existing work instead of re-uploading or re-importing.
// Parent: NewProjectDialog.

import { useEffect, useState } from 'react';
import { Clock, FileText, Loader2, Table2 } from 'lucide-react';

import { loadAPI, schematiqAPI } from '@/services/api';
import type { VisualizationSession } from '@/types';

const MAX_RECENT = 6;

function projectDisplayName(session: VisualizationSession): string {
  const query =
    session.creation_metadata?.creation_query?.trim() ||
    session.schema_query?.trim() ||
    session.metadata?.source?.trim();
  if (query) return query.length > 80 ? `${query.slice(0, 79)}\u2026` : query;
  return `Untitled project (${session.id.slice(0, 8)})`;
}

function sessionTime(session: VisualizationSession): number {
  const iso = session.metadata?.last_modified || session.metadata?.created;
  const value = iso ? new Date(iso).getTime() : 0;
  return Number.isNaN(value) ? 0 : value;
}

function formatRelativeTime(iso?: string): string {
  if (!iso) return '';
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return '';
  const diffMs = then - Date.now();
  const abs = Math.abs(diffMs);
  const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' });
  const minute = 60_000;
  const hour = 60 * minute;
  const day = 24 * hour;
  const week = 7 * day;
  const month = 30 * day;
  const year = 365 * day;
  if (abs < hour) return rtf.format(Math.round(diffMs / minute), 'minute');
  if (abs < day) return rtf.format(Math.round(diffMs / hour), 'hour');
  if (abs < week) return rtf.format(Math.round(diffMs / day), 'day');
  if (abs < month) return rtf.format(Math.round(diffMs / week), 'week');
  if (abs < year) return rtf.format(Math.round(diffMs / month), 'month');
  return rtf.format(Math.round(diffMs / year), 'year');
}

export function RecentProjects({
  open,
  onOpenProject,
}: {
  open: boolean;
  onOpenProject: (session: VisualizationSession) => void;
}) {
  const [sessions, setSessions] = useState<VisualizationSession[] | null>(null);
  const [loading, setLoading] = useState(false);

  // Fetch once each time the dialog opens. Both lists are pulled in parallel and
  // merged; the endpoints are backed by an in-memory store so this is cheap.
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setLoading(true);
    (async () => {
      const [schematiqSessions, loadSessions] = await Promise.all([
        schematiqAPI.listSessions().catch(() => [] as VisualizationSession[]),
        loadAPI.listSessions().catch(() => [] as VisualizationSession[]),
      ]);
      if (cancelled) return;
      const byId = new Map<string, VisualizationSession>();
      for (const session of [...schematiqSessions, ...loadSessions]) {
        if (session?.id && !byId.has(session.id)) byId.set(session.id, session);
      }
      const merged = Array.from(byId.values()).sort(
        (a, b) => sessionTime(b) - sessionTime(a),
      );
      setSessions(merged);
      setLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [open]);

  if (loading && !sessions) {
    return (
      <div className="flex items-center gap-2 rounded-md border bg-muted/40 px-3 py-3 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading recent projects{'\u2026'}
      </div>
    );
  }

  if (!sessions || sessions.length === 0) return null;

  const recent = sessions.slice(0, MAX_RECENT);

  return (
    <div className="grid gap-2">
      <div className="flex items-center gap-1.5 text-sm font-medium text-foreground">
        <Clock className="h-4 w-4 text-muted-foreground" />
        Recent projects
      </div>
      <div className="grid gap-1.5">
        {recent.map((session) => {
          const rowCount = session.metadata?.row_count;
          return (
            <button
              key={session.id}
              type="button"
              onClick={() => onOpenProject(session)}
              className="flex items-center gap-3 rounded-md border border-border/60 bg-background px-3 py-2 text-left transition-colors hover:border-primary hover:bg-muted/50"
            >
              <FileText className="h-4 w-4 shrink-0 text-muted-foreground" />
              <div className="min-w-0 flex-1">
                <div className="truncate text-sm font-medium text-foreground">
                  {projectDisplayName(session)}
                </div>
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <span>{formatRelativeTime(session.metadata?.last_modified || session.metadata?.created)}</span>
                  {typeof rowCount === 'number' && (
                    <>
                      <span aria-hidden>{'\u00b7'}</span>
                      <span className="inline-flex items-center gap-1">
                        <Table2 className="h-3 w-3" />
                        {rowCount} {rowCount === 1 ? 'row' : 'rows'}
                      </span>
                    </>
                  )}
                </div>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
