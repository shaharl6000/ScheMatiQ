// "Recent projects" control for the New Project dialog. Rendered as a trigger
// button that opens a modal dialog; the list is only fetched and shown once the
// window is opened.
//
// The list is scoped to this browser via localStorage (see utils/recentProjects):
// it only shows projects opened here, never the full multi-user session list,
// because the backend has no per-user scoping. Parent: NewProjectDialog.

import { useEffect, useState } from 'react';
import { Clock, FileText, Loader2, Table2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import { loadAPI } from '@/services/api';
import type { VisualizationSession } from '@/types';
import { forgetProjects, getRecentProjectIds } from '@/utils/recentProjects';

// How many stored ids to fetch details for (a few more than we display, so the
// list still fills after pruning ids that no longer resolve).
const FETCH_LIMIT = 10;
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
  onOpenProject,
}: {
  onOpenProject: (session: VisualizationSession) => void;
}) {
  const [dialogOpen, setDialogOpen] = useState(false);
  const [sessions, setSessions] = useState<VisualizationSession[] | null>(null);
  const [loading, setLoading] = useState(false);

  // Fetch only when the window is opened, so the parent dialog stays cheap.
  useEffect(() => {
    if (!dialogOpen) return;
    const ids = getRecentProjectIds().slice(0, FETCH_LIMIT);
    if (ids.length === 0) {
      setSessions([]);
      return;
    }
    let cancelled = false;
    setLoading(true);
    (async () => {
      // Fetch each remembered project by id. The backend resolves a session by
      // id regardless of type, so one endpoint covers both load and schematiq
      // projects; the returned `type` drives navigation mode.
      const results = await Promise.all(
        ids.map((id) =>
          loadAPI.getSession(id).then(
            (session) => ({ id, session: session as VisualizationSession | null }),
            () => ({ id, session: null as VisualizationSession | null }),
          ),
        ),
      );
      if (cancelled) return;
      const missing = results.filter((r) => r.session === null).map((r) => r.id);
      if (missing.length > 0) forgetProjects(missing);
      const found = results
        .map((r) => r.session)
        .filter((s): s is VisualizationSession => s !== null)
        .sort((a, b) => sessionTime(b) - sessionTime(a));
      setSessions(found);
      setLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [dialogOpen]);

  const handleOpen = (session: VisualizationSession) => {
    setDialogOpen(false);
    onOpenProject(session);
  };

  return (
    <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
      <DialogTrigger asChild>
        <Button type="button" variant="outline" size="sm" className="w-fit justify-start gap-2">
          <Clock className="h-4 w-4" />
          Recent projects
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Recent projects</DialogTitle>
          <DialogDescription>Projects you've opened in this browser.</DialogDescription>
        </DialogHeader>

        {loading && !sessions ? (
          <div className="flex items-center gap-2 px-1 py-6 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading recent projects{'\u2026'}
          </div>
        ) : !sessions || sessions.length === 0 ? (
          <div className="px-1 py-6 text-sm text-muted-foreground">
            No recent projects in this browser yet.
          </div>
        ) : (
          <div className="grid max-h-[60vh] gap-1.5 overflow-y-auto">
            {sessions.slice(0, MAX_RECENT).map((session) => {
              const rowCount = session.metadata?.row_count;
              return (
                <button
                  key={session.id}
                  type="button"
                  onClick={() => handleOpen(session)}
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
        )}
      </DialogContent>
    </Dialog>
  );
}
