// Muted top-bar Open / Import popover housing Import from file, Recent projects,
// and Sample tables so the home screen stays ultra-minimal while returning users
// can open any previous or saved project in one click from anywhere.
// Parent: SpreadsheetChrome.

import { useEffect, useState } from 'react';
import { Clock, FileUp, FolderOpen, Loader2, Sparkles, Table2 } from 'lucide-react';

import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { useToast } from '@/components/ui/use-toast';
import { cloudAPI, loadAPI } from '@/services/api';
import type { VisualizationSession } from '@/types';
import { forgetProjects, getRecentProjectIds } from '@/utils/recentProjects';

interface TemplateInfo {
  name: string;
  description?: string;
  row_count?: number;
  column_count?: number;
}

function projectDisplayName(session: VisualizationSession): string {
  const query =
    session.creation_metadata?.creation_query?.trim() ||
    session.schema_query?.trim() ||
    session.metadata?.source?.trim();
  if (query) return query.length > 62 ? `${query.slice(0, 61)}…` : query;
  return `Untitled project (${session.id.slice(0, 8)})`;
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
  if (abs < hour) return rtf.format(Math.round(diffMs / minute), 'minute');
  if (abs < day) return rtf.format(Math.round(diffMs / hour), 'hour');
  return rtf.format(Math.round(diffMs / day), 'day');
}

export function OpenProjectPopover({
  onImportProject,
  onOpenSession,
}: {
  onImportProject: () => void;
  onOpenSession?: (sessionId: string, mode: 'load' | 'schematiq') => void;
}) {
  const { toast } = useToast();
  const [open, setOpen] = useState(false);
  const [recentSessions, setRecentSessions] = useState<VisualizationSession[]>([]);
  const [recentLoading, setRecentLoading] = useState(false);
  const [templates, setTemplates] = useState<TemplateInfo[]>([]);
  const [templatesLoaded, setTemplatesLoaded] = useState(false);
  const [loadingTemplate, setLoadingTemplate] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return undefined;
    let cancelled = false;

    const ids = getRecentProjectIds().slice(0, 8);
    if (ids.length === 0) {
      setRecentSessions([]);
      setRecentLoading(false);
    } else {
      setRecentLoading(true);
      Promise.all(
        ids.map((id) =>
          loadAPI.getSession(id).then(
            (session) => ({ id, session: session as VisualizationSession | null, gone: false }),
            // Only a 404 means the project was deleted. A network error or a
            // backend restart must not wipe the browser's recent list.
            (err: any) => ({ id, session: null as VisualizationSession | null, gone: err?.response?.status === 404 }),
          ),
        ),
      )
        .then((results) => {
          if (cancelled) return;
          const missing = results.filter((r) => r.gone).map((r) => r.id);
          if (missing.length > 0) forgetProjects(missing);
          const found = results
            .map((r) => r.session)
            .filter((s): s is VisualizationSession => s !== null)
            .sort((a, b) => {
              const ta = new Date(a.metadata?.last_modified || a.metadata?.created || 0).getTime();
              const tb = new Date(b.metadata?.last_modified || b.metadata?.created || 0).getTime();
              return tb - ta;
            });
          setRecentSessions(found.slice(0, 6));
        })
        .finally(() => {
          if (!cancelled) setRecentLoading(false);
        });
    }

    if (!templatesLoaded) {
      cloudAPI.getTemplates()
        .then((data) => {
          if (!cancelled) {
            setTemplates(Array.isArray(data) ? data : []);
            setTemplatesLoaded(true);
          }
        })
        .catch(() => {
          if (!cancelled) {
            setTemplates([]);
            setTemplatesLoaded(true);
          }
        });
    }

    return () => {
      cancelled = true;
    };
  }, [open, templatesLoaded]);

  const handleOpenTemplate = async (templateName: string) => {
    if (loadingTemplate) return;
    setLoadingTemplate(templateName);
    try {
      const res = await cloudAPI.loadTemplate(templateName);
      setOpen(false);
      toast({
        title: 'Sample table loaded',
        description: `${templateName} (${res.row_count} rows · ${res.column_count} columns)`,
      });
      onOpenSession?.(res.session_id, 'load');
    } catch (err: any) {
      toast({
        title: 'Could not load sample',
        description: err?.response?.data?.detail || err?.message || 'Sample failed to load.',
        variant: 'destructive',
      });
    } finally {
      setLoadingTemplate(null);
    }
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          type="button"
          className="workspace-chrome-link cursor-pointer"
          title="Open recent or import project"
          aria-label="Open recent or import project"
        >
          <FolderOpen size={15} aria-hidden="true" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="end" sideOffset={6} className="w-80 p-2 space-y-2">
        {/* Import from computer */}
        <button
          type="button"
          onClick={() => {
            setOpen(false);
            onImportProject();
          }}
          className="w-full flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-left transition-colors hover:bg-muted"
        >
          <FileUp className="h-4 w-4 shrink-0 text-muted-foreground" />
          <div className="min-w-0 flex-1">
            <div className="text-xs font-medium text-foreground">Import project…</div>
            <div className="text-[11px] text-muted-foreground">.schematiq.json, .zip bundle, or CSV</div>
          </div>
        </button>

        {/* Recent projects */}
        <div className="pt-1.5 border-t border-border/70 space-y-1">
          <div className="flex items-center gap-1.5 px-2 py-0.5 text-[10.5px] font-semibold uppercase tracking-wider text-muted-foreground">
            <Clock className="h-3 w-3" />
            <span>Recent projects</span>
          </div>

          {recentLoading ? (
            <div className="flex items-center gap-2 px-2 py-2 text-xs text-muted-foreground">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              Loading…
            </div>
          ) : recentSessions.length === 0 ? (
            <div className="px-2 py-1.5 text-xs text-muted-foreground">
              No recent projects yet.
            </div>
          ) : (
            <div className="max-h-56 overflow-y-auto space-y-0.5">
              {recentSessions.map((session) => {
                const rowCount = session.metadata?.row_count;
                return (
                  <button
                    key={session.id}
                    type="button"
                    onClick={() => {
                      setOpen(false);
                      onOpenSession?.(session.id, session.type === 'load' ? 'load' : 'schematiq');
                    }}
                    className="w-full flex items-center gap-2 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-muted"
                  >
                    <Table2 className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-xs font-medium text-foreground">
                        {projectDisplayName(session)}
                      </div>
                      <div className="flex items-center gap-1.5 text-[10.5px] text-muted-foreground tabular-nums">
                        {typeof rowCount === 'number' && (
                          <span>{rowCount} row{rowCount === 1 ? '' : 's'}</span>
                        )}
                        {typeof rowCount === 'number' && <span>·</span>}
                        <span>{formatRelativeTime(session.metadata?.last_modified || session.metadata?.created)}</span>
                      </div>
                    </div>
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {/* Sample tables (when configured) */}
        {templates.length > 0 && (
          <div className="pt-1.5 border-t border-border/70 space-y-1">
            <div className="flex items-center gap-1.5 px-2 py-0.5 text-[10.5px] font-semibold uppercase tracking-wider text-muted-foreground">
              <Sparkles className="h-3 w-3" />
              <span>Sample tables</span>
            </div>
            <div className="space-y-0.5">
              {templates.slice(0, 4).map((tpl) => {
                const isBusy = loadingTemplate === tpl.name;
                return (
                  <button
                    key={tpl.name}
                    type="button"
                    disabled={Boolean(loadingTemplate)}
                    onClick={() => void handleOpenTemplate(tpl.name)}
                    className="w-full flex items-center gap-2 rounded-md px-2 py-1.5 text-left text-xs hover:bg-muted transition-colors disabled:opacity-60"
                  >
                    {isBusy ? (
                      <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin text-primary" />
                    ) : (
                      <Table2 className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                    )}
                    <span className="flex-1 truncate font-medium text-foreground">{tpl.name}</span>
                    {tpl.row_count != null && (
                      <span className="text-[10.5px] text-muted-foreground tabular-nums">
                        {tpl.row_count} rows
                      </span>
                    )}
                  </button>
                );
              })}
            </div>
          </div>
        )}
      </PopoverContent>
    </Popover>
  );
}
