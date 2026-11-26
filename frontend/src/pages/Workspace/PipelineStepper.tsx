// Live 3-stage pipeline progress stepper rendered inline above the Data sheet
// whenever schema discovery, observation-unit review, or extraction/re-extraction
// is running. Replaces staring at a tiny bottombar percentage with clear stage
// feedback, elapsed time, and direct Review / Stop controls.
// Parent: Workspace (index.tsx).

import { useEffect, useState } from 'react';
import {
  Activity,
  CheckCircle2,
  Circle,
  Layers,
  Loader2,
  Square,
} from 'lucide-react';

import type { ScheMatiQStatus } from '@/types';
import type { WorkspaceReextractionState } from './types';

function formatElapsed(sec: number): string {
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}:${String(s).padStart(2, '0')}`;
}

export function PipelineStepper({
  status,
  reextraction,
  stoppingReextraction,
  onStopReextraction,
  onStopPipeline,
  onOpenMonitor,
}: {
  status: ScheMatiQStatus | null;
  reextraction: WorkspaceReextractionState | null;
  stoppingReextraction?: boolean;
  onStopReextraction?: () => void;
  onStopPipeline?: () => void;
  onOpenMonitor: () => void;
}) {
  const isPipelineActive =
    status?.status === 'processing' || status?.status === 'observation_unit_review';
  const active = Boolean(reextraction || isPipelineActive);
  const [elapsedSec, setElapsedSec] = useState(0);

  useEffect(() => {
    if (!active) {
      setElapsedSec(0);
      return undefined;
    }
    const startedAt = Date.now();
    const id = window.setInterval(() => {
      setElapsedSec(Math.floor((Date.now() - startedAt) / 1000));
    }, 1000);
    return () => window.clearInterval(id);
  }, [active, reextraction?.operationId]);

  if (!active) return null;

  // Re-extraction banner mode
  if (reextraction) {
    const pct = Math.round((reextraction.progress || 0) * 100);
    const colsLabel = reextraction.columns.length > 0
      ? reextraction.columns.slice(0, 3).join(', ') + (reextraction.columns.length > 3 ? ` +${reextraction.columns.length - 3}` : '')
      : 'selected columns';
    return (
      <div className="workspace-pipeline-stepper" role="status" aria-live="polite">
        <div className="flex items-center gap-2 min-w-0 flex-1">
          <Loader2 className="h-4 w-4 shrink-0 animate-spin text-violet-600 dark:text-violet-400" />
          <span className="font-semibold text-xs text-foreground whitespace-nowrap">
            Extracting {colsLabel}
          </span>
          <span className="text-xs text-muted-foreground truncate">
            {reextraction.processedDocuments}/{reextraction.totalDocuments || '?'} docs · {pct}%
          </span>
          <div className="hidden sm:block w-28 h-1.5 rounded-full bg-muted overflow-hidden shrink-0">
            <div
              className="h-full rounded-full bg-violet-500 transition-all duration-300"
              style={{ width: `${Math.max(4, pct)}%` }}
            />
          </div>
          <span className="text-[11px] tabular-nums text-muted-foreground shrink-0">
            {formatElapsed(elapsedSec)}
          </span>
        </div>
        <div className="flex items-center gap-1.5 shrink-0">
          {onStopReextraction && (
            <button
              type="button"
              disabled={stoppingReextraction}
              onClick={onStopReextraction}
              className="inline-flex items-center gap-1 rounded-md border border-border bg-background px-2 py-1 text-[11px] font-medium text-foreground hover:bg-muted disabled:opacity-50"
            >
              <Square className="h-3 w-3" />
              {stoppingReextraction ? 'Stopping…' : 'Stop'}
            </button>
          )}
        </div>
      </div>
    );
  }

  // Full 3-stage pipeline mode
  const isObsReview = status?.status === 'observation_unit_review';
  const schemaDone = Boolean(status?.schema_completed);
  const obsDone = !isObsReview && (schemaDone || (status?.columns_discovered ?? 0) > 0 || (status?.progress ?? 0) > 0.15);

  const stage1State = isObsReview ? 'review' : obsDone ? 'done' : 'active';
  const stage2State = schemaDone ? 'done' : obsDone ? 'active' : 'pending';
  const stage3State = schemaDone ? 'active' : 'pending';

  const renderIcon = (state: 'done' | 'active' | 'review' | 'pending') => {
    if (state === 'done') return <CheckCircle2 className="h-3.5 w-3.5 text-emerald-600 dark:text-emerald-400" />;
    if (state === 'review') return <Layers className="h-3.5 w-3.5 text-violet-600 dark:text-violet-400 animate-pulse" />;
    if (state === 'active') return <Loader2 className="h-3.5 w-3.5 text-primary animate-spin" />;
    return <Circle className="h-3.5 w-3.5 text-muted-foreground/45" />;
  };

  return (
    <div className="workspace-pipeline-stepper" role="status" aria-live="polite">
      <div className="flex items-center gap-3 min-w-0 flex-1 overflow-x-auto">
        <div className="flex items-center gap-1.5 text-xs whitespace-nowrap">
          {renderIcon(stage1State)}
          <span className="font-medium text-foreground">
            1. Observation Unit
          </span>
        </div>
        <span className="text-muted-foreground/40 text-xs">→</span>
        <div className="flex items-center gap-1.5 text-xs whitespace-nowrap">
          {renderIcon(stage2State)}
          <span className={stage2State === 'pending' ? 'text-muted-foreground' : 'font-medium text-foreground'}>
            2. Schema{status?.columns_discovered ? ` (${status.columns_discovered})` : ''}
          </span>
        </div>
        <span className="text-muted-foreground/40 text-xs">→</span>
        <div className="flex items-center gap-1.5 text-xs whitespace-nowrap">
          {renderIcon(stage3State)}
          <span className={stage3State === 'pending' ? 'text-muted-foreground' : 'font-medium text-foreground'}>
            3. Extraction
            {status?.total_documents ? ` (${status.processed_documents ?? 0}/${status.total_documents})` : ''}
          </span>
        </div>
        <span className="text-[11px] tabular-nums text-muted-foreground ml-1 shrink-0">
          {formatElapsed(elapsedSec)}
        </span>
      </div>

      <div className="flex items-center gap-1.5 shrink-0">
        <button
          type="button"
          onClick={onOpenMonitor}
          className="inline-flex items-center gap-1 rounded-md border border-border bg-background px-2 py-1 text-[11px] font-medium text-foreground hover:bg-muted"
        >
          <Activity className="h-3 w-3" />
          {isObsReview ? 'Review Unit' : 'Details'}
        </button>
        {onStopPipeline && (
          <button
            type="button"
            onClick={onStopPipeline}
            className="inline-flex items-center gap-1 rounded-md border border-border bg-background px-2 py-1 text-[11px] font-medium text-foreground hover:bg-muted"
          >
            <Square className="h-3 w-3" />
            Stop
          </button>
        )}
      </div>
    </div>
  );
}
