import { ChevronDown } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import type { CostEstimate } from '@/types';

interface CostBreakdownProps {
  estimate: CostEstimate;
  /** When true, the value-extraction phase is dimmed and marked as skipped. */
  skipValueExtraction?: boolean;
  className?: string;
}

/**
 * Collapsible per-phase cost breakdown (schema discovery + value extraction),
 * with document stats and estimate warnings. Shared by the classic config
 * screen and the Workspace new-project dialog so the breakdown lives in one place.
 */
export function CostBreakdown({ estimate, skipValueExtraction = false, className }: CostBreakdownProps) {
  return (
    <Collapsible className={className}>
      <CollapsibleTrigger className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors">
        <ChevronDown className="h-3 w-3" />
        <span>View breakdown</span>
      </CollapsibleTrigger>
      <CollapsibleContent className="mt-2 space-y-3">
        {/* Schema Discovery */}
        <div className="p-2 bg-muted/30 rounded border text-xs">
          <div className="font-medium text-muted-foreground mb-1">Schema Discovery</div>
          <div className="space-y-0.5">
            <div className="flex justify-between">
              <span>API Calls:</span>
              <span className="font-mono">{estimate.schema_discovery.api_calls}</span>
            </div>
            <div className="flex justify-between">
              <span>Input:</span>
              <span className="font-mono">{estimate.schema_discovery.input_tokens.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span>Output:</span>
              <span className="font-mono">{estimate.schema_discovery.output_tokens.toLocaleString()}</span>
            </div>
            <div className="flex justify-between pt-0.5 border-t font-medium">
              <span>Cost:</span>
              <span className="font-mono">${estimate.schema_discovery.cost_usd.toFixed(4)}</span>
            </div>
          </div>
        </div>

        {/* Value Extraction */}
        <div className={`p-2 bg-muted/30 rounded border text-xs ${skipValueExtraction ? 'opacity-50' : ''}`}>
          <div className="font-medium text-muted-foreground mb-1">
            Value Extraction
            {skipValueExtraction && <Badge variant="secondary" className="ml-1 text-[10px]">Skipped</Badge>}
          </div>
          <div className="space-y-0.5">
            <div className="flex justify-between">
              <span>API Calls:</span>
              <span className="font-mono">{estimate.value_extraction.api_calls}</span>
            </div>
            <div className="flex justify-between">
              <span>Input:</span>
              <span className="font-mono">{estimate.value_extraction.input_tokens.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span>Output:</span>
              <span className="font-mono">{estimate.value_extraction.output_tokens.toLocaleString()}</span>
            </div>
            <div className="flex justify-between pt-0.5 border-t font-medium">
              <span>Cost:</span>
              <span className="font-mono">${estimate.value_extraction.cost_usd.toFixed(4)}</span>
            </div>
          </div>
        </div>

        {/* Document Stats */}
        {estimate.document_stats.num_documents > 0 && (
          <div className="text-[11px] text-muted-foreground space-y-0.5">
            <div>
              {estimate.document_stats.num_documents} docs, ~
              {estimate.document_stats.avg_tokens_per_document.toLocaleString()} tok/doc
            </div>
          </div>
        )}

        {/* Warnings */}
        {estimate.warnings.length > 0 && (
          <div className="space-y-1">
            {estimate.warnings.map((warning, idx) => (
              <p key={idx} className="text-[11px] text-amber-600">{warning}</p>
            ))}
          </div>
        )}

        <p className="text-[11px] text-muted-foreground italic">
          * Estimate may vary with actual usage.
        </p>
      </CollapsibleContent>
    </Collapsible>
  );
}

export default CostBreakdown;
