/**
 * Component for rendering a single unit group header row in the grouped table view.
 */

import React from 'react';
import { ChevronDown, ChevronRight, FileText, Merge } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/checkbox';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';
import { UnitSummary } from '../../types/unit';

interface UnitGroupRowProps {
  /** The unit summary data */
  unit: UnitSummary;
  /** Whether this group is expanded */
  isExpanded: boolean;
  /** Callback to toggle expansion */
  onToggleExpand: () => void;
  /** Whether this unit is selected for merge */
  isSelectedForMerge: boolean;
  /** Callback to toggle merge selection */
  onToggleMergeSelection: () => void;
  /** Number of columns in the table (for colspan) */
  columnCount: number;
}

export const UnitGroupRow: React.FC<UnitGroupRowProps> = ({
  unit,
  isExpanded,
  onToggleExpand,
  isSelectedForMerge,
  onToggleMergeSelection,
  columnCount,
}) => {
  // Ensure sourceDocuments is always an array (null safety)
  const sourceDocuments = unit.sourceDocuments ?? [];
  const sourceDocCount = sourceDocuments.length;

  return (
    <tr
      className={cn(
        'bg-muted/50 border-y-2 border-primary/20 cursor-pointer hover:bg-muted/70 transition-colors',
        isSelectedForMerge && 'bg-primary/10 border-primary/40'
      )}
      onClick={onToggleExpand}
    >
      <td colSpan={columnCount} className="px-4 py-3">
        <div className="flex items-center gap-3">
          {/* Expand/Collapse button */}
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 shrink-0"
            onClick={(e) => {
              e.stopPropagation();
              onToggleExpand();
            }}
          >
            {isExpanded ? (
              <ChevronDown className="h-4 w-4" />
            ) : (
              <ChevronRight className="h-4 w-4" />
            )}
          </Button>

          {/* Merge selection checkbox */}
          <div
            onClick={(e) => e.stopPropagation()}
            className="shrink-0"
          >
            <Checkbox
              checked={isSelectedForMerge}
              onCheckedChange={() => onToggleMergeSelection()}
              aria-label={`Select ${unit.name} for merge`}
            />
          </div>

          {/* Unit name and source document count badge */}
          <div className="flex items-center gap-2 min-w-0">
            <span className="font-medium text-base truncate">
              {unit.name}
            </span>

            {/* Source document count badge */}
            <Tooltip>
              <TooltipTrigger asChild>
                <Badge
                  variant={sourceDocCount > 0 ? "info" : "outline"}
                  className="shrink-0 gap-1.5 cursor-help"
                >
                  <FileText className="h-3 w-3" aria-hidden="true" />
                  <span>{sourceDocCount}</span>
                  <span className="sr-only">source documents</span>
                </Badge>
              </TooltipTrigger>
              <TooltipContent>
                <div className="max-w-[300px]">
                  <p className="font-medium mb-1">
                    Source Documents ({sourceDocCount})
                  </p>
                  {sourceDocCount > 1 && (
                    <p className="text-xs text-muted-foreground mb-2">
                      This unit contains data from multiple documents
                    </p>
                  )}
                  {sourceDocCount === 0 ? (
                    <p className="text-xs text-muted-foreground">
                      No source documents tracked for this unit
                    </p>
                  ) : (
                    <ul className="text-xs space-y-0.5">
                      {sourceDocuments.slice(0, 5).map((doc) => (
                        <li key={doc} className="truncate">
                          • {doc}
                        </li>
                      ))}
                      {sourceDocCount > 5 && (
                        <li className="text-muted-foreground">
                          ...and {sourceDocCount - 5} more
                        </li>
                      )}
                    </ul>
                  )}
                </div>
              </TooltipContent>
            </Tooltip>

            {unit.isMerged && (
              <Tooltip>
                <TooltipTrigger asChild>
                  <Badge variant="secondary" className="gap-1 shrink-0">
                    <Merge className="h-3 w-3" />
                    Merged
                  </Badge>
                </TooltipTrigger>
                <TooltipContent>
                  {unit.originalUnits && unit.originalUnits.length > 0 ? (
                    <div>
                      <p className="font-medium">Original units:</p>
                      <ul className="text-xs">
                        {unit.originalUnits.map((name) => (
                          <li key={name}>• {name}</li>
                        ))}
                      </ul>
                    </div>
                  ) : (
                    <p>This unit was created from a merge</p>
                  )}
                </TooltipContent>
              </Tooltip>
            )}
          </div>
        </div>
      </td>
    </tr>
  );
};

export default UnitGroupRow;
