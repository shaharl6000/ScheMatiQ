/**
 * Component for rendering a single unit group header row in the grouped table view.
 */

import React from 'react';
import { ChevronDown, ChevronRight, Merge } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { UnitSummary } from '../../types/unit';

interface UnitGroupRowProps {
  /** The unit summary data */
  unit: UnitSummary;
  /** Whether this group is expanded */
  isExpanded: boolean;
  /** Callback to toggle expansion */
  onToggleExpand: () => void;
  /** Number of columns in the table (for colspan) */
  columnCount: number;
}

export const UnitGroupRow: React.FC<UnitGroupRowProps> = ({
  unit,
  isExpanded,
  onToggleExpand,
  columnCount,
}) => {
  return (
    <tr
      className="bg-muted/50 border-y-2 border-primary/20 cursor-pointer hover:bg-muted/70 transition-colors"
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

          {/* Unit name */}
          <div className="flex items-center gap-2 min-w-0">
            <span className="font-medium text-base truncate">
              {unit.name}
            </span>

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
