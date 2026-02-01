/**
 * Toggle component for switching between standard and observation unit view modes.
 */

import React from 'react';
import { Table2, Layers } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';
import { ViewMode } from '../../types/unit';

interface ViewModeToggleProps {
  /** Current view mode */
  viewMode: ViewMode;
  /** Callback when view mode changes */
  onViewModeChange: (mode: ViewMode) => void;
  /** Whether the toggle is disabled (e.g., no observation units exist) */
  disabled?: boolean;
  /** Tooltip text when disabled */
  disabledTooltip?: string;
  /** Total number of observation units (for badge display) */
  unitCount?: number;
}

export const ViewModeToggle: React.FC<ViewModeToggleProps> = ({
  viewMode,
  onViewModeChange,
  disabled = false,
  disabledTooltip = 'No observation units available',
  unitCount,
}) => {
  const toggleContent = (
    <div className="inline-flex rounded-lg border bg-muted p-1">
      <Button
        variant="ghost"
        size="sm"
        className={cn(
          'relative h-8 rounded-md px-3 text-sm font-medium transition-colors',
          viewMode === 'standard'
            ? 'bg-background text-foreground shadow-sm'
            : 'text-muted-foreground hover:text-foreground'
        )}
        onClick={() => !disabled && onViewModeChange('standard')}
        disabled={disabled}
      >
        <Table2 className="h-4 w-4 mr-1.5" />
        Standard
      </Button>
      <Button
        variant="ghost"
        size="sm"
        className={cn(
          'relative h-8 rounded-md px-3 text-sm font-medium transition-colors',
          viewMode === 'by_unit'
            ? 'bg-background text-foreground shadow-sm'
            : 'text-muted-foreground hover:text-foreground',
          disabled && 'opacity-50 cursor-not-allowed'
        )}
        onClick={() => !disabled && onViewModeChange('by_unit')}
        disabled={disabled}
      >
        <Layers className="h-4 w-4 mr-1.5" />
        By Unit
        {unitCount !== undefined && unitCount > 0 && (
          <span className="ml-1.5 inline-flex items-center justify-center px-1.5 py-0.5 text-xs font-medium rounded-full bg-primary/10 text-primary">
            {unitCount}
          </span>
        )}
      </Button>
    </div>
  );

  if (disabled) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <div>{toggleContent}</div>
        </TooltipTrigger>
        <TooltipContent>
          <p>{disabledTooltip}</p>
        </TooltipContent>
      </Tooltip>
    );
  }

  return toggleContent;
};

export default ViewModeToggle;
