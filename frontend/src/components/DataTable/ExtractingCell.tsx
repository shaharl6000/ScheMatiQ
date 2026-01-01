import React from 'react';
import { Skeleton } from '@/components/ui/skeleton';
import { cn } from '@/lib/utils';

interface ExtractingCellProps {
  className?: string;
}

/**
 * A cell placeholder shown during value extraction.
 * Displays a pulsing skeleton animation to indicate the cell is being populated.
 */
const ExtractingCell: React.FC<ExtractingCellProps> = ({ className }) => {
  return (
    <div className={cn("flex items-center gap-1", className)}>
      <Skeleton className="h-5 w-16 rounded" />
      <span className="text-xs text-muted-foreground/50 animate-pulse">...</span>
    </div>
  );
};

export default ExtractingCell;
