import React from 'react';
import { Trash2, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface BulkActionToolbarProps {
  selectedCount: number;
  onDelete: () => void;
  onClearSelection: () => void;
  className?: string;
}

const BulkActionToolbar: React.FC<BulkActionToolbarProps> = ({
  selectedCount,
  onDelete,
  onClearSelection,
  className,
}) => {
  if (selectedCount === 0) {
    return null;
  }

  return (
    <div
      className={cn(
        "fixed bottom-6 left-1/2 -translate-x-1/2 z-50",
        "flex items-center gap-3 px-5 py-2.5",
        "bg-background border border-border rounded-full shadow-lg",
        "animate-in fade-in slide-in-from-bottom-2 duration-200",
        className
      )}
      role="toolbar"
      aria-label="Bulk actions"
    >
      <span className="text-sm font-medium text-foreground whitespace-nowrap">
        {selectedCount} row{selectedCount !== 1 ? 's' : ''} selected
      </span>

      <div className="w-px h-5 bg-border" />

      <div className="flex items-center gap-1.5">
        <Button
          variant="ghost"
          size="sm"
          onClick={onClearSelection}
          className="text-muted-foreground hover:text-foreground h-8"
        >
          <X className="h-4 w-4 mr-1" />
          Clear
        </Button>

        <Button
          variant="destructive"
          size="sm"
          onClick={onDelete}
          className="h-8"
        >
          <Trash2 className="h-4 w-4 mr-1" />
          Delete
        </Button>
      </div>
    </div>
  );
};

export default BulkActionToolbar;
