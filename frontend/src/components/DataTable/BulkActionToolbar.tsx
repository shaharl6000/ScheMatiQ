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
        "flex items-center gap-3 px-4 py-2 bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-md",
        className
      )}
      role="toolbar"
      aria-label="Bulk actions"
    >
      <span className="text-sm font-medium text-blue-700 dark:text-blue-300">
        {selectedCount} row{selectedCount !== 1 ? 's' : ''} selected
      </span>

      <div className="flex items-center gap-2 ml-auto">
        <Button
          variant="ghost"
          size="sm"
          onClick={onClearSelection}
          className="text-muted-foreground hover:text-foreground"
        >
          <X className="h-4 w-4 mr-1" />
          Clear
        </Button>

        <Button
          variant="destructive"
          size="sm"
          onClick={onDelete}
        >
          <Trash2 className="h-4 w-4 mr-1" />
          Delete Selected
        </Button>
      </div>
    </div>
  );
};

export default BulkActionToolbar;
