import React from 'react';
import { Columns3, Eye, EyeOff } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { ColumnVisibilityState } from './types/filters';
import { formatColumnName } from '../../utils/formatting';

interface ColumnVisibilityDropdownProps {
  columns: string[];
  visibility: ColumnVisibilityState;
  onToggleColumn: (columnName: string) => void;
  onShowAll: () => void;
  onHideAll: () => void;
}

const ColumnVisibilityDropdown: React.FC<ColumnVisibilityDropdownProps> = ({
  columns,
  visibility,
  onToggleColumn,
  onShowAll,
  onHideAll,
}) => {
  const visibleCount = columns.filter(col => visibility[col] !== false).length;
  const hiddenCount = columns.length - visibleCount;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="gap-1">
          <Columns3 className="h-4 w-4" />
          Columns
          {hiddenCount > 0 && (
            <Badge variant="secondary" className="ml-1 h-5 px-1.5">
              {visibleCount}/{columns.length}
            </Badge>
          )}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-56">
        {/* Quick actions */}
        <div className="flex gap-1 p-2">
          <Button
            variant="outline"
            size="sm"
            className="flex-1 gap-1"
            onClick={onShowAll}
            disabled={hiddenCount === 0}
          >
            <Eye className="h-3 w-3" />
            Show All
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="flex-1 gap-1"
            onClick={onHideAll}
            disabled={visibleCount === 0}
          >
            <EyeOff className="h-3 w-3" />
            Hide All
          </Button>
        </div>

        <DropdownMenuSeparator />

        {/* Column list */}
        <ScrollArea className="h-[300px]">
          <div className="p-2 space-y-1">
            {columns.map((column) => {
              const isVisible = visibility[column] !== false;
              const displayName = formatColumnName(column);

              return (
                <div
                  key={column}
                  className="flex items-center space-x-2 py-1 px-1 rounded hover:bg-muted/50 cursor-pointer"
                  onClick={() => onToggleColumn(column)}
                >
                  <Checkbox
                    id={`col-vis-${column}`}
                    checked={isVisible}
                    onCheckedChange={() => onToggleColumn(column)}
                  />
                  <Label
                    htmlFor={`col-vis-${column}`}
                    className="flex-1 text-sm cursor-pointer truncate"
                  >
                    {displayName}
                  </Label>
                  {column.startsWith('_') && (
                    <Badge variant="outline" className="text-xs h-5">
                      Meta
                    </Badge>
                  )}
                </div>
              );
            })}
          </div>
        </ScrollArea>

        <DropdownMenuSeparator />

        {/* Status footer */}
        <div className="p-2 text-xs text-center text-muted-foreground">
          {visibleCount} of {columns.length} columns visible
        </div>
      </DropdownMenuContent>
    </DropdownMenu>
  );
};

export default ColumnVisibilityDropdown;
