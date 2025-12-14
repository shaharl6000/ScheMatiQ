import React from 'react';
import { Filter, X, Plus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';
import { FilterRule, OPERATOR_LABELS, FilterOperator } from './types/filters';

interface FilterBarProps {
  filters: FilterRule[];
  onRemoveFilter: (id: string) => void;
  onClearAll: () => void;
  onAddFilter: () => void;
  className?: string;
}

const FilterBar: React.FC<FilterBarProps> = ({
  filters,
  onRemoveFilter,
  onClearAll,
  onAddFilter,
  className,
}) => {
  const formatFilterDisplay = (filter: FilterRule): string => {
    const operatorLabel = OPERATOR_LABELS[filter.operator as FilterOperator] || filter.operator;

    // For null operators, don't show value
    if (filter.operator === 'isNull' || filter.operator === 'isNotNull') {
      return `${filter.column} ${operatorLabel}`;
    }

    // For 'in' operator with array values
    if (filter.operator === 'in' && Array.isArray(filter.value)) {
      if (filter.value.length <= 2) {
        return `${filter.column} = ${filter.value.join(' or ')}`;
      }
      return `${filter.column} in [${filter.value.length} values]`;
    }

    // For 'between' operator
    if (filter.operator === 'between' && Array.isArray(filter.value)) {
      return `${filter.column} between ${filter.value[0]} and ${filter.value[1]}`;
    }

    // Default format
    const valueDisplay = typeof filter.value === 'string' && filter.value.length > 20
      ? `${filter.value.substring(0, 20)}...`
      : String(filter.value);

    return `${filter.column} ${operatorLabel.toLowerCase()} "${valueDisplay}"`;
  };

  if (filters.length === 0) {
    return (
      <div className={cn("flex items-center gap-2", className)}>
        <Button
          variant="outline"
          size="sm"
          onClick={onAddFilter}
          className="gap-1"
        >
          <Plus className="h-4 w-4" />
          Add Filter
        </Button>
      </div>
    );
  }

  return (
    <div className={cn("flex flex-wrap items-center gap-2", className)}>
      <div className="flex items-center gap-1 text-sm text-muted-foreground">
        <Filter className="h-4 w-4" />
        <span>Filters:</span>
      </div>

      {filters.map((filter) => (
        <Tooltip key={filter.id}>
          <TooltipTrigger asChild>
            <Badge
              variant="secondary"
              className="gap-1 pr-1 cursor-default hover:bg-secondary/80"
            >
              <span className="max-w-[200px] truncate">
                {formatFilterDisplay(filter)}
              </span>
              <Button
                variant="ghost"
                size="icon"
                className="h-4 w-4 p-0 hover:bg-destructive/20 rounded-full"
                onClick={(e) => {
                  e.stopPropagation();
                  onRemoveFilter(filter.id);
                }}
                aria-label={`Remove filter: ${filter.column}`}
              >
                <X className="h-3 w-3" />
              </Button>
            </Badge>
          </TooltipTrigger>
          <TooltipContent>
            <p>Click X to remove this filter</p>
          </TooltipContent>
        </Tooltip>
      ))}

      <Button
        variant="outline"
        size="sm"
        onClick={onAddFilter}
        className="gap-1 h-6"
      >
        <Plus className="h-3 w-3" />
        Add
      </Button>

      {filters.length > 0 && (
        <Button
          variant="ghost"
          size="sm"
          onClick={onClearAll}
          className="text-destructive hover:text-destructive hover:bg-destructive/10 h-6"
        >
          Clear all
        </Button>
      )}

      <Badge variant="outline" className="ml-auto">
        {filters.length} active
      </Badge>
    </div>
  );
};

export default FilterBar;
