import React, { useState, useMemo } from 'react';
import { Check, Filter } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Checkbox } from '@/components/ui/checkbox';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  FilterOperator,
  OPERATORS_BY_TYPE,
  OPERATOR_LABELS,
  FilterValue,
  ColumnMetadata,
} from './types/filters';

interface FilterDialogProps {
  open: boolean;
  onClose: () => void;
  onApply: (column: string, operator: FilterOperator, value: FilterValue, caseSensitive?: boolean) => void;
  columns: ColumnMetadata[];
  selectedColumn?: string;
}

const FilterDialog: React.FC<FilterDialogProps> = ({
  open,
  onClose,
  onApply,
  columns,
  selectedColumn: initialColumn,
}) => {
  const [column, setColumn] = useState<string>(initialColumn || '');
  const [operator, setOperator] = useState<FilterOperator>('contains');
  const [value, setValue] = useState<string>('');
  const [rangeMin, setRangeMin] = useState<string>('');
  const [rangeMax, setRangeMax] = useState<string>('');
  const [selectedValues, setSelectedValues] = useState<string[]>([]);
  const [caseSensitive, setCaseSensitive] = useState(false);

  // Reset when dialog opens with new column
  React.useEffect(() => {
    if (open) {
      setColumn(initialColumn || '');
      setOperator('contains');
      setValue('');
      setRangeMin('');
      setRangeMax('');
      setSelectedValues([]);
      setCaseSensitive(false);
    }
  }, [open, initialColumn]);

  // Get metadata for selected column
  const columnMeta = useMemo(() => {
    return columns.find(c => c.name === column);
  }, [columns, column]);

  // Get available operators for the selected column's data type
  const availableOperators = useMemo(() => {
    const dataType = columnMeta?.dataType || 'string';
    return OPERATORS_BY_TYPE[dataType] || OPERATORS_BY_TYPE.string;
  }, [columnMeta]);

  // Update operator when column changes
  React.useEffect(() => {
    if (column && availableOperators.length > 0) {
      if (!availableOperators.includes(operator)) {
        setOperator(availableOperators[0]);
      }
    }
  }, [column, availableOperators, operator]);

  const handleApply = () => {
    if (!column || !operator) return;

    let filterValue: FilterValue;

    // Handle different operator types
    if (operator === 'isNull' || operator === 'isNotNull' ||
        operator === 'isTrue' || operator === 'isFalse') {
      filterValue = null;
    } else if (operator === 'between') {
      filterValue = [parseFloat(rangeMin) || 0, parseFloat(rangeMax) || 0];
    } else if (operator === 'in' || operator === 'notIn') {
      filterValue = selectedValues;
    } else if (['eq', 'gt', 'lt', 'gte', 'lte'].includes(operator)) {
      filterValue = parseFloat(value) || 0;
    } else {
      filterValue = value;
    }

    onApply(column, operator, filterValue, caseSensitive);
    onClose();
  };

  const renderValueInput = () => {
    // No value needed for null/boolean operators
    if (['isNull', 'isNotNull', 'isTrue', 'isFalse'].includes(operator)) {
      return null;
    }

    // Range input for 'between' operator
    if (operator === 'between') {
      return (
        <div className="grid grid-cols-2 gap-2">
          <div>
            <Label htmlFor="range-min" className="text-xs text-muted-foreground">
              Min
            </Label>
            <Input
              id="range-min"
              type="number"
              value={rangeMin}
              onChange={(e) => setRangeMin(e.target.value)}
              placeholder="Minimum"
            />
          </div>
          <div>
            <Label htmlFor="range-max" className="text-xs text-muted-foreground">
              Max
            </Label>
            <Input
              id="range-max"
              type="number"
              value={rangeMax}
              onChange={(e) => setRangeMax(e.target.value)}
              placeholder="Maximum"
            />
          </div>
        </div>
      );
    }

    // Multi-select for categorical columns with 'in' operator
    if ((operator === 'in' || operator === 'notIn') && columnMeta?.allowedValues) {
      return (
        <div className="space-y-2 max-h-[200px] overflow-y-auto border rounded-md p-2">
          {columnMeta.allowedValues.map((val) => (
            <div key={val} className="flex items-center space-x-2">
              <Checkbox
                id={`value-${val}`}
                checked={selectedValues.includes(val)}
                onCheckedChange={(checked) => {
                  if (checked) {
                    setSelectedValues([...selectedValues, val]);
                  } else {
                    setSelectedValues(selectedValues.filter(v => v !== val));
                  }
                }}
              />
              <Label
                htmlFor={`value-${val}`}
                className="text-sm cursor-pointer"
              >
                {val}
              </Label>
            </div>
          ))}
        </div>
      );
    }

    // Number input for numeric operators
    if (['eq', 'gt', 'lt', 'gte', 'lte'].includes(operator)) {
      return (
        <Input
          type="number"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          placeholder="Enter value..."
        />
      );
    }

    // Text input for text operators
    return (
      <div className="space-y-2">
        <Input
          value={value}
          onChange={(e) => setValue(e.target.value)}
          placeholder={operator === 'regex' ? 'Enter regex pattern...' : 'Enter value...'}
        />
        {['contains', 'equals', 'startsWith', 'endsWith', 'regex'].includes(operator) && (
          <div className="flex items-center space-x-2">
            <Checkbox
              id="case-sensitive"
              checked={caseSensitive}
              onCheckedChange={(checked) => setCaseSensitive(!!checked)}
            />
            <Label
              htmlFor="case-sensitive"
              className="text-sm text-muted-foreground cursor-pointer"
            >
              Case sensitive
            </Label>
          </div>
        )}
      </div>
    );
  };

  const isValid = () => {
    if (!column || !operator) return false;

    // Null/boolean operators don't need a value
    if (['isNull', 'isNotNull', 'isTrue', 'isFalse'].includes(operator)) {
      return true;
    }

    // Between needs both values
    if (operator === 'between') {
      return rangeMin !== '' && rangeMax !== '';
    }

    // In/notIn need at least one selection
    if (operator === 'in' || operator === 'notIn') {
      return selectedValues.length > 0;
    }

    // Other operators need a value
    return value !== '';
  };

  return (
    <Dialog open={open} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Filter className="h-5 w-5" />
            Add Filter
          </DialogTitle>
          <DialogDescription>
            Configure a filter to narrow down the displayed data.
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-4 py-4">
          {/* Column selector */}
          <div className="grid gap-2">
            <Label htmlFor="column">Column</Label>
            <Select value={column} onValueChange={setColumn}>
              <SelectTrigger id="column">
                <SelectValue placeholder="Select column..." />
              </SelectTrigger>
              <SelectContent>
                {columns.map((col) => (
                  <SelectItem key={col.name} value={col.name}>
                    {col.name.startsWith('_') ? col.name.substring(1) : col.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {/* Operator selector */}
          {column && (
            <div className="grid gap-2">
              <Label htmlFor="operator">Condition</Label>
              <Select value={operator} onValueChange={(val) => setOperator(val as FilterOperator)}>
                <SelectTrigger id="operator">
                  <SelectValue placeholder="Select condition..." />
                </SelectTrigger>
                <SelectContent>
                  {availableOperators.map((op) => (
                    <SelectItem key={op} value={op}>
                      {OPERATOR_LABELS[op]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

          {/* Value input */}
          {column && operator && (
            <div className="grid gap-2">
              <Label>Value</Label>
              {renderValueInput()}
            </div>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button onClick={handleApply} disabled={!isValid()}>
            <Check className="h-4 w-4 mr-2" />
            Apply Filter
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default FilterDialog;
