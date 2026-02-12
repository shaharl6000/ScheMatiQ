/**
 * Dropdown filter for selecting a specific observation unit to view.
 */

import React from 'react';
import { X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { UnitSummary } from '../../types/unit';

interface UnitFilterProps {
  /** Available units to filter by */
  units: UnitSummary[];
  /** Currently selected unit (null = show all) */
  selectedUnit: string | null;
  /** Callback when selection changes */
  onUnitChange: (unit: string | null) => void;
  /** Whether the filter is loading */
  loading?: boolean;
}

export const UnitFilter: React.FC<UnitFilterProps> = ({
  units,
  selectedUnit,
  onUnitChange,
  loading = false,
}) => {
  const handleValueChange = (value: string) => {
    if (value === '__all__') {
      onUnitChange(null);
    } else {
      onUnitChange(value);
    }
  };

  const handleClear = () => {
    onUnitChange(null);
  };

  return (
    <div className="flex items-center gap-2">
      <Select
        value={selectedUnit ?? '__all__'}
        onValueChange={handleValueChange}
        disabled={loading || units.length === 0}
      >
        <SelectTrigger className="w-[200px]">
          <SelectValue placeholder="Filter by unit..." />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="__all__">
            All Units
          </SelectItem>
          {units.map((unit) => (
            <SelectItem key={unit.name} value={unit.name}>
              <div className="flex items-center justify-between w-full">
                <span className="truncate max-w-[120px]" title={unit.name}>
                  {unit.name}
                </span>
                <Badge variant="outline" className="ml-2">
                  {unit.rowCount}
                </Badge>
              </div>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {selectedUnit && (
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8"
          onClick={handleClear}
          title="Clear filter"
        >
          <X className="h-4 w-4" />
        </Button>
      )}
    </div>
  );
};

export default UnitFilter;
