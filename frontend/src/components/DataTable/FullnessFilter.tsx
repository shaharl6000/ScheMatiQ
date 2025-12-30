import React from 'react';
import { BarChart3 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Label } from '@/components/ui/label';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { cn } from '@/lib/utils';

interface FullnessFilterProps {
  threshold: number;
  onThresholdChange: (value: number) => void;
  visibleColumnsCount: number;
  totalColumnsCount: number;
  hiddenByFullnessCount: number;
}

const PRESET_VALUES = [0, 25, 50, 75, 100];

const FullnessFilter: React.FC<FullnessFilterProps> = ({
  threshold,
  onThresholdChange,
  visibleColumnsCount,
  totalColumnsCount,
  hiddenByFullnessCount,
}) => {
  const handleSliderChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    onThresholdChange(parseInt(e.target.value, 10));
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="gap-1">
          <BarChart3 className="h-4 w-4" />
          Fullness
          {threshold > 0 && (
            <Badge variant="secondary" className="ml-1 h-5 px-1.5">
              ≥{threshold}%
            </Badge>
          )}
          {hiddenByFullnessCount > 0 && (
            <Badge variant="destructive" className="ml-1 h-5 px-1.5">
              -{hiddenByFullnessCount}
            </Badge>
          )}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-72">
        {/* Header */}
        <div className="p-3 space-y-2">
          <Label htmlFor="fullness-slider" className="text-sm font-medium">
            Minimum Column Fullness
          </Label>
          <p className="text-xs text-muted-foreground">
            Show only columns with at least {threshold}% non-empty values
          </p>
        </div>

        <DropdownMenuSeparator />

        {/* Slider */}
        <div className="p-3 space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium">{threshold}%</span>
          </div>
          <input
            id="fullness-slider"
            type="range"
            min="0"
            max="100"
            step="5"
            value={threshold}
            onChange={handleSliderChange}
            className="w-full h-2 bg-muted rounded-lg appearance-none cursor-pointer accent-primary"
          />
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>0%</span>
            <span>50%</span>
            <span>100%</span>
          </div>
        </div>

        <DropdownMenuSeparator />

        {/* Preset buttons */}
        <div className="p-3 space-y-2">
          <Label className="text-xs text-muted-foreground">Quick presets</Label>
          <div className="flex gap-1">
            {PRESET_VALUES.map((preset) => (
              <Button
                key={preset}
                variant={threshold === preset ? 'default' : 'outline'}
                size="sm"
                className={cn(
                  'flex-1 text-xs h-7',
                  threshold === preset && 'pointer-events-none'
                )}
                onClick={() => onThresholdChange(preset)}
              >
                {preset === 0 ? 'All' : `${preset}%`}
              </Button>
            ))}
          </div>
        </div>

        <DropdownMenuSeparator />

        {/* Status footer */}
        <div className="p-3 space-y-1">
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Columns meeting threshold:</span>
            <span className="font-medium">
              {visibleColumnsCount} / {totalColumnsCount}
            </span>
          </div>
          {hiddenByFullnessCount > 0 && (
            <p className="text-xs text-orange-600 dark:text-orange-400">
              {hiddenByFullnessCount} column{hiddenByFullnessCount !== 1 ? 's' : ''} hidden due to low fullness
            </p>
          )}
        </div>
      </DropdownMenuContent>
    </DropdownMenu>
  );
};

export default FullnessFilter;
