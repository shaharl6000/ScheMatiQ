/**
 * Dialog for selecting units to merge via a searchable multi-select list.
 * Replaces the previous checkbox-on-each-row approach.
 */

import React, { useState, useMemo } from 'react';
import { Merge, Search } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Checkbox } from '@/components/ui/checkbox';
import { UnitSummary } from '../../types/unit';

interface UnitMergePickerDialogProps {
  /** Whether the dialog is open */
  open: boolean;
  /** Callback to close the dialog */
  onClose: () => void;
  /** All available units */
  units: UnitSummary[];
  /** Callback when units are selected and user clicks Continue */
  onContinue: (selectedUnits: UnitSummary[]) => void;
}

export const UnitMergePickerDialog: React.FC<UnitMergePickerDialogProps> = ({
  open,
  onClose,
  units,
  onContinue,
}) => {
  const [search, setSearch] = useState('');
  const [selected, setSelected] = useState<Set<string>>(new Set());

  // Reset state when dialog opens
  React.useEffect(() => {
    if (open) {
      setSearch('');
      setSelected(new Set());
    }
  }, [open]);

  const filteredUnits = useMemo(() => {
    if (!search.trim()) return units;
    const term = search.toLowerCase();
    return units.filter(u => u.name.toLowerCase().includes(term));
  }, [units, search]);

  const toggleUnit = (name: string) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(name)) {
        next.delete(name);
      } else {
        next.add(name);
      }
      return next;
    });
  };

  const handleContinue = () => {
    const selectedUnits = units.filter(u => selected.has(u.name));
    onContinue(selectedUnits);
  };

  return (
    <Dialog open={open} onOpenChange={(isOpen) => { if (!isOpen) onClose(); }}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Merge className="h-5 w-5" />
            Select Units to Merge
          </DialogTitle>
          <DialogDescription>
            Select 2 or more units to merge into a single observation unit.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-3 py-2">
          {/* Search input */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search units..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>

          {/* Unit list */}
          <div className="max-h-[300px] overflow-y-auto border rounded-md">
            {filteredUnits.length === 0 ? (
              <div className="p-4 text-center text-sm text-muted-foreground">
                No units match your search.
              </div>
            ) : (
              filteredUnits.map(unit => (
                <label
                  key={unit.name}
                  className="flex items-center gap-3 px-3 py-2.5 hover:bg-muted/50 cursor-pointer border-b last:border-b-0"
                >
                  <Checkbox
                    checked={selected.has(unit.name)}
                    onCheckedChange={() => toggleUnit(unit.name)}
                  />
                  <span className="flex-1 truncate text-sm font-medium">
                    {unit.name}
                  </span>
                  <span className="text-xs text-muted-foreground shrink-0">
                    {unit.rowCount} {unit.rowCount === 1 ? 'row' : 'rows'}
                  </span>
                </label>
              ))
            )}
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button
            onClick={handleContinue}
            disabled={selected.size < 2}
          >
            <Merge className="mr-2 h-4 w-4" />
            Merge {selected.size} Unit{selected.size !== 1 ? 's' : ''}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default UnitMergePickerDialog;
