/**
 * Dialog for merging multiple observation units into one.
 */

import React, { useState, useEffect } from 'react';
import { Loader2, Merge, AlertCircle } from 'lucide-react';
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
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { UnitSummary, MergeUnitsRequest } from '../../types/unit';

interface UnitMergeDialogProps {
  /** Whether the dialog is open */
  open: boolean;
  /** Callback to close the dialog */
  onClose: () => void;
  /** Units selected for merging */
  selectedUnits: UnitSummary[];
  /** Callback when merge is confirmed */
  onMerge: (request: MergeUnitsRequest) => Promise<void>;
  /** Whether merge is in progress */
  loading?: boolean;
  /** Error message if merge failed */
  error?: string | null;
}

export const UnitMergeDialog: React.FC<UnitMergeDialogProps> = ({
  open,
  onClose,
  selectedUnits,
  onMerge,
  loading = false,
  error,
}) => {
  const [targetName, setTargetName] = useState('');

  // Reset target name when dialog opens or selected units change
  useEffect(() => {
    if (open && selectedUnits.length > 0) {
      // Default to the name of the unit with the most rows
      const largestUnit = selectedUnits.reduce((a, b) =>
        a.rowCount >= b.rowCount ? a : b
      );
      setTargetName(largestUnit.name);
    }
  }, [open, selectedUnits]);

  const totalRows = selectedUnits.reduce((sum, u) => sum + u.rowCount, 0);
  const allDocuments = Array.from(
    new Set(selectedUnits.flatMap((u) => u.sourceDocuments))
  );

  const handleSubmit = async () => {
    if (!targetName.trim()) return;

    await onMerge({
      source_units: selectedUnits.map((u) => u.name),
      target_unit: targetName.trim(),
      strategy: 'rename',
    });
  };

  const handleClose = () => {
    if (!loading) {
      onClose();
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Merge className="h-5 w-5" />
            Merge Observation Units
          </DialogTitle>
          <DialogDescription>
            Combine {selectedUnits.length} units into a single observation unit.
            All rows from the selected units will be updated.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          {/* Selected units preview */}
          <div className="space-y-2">
            <Label className="text-sm font-medium">Units to merge:</Label>
            <div className="flex flex-wrap gap-2 max-h-[120px] overflow-y-auto p-2 bg-muted/50 rounded-md">
              {selectedUnits.map((unit) => (
                <Badge
                  key={unit.name}
                  variant="secondary"
                  className="flex items-center gap-1"
                >
                  <span className="max-w-[150px] truncate">{unit.name}</span>
                  <span className="text-muted-foreground">
                    ({unit.rowCount})
                  </span>
                </Badge>
              ))}
            </div>
          </div>

          {/* Merge preview */}
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <span className="text-muted-foreground">Total rows:</span>
              <span className="ml-2 font-medium">{totalRows}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Documents:</span>
              <span className="ml-2 font-medium">{allDocuments.length}</span>
            </div>
          </div>

          {/* Target name input */}
          <div className="space-y-2">
            <Label htmlFor="target-name">Merged unit name:</Label>
            <Input
              id="target-name"
              value={targetName}
              onChange={(e) => setTargetName(e.target.value)}
              placeholder="Enter name for merged unit"
              disabled={loading}
            />
            <p className="text-xs text-muted-foreground">
              All rows will be updated to use this unit name.
            </p>
          </div>

          {/* Error message */}
          {error && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleClose} disabled={loading}>
            Cancel
          </Button>
          <Button
            onClick={handleSubmit}
            disabled={loading || !targetName.trim()}
          >
            {loading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Merging...
              </>
            ) : (
              <>
                <Merge className="mr-2 h-4 w-4" />
                Merge Units
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default UnitMergeDialog;
