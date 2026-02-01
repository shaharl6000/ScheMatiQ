import React, { useState } from 'react';
import { Loader2, AlertCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { ScrollArea } from '@/components/ui/scroll-area';

interface BulkDeleteDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  selectedRows: string[];
  onConfirm: () => Promise<void>;
}

const BulkDeleteDialog: React.FC<BulkDeleteDialogProps> = ({
  open,
  onOpenChange,
  selectedRows,
  onConfirm,
}) => {
  const [isDeleting, setIsDeleting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleConfirm = async () => {
    setIsDeleting(true);
    setError(null);

    try {
      await onConfirm();
      onOpenChange(false);
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || 'Failed to delete rows');
    } finally {
      setIsDeleting(false);
    }
  };

  const count = selectedRows.length;
  const showList = count <= 10;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Delete {count} Row{count !== 1 ? 's' : ''}</DialogTitle>
          <DialogDescription>
            {showList ? (
              <>You are about to permanently delete:</>
            ) : (
              <>
                You are about to permanently delete <strong>{count} rows</strong> and all their data from the table.
              </>
            )}
          </DialogDescription>
        </DialogHeader>

        {showList && (
          <ScrollArea className="max-h-48 rounded-md border p-3">
            <ul className="space-y-1">
              {selectedRows.map((rowName, index) => (
                <li key={index} className="text-sm text-muted-foreground">
                  • {rowName}
                </li>
              ))}
            </ul>
          </ScrollArea>
        )}

        <p className="text-sm text-muted-foreground">
          This action cannot be undone.
        </p>

        {error && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={isDeleting}
          >
            Cancel
          </Button>
          <Button
            variant="destructive"
            onClick={handleConfirm}
            disabled={isDeleting}
          >
            {isDeleting ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Deleting...
              </>
            ) : (
              `Delete ${count} Row${count !== 1 ? 's' : ''}`
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default BulkDeleteDialog;
