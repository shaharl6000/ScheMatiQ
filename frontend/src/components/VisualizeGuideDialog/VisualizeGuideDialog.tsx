import { useState, useEffect, useRef } from 'react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';

const STORAGE_KEY = 'qbsd-visualize-guide-dismissed';

interface VisualizeGuideDialogProps {
  autoOpen?: boolean;
  forceOpen?: boolean;
  onOpenChange?: (open: boolean) => void;
  onDismiss?: () => void;
}

const steps = [
  {
    number: 1,
    title: 'Explore Your Results',
    description: 'The Data tab shows your extracted table, the Schema tab shows the discovered columns, and the Statistics tab summarizes the run.',
  },
  {
    number: 2,
    title: 'Edit, Refine, and Expand',
    description: 'Click any cell to edit it, modify the schema and re-extract, or add more documents to grow your table.',
  },
  {
    number: 3,
    title: 'Save Your Work',
    description: 'Use the Export button in the top-right to download as CSV or save the full project.',
  },
];

export function VisualizeGuideDialog({ autoOpen, forceOpen, onOpenChange, onDismiss }: VisualizeGuideDialogProps) {
  const [open, setOpen] = useState(false);
  const [dontShowAgain, setDontShowAgain] = useState(false);
  const prevAutoOpenRef = useRef(autoOpen);
  const prevForceOpenRef = useRef(forceOpen);

  // Detect autoOpen false→true transition, check localStorage before opening
  useEffect(() => {
    if (autoOpen && !prevAutoOpenRef.current) {
      const dismissed = localStorage.getItem(STORAGE_KEY);
      if (!dismissed) {
        setOpen(true);
      }
    }
    prevAutoOpenRef.current = autoOpen;
  }, [autoOpen]);

  // Handle forceOpen from parent (help button) — detect false→true transition
  useEffect(() => {
    if (forceOpen && !prevForceOpenRef.current) {
      setOpen(true);
    }
    prevForceOpenRef.current = forceOpen;
  }, [forceOpen]);

  const handleClose = () => {
    if (dontShowAgain) {
      localStorage.setItem(STORAGE_KEY, 'true');
    }
    setOpen(false);
    onOpenChange?.(false);
    onDismiss?.();
  };

  const handleOpenChange = (newOpen: boolean) => {
    if (!newOpen) {
      handleClose();
    } else {
      setOpen(true);
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle className="text-xl">Your Results Are Ready!</DialogTitle>
          <DialogDescription className="text-base pt-2">
            Here's a quick guide to what you can do:
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
          {steps.map((step) => (
            <div key={step.number} className="flex items-start gap-3">
              <div className="flex-shrink-0 w-7 h-7 rounded-full bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-300 flex items-center justify-center text-sm font-semibold">
                {step.number}
              </div>
              <div>
                <p className="font-medium">
                  {step.title}
                  <span className="font-normal text-muted-foreground"> — {step.description}</span>
                </p>
              </div>
            </div>
          ))}
        </div>

        <p className="text-sm text-muted-foreground">
          You can reopen this guide anytime using the <span className="font-medium">(?)</span> icon in the header.
        </p>

        <DialogFooter className="flex-col sm:flex-row items-start sm:items-center gap-3 sm:justify-between">
          <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer select-none">
            <input
              type="checkbox"
              checked={dontShowAgain}
              onChange={(e) => setDontShowAgain(e.target.checked)}
              className="rounded border-gray-300"
            />
            Don't show this again
          </label>
          <Button onClick={handleClose}>
            Got it, show me my data
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
