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

const STORAGE_KEY = 'qbsd-welcome-dismissed';

interface WelcomeDialogProps {
  forceOpen?: boolean;
  onOpenChange?: (open: boolean) => void;
}

const steps = [
  {
    number: 1,
    title: 'Ask a Question',
    description: 'What do you want to know?',
    example: 'e.g., "What methods were used to detect nuclear export signals in proteins?"',
  },
  {
    number: 2,
    title: 'Upload Documents',
    description: 'Add your PDFs / text files or select sample datasets to analyze',
  },
  {
    number: 3,
    title: 'Get Your Table',
    description: "We'll build a schema and extract the data automatically",
  },
];

export function WelcomeDialog({ forceOpen, onOpenChange }: WelcomeDialogProps) {
  const [open, setOpen] = useState(false);
  const [dontShowAgain, setDontShowAgain] = useState(false);
  const prevForceOpenRef = useRef(forceOpen);

  // Auto-show on first visit (localStorage check)
  useEffect(() => {
    const dismissed = localStorage.getItem(STORAGE_KEY);
    if (!dismissed) {
      setOpen(true);
    }
  }, []);

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
          <DialogTitle className="text-xl">Welcome to QueryBasedSchemaDiscovery!</DialogTitle>
          <DialogDescription className="text-base pt-2">
            Turn your documents into structured data in three simple steps:
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
                {step.example && (
                  <p className="text-sm text-muted-foreground mt-0.5">{step.example}</p>
                )}
              </div>
            </div>
          ))}
        </div>

        <p className="text-sm text-muted-foreground">
          Look for the <span className="inline-flex items-center text-muted-foreground">(i)</span> icons to learn more about any option.
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
            Got it, let's start
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
