import { useState, useEffect } from 'react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';

const CONSENT_STORAGE_KEY = 'qbsd-consent-accepted';
const OPT_OUT_STORAGE_KEY = 'qbsd-consent-opt-out';

interface ConsentDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm: (optOutDataCollection: boolean) => void;
}

/**
 * Returns the saved consent state from localStorage.
 * - consentGiven: true if the user previously accepted with "Don't show again"
 * - savedOptOut: true if the user opted out of data collection
 */
export function getSavedConsent(): { consentGiven: boolean; savedOptOut: boolean } {
  const consentGiven = localStorage.getItem(CONSENT_STORAGE_KEY) === 'true';
  const savedOptOut = localStorage.getItem(OPT_OUT_STORAGE_KEY) === 'true';
  return { consentGiven, savedOptOut };
}

export function ConsentDialog({ open, onOpenChange, onConfirm }: ConsentDialogProps) {
  const [optOut, setOptOut] = useState(false);
  const [dontShowAgain, setDontShowAgain] = useState(false);

  // Reset opt-out state when dialog opens
  useEffect(() => {
    if (open) {
      const { savedOptOut } = getSavedConsent();
      setOptOut(savedOptOut);
      setDontShowAgain(false);
    }
  }, [open]);

  const handleConfirm = () => {
    if (dontShowAgain) {
      localStorage.setItem(CONSENT_STORAGE_KEY, 'true');
      localStorage.setItem(OPT_OUT_STORAGE_KEY, String(optOut));
    }
    onOpenChange(false);
    onConfirm(optOut);
  };

  const handleCancel = () => {
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle className="text-xl">Research Data Collection</DialogTitle>
          <DialogDescription className="text-base pt-2">
            Please review before proceeding.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-3 py-2 text-sm">
          <p>
            QueryDiscovery is a research project by the{' '}
            <strong>Hebrew University of Jerusalem</strong>. To improve our schema discovery
            algorithms, we may collect anonymized session data including your query,
            uploaded documents, discovered schema, and extracted table.
          </p>
          <p>
            <strong>We never collect API keys or personal identifiers.</strong> However,
            your documents may contain identifying information. Data is stored securely
            and used solely for open academic research.
          </p>
          <p className="text-muted-foreground">
            You must be 18 or older to use this tool. Participation is voluntary.{' '}
            <a
              href="https://github.com/shahaf-shar/QueryDiscovery/blob/main/privacy_policy.md"
              target="_blank"
              rel="noopener noreferrer"
              className="underline hover:text-foreground transition-colors"
            >
              Read the full privacy policy
            </a>
          </p>
        </div>

        {/* Opt-out checkbox */}
        <label className="flex items-start gap-2 text-sm cursor-pointer select-none py-2 px-3 rounded-md border bg-muted/30">
          <input
            type="checkbox"
            checked={optOut}
            onChange={(e) => setOptOut(e.target.checked)}
            className="rounded border-gray-300 mt-0.5"
          />
          <span>
            <strong>Do not share my data</strong> for research purposes.
            <span className="text-muted-foreground ml-1">
              Your session will work identically — no data will be archived.
            </span>
          </span>
        </label>

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
          <div className="flex gap-2">
            <Button variant="outline" onClick={handleCancel}>
              Cancel
            </Button>
            <Button onClick={handleConfirm}>
              I Agree & Continue
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
