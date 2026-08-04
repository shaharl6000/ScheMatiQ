import React, { useState, useCallback } from 'react';
import { Loader2, CheckCircle2 } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { supportAPI } from '../../services/api';

interface ReportIssueDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sessionId?: string;
  sessionMode?: string;
  activeSheet?: string;
}

const ReportIssueDialog: React.FC<ReportIssueDialogProps> = ({
  open,
  onOpenChange,
  sessionId,
  sessionMode = 'schematiq',
  activeSheet,
}) => {
  const [description, setDescription] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [done, setDone] = useState(false);

  const reset = useCallback(() => {
    setDescription('');
    setSubmitting(false);
    setDone(false);
  }, []);

  const handleOpenChange = useCallback((next: boolean) => {
    if (!next) reset();
    onOpenChange(next);
  }, [onOpenChange, reset]);

  const handleSubmit = useCallback(async () => {
    const text = description.trim();
    if (!text || submitting) return;
    setSubmitting(true);

    // Grab the project export JSON (same as "Save project"). Best-effort.
    let projectJson: string | undefined;
    if (sessionId) {
      projectJson = await supportAPI.fetchProjectJson(sessionId, sessionMode);
    }

    try {
      await supportAPI.reportIssue({
        session_id: sessionId,
        description: text,
        project_json: projectJson,
        client_context: {
          url: window.location.href,
          activeSheet,
          userAgent: navigator.userAgent,
        },
      });
    } catch {
      // Fire-and-forget: the backend never hard-fails, and a report is not
      // worth blocking the user over. Show the confirmation regardless.
    }

    setSubmitting(false);
    setDone(true);
  }, [description, submitting, sessionId, sessionMode, activeSheet]);

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-[480px]">
        {done ? (
          <div className="flex flex-col items-center gap-3 py-6 text-center">
            <CheckCircle2 className="h-10 w-10 text-green-600" />
            <DialogTitle>Thanks, your report was sent</DialogTitle>
            <DialogDescription>
              We included your current project so we can look into it.
            </DialogDescription>
            <Button className="mt-2" onClick={() => handleOpenChange(false)}>
              Close
            </Button>
          </div>
        ) : (
          <>
            <DialogHeader>
              <DialogTitle>Report an issue</DialogTitle>
              <DialogDescription>
                Tell us what happened. Your current project is attached automatically
                so we can reproduce it.
              </DialogDescription>
            </DialogHeader>
            <Textarea
              autoFocus
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Describe what happened, what you expected, and any steps to reproduce."
              rows={6}
              className="resize-none"
            />
            <DialogFooter>
              <Button variant="outline" onClick={() => handleOpenChange(false)} disabled={submitting}>
                Cancel
              </Button>
              <Button onClick={handleSubmit} disabled={!description.trim() || submitting}>
                {submitting ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Sending
                  </>
                ) : (
                  'Send report'
                )}
              </Button>
            </DialogFooter>
          </>
        )}
      </DialogContent>
    </Dialog>
  );
};

export default ReportIssueDialog;
