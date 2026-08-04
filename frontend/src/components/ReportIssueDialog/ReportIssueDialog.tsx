import React, { useState, useCallback, useRef } from 'react';
import { Loader2, CheckCircle2, Paperclip, ImagePlus, X } from 'lucide-react';
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
import { Input } from '@/components/ui/input';
import { supportAPI } from '../../services/api';

interface ReportIssueDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sessionId?: string;
  sessionMode?: string;
  activeSheet?: string;
}

interface Shot {
  id: string;
  name: string;
  mime: string;
  dataUrl: string;
}

const MAX_SHOTS = 5;
const MAX_EACH_BYTES = 8 * 1024 * 1024; // 8 MB per image

const readImage = (file: File): Promise<Shot | null> =>
  new Promise((resolve) => {
    const reader = new FileReader();
    reader.onload = () =>
      resolve({
        id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
        name: file.name || 'screenshot.png',
        mime: file.type || 'image/png',
        dataUrl: String(reader.result || ''),
      });
    reader.onerror = () => resolve(null);
    reader.readAsDataURL(file);
  });

const ReportIssueDialog: React.FC<ReportIssueDialogProps> = ({
  open,
  onOpenChange,
  sessionId,
  sessionMode = 'schematiq',
  activeSheet,
}) => {
  const [description, setDescription] = useState('');
  const [email, setEmail] = useState('');
  const [shots, setShots] = useState<Shot[]>([]);
  const [submitting, setSubmitting] = useState(false);
  const [done, setDone] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const dragDepth = useRef(0);

  const reset = useCallback(() => {
    setDescription('');
    setEmail('');
    setShots([]);
    setSubmitting(false);
    setDone(false);
    setError(null);
    setDragActive(false);
    dragDepth.current = 0;
  }, []);

  const handleOpenChange = useCallback((next: boolean) => {
    if (!next) reset();
    onOpenChange(next);
  }, [onOpenChange, reset]);

  const addFiles = useCallback(async (files: File[]) => {
    const images = files.filter(
      (f) => f.type.startsWith('image/') && f.size <= MAX_EACH_BYTES,
    );
    if (images.length === 0) return;
    const read = (await Promise.all(images.map(readImage))).filter(
      (s): s is Shot => s !== null,
    );
    setShots((prev) => [...prev, ...read].slice(0, MAX_SHOTS));
  }, []);

  const onFileInput = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files) addFiles(Array.from(e.target.files));
    e.target.value = '';
  }, [addFiles]);

  // Drag a file from the OS onto the dialog to attach it, same as the picker.
  // A depth counter keeps the highlight stable while dragging over child nodes.
  const isFileDrag = (e: React.DragEvent) =>
    Array.from(e.dataTransfer?.types || []).includes('Files');

  const onDragEnter = useCallback((e: React.DragEvent) => {
    if (!isFileDrag(e)) return;
    e.preventDefault();
    dragDepth.current += 1;
    setDragActive(true);
  }, []);

  const onDragOver = useCallback((e: React.DragEvent) => {
    if (isFileDrag(e)) e.preventDefault();
  }, []);

  const onDragLeave = useCallback((e: React.DragEvent) => {
    if (!isFileDrag(e)) return;
    e.preventDefault();
    dragDepth.current -= 1;
    if (dragDepth.current <= 0) {
      dragDepth.current = 0;
      setDragActive(false);
    }
  }, []);

  const onDrop = useCallback((e: React.DragEvent) => {
    if (!isFileDrag(e)) return;
    e.preventDefault();
    dragDepth.current = 0;
    setDragActive(false);
    const files = e.dataTransfer?.files;
    if (files && files.length) addFiles(Array.from(files));
  }, [addFiles]);

  const removeShot = useCallback((id: string) => {
    setShots((prev) => prev.filter((s) => s.id !== id));
  }, []);

  const handleSubmit = useCallback(async () => {
    const text = description.trim();
    if (!text || submitting) return;
    setSubmitting(true);
    setError(null);

    // Grab the project export JSON (same as "Save project"). Best-effort.
    let projectJson: string | undefined;
    if (sessionId) {
      projectJson = await supportAPI.fetchProjectJson(sessionId, sessionMode);
    }

    const screenshots = shots
      .map((s) => {
        const comma = s.dataUrl.indexOf(',');
        return comma === -1
          ? null
          : { name: s.name, mime: s.mime, data_b64: s.dataUrl.slice(comma + 1) };
      })
      .filter((s): s is { name: string; mime: string; data_b64: string } => s !== null);

    try {
      const res = await supportAPI.reportIssue({
        session_id: sessionId,
        description: text,
        reporter_email: email.trim() || undefined,
        project_json: projectJson,
        screenshots,
        client_context: {
          url: window.location.href,
          activeSheet,
          userAgent: navigator.userAgent,
        },
      });
      setSubmitting(false);
      if (res && res.status === 'ok') {
        setDone(true);
      } else {
        setError("Your report couldn't be sent. Please try again.");
      }
    } catch {
      setSubmitting(false);
      setError("Your report couldn't be sent. Please try again.");
    }
  }, [description, submitting, sessionId, sessionMode, activeSheet, shots]);

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent
        className="relative sm:max-w-[480px]"
        onDragEnter={onDragEnter}
        onDragOver={onDragOver}
        onDragLeave={onDragLeave}
        onDrop={onDrop}
      >
        {!done && dragActive && (
          <div className="pointer-events-none absolute inset-0 z-50 flex flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed border-primary bg-background/90 text-primary">
            <ImagePlus className="h-8 w-8" />
            <span className="text-sm font-medium">Drop image to attach</span>
          </div>
        )}
        {done ? (
          <div className="flex flex-col items-center gap-3 py-6 text-center">
            <CheckCircle2 className="h-10 w-10 text-green-600" />
            <DialogTitle>Thanks, your report was sent</DialogTitle>
            <DialogDescription>
              We included your current project and its documents so we can look into it.
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
                Tell us what happened so we can help.
              </DialogDescription>
            </DialogHeader>

            <div className="flex items-start gap-2 rounded-md border border-primary/30 bg-primary/10 px-3 py-2.5 text-sm font-medium text-foreground">
              <Paperclip className="mt-0.5 h-4 w-4 shrink-0 text-primary" />
              <span>
                Your current project and its documents are attached automatically so we
                can reproduce the issue.
              </span>
            </div>

            <Textarea
              autoFocus
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Describe what happened, what you expected, and any steps to reproduce."
              rows={6}
              className="resize-none"
            />

            <div>
              <Input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="Your email (optional)"
              />
              <p className="mt-1.5 text-xs text-muted-foreground">
                Add your email if you'd like us to reply or let you know when it's fixed.
              </p>
            </div>

            <div>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => fileInputRef.current?.click()}
                disabled={shots.length >= MAX_SHOTS}
              >
                <ImagePlus className="mr-2 h-4 w-4" />
                Add image
              </Button>
              <p className="mt-1.5 text-xs text-muted-foreground">
                We recommend attaching a screenshot. Drag an image here or use Add image
                ({shots.length}/{MAX_SHOTS}).
              </p>
              <input
                ref={fileInputRef}
                type="file"
                accept="image/*"
                multiple
                className="hidden"
                onChange={onFileInput}
              />
              {shots.length > 0 && (
                <div className="mt-3 flex flex-wrap gap-2">
                  {shots.map((s) => (
                    <div
                      key={s.id}
                      className="relative h-16 w-16 overflow-hidden rounded-md border border-border"
                    >
                      <img
                        src={s.dataUrl}
                        alt={s.name}
                        className="h-full w-full object-cover"
                      />
                      <button
                        type="button"
                        onClick={() => removeShot(s.id)}
                        aria-label="Remove image"
                        className="absolute right-0.5 top-0.5 flex h-4 w-4 items-center justify-center rounded-full bg-background/90 text-foreground shadow-sm hover:bg-background"
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {error && (
              <p className="text-sm font-medium text-destructive">{error}</p>
            )}

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
