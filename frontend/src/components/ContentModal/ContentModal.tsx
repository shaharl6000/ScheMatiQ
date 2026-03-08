import React, { useRef, useEffect } from 'react';
import { Copy, Check, Pencil, Loader2 } from 'lucide-react';

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Textarea } from '@/components/ui/textarea';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

import { CellValue, ScheMatiQAnswerWithExcerpts, Excerpt, ExcerptWithSource } from '../../types';
import { copyToClipboard } from '../../utils/clipboard';
import { extractDisplayValue } from '../DataTable/utils/valueUtils';

const IS_MAC = typeof navigator !== 'undefined' && /Mac/.test(navigator.platform);

interface ContentModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  content: CellValue;
  onSave?: (newValue: string) => Promise<void>;
}

/** Extract the editable string value from a CellValue. */
function getEditableValue(value: CellValue): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  if (typeof value === 'object' && 'answer' in value) {
    const answer = (value as { answer: unknown }).answer;
    if (answer === null || answer === undefined) return '';
    return extractDisplayValue(answer);
  }
  if (Array.isArray(value)) return value.join(', ');
  return extractDisplayValue(value);
}

// Helper to parse Python-style dict/list strings to JSON
const parsePythonString = (val: string): any => {
  const trimmed = val.trim();
  if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return val;

  try {
    return JSON.parse(trimmed);
  } catch {
    try {
      const jsonified = trimmed
        .replace(/'/g, '"')
        .replace(/None/g, 'null')
        .replace(/True/g, 'true')
        .replace(/False/g, 'false');
      return JSON.parse(jsonified);
    } catch {
      return val;
    }
  }
};

// Parse excerpts to ensure they're in a consistent format
const parseExcerptItem = (excerpt: Excerpt, index: number): ExcerptWithSource => {
  if (typeof excerpt === 'string') {
    // Try to parse if it looks like a Python dict
    const parsed = parsePythonString(excerpt);
    if (typeof parsed === 'object' && parsed !== null && 'text' in parsed) {
      return {
        text: parsed.text,
        source: parsed.source || `Source ${index + 1}`
      };
    }
    return { text: excerpt, source: `Source ${index + 1}` };
  }
  if (typeof excerpt === 'object' && excerpt !== null && 'text' in excerpt) {
    return {
      text: (excerpt as ExcerptWithSource).text,
      source: (excerpt as ExcerptWithSource).source || `Source ${index + 1}`
    };
  }
  return { text: String(excerpt), source: `Source ${index + 1}` };
};

// Parse all excerpts, handling pipe-separated strings
const parseAllExcerpts = (excerpts: Excerpt[]): ExcerptWithSource[] => {
  const result: ExcerptWithSource[] = [];

  for (let i = 0; i < excerpts.length; i++) {
    const exc = excerpts[i];
    if (typeof exc === 'string') {
      // Check if it's pipe-separated
      if (exc.includes("'text':") || exc.includes('"text":')) {
        const parts = exc.split(/\s*\|\s*/);
        for (let j = 0; j < parts.length; j++) {
          result.push(parseExcerptItem(parts[j].trim(), result.length));
        }
      } else {
        result.push(parseExcerptItem(exc, result.length));
      }
    } else {
      result.push(parseExcerptItem(exc, result.length));
    }
  }

  return result;
};

const ContentModal: React.FC<ContentModalProps> = ({ open, onClose, title, content, onSave }) => {
  const [copied, setCopied] = React.useState(false);
  const [isEditing, setIsEditing] = React.useState(false);
  const [editValue, setEditValue] = React.useState('');
  const [isSaving, setIsSaving] = React.useState(false);
  const [saveError, setSaveError] = React.useState<string | null>(null);
  const [saved, setSaved] = React.useState(false);
  // Local override only used after optimistic save; null means use content prop directly
  const [localOverride, setLocalOverride] = React.useState<CellValue | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Use content prop directly; only fall back to localOverride after a save
  const displayContent = localOverride ?? content;

  // Reset edit state and local override when modal closes
  useEffect(() => {
    if (!open) {
      setIsEditing(false);
      setEditValue('');
      setSaveError(null);
      setSaved(false);
      setLocalOverride(null);
    }
  }, [open]);

  // Clear local override when content prop changes externally (e.g., after undo + refetch)
  useEffect(() => {
    setLocalOverride(null);
  }, [content]);

  // Focus textarea when entering edit mode
  useEffect(() => {
    if (isEditing && textareaRef.current) {
      textareaRef.current.focus();
      textareaRef.current.select();
    }
  }, [isEditing]);

  const startEditing = () => {
    setEditValue(getEditableValue(displayContent));
    setSaveError(null);
    setSaved(false);
    setIsEditing(true);
  };

  const handleSave = async () => {
    if (isSaving || !onSave) return;
    const originalValue = getEditableValue(displayContent);
    if (editValue === originalValue) {
      setIsEditing(false);
      return;
    }
    setIsSaving(true);
    setSaveError(null);
    try {
      await onSave(editValue);
      // Optimistically override displayed content until modal closes
      if (typeof displayContent === 'object' && displayContent !== null && 'answer' in displayContent) {
        setLocalOverride({ ...displayContent as Record<string, unknown>, answer: editValue, excerpts: [], manually_edited: true } as CellValue);
      } else {
        setLocalOverride({ answer: editValue, excerpts: [], manually_edited: true });
      }
      setIsEditing(false);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch {
      setSaveError('Failed to save. Please try again.');
    } finally {
      setIsSaving(false);
    }
  };

  const handleCancel = () => {
    setIsEditing(false);
    setEditValue('');
    setSaveError(null);
  };

  const handleTextareaKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      handleCancel();
    } else if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault();
      handleSave();
    }
  };

  const handleOpenChange = (isOpen: boolean) => {
    if (!isOpen) {
      if (isEditing) handleCancel();
      onClose();
    }
  };

  // Extract text content for copying
  const getTextContent = (value: CellValue): string => {
    if (value === null || value === undefined) return '';
    if (Array.isArray(value)) {
      return value.map((item, i) => `${i}: ${typeof item === 'object' ? JSON.stringify(item) : String(item)}`).join('\n');
    }
    if (typeof value === 'object' && value !== null) {
      if ('answer' in value && 'excerpts' in value) {
        const schematiqValue = value as ScheMatiQAnswerWithExcerpts;
        const rawExcerpts = schematiqValue.excerpts || [];
        const excerpts = parseAllExcerpts(rawExcerpts);
        const answerText = extractDisplayValue(schematiqValue.answer);
        let text = `Answer: ${answerText}`;
        if (excerpts.length > 0) {
          text += '\n\nSupporting Evidence:\n';
          excerpts.forEach((excerpt) => {
            text += `\n[${excerpt.source}]: ${excerpt.text}`;
          });
        }
        return text;
      }
      return JSON.stringify(value, null, 2);
    }
    return String(value);
  };

  const handleCopy = async () => {
    const text = getTextContent(displayContent);
    const success = await copyToClipboard(text);
    if (success) {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  /** Render the excerpts section (shared between read and edit modes). */
  const renderExcerpts = (excerpts: ExcerptWithSource[]) => {
    if (excerpts.length === 0) return null;
    return (
      <div>
        <h4 className="font-semibold text-muted-foreground mb-2">
          {excerpts.length === 1 ? 'Supporting Evidence:' : `Supporting Evidence (${excerpts.length} sources):`}
        </h4>
        {excerpts.map((excerpt: ExcerptWithSource, index: number) => (
          <div key={index} className="mb-3 p-4 bg-muted/50 rounded-md border-l-4 border-primary">
            <p className="text-xs text-muted-foreground mb-1">
              From: <span className="font-medium">{excerpt.source}</span>
            </p>
            <p className="text-sm leading-relaxed">
              {excerpt.text}
            </p>
          </div>
        ))}
      </div>
    );
  };

  /** Render the edit UI: textarea + inline save/cancel buttons + excerpts below. */
  const renderEditMode = (excerpts: ExcerptWithSource[]) => {
    return (
      <div className="space-y-4">
        <div>
          <h4 className="font-semibold text-primary mb-2">Edit Value:</h4>
          <Textarea
            ref={textareaRef}
            value={editValue}
            onChange={(e) => setEditValue(e.target.value)}
            onKeyDown={handleTextareaKeyDown}
            disabled={isSaving}
            className={`min-h-[60px] max-h-[200px] resize-y ${saveError ? 'border-red-500' : ''}`}
            aria-label="Cell value editor"
          />
          {saveError && (
            <p className="text-xs text-red-500 mt-1" role="alert">{saveError}</p>
          )}
          <div className="flex items-center justify-between mt-2">
            <span className="text-xs text-muted-foreground">
              {IS_MAC ? '⌘' : 'Ctrl'}+Enter to save, Esc to cancel
            </span>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" onClick={handleCancel} disabled={isSaving}>
                Cancel
              </Button>
              <Button size="sm" onClick={handleSave} disabled={isSaving}>
                {isSaving ? (
                  <><Loader2 className="h-3 w-3 mr-1 animate-spin" />Saving</>
                ) : 'Save'}
              </Button>
            </div>
          </div>
        </div>
        {renderExcerpts(excerpts)}
      </div>
    );
  };

  const formatContent = (value: CellValue): React.ReactNode => {
    if (value === null || value === undefined) {
      return <Badge variant="outline">null</Badge>;
    }

    if (Array.isArray(value)) {
      return (
        <div>
          <p className="font-semibold text-sm mb-2">
            Array ({value.length} items):
          </p>
          {value.map((item, index) => (
            <div key={index} className="mb-2 pl-4 border-l-2 border-muted">
              <p className="text-sm">
                <strong>{index}:</strong> {typeof item === 'object' ? JSON.stringify(item, null, 2) : String(item)}
              </p>
            </div>
          ))}
        </div>
      );
    }

    if (typeof value === 'object' && value !== null) {
      // Check if this is ScheMatiQ format: {answer: "...", excerpts: [...]}
      if ('answer' in value && 'excerpts' in value) {
        const schematiqValue = value as ScheMatiQAnswerWithExcerpts;
        const answer = schematiqValue.answer;
        const rawExcerpts = schematiqValue.excerpts || [];
        const excerpts = parseAllExcerpts(rawExcerpts);
        const isManuallyEdited = !!schematiqValue.manually_edited;

        if (isEditing) {
          return renderEditMode(isManuallyEdited ? [] : excerpts);
        }

        return (
          <div className="space-y-4">
            <div>
              <h4 className="font-semibold text-primary mb-2">Content:</h4>
              <div className="p-4 bg-blue-50 dark:bg-blue-950 rounded-md border border-blue-200 dark:border-blue-800">
                <p className="text-base font-medium leading-relaxed">
                  {extractDisplayValue(answer)}
                </p>
              </div>
            </div>
            {isManuallyEdited ? (
              <p className="text-sm text-muted-foreground italic flex items-center gap-1.5">
                <Pencil className="h-3 w-3" />
                Manually edited
              </p>
            ) : renderExcerpts(excerpts)}
          </div>
        );
      }

      // Regular object handling
      if (isEditing) {
        return renderEditMode([]);
      }
      return (
        <div>
          <p className="font-semibold text-sm mb-2">Object:</p>
          <pre className="bg-muted p-4 rounded-md overflow-auto text-sm font-mono whitespace-pre-wrap break-words">
            {JSON.stringify(value, null, 2)}
          </pre>
        </div>
      );
    }

    // For long text content - check if it looks like an excerpt
    const textValue = String(value);
    const isLikelyExcerpt = textValue.length > 200 ||
                           title.toLowerCase().includes('excerpt') ||
                           title.toLowerCase().includes('evidence') ||
                           title.toLowerCase().includes('source');

    if (isEditing) {
      return renderEditMode([]);
    }

    if (isLikelyExcerpt) {
      return (
        <div>
          <h4 className="font-semibold text-primary mb-2">Content:</h4>
          <ScrollArea className="max-h-[400px]">
            <div className="p-4 bg-muted/50 rounded-md border-l-4 border-primary">
              <p className="whitespace-pre-wrap break-words leading-relaxed">
                {textValue}
              </p>
            </div>
          </ScrollArea>
        </div>
      );
    }

    // Regular text content
    return (
      <p className="whitespace-pre-wrap break-words leading-relaxed">
        {textValue}
      </p>
    );
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent
        className="sm:max-w-2xl max-h-[80vh] overflow-hidden flex flex-col"
        onEscapeKeyDown={(e) => {
          if (isEditing) {
            e.preventDefault();
            handleCancel();
          }
        }}
        onInteractOutside={(e) => {
          if (isEditing) {
            e.preventDefault();
          }
        }}
      >
        <DialogHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <DialogTitle className="pr-8">{title}</DialogTitle>
          <div className="flex items-center gap-1">
            {onSave && !isEditing && (
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1 text-xs"
                    onClick={startEditing}
                    aria-label="Edit cell value"
                  >
                    {saved ? (
                      <Check className="h-3 w-3 text-green-500" />
                    ) : (
                      <Pencil className="h-3 w-3" />
                    )}
                    {saved ? 'Saved' : 'Edit'}
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="bottom" sideOffset={5}>
                  {saved ? 'Changes saved!' : 'Edit cell value'}
                </TooltipContent>
              </Tooltip>
            )}
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6 shrink-0"
                  onClick={handleCopy}
                  aria-label="Copy content to clipboard"
                >
                  {copied ? (
                    <Check className="h-3 w-3 text-green-500" />
                  ) : (
                    <Copy className="h-3 w-3" />
                  )}
                </Button>
              </TooltipTrigger>
              <TooltipContent side="bottom" sideOffset={5}>
                {copied ? 'Copied!' : 'Copy to clipboard'}
              </TooltipContent>
            </Tooltip>
          </div>
        </DialogHeader>

        <ScrollArea className="flex-1 min-h-[200px]">
          <div className="pr-4">
            {formatContent(displayContent)}
          </div>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
};

export default ContentModal;
