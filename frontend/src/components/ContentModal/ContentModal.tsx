import React from 'react';
import { Copy, Check } from 'lucide-react';

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

import { CellValue, QBSDAnswerWithExcerpts, Excerpt, ExcerptWithSource } from '../../types';
import { copyToClipboard } from '../../utils/clipboard';

interface ContentModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  content: CellValue;
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

const ContentModal: React.FC<ContentModalProps> = ({ open, onClose, title, content }) => {
  const [copied, setCopied] = React.useState(false);

  // Extract text content for copying
  const getTextContent = (value: CellValue): string => {
    if (value === null || value === undefined) return '';
    if (Array.isArray(value)) {
      return value.map((item, i) => `${i}: ${typeof item === 'object' ? JSON.stringify(item) : String(item)}`).join('\n');
    }
    if (typeof value === 'object' && value !== null) {
      if ('answer' in value && 'excerpts' in value) {
        const qbsdValue = value as QBSDAnswerWithExcerpts;
        const rawExcerpts = qbsdValue.excerpts || [];
        const excerpts = parseAllExcerpts(rawExcerpts);
        let text = `Answer: ${qbsdValue.answer}`;
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
    const text = getTextContent(content);
    const success = await copyToClipboard(text);
    if (success) {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
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
      // Check if this is QBSD format: {answer: "...", excerpts: [...]}
      if ('answer' in value && 'excerpts' in value) {
        const qbsdValue = value as QBSDAnswerWithExcerpts;
        const answer = qbsdValue.answer;
        const rawExcerpts = qbsdValue.excerpts || [];
        // Parse excerpts to handle pipe-separated strings and Python dicts
        const excerpts = parseAllExcerpts(rawExcerpts);

        return (
          <div className="space-y-4">
            <div>
              <h4 className="font-semibold text-primary mb-2">Content:</h4>
              <div className="p-4 bg-blue-50 dark:bg-blue-950 rounded-md border border-blue-200 dark:border-blue-800">
                <p className="text-base font-medium leading-relaxed">
                  {String(answer)}
                </p>
              </div>
            </div>

            {excerpts.length > 0 && (
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
            )}
          </div>
        );
      }

      // Regular object handling
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
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <DialogContent className="sm:max-w-2xl max-h-[80vh] overflow-hidden flex flex-col">
        <DialogHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <DialogTitle className="pr-8">{title}</DialogTitle>
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
        </DialogHeader>

        <ScrollArea className="flex-1 min-h-[200px]">
          <div className="pr-4">
            {formatContent(content)}
          </div>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
};

export default ContentModal;
