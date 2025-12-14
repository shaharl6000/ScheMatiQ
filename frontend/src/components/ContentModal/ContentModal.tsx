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
        const excerpts = qbsdValue.excerpts || [];
        let text = `Answer: ${qbsdValue.answer}`;
        if (excerpts.length > 0) {
          text += '\n\nSupporting Evidence:\n';
          excerpts.forEach((excerpt, i) => {
            const isObj = typeof excerpt === 'object' && excerpt !== null && 'text' in excerpt;
            const excerptText = isObj ? (excerpt as ExcerptWithSource).text : String(excerpt);
            const source = isObj ? (excerpt as ExcerptWithSource).source : `Source ${i + 1}`;
            text += `\n[${source}]: ${excerptText}`;
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
      // Check if this is QBSD format or integrated format: {answer: "...", excerpts: [...]}
      if ('answer' in value && 'excerpts' in value) {
        const qbsdValue = value as QBSDAnswerWithExcerpts;
        const answer = qbsdValue.answer;
        const excerpts = qbsdValue.excerpts || [];

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
                {excerpts.map((excerpt: Excerpt, index: number) => {
                  const isObjectFormat = typeof excerpt === 'object' && excerpt !== null && 'text' in excerpt;
                  const excerptText = isObjectFormat ? (excerpt as ExcerptWithSource).text : String(excerpt);
                  const sourceName = isObjectFormat
                    ? (excerpt as ExcerptWithSource).source
                    : `Source ${index + 1}`;

                  return (
                    <div key={index} className="mb-3 p-4 bg-muted/50 rounded-md border-l-4 border-primary">
                      <p className="text-xs text-muted-foreground mb-1">
                        From: <span className="font-medium">{sourceName}</span>
                      </p>
                      <p className="text-sm leading-relaxed">
                        {excerptText}
                      </p>
                    </div>
                  );
                })}
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
                className="h-8 w-8 shrink-0"
                onClick={handleCopy}
                aria-label="Copy content to clipboard"
              >
                {copied ? (
                  <Check className="h-4 w-4 text-green-500" />
                ) : (
                  <Copy className="h-4 w-4" />
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
