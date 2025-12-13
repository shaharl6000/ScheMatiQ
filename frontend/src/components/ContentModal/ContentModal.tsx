import React from 'react';
import { X } from 'lucide-react';

import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';

import { CellValue, QBSDAnswerWithExcerpts, Excerpt, ExcerptWithSource } from '../../types';

interface ContentModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  content: CellValue;
}

const ContentModal: React.FC<ContentModalProps> = ({ open, onClose, title, content }) => {
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
                      {excerpts.length > 1 && (
                        <p className="text-sm italic mb-2">
                          <strong>{sourceName}:</strong>
                        </p>
                      )}
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
        <DialogHeader className="flex-row justify-between items-center">
          <DialogTitle>{title}</DialogTitle>
          <Button variant="ghost" size="sm" onClick={onClose}>
            <X className="h-4 w-4 mr-1" />
            Close
          </Button>
        </DialogHeader>

        <ScrollArea className="flex-1 min-h-[200px]">
          <div className="pr-4">
            {formatContent(content)}
          </div>
        </ScrollArea>

        <DialogFooter>
          <Button onClick={onClose}>Close</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default ContentModal;
