import React from 'react';
import { Loader2, Square } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import { cn } from '@/lib/utils';

const COLUMN_CHIP_LIMIT = 5;

interface ExtractionProgressBarProps {
  processingColumns: Set<string>;
  currentColumn?: string | null;
  currentDocumentProgress?: {
    documentName: string;
    documentIndex: number;
    totalDocuments: number;
  } | null;
  onStop?: () => void;
  isStopping?: boolean;
  isProcessingDocuments?: boolean;
  unitLabel?: string;
  variant?: 'blue' | 'neutral';
}

const ExtractionProgressBar: React.FC<ExtractionProgressBarProps> = ({
  processingColumns,
  currentColumn,
  currentDocumentProgress,
  onStop,
  isStopping,
  isProcessingDocuments,
  unitLabel = 'Document',
  variant = 'blue',
}) => {
  const isReextraction = processingColumns.size > 0;
  const columnList = Array.from(processingColumns);
  const visibleChips = columnList.slice(0, COLUMN_CHIP_LIMIT);
  const overflowCount = columnList.length - COLUMN_CHIP_LIMIT;

  const containerClass = variant === 'blue'
    ? 'mb-4 p-4 bg-blue-50 dark:bg-blue-950/30 border border-blue-200 dark:border-blue-800 rounded-lg'
    : 'mb-4 p-4 bg-muted/30 border rounded-lg';

  // Single column: show name inline in title
  const titleText = isReextraction
    ? processingColumns.size === 1
      ? `Re-extracting "${columnList[0]}"`
      : `Re-extracting ${processingColumns.size} columns`
    : 'Extracting data from new documents';

  return (
    <div className={containerClass}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <Loader2 className="h-4 w-4 animate-spin text-primary" />
          <span className="font-medium">{titleText}</span>
          {currentDocumentProgress && (
            <span className="text-sm text-muted-foreground">
              — {unitLabel} {currentDocumentProgress.documentIndex} of {currentDocumentProgress.totalDocuments}
            </span>
          )}
        </div>
        {onStop && (
          <Button
            variant="destructive"
            size="sm"
            onClick={onStop}
            disabled={isStopping}
            className="gap-1"
          >
            {isStopping ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                Stopping...
              </>
            ) : (
              <>
                <Square className="h-4 w-4" />
                Stop
              </>
            )}
          </Button>
        )}
      </div>

      {/* Column chips — shown during re-extraction with 2+ columns (not during add-more-documents flow) */}
      {isReextraction && !isProcessingDocuments && processingColumns.size > 1 && (
        <div className="flex flex-wrap gap-1.5 mb-3">
          {visibleChips.map((col) => {
            const isActive = col === currentColumn;
            return (
              <span
                key={col}
                className={cn(
                  'inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium border transition-colors',
                  isActive
                    ? 'border-primary bg-primary/10 text-primary dark:border-blue-400 dark:bg-blue-950/50 dark:text-blue-300'
                    : 'border-muted-foreground/30 bg-muted/40 text-muted-foreground'
                )}
              >
                {isActive && <Loader2 className="h-3 w-3 animate-spin" aria-hidden="true" />}
                <span
                  className="inline-block max-w-[140px] truncate"
                  title={col}
                >
                  {col}
                </span>
              </span>
            );
          })}
          {overflowCount > 0 && (
            <span
              className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border border-muted-foreground/30 bg-muted/40 text-muted-foreground"
              title={columnList.slice(COLUMN_CHIP_LIMIT).join(' | ')}
            >
              +{overflowCount} more
            </span>
          )}
        </div>
      )}

      {currentDocumentProgress && currentDocumentProgress.totalDocuments > 0 && (
        <div className="space-y-1">
          <Progress
            value={(currentDocumentProgress.documentIndex / currentDocumentProgress.totalDocuments) * 100}
            className="h-2"
          />
          <p className="text-xs text-muted-foreground">
            Processing: {currentDocumentProgress.documentName}
          </p>
        </div>
      )}
    </div>
  );
};

export default ExtractionProgressBar;
