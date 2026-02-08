import React, { useState, useEffect, useRef, useCallback } from 'react';
import { ThumbsUp, ThumbsDown, MessageSquare, X, Check } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader } from '@/components/ui/card';
import { Textarea } from '@/components/ui/textarea';
import { feedbackAPI } from '../../services/api';

interface TableFeedbackWidgetProps {
  sessionId: string;
  sessionStatus: string;
  activeTab: string;
  tableRowCount: number;
  tableColumnCount: number;
}

type WidgetState = 'hidden' | 'collapsed' | 'expanded' | 'submitted';

const SHOW_DELAY_MS = 10_000;

const TableFeedbackWidget: React.FC<TableFeedbackWidgetProps> = ({
  sessionId,
  sessionStatus,
  activeTab,
  tableRowCount,
  tableColumnCount,
}) => {
  const [widgetState, setWidgetState] = useState<WidgetState>('hidden');
  const [rating, setRating] = useState<'positive' | 'negative' | null>(null);
  const [comment, setComment] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const timerRef = useRef<NodeJS.Timeout | null>(null);
  const interactionTriggered = useRef(false);

  const dismissedKey = `qbsd_feedback_dismissed_${sessionId}`;
  const submittedKey = `qbsd_feedback_submitted_${sessionId}`;

  // Check if should show based on conditions
  const shouldBeVisible =
    sessionStatus === 'completed' &&
    activeTab === 'data';

  // Reset feedback state on reextraction completion
  useEffect(() => {
    const handleReextractionReset = () => {
      sessionStorage.removeItem(dismissedKey);
      sessionStorage.removeItem(submittedKey);
      setWidgetState('hidden');
      setRating(null);
      setComment('');
      interactionTriggered.current = false;
    };

    window.addEventListener('reextraction_completed', handleReextractionReset);
    return () => window.removeEventListener('reextraction_completed', handleReextractionReset);
  }, [dismissedKey, submittedKey]);

  // Timer-based trigger: show after SHOW_DELAY_MS on the Data tab
  useEffect(() => {
    if (!shouldBeVisible) {
      if (timerRef.current) {
        clearTimeout(timerRef.current);
        timerRef.current = null;
      }
      return;
    }

    // Already dismissed or submitted for this session
    if (sessionStorage.getItem(dismissedKey) || sessionStorage.getItem(submittedKey)) {
      if (sessionStorage.getItem(submittedKey)) {
        setWidgetState('submitted');
      }
      return;
    }

    // Already visible
    if (widgetState !== 'hidden') return;

    timerRef.current = setTimeout(() => {
      setWidgetState('collapsed');
    }, SHOW_DELAY_MS);

    return () => {
      if (timerRef.current) {
        clearTimeout(timerRef.current);
        timerRef.current = null;
      }
    };
  }, [shouldBeVisible, dismissedKey, submittedKey, widgetState]);

  // Interaction-based trigger: show on table scroll, sort, filter, or click
  // Use refs for dynamic values to keep the callback identity stable
  const shouldBeVisibleRef = useRef(shouldBeVisible);
  const widgetStateRef = useRef(widgetState);
  shouldBeVisibleRef.current = shouldBeVisible;
  widgetStateRef.current = widgetState;

  const handleUserInteraction = useCallback(() => {
    if (
      interactionTriggered.current ||
      !shouldBeVisibleRef.current ||
      widgetStateRef.current !== 'hidden' ||
      sessionStorage.getItem(dismissedKey) ||
      sessionStorage.getItem(submittedKey)
    ) {
      return;
    }

    interactionTriggered.current = true;
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    setWidgetState('collapsed');
  }, [dismissedKey, submittedKey]);

  useEffect(() => {
    if (!shouldBeVisible || widgetState !== 'hidden') return;

    // Listen for interactions on the table area
    const tableContainer = document.querySelector('[data-table-container]');
    if (!tableContainer) return;

    const events = ['scroll', 'click'] as const;
    events.forEach((event) => {
      tableContainer.addEventListener(event, handleUserInteraction, { passive: true });
    });

    return () => {
      events.forEach((event) => {
        tableContainer.removeEventListener(event, handleUserInteraction);
      });
    };
  }, [shouldBeVisible, widgetState, handleUserInteraction]);

  const handleDismiss = () => {
    sessionStorage.setItem(dismissedKey, 'true');
    setWidgetState('hidden');
  };

  const handleSubmit = async () => {
    if (!rating) return;

    setSubmitting(true);
    try {
      await feedbackAPI.submitFeedback({
        session_id: sessionId,
        rating,
        comment: comment.trim() || undefined,
        table_row_count: tableRowCount,
        table_column_count: tableColumnCount,
      });
    } catch {
      // Fire-and-forget: don't block the user on failure
    }

    sessionStorage.setItem(submittedKey, 'true');
    setWidgetState('submitted');
    setSubmitting(false);
  };

  // Don't render anything if hidden or conditions not met
  if (widgetState === 'hidden' && !shouldBeVisible) return null;
  if (widgetState === 'hidden') return null;

  return (
    <div className="fixed bottom-6 right-6 z-50">
      {/* Submitted state: small checkmark */}
      {widgetState === 'submitted' && (
        <div
          className="flex items-center gap-1.5 px-3 py-1.5 bg-green-50 dark:bg-green-950 border border-green-200 dark:border-green-800 rounded-full text-sm text-green-700 dark:text-green-300 shadow-sm animate-in fade-in slide-in-from-bottom-2 duration-300"
        >
          <Check className="h-4 w-4" />
          Thanks!
        </div>
      )}

      {/* Collapsed state: small button */}
      {widgetState === 'collapsed' && (
        <Button
          variant="secondary"
          size="sm"
          onClick={() => setWidgetState('expanded')}
          className="shadow-md animate-in fade-in slide-in-from-right-4 duration-300 gap-2"
        >
          <MessageSquare className="h-4 w-4" />
          Rate this table
        </Button>
      )}

      {/* Expanded state: feedback card */}
      {widgetState === 'expanded' && (
        <Card className="w-80 shadow-lg animate-in fade-in slide-in-from-right-4 duration-300">
          <CardHeader className="flex flex-row items-center justify-between py-3 px-4 space-y-0">
            <span className="text-sm font-medium">How useful is this table?</span>
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6"
              onClick={handleDismiss}
            >
              <X className="h-4 w-4" />
            </Button>
          </CardHeader>
          <CardContent className="px-4 pb-4 pt-0 space-y-3">
            {!rating ? (
              <div className="flex gap-3">
                <Button
                  variant="outline"
                  className="flex-1 gap-2 hover:bg-green-50 hover:border-green-300 hover:text-green-700 dark:hover:bg-green-950 dark:hover:border-green-700 dark:hover:text-green-300"
                  onClick={() => setRating('positive')}
                >
                  <ThumbsUp className="h-4 w-4" />
                  Useful
                </Button>
                <Button
                  variant="outline"
                  className="flex-1 gap-2 hover:bg-red-50 hover:border-red-300 hover:text-red-700 dark:hover:bg-red-950 dark:hover:border-red-700 dark:hover:text-red-300"
                  onClick={() => setRating('negative')}
                >
                  <ThumbsDown className="h-4 w-4" />
                  Not useful
                </Button>
              </div>
            ) : (
              <>
                <div className="flex items-center gap-2 text-sm">
                  {rating === 'positive' ? (
                    <span className="flex items-center gap-1 text-green-700 dark:text-green-300">
                      <ThumbsUp className="h-3.5 w-3.5" /> Useful
                    </span>
                  ) : (
                    <span className="flex items-center gap-1 text-red-700 dark:text-red-300">
                      <ThumbsDown className="h-3.5 w-3.5" /> Not useful
                    </span>
                  )}
                  <button
                    className="text-xs text-muted-foreground hover:text-foreground underline"
                    onClick={() => setRating(null)}
                    aria-label="Change rating"
                  >
                    change
                  </button>
                </div>
                <Textarea
                  rows={3}
                  value={comment}
                  onChange={(e) => setComment(e.target.value)}
                  placeholder={
                    rating === 'positive'
                      ? 'What did you find useful? (optional)'
                      : 'What could be improved? (optional)'
                  }
                  className="text-sm resize-none"
                />
                <Button
                  size="sm"
                  onClick={handleSubmit}
                  disabled={submitting}
                  className="w-full"
                >
                  {submitting ? 'Submitting...' : 'Submit'}
                </Button>
              </>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default TableFeedbackWidget;
