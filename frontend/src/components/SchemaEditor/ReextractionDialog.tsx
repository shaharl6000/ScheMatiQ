import React, { useState, useEffect, useCallback, useRef } from 'react';
import { RefreshCw, AlertTriangle, FileText, Loader2, Check, Info, Square } from 'lucide-react';
import { Progress } from '@/components/ui/progress';

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Label } from '@/components/ui/label';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { ScrollArea } from '@/components/ui/scroll-area';

import {
  SchemaChangeStatus,
  PaperDiscoveryResult,
  ColumnChangeDetail,
  ReextractionRequest,
} from '../../types';
import { schemaAPI } from '../../services/api';
import { getApiKeyForProvider } from '../../utils/apiKeyStorage';

interface ReextractionDialogProps {
  open: boolean;
  sessionId: string;
  onClose: () => void;
  onSuccess: (message: string, refreshData?: boolean) => void;
  onError: (error: string) => void;
  /** Called when re-extraction starts with the list of columns being re-extracted */
  onReextractionStarted?: (columns: string[]) => void;
}

const ReextractionDialog: React.FC<ReextractionDialogProps> = ({
  open,
  sessionId,
  onClose,
  onSuccess,
  onError,
  onReextractionStarted
}) => {
  const [loading, setLoading] = useState(false);
  const [loadingStatus, setLoadingStatus] = useState(true);
  const [schemaChanges, setSchemaChanges] = useState<SchemaChangeStatus | null>(null);
  const [paperStatus, setPaperStatus] = useState<PaperDiscoveryResult | null>(null);
  const [selectedColumns, setSelectedColumns] = useState<Set<string>>(new Set());

  // Extraction progress state
  const [isExtracting, setIsExtracting] = useState(false);
  const [extractionOperationId, setExtractionOperationId] = useState<string | null>(null);
  const [extractionProgress, setExtractionProgress] = useState(0);
  const [isStopping, setIsStopping] = useState(false);
  const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Load schema change status and paper discovery when dialog opens
  const loadStatus = useCallback(async () => {
    if (!open || !sessionId) return;

    setLoadingStatus(true);
    try {
      const [changes, papers] = await Promise.all([
        schemaAPI.getSchemaChangeStatus(sessionId),
        schemaAPI.discoverPapers(sessionId)
      ]);

      setSchemaChanges(changes);
      setPaperStatus(papers);

      // Pre-select all changed/new columns
      const allChanged = new Set([
        ...changes.changed_columns,
        ...changes.new_columns
      ]);
      setSelectedColumns(allChanged);
    } catch (error: any) {
      onError(error.response?.data?.detail || 'Failed to load schema status');
    } finally {
      setLoadingStatus(false);
    }
  }, [open, sessionId, onError]);

  useEffect(() => {
    loadStatus();
  }, [loadStatus]);

  // Reset state when dialog closes
  useEffect(() => {
    if (!open) {
      setSelectedColumns(new Set());
      setIsExtracting(false);
      setExtractionOperationId(null);
      setExtractionProgress(0);
      setIsStopping(false);
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
    }
  }, [open]);

  // Cleanup polling on unmount
  useEffect(() => {
    return () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
      }
    };
  }, []);

  const handleColumnToggle = (columnName: string, checked: boolean) => {
    const newSet = new Set(selectedColumns);
    if (checked) {
      newSet.add(columnName);
    } else {
      newSet.delete(columnName);
    }
    setSelectedColumns(newSet);
  };

  const handleSelectAll = () => {
    if (!schemaChanges) return;
    const allColumns = new Set([
      ...schemaChanges.changed_columns,
      ...schemaChanges.new_columns
    ]);
    setSelectedColumns(allColumns);
  };

  const handleDeselectAll = () => {
    setSelectedColumns(new Set());
  };

  const handleStopReextraction = async () => {
    if (!extractionOperationId) return;

    setIsStopping(true);

    try {
      await schemaAPI.stopReextraction(sessionId, extractionOperationId);
      // The polling will pick up the 'stopped' status and handle cleanup
    } catch (error: any) {
      console.error('Failed to stop re-extraction:', error);
      onError(error.response?.data?.detail || 'Failed to stop re-extraction');
      setIsStopping(false);
    }
  };

  const handleStartReextraction = async () => {
    if (selectedColumns.size === 0) {
      onError('Please select at least one column to re-extract');
      return;
    }

    setLoading(true);

    try {
      // Get API key from localStorage
      const apiKey = await getApiKeyForProvider('gemini');

      // Build the request with columns
      const request: ReextractionRequest = {
        columns: Array.from(selectedColumns),
      };

      // Include LLM config if API key is available
      if (apiKey) {
        request.llm_config = {
          provider: 'gemini',
          model: 'gemini-2.5-flash-lite',
          api_key: apiKey,
          max_output_tokens: 2048,
          temperature: 0.1
        };
      }

      const response = await schemaAPI.startReextraction(sessionId, request);

      // Store operation ID and start extraction mode
      setExtractionOperationId(response.operation_id);
      setIsExtracting(true);
      setExtractionProgress(0);

      // Notify parent about the columns being re-extracted (for WebSocket connection and skeleton display)
      if (onReextractionStarted) {
        onReextractionStarted(response.columns);
      }

      // Start polling for progress
      pollIntervalRef.current = setInterval(async () => {
        try {
          const status = await schemaAPI.getReextractionStatus(sessionId, response.operation_id);
          setExtractionProgress(status.progress * 100);

          if (status.status === 'completed') {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;
            setIsExtracting(false);
            onSuccess(`Re-extraction completed for ${response.columns.length} column${response.columns.length !== 1 ? 's' : ''}.`, true);
            onClose();
          } else if (status.status === 'failed') {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;
            setIsExtracting(false);
            onError(status.error || 'Re-extraction failed');
          } else if (status.status === 'stopped') {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;
            setIsExtracting(false);
            onSuccess(`Re-extraction stopped. ${status.processed_documents}/${status.total_documents} documents processed.`, true);
            onClose();
          }
        } catch (err) {
          console.error('Failed to poll reextraction status:', err);
        }
      }, 2000);

    } catch (error: any) {
      onError(error.response?.data?.detail || 'Failed to start re-extraction');
    } finally {
      setLoading(false);
    }
  };

  const getChangeTypeBadge = (changeType: string) => {
    const variants: Record<string, 'default' | 'secondary' | 'outline' | 'destructive'> = {
      definition: 'default',
      rationale: 'secondary',
      allowed_values: 'outline',
      new: 'default'
    };

    const labels: Record<string, string> = {
      definition: 'Definition changed',
      rationale: 'Rationale changed',
      allowed_values: 'Allowed values changed',
      new: 'New column'
    };

    return (
      <Badge variant={variants[changeType] || 'outline'} className="text-xs">
        {labels[changeType] || changeType}
      </Badge>
    );
  };

  const allColumns = schemaChanges
    ? [...schemaChanges.changed_columns, ...schemaChanges.new_columns]
    : [];

  const hasChanges = allColumns.length > 0;

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <RefreshCw className="h-5 w-5" />
            Re-extract Column Values
          </DialogTitle>
          <DialogDescription>
            Select columns to re-extract values from source documents.
            This will replace existing values for the selected columns.
          </DialogDescription>
        </DialogHeader>

        {loadingStatus ? (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : schemaChanges?.missing_baseline ? (
          <Alert>
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="space-y-3">
              <p>
                <strong>No baseline captured.</strong> This session was created before schema change tracking was enabled.
                All {schemaChanges.new_columns.length} columns appear as "new" because there's nothing to compare against.
              </p>
              <p className="text-sm text-muted-foreground">
                To use re-extraction properly, first capture the current schema as a baseline, then make your edits.
              </p>
              <Button
                size="sm"
                variant="outline"
                onClick={async () => {
                  try {
                    await schemaAPI.captureBaseline(sessionId);
                    onSuccess('Baseline captured! Now edit columns and re-open this dialog.');
                    onClose();
                  } catch (e: any) {
                    onError(e.response?.data?.detail || 'Failed to capture baseline');
                  }
                }}
              >
                <Check className="h-4 w-4 mr-1" />
                Capture Current Schema as Baseline
              </Button>
            </AlertDescription>
          </Alert>
        ) : !hasChanges ? (
          <Alert>
            <Info className="h-4 w-4" />
            <AlertDescription>
              No schema changes detected. Edit column definitions, rationales, or allowed values to enable re-extraction.
            </AlertDescription>
          </Alert>
        ) : (
          <>
            {/* Paper Discovery Status */}
            {paperStatus && (
              <Alert variant={paperStatus.missing_papers.length > 0 ? 'default' : 'default'}>
                <FileText className="h-4 w-4" />
                <AlertDescription>
                  <span className="font-medium">
                    {paperStatus.available_papers.length} source documents available
                  </span>
                  {' '}across {paperStatus.total_rows} rows.
                  {/* Show cloud vs local breakdown */}
                  {paperStatus.cloud_papers && Object.keys(paperStatus.cloud_papers).length > 0 && (
                    <span className="text-blue-600 dark:text-blue-400">
                      {' '}({Object.keys(paperStatus.cloud_papers).length} from cloud storage)
                    </span>
                  )}
                  {paperStatus.missing_papers.length > 0 && (
                    <span className="text-amber-600 dark:text-amber-400">
                      {' '}{paperStatus.missing_papers.length} documents are missing.
                    </span>
                  )}
                </AlertDescription>
              </Alert>
            )}

            {/* Missing Papers Warning */}
            {paperStatus && paperStatus.missing_papers.length > 0 && (
              <Alert variant="destructive" className="mt-2">
                <AlertTriangle className="h-4 w-4" />
                <AlertDescription>
                  <div className="space-y-2">
                    <p className="font-medium">Missing documents (rows will be skipped):</p>
                    <div className="flex flex-wrap gap-1 max-h-20 overflow-y-auto">
                      {paperStatus.missing_papers.slice(0, 10).map((paper) => (
                        <Badge key={paper} variant="outline" className="text-xs">
                          {paper}
                        </Badge>
                      ))}
                      {paperStatus.missing_papers.length > 10 && (
                        <Badge variant="outline" className="text-xs">
                          +{paperStatus.missing_papers.length - 10} more
                        </Badge>
                      )}
                    </div>
                  </div>
                </AlertDescription>
              </Alert>
            )}

            <Separator />

            {/* Column Selection */}
            <div className="space-y-3 flex-1 overflow-hidden">
              <div className="flex items-center justify-between">
                <Label className="text-sm font-medium">
                  Select columns to re-extract ({selectedColumns.size} selected)
                </Label>
                <div className="space-x-2">
                  <Button variant="ghost" size="sm" onClick={handleSelectAll}>
                    Select All
                  </Button>
                  <Button variant="ghost" size="sm" onClick={handleDeselectAll}>
                    Clear
                  </Button>
                </div>
              </div>

              <ScrollArea className="h-[250px] border rounded-md p-3">
                <div className="space-y-3">
                  {/* Changed Columns */}
                  {schemaChanges?.changed_columns.length ? (
                    <div className="space-y-2">
                      <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">
                        Modified Columns
                      </p>
                      {schemaChanges.changed_columns.map((colName) => {
                        const change = schemaChanges.column_changes[colName];
                        return (
                          <ColumnCheckboxItem
                            key={colName}
                            name={colName}
                            change={change}
                            checked={selectedColumns.has(colName)}
                            onCheckedChange={(checked) => handleColumnToggle(colName, checked)}
                            getChangeTypeBadge={getChangeTypeBadge}
                          />
                        );
                      })}
                    </div>
                  ) : null}

                  {/* New Columns */}
                  {schemaChanges?.new_columns.length ? (
                    <div className="space-y-2">
                      <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">
                        New Columns
                      </p>
                      {schemaChanges.new_columns.map((colName) => {
                        const change = schemaChanges.column_changes[colName];
                        return (
                          <ColumnCheckboxItem
                            key={colName}
                            name={colName}
                            change={change}
                            checked={selectedColumns.has(colName)}
                            onCheckedChange={(checked) => handleColumnToggle(colName, checked)}
                            getChangeTypeBadge={getChangeTypeBadge}
                          />
                        );
                      })}
                    </div>
                  ) : null}
                </div>
              </ScrollArea>
            </div>

          </>
        )}

        {/* Extraction Progress UI */}
        {isExtracting && (
          <div className="py-4 space-y-4">
            <Separator />
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span className="font-medium">Re-extracting values...</span>
                <span className="text-muted-foreground">{Math.round(extractionProgress)}%</span>
              </div>
              <Progress value={extractionProgress} className="h-2" />
              <p className="text-sm text-muted-foreground">
                Processing {selectedColumns.size} column{selectedColumns.size !== 1 ? 's' : ''}.
                The table is showing live updates.
              </p>
            </div>
            <div className="flex justify-end gap-2">
              <Button
                variant="destructive"
                size="sm"
                onClick={handleStopReextraction}
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
                    Stop Re-extraction
                  </>
                )}
              </Button>
            </div>
          </div>
        )}

        {/* Normal footer when not extracting */}
        {!isExtracting && (
          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={onClose} disabled={loading}>
              Cancel
            </Button>
            <Button
              onClick={handleStartReextraction}
              disabled={loading || selectedColumns.size === 0 || !hasChanges || !schemaChanges?.can_reextract}
            >
              {loading ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Starting...
                </>
              ) : (
                <>
                  <RefreshCw className="mr-2 h-4 w-4" />
                  Re-extract {selectedColumns.size} Column{selectedColumns.size !== 1 ? 's' : ''}
                </>
              )}
            </Button>
          </DialogFooter>
        )}
      </DialogContent>
    </Dialog>
  );
};

// Sub-component for column checkbox items
interface ColumnCheckboxItemProps {
  name: string;
  change?: ColumnChangeDetail;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
  getChangeTypeBadge: (changeType: string) => React.ReactNode;
}

const ColumnCheckboxItem: React.FC<ColumnCheckboxItemProps> = ({
  name,
  change,
  checked,
  onCheckedChange,
  getChangeTypeBadge
}) => {
  return (
    <div className="flex items-start gap-3 p-2 rounded-md hover:bg-muted/50">
      <Checkbox
        id={`col-${name}`}
        checked={checked}
        onCheckedChange={(checked) => onCheckedChange(checked as boolean)}
        className="mt-0.5"
      />
      <div className="flex-1 space-y-1">
        <div className="flex items-center gap-2">
          <Label htmlFor={`col-${name}`} className="font-medium cursor-pointer">
            {name}
          </Label>
          {change && getChangeTypeBadge(change.change_type)}
        </div>
        {change?.row_count_affected ? (
          <p className="text-xs text-muted-foreground">
            {change.row_count_affected} rows will be updated
          </p>
        ) : null}
        {change?.old_value && change?.new_value && change.change_type !== 'new' && (
          <div className="text-xs text-muted-foreground space-y-0.5">
            <p className="line-through opacity-60">{change.old_value.slice(0, 100)}...</p>
            <p className="text-foreground">{change.new_value.slice(0, 100)}...</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReextractionDialog;
