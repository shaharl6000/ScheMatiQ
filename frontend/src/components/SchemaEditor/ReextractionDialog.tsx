import React, { useState, useEffect, useRef } from 'react';
import { RefreshCw, AlertTriangle, FileText, Loader2, Check, Info, Square, Brain } from 'lucide-react';
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
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { ChevronDown } from 'lucide-react';

import {
  SchemaChangeStatus,
  PaperDiscoveryResult,
  ColumnChangeDetail,
  ReextractionRequest,
  DocumentAvailabilityResponse,
} from '../../types';
import { schemaAPI, configAPI } from '../../services/api';
import MissingDocumentsSection from './MissingDocumentsSection';
import { getApiKeyForProvider, getConfiguredProviders } from '../../utils/apiKeyStorage';
import {
  LLMProviderKey,
  getModelsForProvider,
  getDefaultModelForProvider,
  getAvailableProviders,
  LLM_PROVIDER_NAMES,
} from '@/constants/llmModels';

interface ReextractionDialogProps {
  open: boolean;
  sessionId: string;
  onClose: () => void;
  onSuccess: (message: string, refreshData?: boolean) => void;
  onError: (error: string) => void;
  /** Called when re-extraction starts with the list of columns being re-extracted */
  onReextractionStarted?: (columns: string[], operationId?: string) => void;
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

  // Document availability pre-check state
  const [documentAvailability, setDocumentAvailability] = useState<DocumentAvailabilityResponse | null>(null);
  const [checkingAvailability, setCheckingAvailability] = useState(false);

  // LLM Model selection state
  const [configuredProviders, setConfiguredProviders] = useState<LLMProviderKey[]>([]);
  const [llmProvider, setLlmProvider] = useState<LLMProviderKey>('gemini');
  const [llmModel, setLlmModel] = useState('gemini-2.5-flash-lite');
  const [showModelSettings, setShowModelSettings] = useState(false);
  const [allowLlmConfig, setAllowLlmConfig] = useState(false);
  const [serverHasApiKeys, setServerHasApiKeys] = useState(false);

  // Extraction progress state
  const [isExtracting, setIsExtracting] = useState(false);
  const [extractionOperationId, setExtractionOperationId] = useState<string | null>(null);
  const [extractionProgress, setExtractionProgress] = useState(0);
  const [isStopping, setIsStopping] = useState(false);
  const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Ref for onError to decouple callback identity from effect dependencies
  const onErrorRef = useRef(onError);
  useEffect(() => { onErrorRef.current = onError; });

  // Load schema change status, paper discovery, and document availability when dialog opens
  useEffect(() => {
    if (!open || !sessionId) return;

    let cancelled = false;

    const loadStatus = async () => {
      setLoadingStatus(true);
      try {
        const [changes, papers] = await Promise.all([
          schemaAPI.getSchemaChangeStatus(sessionId),
          schemaAPI.discoverPapers(sessionId),
        ]);
        if (cancelled) return;

        setSchemaChanges(changes);
        setPaperStatus(papers);

        const allChanged = new Set([
          ...changes.changed_columns,
          ...changes.new_columns,
        ]);
        setSelectedColumns(allChanged);

        // Check document availability
        setCheckingAvailability(true);
        try {
          const availability = await schemaAPI.precheckDocuments(sessionId, {
            operation_type: 'reextraction',
          });
          if (!cancelled) setDocumentAvailability(availability);
        } catch (error) {
          console.error('Failed to check document availability:', error);
        } finally {
          if (!cancelled) setCheckingAvailability(false);
        }
      } catch (error: any) {
        if (!cancelled) {
          onErrorRef.current(error.response?.data?.detail || 'Failed to load schema status');
        }
      } finally {
        if (!cancelled) setLoadingStatus(false);
      }
    };

    loadStatus();
    return () => { cancelled = true; };
  }, [open, sessionId]);

  // Load configured providers and config when dialog opens
  useEffect(() => {
    const loadProviders = async () => {
      if (!open) return;

      // Check if LLM config is allowed (release mode vs developer mode)
      const cfg = await configAPI.getConfig().catch(() => ({ allow_llm_config: true, server_has_api_keys: false }));
      setAllowLlmConfig(cfg.allow_llm_config);
      setServerHasApiKeys(cfg.server_has_api_keys ?? false);

      const providers = await getConfiguredProviders();
      const available = getAvailableProviders(providers);
      setConfiguredProviders(available);

      // Set default provider if current one is not available
      if (available.length > 0 && !available.includes(llmProvider)) {
        const defaultProvider = available[0];
        setLlmProvider(defaultProvider);
        setLlmModel(getDefaultModelForProvider(defaultProvider));
      }
    };
    loadProviders();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]); // Intentionally exclude llmProvider to avoid re-running when provider changes

  // Update model when provider changes
  const handleProviderChange = (provider: LLMProviderKey) => {
    setLlmProvider(provider);
    setLlmModel(getDefaultModelForProvider(provider));
  };

  // Reset state when dialog closes
  useEffect(() => {
    if (!open) {
      setSelectedColumns(new Set());
      setIsExtracting(false);
      setExtractionOperationId(null);
      setExtractionProgress(0);
      setIsStopping(false);
      setShowModelSettings(false);
      setDocumentAvailability(null);
      setCheckingAvailability(false);
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
      // Get API key for the selected provider
      const apiKey = await getApiKeyForProvider(llmProvider);

      // Build the request with columns — always include llm_config so backend gets provider/model
      const request: ReextractionRequest = {
        columns: Array.from(selectedColumns),
        llm_config: {
          provider: llmProvider,
          model: llmModel,
          api_key: apiKey || undefined,
          max_output_tokens: 2048,
          temperature: 0
        },
      };

      const response = await schemaAPI.startReextraction(sessionId, request);

      // Notify parent about the columns being re-extracted (for WebSocket connection and skeleton display)
      if (onReextractionStarted) {
        onReextractionStarted(response.columns, response.operation_id);
      }

      // Show success message and close dialog immediately so user can see the table
      const columnCount = response.columns.length;
      const docCount = response.rows_to_process;
      onSuccess(
        `Re-extraction started for ${columnCount} column${columnCount !== 1 ? 's' : ''} across ${docCount} documents. View the Data tab for live progress.`,
        false // Don't refresh data yet - WebSocket will handle updates
      );
      onClose();

    } catch (error: any) {
      const detail = error.response?.data?.detail;
      if (error.response?.status === 503) {
        onError(detail || 'The server is currently busy. Please try again in a few minutes.');
      } else {
        onError(detail || 'Failed to start re-extraction');
      }
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
            Re-extract Column Data
          </DialogTitle>
          <DialogDescription>
            Select columns to re-extract data from source documents.
            This will replace existing data for the selected columns.
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
            {/* Document Availability Pre-check */}
            <MissingDocumentsSection
              sessionId={sessionId}
              availability={documentAvailability}
              loading={checkingAvailability}
              onRefresh={async () => {
                if (!sessionId) return;
                setCheckingAvailability(true);
                try {
                  const availability = await schemaAPI.precheckDocuments(sessionId, {
                    operation_type: 'reextraction',
                  });
                  setDocumentAvailability(availability);
                } catch (error) {
                  console.error('Failed to check document availability:', error);
                } finally {
                  setCheckingAvailability(false);
                }
              }}
            />

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

            {/* Model Selection (Collapsible) - Only show in developer mode */}
            {allowLlmConfig && (
              <Collapsible open={showModelSettings} onOpenChange={setShowModelSettings}>
                <CollapsibleTrigger asChild>
                  <Button variant="ghost" className="w-full justify-between p-2 h-auto">
                    <div className="flex items-center gap-2 text-sm">
                      <Brain className="h-4 w-4" />
                      <span>Model Settings</span>
                      <Badge variant="outline" className="text-xs">
                        {LLM_PROVIDER_NAMES[llmProvider]} / {llmModel}
                      </Badge>
                    </div>
                    <ChevronDown className={`h-4 w-4 transition-transform ${showModelSettings ? 'rotate-180' : ''}`} />
                  </Button>
                </CollapsibleTrigger>
                <CollapsibleContent className="pt-2 space-y-3">
                  <p className="text-xs text-muted-foreground">
                    Choose which AI model will be used for re-extracting values.
                  </p>
                  <div className="grid grid-cols-2 gap-3">
                    <div className="space-y-1.5">
                      <Label className="text-xs">Provider</Label>
                      <Select
                        value={llmProvider}
                        onValueChange={(value) => handleProviderChange(value as LLMProviderKey)}
                        disabled={configuredProviders.length === 0}
                      >
                        <SelectTrigger className="h-8">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {configuredProviders.map((provider) => (
                            <SelectItem key={provider} value={provider}>
                              {LLM_PROVIDER_NAMES[provider]}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-1.5">
                      <Label className="text-xs">Model</Label>
                      <Select
                        value={llmModel}
                        onValueChange={setLlmModel}
                        disabled={getModelsForProvider(llmProvider).length === 0}
                      >
                        <SelectTrigger className="h-8">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {getModelsForProvider(llmProvider).map((model) => (
                            <SelectItem key={model.id} value={model.id}>
                              {model.label}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  {configuredProviders.length === 0 && !serverHasApiKeys && (
                    <Alert variant="warning">
                      <AlertDescription className="text-xs">
                        No API keys configured. Add an API key on the home page to select a model.
                      </AlertDescription>
                    </Alert>
                  )}
                </CollapsibleContent>
              </Collapsible>
            )}

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
