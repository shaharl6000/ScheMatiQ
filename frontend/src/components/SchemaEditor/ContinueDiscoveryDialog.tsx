import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Plus, Loader2, Check, Info, AlertTriangle, Square, Upload, Cloud } from 'lucide-react';
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
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Input } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

import {
  ContinueDiscoveryDocuments,
  ContinueDiscoveryStatus,
  NewColumnInfo,
  ColumnInfo,
} from '../../types';
import { schemaAPI, loadAPI } from '../../services/api';
import { getApiKeyForProvider, encryptAndStore } from '../../utils/apiKeyStorage';

type DialogStep = 'documents' | 'llm_config' | 'discovery' | 'review' | 'rows' | 'extraction' | 'no_new_columns';

interface ContinueDiscoveryDialogProps {
  open: boolean;
  sessionId: string;
  sessionType: 'load' | 'qbsd';
  currentColumns: ColumnInfo[];
  query: string;
  onClose: () => void;
  onSuccess: (message: string, newColumns: ColumnInfo[]) => void;
  onError: (error: string) => void;
  /** Called when extraction starts for live updates */
  onExtractionStarted?: (columns: string[]) => void;
}

const ContinueDiscoveryDialog: React.FC<ContinueDiscoveryDialogProps> = ({
  open,
  sessionId,
  currentColumns,
  query,
  onClose,
  onSuccess,
  onError,
  onExtractionStarted
}) => {
  // Step state
  const [step, setStep] = useState<DialogStep>('documents');
  const [loading, setLoading] = useState(false);

  // Document source state
  const [documentInfo, setDocumentInfo] = useState<ContinueDiscoveryDocuments | null>(null);
  const [documentSource, setDocumentSource] = useState<'upload' | 'cloud'>('cloud');
  const [uploadedFiles, setUploadedFiles] = useState<File[]>([]);
  const [selectedCloudDataset, setSelectedCloudDataset] = useState<string>('');

  // LLM config state
  const [llmProvider, setLlmProvider] = useState('gemini');
  const [llmModel, setLlmModel] = useState('gemini-2.5-flash');
  const [apiKey, setApiKey] = useState('');

  // Discovery state
  const [operationId, setOperationId] = useState<string | null>(null);
  const [discoveryProgress, setDiscoveryProgress] = useState(0);
  const [newColumns, setNewColumns] = useState<NewColumnInfo[]>([]);

  // Column selection state
  const [selectedNewColumns, setSelectedNewColumns] = useState<Set<string>>(new Set());

  // Row selection state
  const [rowSelection, setRowSelection] = useState<'all' | 'selected'>('all');
  const [selectedRows, setSelectedRows] = useState<Set<string>>(new Set());
  const [availableRows, setAvailableRows] = useState<string[]>([]);

  // Extraction state
  const [extractionProgress, setExtractionProgress] = useState(0);
  const [isStopping, setIsStopping] = useState(false);

  const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Load document info when dialog opens
  useEffect(() => {
    if (open && sessionId) {
      loadDocumentInfo();
      loadApiKey();
    }
  }, [open, sessionId]);

  // Reset state when dialog closes
  useEffect(() => {
    if (!open) {
      setStep('documents');
      setDocumentSource('cloud');
      setSelectedCloudDataset('');
      setUploadedFiles([]);
      setNewColumns([]);
      setSelectedNewColumns(new Set());
      setRowSelection('all');
      setSelectedRows(new Set());
      setOperationId(null);
      setDiscoveryProgress(0);
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

  const loadDocumentInfo = async () => {
    setLoading(true);
    try {
      const info = await schemaAPI.continueDiscovery.getDocuments(sessionId);
      setDocumentInfo(info);

      // Pre-select cloud if datasets available, auto-select original dataset
      if (info.cloud_datasets.length > 0) {
        setDocumentSource('cloud');
        if (info.original_cloud_dataset) {
          setSelectedCloudDataset(info.original_cloud_dataset);
        }
      }
    } catch (error: any) {
      onError(error.response?.data?.detail || 'Failed to load document info');
    } finally {
      setLoading(false);
    }
  };

  const loadApiKey = async () => {
    const key = await getApiKeyForProvider('gemini');
    if (key) {
      setApiKey(key);
    }
  };

  const handleApiKeyChange = (value: string) => {
    setApiKey(value);
    if (value) {
      encryptAndStore('gemini', value);
    }
  };

  const handleStartDiscovery = async () => {
    if (!apiKey) {
      onError('API key is required');
      return;
    }

    setLoading(true);
    setStep('discovery');

    try {
      // If uploading files, upload them first
      if (documentSource === 'upload' && uploadedFiles.length > 0) {
        try {
          await loadAPI.addDocuments(sessionId, uploadedFiles);
        } catch (uploadError: any) {
          onError(uploadError.response?.data?.detail || 'Failed to upload documents');
          setStep('documents');
          setLoading(false);
          return;
        }
      }

      const response = await schemaAPI.continueDiscovery.start(sessionId, {
        document_source: documentSource,
        cloud_dataset: documentSource === 'cloud' ? selectedCloudDataset : undefined,
        llm_config: {
          provider: llmProvider,
          model: llmModel,
          api_key: apiKey,
          max_output_tokens: 4096,
          temperature: 0.7,
          context_window_size: 8192
        },
        max_keys_schema: 100,
        documents_batch_size: 1
      });

      setOperationId(response.operation_id);

      // Start polling for progress
      pollIntervalRef.current = setInterval(async () => {
        try {
          const status = await schemaAPI.continueDiscovery.getStatus(sessionId, response.operation_id);
          setDiscoveryProgress(status.progress * 100);

          if (status.status === 'completed') {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;

            if (status.new_columns.length === 0) {
              setStep('no_new_columns');
            } else {
              setNewColumns(status.new_columns);
              // Pre-select all new columns
              setSelectedNewColumns(new Set(status.new_columns.map(c => c.name)));
              setStep('review');
            }
          } else if (status.status === 'failed') {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;
            onError(status.error || 'Schema discovery failed');
            setStep('documents');
          } else if (status.status === 'stopped') {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;
            onClose();
          }
        } catch (err) {
          console.error('Failed to poll discovery status:', err);
        }
      }, 2000);

    } catch (error: any) {
      onError(error.response?.data?.detail || 'Failed to start schema discovery');
      setStep('documents');
    } finally {
      setLoading(false);
    }
  };

  const handleConfirmColumns = async () => {
    if (selectedNewColumns.size === 0) {
      onError('Please select at least one column');
      return;
    }

    setLoading(true);
    setStep('extraction');

    try {
      await schemaAPI.continueDiscovery.confirmColumns(sessionId, operationId!, {
        selected_columns: Array.from(selectedNewColumns),
        row_selection: rowSelection,
        selected_rows: rowSelection === 'selected' ? Array.from(selectedRows) : undefined,
        llm_config: {
          provider: llmProvider,
          model: llmModel,
          api_key: apiKey,
          max_output_tokens: 2048,
          temperature: 0.1
        }
      });

      // Notify parent about extraction starting
      if (onExtractionStarted) {
        onExtractionStarted(Array.from(selectedNewColumns));
      }

      // Start polling for extraction progress
      pollIntervalRef.current = setInterval(async () => {
        try {
          const status = await schemaAPI.continueDiscovery.getStatus(sessionId, operationId!);

          if (status.phase === 'extraction') {
            setExtractionProgress(status.progress * 100);
          }

          if (status.status === 'completed' && status.phase === 'extraction') {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;

            // Convert new columns to ColumnInfo format
            const addedColumns: ColumnInfo[] = newColumns
              .filter(nc => selectedNewColumns.has(nc.name))
              .map(nc => ({
                name: nc.name,
                definition: nc.definition,
                rationale: nc.rationale,
                allowed_values: nc.allowed_values,
                source_document: nc.source_document,
                discovery_iteration: nc.discovery_iteration
              }));

            onSuccess(`Added ${addedColumns.length} new column${addedColumns.length !== 1 ? 's' : ''} with extracted values.`, addedColumns);
            onClose();
          } else if (status.status === 'failed') {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;
            onError(status.error || 'Value extraction failed');
          } else if (status.status === 'stopped') {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;
            onSuccess('Extraction stopped. Partial values may have been saved.', []);
            onClose();
          }
        } catch (err) {
          console.error('Failed to poll extraction status:', err);
        }
      }, 2000);

    } catch (error: any) {
      onError(error.response?.data?.detail || 'Failed to start extraction');
      setStep('review');
    } finally {
      setLoading(false);
    }
  };

  const handleStop = async () => {
    if (!operationId) return;

    setIsStopping(true);
    try {
      await schemaAPI.continueDiscovery.stop(sessionId, operationId);
    } catch (error: any) {
      console.error('Failed to stop operation:', error);
      onError(error.response?.data?.detail || 'Failed to stop operation');
      setIsStopping(false);
    }
  };

  const handleColumnToggle = (columnName: string, checked: boolean) => {
    const newSet = new Set(selectedNewColumns);
    if (checked) {
      newSet.add(columnName);
    } else {
      newSet.delete(columnName);
    }
    setSelectedNewColumns(newSet);
  };

  const renderDocumentStep = () => (
    <>
      <DialogHeader>
        <DialogTitle className="flex items-center gap-2">
          <Plus className="h-5 w-5" />
          Continue Schema Discovery
        </DialogTitle>
        <DialogDescription>
          Run schema discovery again to find additional columns.
          Current schema has {currentColumns.length} columns.
          {query && <span className="block mt-1 text-xs">Query: "{query}"</span>}
        </DialogDescription>
      </DialogHeader>

      {loading ? (
        <div className="flex items-center justify-center py-8">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <div className="space-y-4 py-4">
          <Label className="text-sm font-medium">Select Document Source</Label>

          <RadioGroup value={documentSource} onValueChange={(v) => setDocumentSource(v as any)}>
            {/* Cloud Storage */}
            <div className="flex items-start space-x-3 p-3 border rounded-lg">
              <RadioGroupItem value="cloud" id="cloud" disabled={!documentInfo?.cloud_datasets.length} />
              <div className="flex-1">
                <Label htmlFor="cloud" className="flex items-center gap-2 cursor-pointer">
                  <Cloud className="h-4 w-4" />
                  Use Cloud Storage
                </Label>
                <p className="text-sm text-muted-foreground mt-1">
                  {documentInfo?.cloud_datasets.length
                    ? `${documentInfo.cloud_datasets.length} datasets available`
                    : 'No cloud datasets available'}
                </p>
                {documentSource === 'cloud' && documentInfo?.cloud_datasets.length ? (
                  <Select value={selectedCloudDataset} onValueChange={setSelectedCloudDataset}>
                    <SelectTrigger className="mt-2">
                      <SelectValue placeholder="Select a dataset" />
                    </SelectTrigger>
                    <SelectContent>
                      {documentInfo.cloud_datasets.map((dataset) => (
                        <SelectItem key={dataset} value={dataset}>
                          {dataset}
                          {dataset === documentInfo.original_cloud_dataset && ' (original)'}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                ) : null}
              </div>
            </div>

            {/* Upload New */}
            <div className="flex items-start space-x-3 p-3 border rounded-lg">
              <RadioGroupItem value="upload" id="upload" />
              <div className="flex-1">
                <Label htmlFor="upload" className="flex items-center gap-2 cursor-pointer">
                  <Upload className="h-4 w-4" />
                  Upload New Documents
                </Label>
                <p className="text-sm text-muted-foreground mt-1">
                  Upload documents for schema discovery
                </p>
                {documentSource === 'upload' && (
                  <div className="mt-3">
                    <input
                      type="file"
                      multiple
                      accept=".txt,.md,.pdf"
                      onChange={(e) => {
                        const files = Array.from(e.target.files || []);
                        setUploadedFiles(files);
                      }}
                      className="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-primary file:text-primary-foreground hover:file:bg-primary/90 cursor-pointer"
                    />
                    {uploadedFiles.length > 0 && (
                      <p className="text-sm text-muted-foreground mt-2">
                        {uploadedFiles.length} file{uploadedFiles.length !== 1 ? 's' : ''} selected
                      </p>
                    )}
                  </div>
                )}
              </div>
            </div>
          </RadioGroup>
        </div>
      )}

      <DialogFooter>
        <Button variant="outline" onClick={onClose}>Cancel</Button>
        <Button
          onClick={() => setStep('llm_config')}
          disabled={loading || (documentSource === 'cloud' && !selectedCloudDataset) || (documentSource === 'upload' && uploadedFiles.length === 0)}
        >
          Next
        </Button>
      </DialogFooter>
    </>
  );

  const renderLLMConfigStep = () => (
    <>
      <DialogHeader>
        <DialogTitle>LLM Configuration</DialogTitle>
        <DialogDescription>
          Configure the language model for schema discovery.
        </DialogDescription>
      </DialogHeader>

      <div className="space-y-4 py-4">
        <div className="space-y-2">
          <Label htmlFor="provider">Provider</Label>
          <Select value={llmProvider} onValueChange={setLlmProvider}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="gemini">Google Gemini</SelectItem>
              <SelectItem value="openai">OpenAI</SelectItem>
              <SelectItem value="together">Together AI</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-2">
          <Label htmlFor="model">Model</Label>
          <Select value={llmModel} onValueChange={setLlmModel}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {llmProvider === 'gemini' && (
                <>
                  <SelectItem value="gemini-2.5-flash">Gemini 2.5 Flash</SelectItem>
                  <SelectItem value="gemini-2.5-flash-lite">Gemini 2.5 Flash Lite</SelectItem>
                  <SelectItem value="gemini-2.5-pro">Gemini 2.5 Pro</SelectItem>
                </>
              )}
              {llmProvider === 'openai' && (
                <>
                  <SelectItem value="gpt-4o">GPT-4o</SelectItem>
                  <SelectItem value="gpt-4o-mini">GPT-4o Mini</SelectItem>
                </>
              )}
              {llmProvider === 'together' && (
                <>
                  <SelectItem value="meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo">Llama 3.1 70B</SelectItem>
                  <SelectItem value="meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo">Llama 3.1 8B</SelectItem>
                </>
              )}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-2">
          <Label htmlFor="apiKey">API Key</Label>
          <Input
            id="apiKey"
            type="password"
            value={apiKey}
            onChange={(e) => handleApiKeyChange(e.target.value)}
            placeholder={`Enter your ${llmProvider} API key`}
          />
          <p className="text-xs text-muted-foreground">
            Your API key is stored locally and never sent to our servers.
          </p>
        </div>
      </div>

      <DialogFooter>
        <Button variant="outline" onClick={() => setStep('documents')}>Back</Button>
        <Button onClick={handleStartDiscovery} disabled={!apiKey || loading}>
          {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
          Start Discovery
        </Button>
      </DialogFooter>
    </>
  );

  const renderDiscoveryStep = () => (
    <>
      <DialogHeader>
        <DialogTitle>Discovering New Columns</DialogTitle>
        <DialogDescription>
          Running schema discovery with your current schema as the starting point...
        </DialogDescription>
      </DialogHeader>

      <div className="py-8 space-y-4">
        <div className="flex items-center justify-center">
          <Loader2 className="h-12 w-12 animate-spin text-primary" />
        </div>
        <Progress value={discoveryProgress} className="w-full" />
        <p className="text-center text-sm text-muted-foreground">
          {discoveryProgress < 100 ? 'Analyzing documents...' : 'Finalizing...'}
        </p>
      </div>

      <DialogFooter>
        <Button variant="destructive" onClick={handleStop} disabled={isStopping}>
          {isStopping ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Square className="h-4 w-4 mr-2" />}
          Stop
        </Button>
      </DialogFooter>
    </>
  );

  const renderNoNewColumnsStep = () => (
    <>
      <DialogHeader>
        <DialogTitle>No New Columns Found</DialogTitle>
      </DialogHeader>

      <div className="py-8">
        <Alert>
          <Info className="h-4 w-4" />
          <AlertDescription>
            Schema discovery did not find any new columns. The current schema appears to be complete
            for the provided documents.
          </AlertDescription>
        </Alert>

        <div className="mt-4 space-y-2">
          <p className="text-sm text-muted-foreground">You can try:</p>
          <ul className="text-sm text-muted-foreground list-disc list-inside space-y-1">
            <li>Using different documents</li>
            <li>Modifying your query</li>
            <li>Using a different LLM model</li>
          </ul>
        </div>
      </div>

      <DialogFooter>
        <Button variant="outline" onClick={() => setStep('documents')}>Try Again</Button>
        <Button onClick={onClose}>Close</Button>
      </DialogFooter>
    </>
  );

  const renderReviewStep = () => (
    <>
      <DialogHeader>
        <DialogTitle className="flex items-center gap-2">
          <Check className="h-5 w-5 text-green-500" />
          {newColumns.length} New Column{newColumns.length !== 1 ? 's' : ''} Discovered
        </DialogTitle>
        <DialogDescription>
          Select which columns to add and extract values for.
        </DialogDescription>
      </DialogHeader>

      <div className="space-y-4 py-4">
        <div className="flex items-center justify-between">
          <Label className="text-sm font-medium">
            Select columns ({selectedNewColumns.size} selected)
          </Label>
          <div className="space-x-2">
            <Button variant="ghost" size="sm" onClick={() => setSelectedNewColumns(new Set(newColumns.map(c => c.name)))}>
              Select All
            </Button>
            <Button variant="ghost" size="sm" onClick={() => setSelectedNewColumns(new Set())}>
              Clear
            </Button>
          </div>
        </div>

        <ScrollArea className="h-[300px] border rounded-md p-3">
          <div className="space-y-3">
            {newColumns.map((col) => (
              <div key={col.name} className="flex items-start space-x-3 p-2 hover:bg-muted/50 rounded">
                <Checkbox
                  id={`col-${col.name}`}
                  checked={selectedNewColumns.has(col.name)}
                  onCheckedChange={(checked) => handleColumnToggle(col.name, checked as boolean)}
                />
                <div className="flex-1 min-w-0">
                  <Label htmlFor={`col-${col.name}`} className="font-medium cursor-pointer">
                    {col.name}
                  </Label>
                  {col.definition && (
                    <p className="text-sm text-muted-foreground mt-1 line-clamp-2">
                      {col.definition}
                    </p>
                  )}
                  {col.source_document && (
                    <Badge variant="outline" className="text-xs mt-1">
                      From: {col.source_document}
                    </Badge>
                  )}
                </div>
              </div>
            ))}
          </div>
        </ScrollArea>
      </div>

      <DialogFooter>
        <Button variant="outline" onClick={onClose}>Cancel</Button>
        <Button
          onClick={() => setStep('rows')}
          disabled={selectedNewColumns.size === 0}
        >
          Next: Select Rows
        </Button>
      </DialogFooter>
    </>
  );

  const renderRowsStep = () => (
    <>
      <DialogHeader>
        <DialogTitle>Select Rows to Process</DialogTitle>
        <DialogDescription>
          Choose which rows to extract values for the new columns.
        </DialogDescription>
      </DialogHeader>

      <div className="space-y-4 py-4">
        <RadioGroup value={rowSelection} onValueChange={(v) => setRowSelection(v as any)}>
          <div className="flex items-center space-x-3 p-3 border rounded-lg">
            <RadioGroupItem value="all" id="all-rows" />
            <div>
              <Label htmlFor="all-rows" className="cursor-pointer">All Rows</Label>
              <p className="text-sm text-muted-foreground">
                Extract values for all existing rows in the table
              </p>
            </div>
          </div>

          <div className="flex items-start space-x-3 p-3 border rounded-lg opacity-50">
            <RadioGroupItem value="selected" id="selected-rows" disabled />
            <div>
              <Label htmlFor="selected-rows" className="cursor-pointer">Select Specific Rows</Label>
              <p className="text-sm text-muted-foreground">
                Coming soon - choose specific rows to process
              </p>
            </div>
          </div>
        </RadioGroup>

        <Separator />

        <Alert>
          <Info className="h-4 w-4" />
          <AlertDescription>
            <strong>{selectedNewColumns.size} column{selectedNewColumns.size !== 1 ? 's' : ''}</strong> will be added
            and values will be extracted for {rowSelection === 'all' ? 'all rows' : `${selectedRows.size} rows`}.
          </AlertDescription>
        </Alert>
      </div>

      <DialogFooter>
        <Button variant="outline" onClick={() => setStep('review')}>Back</Button>
        <Button onClick={handleConfirmColumns} disabled={loading}>
          {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
          Start Extraction
        </Button>
      </DialogFooter>
    </>
  );

  const renderExtractionStep = () => (
    <>
      <DialogHeader>
        <DialogTitle>Extracting Values</DialogTitle>
        <DialogDescription>
          Extracting values for {selectedNewColumns.size} new column{selectedNewColumns.size !== 1 ? 's' : ''}...
        </DialogDescription>
      </DialogHeader>

      <div className="py-8 space-y-4">
        <div className="flex items-center justify-center">
          <Loader2 className="h-12 w-12 animate-spin text-primary" />
        </div>
        <Progress value={extractionProgress} className="w-full" />
        <p className="text-center text-sm text-muted-foreground">
          {extractionProgress < 100 ? 'Processing documents...' : 'Finalizing...'}
        </p>
      </div>

      <DialogFooter>
        <Button variant="destructive" onClick={handleStop} disabled={isStopping}>
          {isStopping ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Square className="h-4 w-4 mr-2" />}
          Stop
        </Button>
      </DialogFooter>
    </>
  );

  const renderStep = () => {
    switch (step) {
      case 'documents':
        return renderDocumentStep();
      case 'llm_config':
        return renderLLMConfigStep();
      case 'discovery':
        return renderDiscoveryStep();
      case 'no_new_columns':
        return renderNoNewColumnsStep();
      case 'review':
        return renderReviewStep();
      case 'rows':
        return renderRowsStep();
      case 'extraction':
        return renderExtractionStep();
      default:
        return null;
    }
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl max-h-[85vh] flex flex-col">
        {renderStep()}
      </DialogContent>
    </Dialog>
  );
};

export default ContinueDiscoveryDialog;
