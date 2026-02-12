import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Plus, Loader2, Check, Info, AlertTriangle, Square, Upload, Cloud, ChevronDown, ChevronRight, Settings, Brain, FileText } from 'lucide-react';
import { Progress } from '@/components/ui/progress';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';

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
import { Switch } from '@/components/ui/switch';
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
  DocumentAvailabilityResponse,
} from '../../types';
import { schemaAPI, loadAPI, configAPI } from '../../services/api';
import MissingDocumentsSection from './MissingDocumentsSection';
import { getApiKeyForProvider, encryptAndStore, getConfiguredProviders } from '../../utils/apiKeyStorage';
import {
  LLMProviderKey,
  getModelsForProvider,
  getDefaultModelForProvider,
  getAvailableProviders,
  LLM_PROVIDER_NAMES,
} from '@/constants/llmModels';
import { DEFAULT_MAX_DOCUMENTS } from '@/constants';

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
  /** Called when discovery starts - pass operationId to show full-page monitor */
  onDiscoveryStarted?: (operationId: string) => void;
}

const ContinueDiscoveryDialog: React.FC<ContinueDiscoveryDialogProps> = ({
  open,
  sessionId,
  currentColumns,
  query,
  onClose,
  onSuccess,
  onError,
  onExtractionStarted,
  onDiscoveryStarted,
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
  const [llmProvider, setLlmProvider] = useState<LLMProviderKey>('gemini');
  const [llmModel, setLlmModel] = useState('gemini-2.5-flash');
  const [apiKey, setApiKey] = useState('');

  // Model settings for extraction (collapsible)
  const [showModelSettings, setShowModelSettings] = useState(false);
  const [configuredProviders, setConfiguredProviders] = useState<LLMProviderKey[]>([]);
  const [extractionProvider, setExtractionProvider] = useState<LLMProviderKey>('gemini');
  const [extractionModel, setExtractionModel] = useState('gemini-2.5-flash-lite');
  const [allowLlmConfig, setAllowLlmConfig] = useState(false);
  const [serverHasKey, setServerHasKey] = useState(false);

  // Document limit state
  const [maxDocuments, setMaxDocuments] = useState(DEFAULT_MAX_DOCUMENTS);
  const [developerMode, setDeveloperMode] = useState(false);
  const [limitBypassEnabled, setLimitBypassEnabled] = useState(false);

  // Retriever config state (collapsed by default, empty = use defaults)
  const [showRetrieverConfig, setShowRetrieverConfig] = useState(false);
  const [retrieverConfig, setRetrieverConfig] = useState({
    model_name: '',
    passage_chars: '',
    k: '',
    enable_dynamic_k: true,
    dynamic_k_threshold: '',
    dynamic_k_minimum: ''
  });

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

  // Document availability pre-check state (for cloud source)
  const [documentAvailability, setDocumentAvailability] = useState<DocumentAvailabilityResponse | null>(null);
  const [checkingAvailability, setCheckingAvailability] = useState(false);

  const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Load document info when dialog opens
  useEffect(() => {
    if (open && sessionId) {
      loadDocumentInfo();
      loadApiKey();
    }
  }, [open, sessionId]);

  // Load configured providers and config when dialog opens
  useEffect(() => {
    const loadProviders = async () => {
      if (!open) return;

      // Check if LLM config is allowed (release mode vs developer mode)
      const cfg = await configAPI.getConfig().catch(() => ({
        allow_llm_config: true,
        server_has_llm_key: false,
        max_documents: DEFAULT_MAX_DOCUMENTS,
        developer_mode: false
      }));
      setAllowLlmConfig(cfg.allow_llm_config);
      setServerHasKey(cfg.server_has_llm_key ?? false);
      setMaxDocuments(cfg.max_documents ?? DEFAULT_MAX_DOCUMENTS);
      setDeveloperMode(cfg.developer_mode ?? false);

      const providers = await getConfiguredProviders();
      const available = getAvailableProviders(providers);
      setConfiguredProviders(available);

      // Set default extraction provider if current one is not available
      if (available.length > 0 && !available.includes(extractionProvider)) {
        const defaultProvider = available[0];
        setExtractionProvider(defaultProvider);
        setExtractionModel(getDefaultModelForProvider(defaultProvider));
      }
    };
    loadProviders();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]); // Intentionally exclude extractionProvider to avoid re-running when provider changes

  // Check document availability when cloud dataset is selected
  const checkDocumentAvailability = useCallback(async () => {
    if (!sessionId || documentSource !== 'cloud' || !selectedCloudDataset) {
      setDocumentAvailability(null);
      return;
    }

    setCheckingAvailability(true);
    try {
      const availability = await schemaAPI.precheckDocuments(sessionId, {
        operation_type: 'continue_discovery',
      });
      setDocumentAvailability(availability);
    } catch (error: any) {
      console.error('Failed to check document availability:', error);
      // Don't show error - not critical for continue discovery
    } finally {
      setCheckingAvailability(false);
    }
  }, [sessionId, documentSource, selectedCloudDataset]);

  // Trigger pre-check when cloud dataset is selected
  useEffect(() => {
    if (open && documentSource === 'cloud' && selectedCloudDataset) {
      checkDocumentAvailability();
    }
  }, [open, documentSource, selectedCloudDataset, checkDocumentAvailability]);

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
      setShowRetrieverConfig(false);
      setRetrieverConfig({
        model_name: '',
        passage_chars: '',
        k: '',
        enable_dynamic_k: true,
        dynamic_k_threshold: '',
        dynamic_k_minimum: ''
      });
      setShowModelSettings(false);
      setExtractionProvider('gemini');
      setExtractionModel('gemini-2.5-flash-lite');
      setDocumentAvailability(null);
      setCheckingAvailability(false);
      setLimitBypassEnabled(false);
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
    }
  }, [open]);

  // Update extraction model when extraction provider changes
  const handleExtractionProviderChange = (provider: LLMProviderKey) => {
    setExtractionProvider(provider);
    setExtractionModel(getDefaultModelForProvider(provider));
  };

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
    if (!apiKey && !serverHasKey) {
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

      // Build retriever config only if user has configured any values
      const hasRetrieverConfig = showRetrieverConfig && (
        retrieverConfig.model_name ||
        retrieverConfig.passage_chars ||
        retrieverConfig.k ||
        retrieverConfig.dynamic_k_threshold ||
        retrieverConfig.dynamic_k_minimum
      );

      const response = await schemaAPI.continueDiscovery.start(sessionId, {
        document_source: documentSource,
        cloud_dataset: documentSource === 'cloud' ? selectedCloudDataset : undefined,
        llm_config: {
          provider: llmProvider,
          model: llmModel,
          ...(apiKey ? { api_key: apiKey } : {}),
          max_output_tokens: 8192,
          temperature: 0,
          context_window_size: 1000000
        },
        retriever_config: hasRetrieverConfig ? {
          model_name: retrieverConfig.model_name || undefined,
          passage_chars: retrieverConfig.passage_chars ? parseInt(retrieverConfig.passage_chars) : undefined,
          k: retrieverConfig.k ? parseInt(retrieverConfig.k) : undefined,
          enable_dynamic_k: retrieverConfig.enable_dynamic_k,
          dynamic_k_threshold: retrieverConfig.dynamic_k_threshold ? parseFloat(retrieverConfig.dynamic_k_threshold) : undefined,
          dynamic_k_minimum: retrieverConfig.dynamic_k_minimum ? parseInt(retrieverConfig.dynamic_k_minimum) : undefined
        } : undefined,
        max_keys_schema: 25,
        documents_batch_size: 1,
        bypass_limit: limitBypassEnabled
      });

      setOperationId(response.operation_id);

      // If parent wants to show full-page monitor, notify and close dialog
      if (onDiscoveryStarted) {
        onDiscoveryStarted(response.operation_id);
        onClose();
        setLoading(false);
        return;
      }

      // Otherwise, continue with in-dialog polling
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
      const detail = error.response?.data?.detail;
      if (error.response?.status === 503) {
        onError(detail || 'The server is currently busy. Please try again in a few minutes.');
      } else {
        onError(detail || 'Failed to start schema discovery');
      }
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
      // Get API key for the extraction provider
      const extractionApiKey = await getApiKeyForProvider(extractionProvider);

      await schemaAPI.continueDiscovery.confirmColumns(sessionId, operationId!, {
        selected_columns: Array.from(selectedNewColumns),
        row_selection: rowSelection,
        selected_rows: rowSelection === 'selected' ? Array.from(selectedRows) : undefined,
        llm_config: {
          provider: extractionProvider,
          model: extractionModel,
          api_key: extractionApiKey || apiKey, // Fallback to discovery API key if not configured
          max_output_tokens: 2048,
          temperature: 0
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
      const detail = error.response?.data?.detail;
      if (error.response?.status === 503) {
        onError(detail || 'The server is currently busy. Please try again in a few minutes.');
      } else {
        onError(detail || 'Failed to start extraction');
      }
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
      // Clean up polling interval on successful stop
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
      // Status will be updated by next poll response or WebSocket
    } catch (error: any) {
      console.error('Failed to stop operation:', error);
      onError(error.response?.data?.detail || 'Failed to stop operation');
    } finally {
      setIsStopping(false);  // Always reset stopping state
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

          {/* Document limit notice - visible upfront before any selection */}
          {!limitBypassEnabled && (
            <Alert className="border-blue-200 bg-blue-50 dark:bg-blue-950/20">
              <FileText className="h-4 w-4 text-blue-600" />
              <AlertDescription className="text-sm text-blue-700 dark:text-blue-400">
                <strong>Document limit:</strong> Analysis is limited to {maxDocuments} documents.
                If you provide more, a representative sample will be used.
              </AlertDescription>
            </Alert>
          )}

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
                    {/* Document limit warning - only show when approaching or exceeding limit */}
                    {!limitBypassEnabled && uploadedFiles.length >= Math.floor(maxDocuments * 0.75) && (
                      <Alert className="border-amber-500 bg-amber-50 dark:bg-amber-950/20 mt-2">
                        <AlertTriangle className="h-4 w-4 text-amber-600" />
                        <AlertDescription className="text-amber-700 dark:text-amber-400">
                          {uploadedFiles.length > maxDocuments
                            ? `You've selected ${uploadedFiles.length} documents. We'll analyze a representative sample of ${maxDocuments} documents.`
                            : `${uploadedFiles.length} of ${maxDocuments} documents selected.`}
                        </AlertDescription>
                      </Alert>
                    )}
                  </div>
                )}
              </div>
            </div>
          </RadioGroup>

          {/* Document Availability Pre-check (for cloud source only) */}
          {documentSource === 'cloud' && selectedCloudDataset && (
            <MissingDocumentsSection
              sessionId={sessionId}
              availability={documentAvailability}
              loading={checkingAvailability}
              onRefresh={checkDocumentAvailability}
            />
          )}

          {/* Developer Mode: Bypass Document Limit */}
          {developerMode && (
            <div className="flex items-center justify-between p-4 border rounded-lg bg-amber-50 border-amber-200 dark:bg-amber-950/20 dark:border-amber-800">
              <div>
                <Label className="text-base font-medium">Developer Mode: Bypass Document Limit</Label>
                <p className="text-sm text-muted-foreground">
                  Disable the {maxDocuments}-document limit for testing.
                </p>
              </div>
              <Switch checked={limitBypassEnabled} onCheckedChange={setLimitBypassEnabled} />
            </div>
          )}
        </div>
      )}

      <DialogFooter>
        <Button variant="outline" onClick={onClose}>Cancel</Button>
        <Button
          onClick={() => {
            if (allowLlmConfig) {
              setStep('llm_config');
            } else {
              // Release mode: skip LLM config, use defaults and start discovery directly
              handleStartDiscovery();
            }
          }}
          disabled={loading || (documentSource === 'cloud' && !selectedCloudDataset) || (documentSource === 'upload' && uploadedFiles.length === 0)}
        >
          {allowLlmConfig ? 'Next' : 'Start Discovery'}
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
          <Select value={llmProvider} onValueChange={(value) => setLlmProvider(value as LLMProviderKey)}>
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
                  <SelectItem value="gemini-3">Gemini 3</SelectItem>
                  <SelectItem value="gemini-3-pro">Gemini 3 Pro</SelectItem>
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

        {/* Collapsible Retriever Settings */}
        <Collapsible open={showRetrieverConfig} onOpenChange={setShowRetrieverConfig}>
          <CollapsibleTrigger asChild>
            <Button variant="ghost" className="w-full justify-start p-0 h-auto hover:bg-transparent">
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                {showRetrieverConfig ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                <Settings className="h-4 w-4" />
                <span>Retriever Settings (Advanced)</span>
                {!showRetrieverConfig && <Badge variant="secondary" className="ml-2">Default</Badge>}
              </div>
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent className="pt-3">
            <div className="grid grid-cols-2 gap-3 p-3 border rounded-lg bg-muted/30">
              <div className="col-span-2 space-y-1">
                <Label className="text-xs">Model Name</Label>
                <Input
                  value={retrieverConfig.model_name}
                  onChange={(e) => setRetrieverConfig({...retrieverConfig, model_name: e.target.value})}
                  placeholder="all-MiniLM-L6-v2"
                  className="h-8 text-sm"
                />
              </div>
              <div className="space-y-1">
                <Label className="text-xs">Passage Characters</Label>
                <Input
                  type="number"
                  value={retrieverConfig.passage_chars}
                  onChange={(e) => setRetrieverConfig({...retrieverConfig, passage_chars: e.target.value})}
                  placeholder="512"
                  min={128}
                  max={2048}
                  className="h-8 text-sm"
                />
              </div>
              <div className="space-y-1">
                <Label className="text-xs">Retrieval K</Label>
                <Input
                  type="number"
                  value={retrieverConfig.k}
                  onChange={(e) => setRetrieverConfig({...retrieverConfig, k: e.target.value})}
                  placeholder="15"
                  min={1}
                  max={50}
                  className="h-8 text-sm"
                />
              </div>
              <div className="space-y-1">
                <Label className="text-xs">Dynamic K Threshold</Label>
                <Input
                  type="number"
                  value={retrieverConfig.dynamic_k_threshold}
                  onChange={(e) => setRetrieverConfig({...retrieverConfig, dynamic_k_threshold: e.target.value})}
                  placeholder="0.65"
                  min={0}
                  max={1}
                  step={0.05}
                  className="h-8 text-sm"
                />
              </div>
              <div className="space-y-1">
                <Label className="text-xs">Dynamic K Minimum</Label>
                <Input
                  type="number"
                  value={retrieverConfig.dynamic_k_minimum}
                  onChange={(e) => setRetrieverConfig({...retrieverConfig, dynamic_k_minimum: e.target.value})}
                  placeholder="3"
                  min={1}
                  max={20}
                  className="h-8 text-sm"
                />
              </div>
            </div>
          </CollapsibleContent>
        </Collapsible>
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

        {/* Model Settings (Collapsible) - Only show in developer mode */}
        {allowLlmConfig && (
          <Collapsible open={showModelSettings} onOpenChange={setShowModelSettings}>
            <CollapsibleTrigger asChild>
              <Button variant="ghost" className="w-full justify-between p-2 h-auto">
                <div className="flex items-center gap-2 text-sm">
                  <Brain className="h-4 w-4" />
                  <span>Model Settings</span>
                  <Badge variant="outline" className="text-xs">
                    {LLM_PROVIDER_NAMES[extractionProvider]} / {extractionModel}
                  </Badge>
                </div>
                <ChevronDown className={`h-4 w-4 transition-transform ${showModelSettings ? 'rotate-180' : ''}`} />
              </Button>
            </CollapsibleTrigger>
            <CollapsibleContent className="pt-2 space-y-3">
              <p className="text-xs text-muted-foreground">
                Choose which AI model will be used for extracting values.
              </p>
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-1.5">
                  <Label className="text-xs">Provider</Label>
                  <Select
                    value={extractionProvider}
                    onValueChange={(value) => handleExtractionProviderChange(value as LLMProviderKey)}
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
                    value={extractionModel}
                    onValueChange={setExtractionModel}
                    disabled={getModelsForProvider(extractionProvider).length === 0}
                  >
                    <SelectTrigger className="h-8">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {getModelsForProvider(extractionProvider).map((model) => (
                        <SelectItem key={model.id} value={model.id}>
                          {model.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
              {configuredProviders.length === 0 && (
                <Alert>
                  <AlertDescription className="text-xs">
                    No API keys configured. Add an API key on the home page to select a model.
                  </AlertDescription>
                </Alert>
              )}
            </CollapsibleContent>
          </Collapsible>
        )}

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
