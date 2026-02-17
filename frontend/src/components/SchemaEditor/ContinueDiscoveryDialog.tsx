import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Plus, Loader2, Check, Info, Square, Upload, Cloud, ChevronDown, ChevronRight, Settings, AlertTriangle } from 'lucide-react';
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
import { Label } from '@/components/ui/label';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/checkbox';
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
import { getApiKeyForProvider, encryptAndStore } from '../../utils/apiKeyStorage';
import {
  LLMProviderKey,
  getModelsForProvider,
  getDefaultModelForProvider,
  LLM_PROVIDER_NAMES,
} from '@/constants/llmModels';
import { DEFAULT_MAX_DOCUMENTS } from '@/constants';

type DialogStep = 'documents' | 'llm_config' | 'discovery' | 'results' | 'no_new_columns';

interface ContinueDiscoveryDialogProps {
  open: boolean;
  sessionId: string;
  sessionType: 'load' | 'schematiq';
  currentColumns: ColumnInfo[];
  query: string;
  onClose: () => void;
  onSuccess: (message: string, newColumns: ColumnInfo[]) => void;
  onError: (error: string) => void;
}

const ContinueDiscoveryDialog: React.FC<ContinueDiscoveryDialogProps> = ({
  open,
  sessionId,
  currentColumns,
  query,
  onClose,
  onSuccess,
  onError,
}) => {
  // Ref for dialog content — used as portal container for Select dropdowns
  const dialogContentRef = useRef<HTMLDivElement>(null);

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

  // Config state
  const [allowLlmConfig, setAllowLlmConfig] = useState(false);

  // Server API key state
  const [serverHasApiKeys, setServerHasApiKeys] = useState(false);

  // Document limit state
  const [maxDocuments, setMaxDocuments] = useState(DEFAULT_MAX_DOCUMENTS);
  const [developerMode, setDeveloperMode] = useState(false);
  const [limitBypassEnabled, setLimitBypassEnabled] = useState(false);

  // Advanced settings state
  const [showAdvancedSettings, setShowAdvancedSettings] = useState(false);
  const [showRetrieverConfig, setShowRetrieverConfig] = useState(false);
  const [maxKeysSchema, setMaxKeysSchema] = useState<string>('');
  const [documentsBatchSize, setDocumentsBatchSize] = useState<string>('1');
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
  const [discoveryBatchInfo, setDiscoveryBatchInfo] = useState<{ current: number; total: number } | null>(null);
  const [discoveryNewColumnCount, setDiscoveryNewColumnCount] = useState(0);
  const [newColumns, setNewColumns] = useState<NewColumnInfo[]>([]);

  const [isStopping, setIsStopping] = useState(false);
  const [adoptDocuments, setAdoptDocuments] = useState(false);

  // Document availability pre-check state (for cloud source)
  const [documentAvailability, setDocumentAvailability] = useState<DocumentAvailabilityResponse | null>(null);
  const [checkingAvailability, setCheckingAvailability] = useState(false);

  const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const finalizedRef = useRef(false);

  // Load document info when dialog opens
  useEffect(() => {
    if (open && sessionId) {
      loadDocumentInfo();
    }
  }, [open, sessionId]);

  // Load API key when dialog opens or provider changes
  useEffect(() => {
    if (open) {
      loadApiKey();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, llmProvider]);

  // Load config when dialog opens
  useEffect(() => {
    const loadConfig = async () => {
      if (!open) return;

      const cfg = await configAPI.getConfig().catch(() => ({
        allow_llm_config: true,
        max_documents: DEFAULT_MAX_DOCUMENTS,
        developer_mode: false,
        server_has_api_keys: false,
      }));
      setAllowLlmConfig(cfg.allow_llm_config);
      setMaxDocuments(cfg.max_documents ?? DEFAULT_MAX_DOCUMENTS);
      setDeveloperMode(cfg.developer_mode ?? false);
      setServerHasApiKeys(cfg.server_has_api_keys ?? false);
    };
    loadConfig();
  }, [open]);

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
      // Clean up orphan discovery documents if finalize wasn't called
      if (!finalizedRef.current && sessionId) {
        schemaAPI.continueDiscovery.finalize(sessionId, { adopt_documents: false })
          .catch(() => {});
      }
      finalizedRef.current = false;
      setStep('documents');
      setDocumentSource('cloud');
      setSelectedCloudDataset('');
      setUploadedFiles([]);
      setNewColumns([]);
      setOperationId(null);
      setDiscoveryProgress(0);
      setDiscoveryBatchInfo(null);
      setDiscoveryNewColumnCount(0);
      setIsStopping(false);
      setAdoptDocuments(false);
      setShowAdvancedSettings(false);
      setShowRetrieverConfig(false);
      setMaxKeysSchema('');
      setDocumentsBatchSize('1');
      setRetrieverConfig({
        model_name: '',
        passage_chars: '',
        k: '',
        enable_dynamic_k: true,
        dynamic_k_threshold: '',
        dynamic_k_minimum: ''
      });
      setDocumentAvailability(null);
      setCheckingAvailability(false);
      setLimitBypassEnabled(false);
      setServerHasApiKeys(false);
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
    }
  }, [open]);

  // Update discovery model when discovery provider changes
  useEffect(() => {
    setLlmModel(getDefaultModelForProvider(llmProvider));
  }, [llmProvider]);

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
    const key = await getApiKeyForProvider(llmProvider);
    if (key) {
      setApiKey(key);
    }
  };

  const handleApiKeyChange = (value: string) => {
    setApiKey(value);
    if (value) {
      encryptAndStore(llmProvider, value);
    }
  };

  const handleStartDiscovery = async () => {
    if (!apiKey && !serverHasApiKeys) {
      onError('API key is required');
      return;
    }

    setLoading(true);
    setStep('discovery');

    try {
      // If uploading files, upload them first and capture server-confirmed filenames
      // (filenames may differ from File.name due to PDF→TXT conversion or dedup suffixes)
      let serverUploadedFiles: string[] | undefined;
      if (documentSource === 'upload' && uploadedFiles.length > 0) {
        try {
          const uploadResult = await loadAPI.addDocuments(sessionId, uploadedFiles);
          serverUploadedFiles = uploadResult.uploaded_files;
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
        uploaded_files: serverUploadedFiles,
        llm_config: {
          provider: llmProvider,
          model: llmModel,
          api_key: apiKey || undefined,
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
        max_keys_schema: maxKeysSchema ? parseInt(maxKeysSchema) : undefined,
        documents_batch_size: documentsBatchSize ? parseInt(documentsBatchSize) : 1,
        bypass_limit: limitBypassEnabled
      });

      setOperationId(response.operation_id);

      // Poll for discovery status in-dialog
      pollIntervalRef.current = setInterval(async () => {
        try {
          const status = await schemaAPI.continueDiscovery.getStatus(sessionId, response.operation_id);
          setDiscoveryProgress(status.progress * 100);
          if (status.current_batch && status.total_batches) {
            setDiscoveryBatchInfo({ current: status.current_batch, total: status.total_batches });
          }
          if (status.new_columns) {
            setDiscoveryNewColumnCount(status.new_columns.length);
          }

          if (status.status === 'completed') {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;

            if (status.new_columns.length === 0) {
              setStep('no_new_columns');
            } else {
              setNewColumns(status.new_columns);
              setStep('results');
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
        } catch (err: any) {
          console.error('Failed to poll discovery status:', err);
          // If operation not found (404), the backend cleaned it up — stop polling
          if (err?.response?.status === 404) {
            clearInterval(pollIntervalRef.current!);
            pollIntervalRef.current = null;
            onError('Discovery operation was lost. Please try again.');
            setStep('documents');
          }
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

  const handleStop = async () => {
    if (!operationId) return;

    setIsStopping(true);
    try {
      await schemaAPI.continueDiscovery.stop(sessionId, operationId);
      // Don't clear polling — let the next poll detect 'stopped' status
      // and handle cleanup naturally
    } catch (error: any) {
      console.error('Failed to stop operation:', error);
      onError(error.response?.data?.detail || 'Failed to stop operation');
      setIsStopping(false);
    }
    // Note: isStopping is cleared when poll detects 'stopped' status and closes dialog
  };

  const handleOk = async () => {
    // Finalize: adopt or discard continue discovery documents
    try {
      finalizedRef.current = true;
      await schemaAPI.continueDiscovery.finalize(sessionId, { adopt_documents: adoptDocuments });
    } catch (err) {
      console.error('Failed to finalize continue discovery documents:', err);
    }

    // Convert new columns to ColumnInfo format and notify parent
    const addedColumns: ColumnInfo[] = newColumns.map(nc => ({
      name: nc.name,
      definition: nc.definition,
      rationale: nc.rationale,
      allowed_values: nc.allowed_values,
      source_document: nc.source_document,
      discovery_iteration: nc.discovery_iteration,
    }));

    const adoptMsg = adoptDocuments
      ? ' Documents will be included as new rows during re-extraction.'
      : '';
    onSuccess(
      `Discovered ${addedColumns.length} new column${addedColumns.length !== 1 ? 's' : ''}. Use Re-extract Data to populate values.${adoptMsg}`,
      addedColumns
    );
    onClose();
  };

  const renderDocumentStep = () => (
    <>
      <DialogHeader>
        <DialogTitle className="flex items-center gap-2">
          <Plus className="h-5 w-5" />
          Continue Schema Discovery
        </DialogTitle>
        <DialogDescription>
          Run schema discovery again to find additional columns. Current schema has {currentColumns.length} columns.
        </DialogDescription>
      </DialogHeader>

      {loading ? (
        <div className="flex items-center justify-center py-8">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <div className="space-y-3 py-4">
          <Alert className="bg-blue-50 border-blue-200 dark:bg-blue-950/20 dark:border-blue-800">
            <Info className="h-4 w-4 text-blue-600" />
            <AlertDescription className="text-blue-700 dark:text-blue-400">
              Documents provided here are used for schema discovery only and won't be automatically added to your data table. After discovery, you'll have the option to include them.
            </AlertDescription>
          </Alert>
          <RadioGroup value={documentSource} onValueChange={(v) => setDocumentSource(v as any)}>
            {/* Cloud Storage */}
            <div className="flex items-start space-x-3 p-3 border rounded-lg">
              <RadioGroupItem value="cloud" id="cloud" disabled={!documentInfo?.cloud_datasets.length} />
              <div className="flex-1">
                <Label htmlFor="cloud" className="flex items-center gap-2 cursor-pointer">
                  <Cloud className="h-4 w-4" />
                  Cloud Storage
                </Label>
                {!documentInfo?.cloud_datasets.length ? (
                  <p className="text-sm text-muted-foreground mt-1">No datasets available</p>
                ) : documentSource === 'cloud' ? (
                  <>
                    <Select value={selectedCloudDataset} onValueChange={setSelectedCloudDataset}>
                      <SelectTrigger className="mt-2">
                        <SelectValue placeholder="Select a dataset" />
                      </SelectTrigger>
                      <SelectContent container={dialogContentRef.current}>
                        {documentInfo.cloud_datasets.map((dataset) => (
                          <SelectItem key={dataset.name} value={dataset.name}>
                            <span className="flex items-center justify-between w-full">
                              <span>{dataset.name}{dataset.name === documentInfo.original_cloud_dataset && ' (original)'}</span>
                              <Badge variant="secondary" className="ml-2 text-xs">{dataset.file_count} files</Badge>
                            </span>
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    {selectedCloudDataset && (() => {
                      const ds = documentInfo.cloud_datasets.find(d => d.name === selectedCloudDataset);
                      if (!ds) return null;
                      return !limitBypassEnabled && ds.file_count > maxDocuments ? (
                        <Alert className="mt-2 border-amber-500 bg-amber-50 dark:bg-amber-950/20">
                          <AlertTriangle className="h-4 w-4 text-amber-600" />
                          <AlertDescription className="text-amber-700 dark:text-amber-400">
                            Dataset has {ds.file_count} documents, but analysis is limited to {maxDocuments}. A sample will be used.
                          </AlertDescription>
                        </Alert>
                      ) : (
                        <p className="text-sm text-muted-foreground mt-1">{ds.file_count} documents</p>
                      );
                    })()}
                  </>
                ) : null}
              </div>
            </div>

            {/* Upload New */}
            <div className="flex items-start space-x-3 p-3 border rounded-lg">
              <RadioGroupItem value="upload" id="upload" />
              <div className="flex-1">
                <Label htmlFor="upload" className="flex items-center gap-2 cursor-pointer">
                  <Upload className="h-4 w-4" />
                  Upload Documents
                </Label>
                {documentSource === 'upload' && (
                  <div className="mt-2">
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
                        {!limitBypassEnabled && uploadedFiles.length > maxDocuments && (
                          <span className="text-amber-600"> — a sample of {maxDocuments} will be used</span>
                        )}
                      </p>
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
            <div className="flex items-center justify-between p-3 border rounded-lg bg-amber-50 border-amber-200 dark:bg-amber-950/20 dark:border-amber-800">
              <div>
                <Label className="text-sm font-medium">Bypass {maxDocuments}-doc limit</Label>
              </div>
              <Switch checked={limitBypassEnabled} onCheckedChange={setLimitBypassEnabled} />
            </div>
          )}

          {/* Advanced Settings */}
          <Collapsible open={showAdvancedSettings} onOpenChange={setShowAdvancedSettings}>
            <CollapsibleTrigger asChild>
              <Button variant="ghost" className="w-full justify-start p-0 h-auto hover:bg-transparent">
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  {showAdvancedSettings ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                  <Settings className="h-4 w-4" />
                  <span>Advanced Settings</span>
                  {!showAdvancedSettings && <Badge variant="secondary" className="ml-2">Default</Badge>}
                </div>
              </Button>
            </CollapsibleTrigger>
            <CollapsibleContent className="pt-3">
              <div className="grid grid-cols-2 gap-3 p-3 border rounded-lg bg-muted/30">
                <div className="space-y-1">
                  <Label className="text-xs">Batch Size</Label>
                  <Input
                    type="number"
                    value={documentsBatchSize}
                    onChange={(e) => setDocumentsBatchSize(e.target.value)}
                    placeholder="1"
                    min={1}
                    max={50}
                    className="h-8 text-sm"
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Max Schema Columns</Label>
                  <Input
                    type="number"
                    value={maxKeysSchema}
                    onChange={(e) => setMaxKeysSchema(e.target.value)}
                    placeholder="No limit (preserves existing)"
                    min={1}
                    max={500}
                    className="h-8 text-sm"
                  />
                  <p className="text-xs text-amber-600">
                    Setting this too low may remove existing columns. Leave empty to preserve all.
                  </p>
                </div>
              </div>
            </CollapsibleContent>
          </Collapsible>
        </div>
      )}

      <DialogFooter>
        <Button variant="outline" onClick={onClose}>Cancel</Button>
        <Button
          onClick={() => {
            if (allowLlmConfig) {
              setStep('llm_config');
            } else {
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
            <SelectContent container={dialogContentRef.current}>
              {(Object.keys(LLM_PROVIDER_NAMES) as LLMProviderKey[]).map((provider) => (
                <SelectItem key={provider} value={provider}>
                  {LLM_PROVIDER_NAMES[provider]}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-2">
          <Label htmlFor="model">Model</Label>
          <Select value={llmModel} onValueChange={setLlmModel}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent container={dialogContentRef.current}>
              {getModelsForProvider(llmProvider).map((model) => (
                <SelectItem key={model.id} value={model.id}>
                  {model.label}
                </SelectItem>
              ))}
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
        <Button onClick={handleStartDiscovery} disabled={(!apiKey && !serverHasApiKeys) || loading}>
          {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
          Start Discovery
        </Button>
      </DialogFooter>
    </>
  );

  const renderDiscoveryStep = () => {
    const progressText = (() => {
      if (discoveryProgress >= 100) return 'Finalizing...';
      if (discoveryBatchInfo && discoveryBatchInfo.total > 0) {
        const batchText = `Processing batch ${discoveryBatchInfo.current}/${discoveryBatchInfo.total}`;
        const columnText = discoveryNewColumnCount > 0
          ? ` — ${discoveryNewColumnCount} new column${discoveryNewColumnCount !== 1 ? 's' : ''} found so far`
          : '';
        return batchText + columnText;
      }
      return 'Analyzing documents...';
    })();

    return (
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
            {progressText}
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
  };

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
        <Button variant="outline" onClick={async () => {
          try { finalizedRef.current = true; await schemaAPI.continueDiscovery.finalize(sessionId, { adopt_documents: false }); } catch {}
          setStep('documents');
        }}>Try Again</Button>
        <Button onClick={async () => {
          try { finalizedRef.current = true; await schemaAPI.continueDiscovery.finalize(sessionId, { adopt_documents: false }); } catch {}
          onClose();
        }}>Close</Button>
      </DialogFooter>
    </>
  );

  const renderResultsStep = () => (
    <>
      <DialogHeader>
        <DialogTitle className="flex items-center gap-2">
          <Check className="h-5 w-5 text-green-500" />
          {newColumns.length} New Column{newColumns.length !== 1 ? 's' : ''} Discovered
        </DialogTitle>
        <DialogDescription>
          The following new columns were found during schema discovery.
        </DialogDescription>
      </DialogHeader>

      <div className="space-y-4 py-4">
        <ScrollArea className="h-[250px] border rounded-md p-3">
          <div className="space-y-3">
            {newColumns.map((col) => (
              <div key={col.name} className="flex items-start space-x-3 p-2 rounded">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="font-medium">{col.name}</span>
                    <Badge variant="success" className="text-xs">New</Badge>
                  </div>
                  {col.definition && (
                    <p className="text-sm text-muted-foreground mt-1 line-clamp-2">
                      {col.definition}
                    </p>
                  )}
                </div>
              </div>
            ))}
          </div>
        </ScrollArea>

        <div className="flex items-start space-x-2 p-3 border rounded-lg">
          <Checkbox
            id="adopt-docs"
            checked={adoptDocuments}
            onCheckedChange={(checked) => setAdoptDocuments(checked === true)}
          />
          <div className="grid gap-1 leading-none">
            <Label htmlFor="adopt-docs" className="text-sm font-medium cursor-pointer">
              Add these documents to session data
            </Label>
            <p className="text-xs text-muted-foreground">
              New rows will be created for these documents during re-extraction.
            </p>
          </div>
        </div>

        <Alert>
          <Info className="h-4 w-4" />
          <AlertDescription>
            {adoptDocuments
              ? <>New columns are visible in the Schema tab. Use <strong>Re-extract Data</strong> to populate values — new rows will also be created from the added documents.</>
              : <>New columns are now visible in the Schema tab. Use <strong>Re-extract Data</strong> to populate values for the new columns.</>
            }
          </AlertDescription>
        </Alert>
      </div>

      <DialogFooter>
        <Button onClick={handleOk}>OK</Button>
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
      case 'results':
        return renderResultsStep();
      default:
        return null;
    }
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent ref={dialogContentRef} className="max-w-2xl max-h-[85vh] flex flex-col">
        {renderStep()}
      </DialogContent>
    </Dialog>
  );
};

export default ContinueDiscoveryDialog;
