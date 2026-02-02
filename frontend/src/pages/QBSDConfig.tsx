import { useState, useEffect, useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { Sparkles, Settings, ArrowLeft, Loader2, ChevronDown, Upload, Trash2, FileText, DollarSign, AlertTriangle, TrendingUp, HelpCircle } from 'lucide-react';
import {
  getConfiguredProviders,
  getApiKeyForProvider,
  LLMProvider,
} from '@/utils/apiKeyStorage';
import {
  getDefaultModelForProvider,
  getAvailableProviders,
  LLM_PROVIDER_NAMES,
  LLMProviderKey,
} from '@/constants/llmModels';
import { ModelSelector } from '@/components/ModelSelector';
import InitialSchemaEditor from '@/components/InitialSchemaEditor/InitialSchemaEditor';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuLabel,
} from '@/components/ui/dropdown-menu';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Switch } from '@/components/ui/switch';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';

import { qbsdAPI, cloudAPI, loadAPI } from '../services/api';
import { useFileUpload } from '../hooks/useFileUpload';
import { formatFileSize } from '../utils/apiHelpers';
import { QBSDConfig, LLMConfig, RetrieverConfig, InitialSchemaColumn, InitialObservationUnit, CostEstimate } from '../types';

const QBSDConfigPage = () => {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Configured providers state
  const [configuredProviders, setConfiguredProviders] = useState<LLMProvider[]>([]);
  const [providersLoading, setProvidersLoading] = useState(true);

  // Initial schema state
  const [initialSchemaPath, setInitialSchemaPath] = useState<string | undefined>(undefined);
  const [initialSchemaData, setInitialSchemaData] = useState<InitialSchemaColumn[] | undefined>(undefined);

  // Observation unit state
  const [observationUnitMode, setObservationUnitMode] = useState<'auto' | 'name_only' | 'full'>('auto');
  const [observationUnitName, setObservationUnitName] = useState('');
  const [observationUnitDefinition, setObservationUnitDefinition] = useState('');

  // Dataset state (for Document Paths dropdown)
  const [datasets, setDatasets] = useState<{ name: string; path: string; file_count: number }[]>([]);
  const [datasetsLoading, setDatasetsLoading] = useState(true);

  // Document source state (upload vs cloud)
  const [documentSource, setDocumentSource] = useState<'upload' | 'cloud'>('cloud');
  const [uploadedFiles, setUploadedFiles] = useState<File[]>([]);
  const [isUploading, setIsUploading] = useState(false);

  // Cost estimate state
  const [costEstimate, setCostEstimate] = useState<CostEstimate | null>(null);
  const [costEstimateLoading, setCostEstimateLoading] = useState(false);
  const [costEstimateError, setCostEstimateError] = useState<string | null>(null);

  // File upload hook for document uploads
  const { getRootProps, getInputProps, isDragActive, dragError } = useFileUpload({
    allowMultiple: true,
    acceptedTypes: {
      'text/plain': ['.txt'],
      'text/markdown': ['.md'],
      'application/pdf': ['.pdf'],
      'application/msword': ['.doc'],
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ['.docx'],
      'application/rtf': ['.rtf'],
    },
    maxSize: 10 * 1024 * 1024, // 10MB per file
    onFilesSelected: setUploadedFiles,
    externalFiles: uploadedFiles,
  });

  const removeUploadedFile = (fileToRemove: File) => {
    setUploadedFiles(prev => prev.filter(f => f !== fileToRemove));
  };

  const totalUploadSize = uploadedFiles.reduce((acc, file) => acc + file.size, 0);

  // Check configured providers on mount and redirect if none
  useEffect(() => {
    const checkProviders = async () => {
      setProvidersLoading(true);
      const providers = await getConfiguredProviders();

      // Filter to only providers that have models defined
      const availableProviders = getAvailableProviders(providers);
      setConfiguredProviders(availableProviders as LLMProvider[]);

      // Redirect if no providers with models are configured
      if (availableProviders.length === 0) {
        navigate('/');
        return;
      }

      // Update default providers if currently selected ones aren't available
      setConfig(prev => {
        let updated = { ...prev };
        let needsUpdate = false;

        const schemaProvider = prev.schema_creation_backend.provider as LLMProviderKey;
        if (!availableProviders.includes(schemaProvider)) {
          const newProvider = availableProviders[0];
          updated = {
            ...updated,
            schema_creation_backend: {
              ...prev.schema_creation_backend,
              provider: newProvider,
              model: getDefaultModelForProvider(newProvider),
            },
          };
          needsUpdate = true;
        }

        const valueProvider = prev.value_extraction_backend.provider as LLMProviderKey;
        if (!availableProviders.includes(valueProvider)) {
          const newProvider = availableProviders[0];
          updated = {
            ...updated,
            value_extraction_backend: {
              ...prev.value_extraction_backend,
              provider: newProvider,
              model: getDefaultModelForProvider(newProvider),
            },
          };
          needsUpdate = true;
        }

        return needsUpdate ? updated : prev;
      });

      setProvidersLoading(false);
    };
    checkProviders();
  }, [navigate]);

  // Fetch available datasets on mount
  useEffect(() => {
    const fetchDatasets = async () => {
      try {
        setDatasetsLoading(true);
        const data = await cloudAPI.getDatasets();
        const datasetsArray = Array.isArray(data) ? data : [];
        setDatasets(datasetsArray);
        // If current selection is empty and datasets are available, select the first one
        if (datasetsArray.length > 0) {
          setConfig(prev => {
            const currentPaths = Array.isArray(prev.docs_path) ? prev.docs_path : [prev.docs_path];
            // Check if current paths exist in the fetched datasets
            const validPaths = currentPaths.filter(path => datasetsArray.some(d => d.name === path));
            if (validPaths.length === 0) {
              return { ...prev, docs_path: [datasetsArray[0].name] };
            }
            return prev;
          });
        }
      } catch (err) {
        console.error('Failed to fetch datasets:', err);
      } finally {
        setDatasetsLoading(false);
      }
    };
    fetchDatasets();
  }, []);

  const [config, setConfig] = useState<QBSDConfig>({
    query: 'Given a protein sequence, can it be determined whether or not it contains a nuclear export signal (NES)? If it does, how strong is the NES, and what is the confidence in that assessment?',
    docs_path: ['../research/data/file'],
    max_keys_schema: 25,
    documents_batch_size: 1,
    schema_creation_backend: {
      provider: 'gemini',
      model: 'gemini-2.5-flash',
      // max_output_tokens and context_window_size are auto-detected from model specs
      temperature: 0,
    },
    value_extraction_backend: {
      provider: 'gemini',
      model: 'gemini-2.5-flash-lite',
      // max_output_tokens and context_window_size are auto-detected from model specs
      temperature: 0,
    },
    output_path: 'outputs/visualization_output.json',
    document_randomization_seed: 42,
    skip_value_extraction: false,
  });

  const handleConfigChange = (field: string, value: any) => {
    setConfig(prev => ({
      ...prev,
      [field]: value,
    }));
  };

  const handleSchemaBackendChange = (field: keyof LLMConfig, value: any) => {
    setConfig(prev => {
      const updates: Partial<LLMConfig> = { [field]: value };

      // When provider changes, auto-select default model for new provider
      if (field === 'provider') {
        updates.model = getDefaultModelForProvider(value as LLMProviderKey);
      }

      return {
        ...prev,
        schema_creation_backend: {
          ...prev.schema_creation_backend,
          ...updates,
        },
      };
    });
  };

  const handleValueBackendChange = (field: keyof LLMConfig, value: any) => {
    setConfig(prev => {
      const updates: Partial<LLMConfig> = { [field]: value };

      // When provider changes, auto-select default model for new provider
      if (field === 'provider') {
        updates.model = getDefaultModelForProvider(value as LLMProviderKey);
      }

      return {
        ...prev,
        value_extraction_backend: {
          ...prev.value_extraction_backend,
          ...updates,
        },
      };
    });
  };

  const handleRetrieverChange = (field: keyof RetrieverConfig, value: any) => {
    setConfig(prev => ({
      ...prev,
      retriever: {
        ...prev.retriever!,
        [field]: value,
      },
    }));
  };

  const handleInitialSchemaChange = useCallback((
    schemaPath: string | undefined,
    schemaData: InitialSchemaColumn[] | undefined
  ) => {
    setInitialSchemaPath(schemaPath);
    setInitialSchemaData(schemaData);
  }, []);

  // Debounced cost estimate fetching
  useEffect(() => {
    // Don't fetch if essential config is missing
    if (!config.schema_creation_backend.provider || !config.schema_creation_backend.model) {
      return;
    }

    // Only fetch if we have documents selected (cloud) or uploaded files
    const hasCloudDocs = documentSource === 'cloud' && config.docs_path && 
      (Array.isArray(config.docs_path) ? config.docs_path.length > 0 : !!config.docs_path);
    const hasUploadedDocs = documentSource === 'upload' && uploadedFiles.length > 0;
    
    if (!hasCloudDocs && !hasUploadedDocs) {
      setCostEstimate(null);
      return;
    }

    const fetchCostEstimate = async () => {
      setCostEstimateLoading(true);
      setCostEstimateError(null);
      
      try {
        // If using uploaded files, send file metadata (name, size) for estimation
        // Backend will estimate tokens from file size (~4 bytes per token)
        let uploadedFileInfo: Array<{ name: string; size: number }> | undefined;
        if (documentSource === 'upload' && uploadedFiles.length > 0) {
          uploadedFileInfo = uploadedFiles.map(file => ({
            name: file.name,
            size: file.size
          }));
        }
        
        const estimate = await qbsdAPI.estimateCostPreview(config, uploadedFileInfo);
        setCostEstimate(estimate);
      } catch (err: any) {
        console.error('Failed to fetch cost estimate:', err);
        // Don't show error for 501 (not available) - just hide the estimate
        if (err.response?.status !== 501) {
          setCostEstimateError(err.response?.data?.detail || 'Failed to estimate cost');
        }
        setCostEstimate(null);
      } finally {
        setCostEstimateLoading(false);
      }
    };

    // Debounce the fetch by 500ms
    const timeoutId = setTimeout(fetchCostEstimate, 500);
    return () => clearTimeout(timeoutId);
  }, [
    config.schema_creation_backend.provider,
    config.schema_creation_backend.model,
    config.schema_creation_backend.max_output_tokens,
    config.value_extraction_backend.provider,
    config.value_extraction_backend.model,
    config.value_extraction_backend.max_output_tokens,
    config.docs_path,
    config.documents_batch_size,
    config.skip_value_extraction,
    config.retriever?.k,
    documentSource,
    uploadedFiles.length,
  ]);

  const handleSubmit = async () => {
    setLoading(true);
    setError(null);

    try {
      // Get API keys from storage
      const schemaApiKey = await getApiKeyForProvider(
        config.schema_creation_backend.provider as LLMProvider
      );
      const valueApiKey = await getApiKeyForProvider(
        config.value_extraction_backend.provider as LLMProvider
      );

      // Build observation unit config based on mode
      let initialObservationUnit: InitialObservationUnit | undefined;
      if (observationUnitMode === 'name_only' && observationUnitName.trim()) {
        initialObservationUnit = { name: observationUnitName.trim() };
      } else if (observationUnitMode === 'full' && observationUnitName.trim()) {
        initialObservationUnit = {
          name: observationUnitName.trim(),
          definition: observationUnitDefinition.trim() || undefined,
        };
      }

      // Build config with API keys and initial schema
      const configWithKeys: QBSDConfig = {
        ...config,
        // If using upload mode, set docs_path to null (documents will be added after session creation)
        docs_path: documentSource === 'upload' ? null : config.docs_path,
        // Tell backend that documents will be uploaded after session creation
        upload_pending: documentSource === 'upload' && uploadedFiles.length > 0,
        schema_creation_backend: {
          ...config.schema_creation_backend,
          api_key: schemaApiKey || undefined,
        },
        value_extraction_backend: {
          ...config.value_extraction_backend,
          api_key: valueApiKey || undefined,
        },
        // Add initial schema (inline data takes priority over file path)
        initial_schema: initialSchemaData,
        initial_schema_path: !initialSchemaData ? initialSchemaPath : undefined,
        // Add initial observation unit
        initial_observation_unit: initialObservationUnit,
      };

      // Step 1: Create session
      const result = await qbsdAPI.configure(configWithKeys);
      const sessionId = result.session_id;

      // Step 2: If files were uploaded, add them to the session
      if (documentSource === 'upload' && uploadedFiles.length > 0) {
        setIsUploading(true);
        try {
          const uploadResult = await loadAPI.addDocuments(sessionId, uploadedFiles);
          if (uploadResult.warnings && uploadResult.warnings.length > 0) {
            console.warn('Upload warnings:', uploadResult.warnings);
          }
        } catch (uploadErr: any) {
          // Session exists but upload failed - still navigate but warn user
          console.error('File upload failed:', uploadErr);
          setError('Session created but some files failed to upload. You can add documents later.');
        } finally {
          setIsUploading(false);
        }
      }

      // Step 3: Navigate to visualization
      navigate(`/visualize/${sessionId}?mode=qbsd`);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to configure QBSD');
    } finally {
      setLoading(false);
    }
  };

  const selectedPaths = Array.isArray(config.docs_path) ? config.docs_path.filter(Boolean) : [config.docs_path].filter(Boolean);
  const hasQuery = config.query.trim() !== '';
  const hasCloudDocuments = documentSource === 'cloud' && selectedPaths.length > 0;
  const hasUploadedFiles = documentSource === 'upload' && uploadedFiles.length > 0;
  const hasDocuments = hasCloudDocuments || hasUploadedFiles;
  // Valid if at least one of query or documents is provided
  const isFormValid = hasQuery || hasDocuments;

  return (
    <div className="max-w-4xl mx-auto">
      <h1 className="text-3xl font-bold tracking-tight mb-2">
        Configure QBSD
      </h1>
      <p className="text-muted-foreground mb-6">
        Set up your Query-Based Schema Discovery parameters to run AI-powered data extraction.
      </p>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-primary" />
            Basic Configuration
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Mode Info */}
          <Alert>
            <AlertDescription>
              <strong>Flexible Input:</strong> Provide a query, documents, or both.
              {!hasQuery && !hasDocuments && (
                <span className="text-destructive ml-1">At least one is required.</span>
              )}
              {hasQuery && hasDocuments && ' Using standard mode (query + documents).'}
              {hasQuery && !hasDocuments && ' Using query-only mode — schema will be planned based on your query.'}
              {!hasQuery && hasDocuments && ' Using document-only mode — schema will be discovered from document content.'}
            </AlertDescription>
          </Alert>

          {/* Research Query */}
          <div className="space-y-2">
            <Label htmlFor="query">
              Research Query {!hasDocuments && <span className="text-destructive">*</span>}
            </Label>
            <Textarea
              id="query"
              rows={3}
              value={config.query}
              onChange={(e) => handleConfigChange('query', e.target.value)}
              placeholder="e.g., Given a protein sequence, can it be determined whether or not it contains a nuclear export signal (NES)?"
              className="resize-none"
              aria-required={!hasDocuments}
              aria-describedby="query-hint"
            />
            <p id="query-hint" className="text-sm text-muted-foreground">
              {hasDocuments && !hasQuery
                ? 'Optional — leave empty to discover schema from document content'
                : 'The research question that will guide schema discovery'}
            </p>
          </div>

          {/* Document Source */}
          <div className="space-y-2">
              <Label>
                Documents {!hasQuery && <span className="text-destructive">*</span>}
              </Label>
              <Tabs value={documentSource} onValueChange={(v) => setDocumentSource(v as 'upload' | 'cloud')}>
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger value="upload" className="flex items-center gap-2">
                    <Upload className="h-4 w-4" />
                    Upload Files
                  </TabsTrigger>
                  <TabsTrigger value="cloud" className="flex items-center gap-2">
                    <FileText className="h-4 w-4" />
                    Cloud Datasets
                  </TabsTrigger>
                </TabsList>

                {/* Upload Files Tab */}
                <TabsContent value="upload" className="mt-3">
                  <div
                    {...getRootProps()}
                    className={`
                      border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors
                      ${isDragActive ? 'border-primary bg-primary/5' : 'border-muted-foreground/25 hover:border-primary/50'}
                    `}
                  >
                    <input {...getInputProps()} />
                    <Upload className="h-8 w-8 mx-auto mb-2 text-muted-foreground" />
                    <p className="text-sm text-muted-foreground">
                      {isDragActive ? 'Drop files here...' : 'Drag and drop files here, or click to browse'}
                    </p>
                    <p className="text-xs text-muted-foreground mt-1">
                      Supports: .txt, .md, .pdf, .doc, .docx, .rtf (max 10MB each)
                    </p>
                  </div>

                  {dragError && (
                    <Alert variant="destructive" className="mt-3">
                      <AlertDescription>{dragError}</AlertDescription>
                    </Alert>
                  )}

                  {uploadedFiles.length > 0 && (
                    <div className="mt-3 space-y-2">
                      {uploadedFiles.map((file, index) => (
                        <div
                          key={`${file.name}-${index}`}
                          className="flex items-center justify-between p-2 bg-muted/50 rounded-md"
                        >
                          <div className="flex items-center gap-2 min-w-0">
                            <FileText className="h-4 w-4 text-muted-foreground flex-shrink-0" />
                            <span className="text-sm truncate">{file.name}</span>
                            <span className="text-xs text-muted-foreground flex-shrink-0">
                              ({formatFileSize(file.size)})
                            </span>
                          </div>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => removeUploadedFile(file)}
                            className="h-7 w-7 p-0 flex-shrink-0"
                          >
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        </div>
                      ))}
                      <p className="text-xs text-muted-foreground">
                        Total: {uploadedFiles.length} file{uploadedFiles.length > 1 ? 's' : ''} ({formatFileSize(totalUploadSize)})
                      </p>
                    </div>
                  )}
                </TabsContent>

                {/* Cloud Datasets Tab */}
                <TabsContent value="cloud" className="mt-3">
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button
                        variant="outline"
                        className="w-full justify-between"
                        disabled={datasetsLoading}
                      >
                        {datasetsLoading ? (
                          <>
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                            Loading datasets...
                          </>
                        ) : selectedPaths.length === 0 ? (
                          'Select datasets...'
                        ) : (
                          `${selectedPaths.length} dataset${selectedPaths.length > 1 ? 's' : ''} selected`
                        )}
                        <ChevronDown className="ml-2 h-4 w-4" />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent className="w-full min-w-[300px] max-h-[300px] overflow-y-auto">
                      <DropdownMenuLabel>Select Datasets</DropdownMenuLabel>
                      {datasets.length === 0 ? (
                        <div className="px-2 py-1.5 text-sm text-muted-foreground">
                          No datasets available
                        </div>
                      ) : (
                        datasets.map((dataset) => (
                          <DropdownMenuCheckboxItem
                            key={dataset.name}
                            checked={selectedPaths.includes(dataset.name)}
                            onSelect={(e) => e.preventDefault()}
                            onCheckedChange={(checked) => {
                              const newPaths = checked
                                ? [...selectedPaths, dataset.name]
                                : selectedPaths.filter(p => p !== dataset.name);
                              handleConfigChange('docs_path', newPaths);
                            }}
                          >
                            <span className="flex items-center justify-between w-full">
                              <span>{dataset.name}</span>
                              <Badge variant="secondary" className="ml-2 text-xs">
                                {dataset.file_count} files
                              </Badge>
                            </span>
                          </DropdownMenuCheckboxItem>
                        ))
                      )}
                    </DropdownMenuContent>
                  </DropdownMenu>
                </TabsContent>
              </Tabs>
              <p className="text-sm text-muted-foreground">
                {hasQuery && !hasDocuments
                  ? 'Optional — leave empty to plan schema based on query alone'
                  : documentSource === 'upload'
                    ? 'Upload documents from your computer'
                    : 'Select one or more document datasets'}
              </p>
          </div>

          {/* Schema Only Mode Toggle */}
          <div className="flex items-center justify-between p-4 border rounded-lg bg-muted/30">
            <div className="space-y-0.5">
              <Label htmlFor="schema-only" className="text-base font-medium">
                Schema Only Mode
              </Label>
              <p className="text-sm text-muted-foreground">
                Skip value extraction — faster and lower cost. Only discover the schema structure.
              </p>
            </div>
            <Switch
              id="schema-only"
              checked={config.skip_value_extraction || false}
              onCheckedChange={(checked) => handleConfigChange('skip_value_extraction', checked)}
            />
          </div>

          {/* Configuration Accordions */}
          <Accordion type="multiple" className="mt-6">
            {/* Advanced Configuration */}
            <AccordionItem value="advanced-config">
              <AccordionTrigger className="hover:no-underline">
                <div className="flex items-center gap-2">
                  <Settings className="h-4 w-4" />
                  <span className="font-semibold">Advanced Configuration</span>
                  {(initialSchemaPath || initialSchemaData || observationUnitMode !== 'auto') && (
                    <Badge variant="secondary" className="ml-2">
                      {[
                        initialSchemaData ? `${initialSchemaData.length} columns` : initialSchemaPath ? 'Schema from file' : null,
                        observationUnitMode !== 'auto' ? 'Custom unit' : null,
                      ].filter(Boolean).join(', ') || 'Configured'}
                    </Badge>
                  )}
                </div>
              </AccordionTrigger>
              <AccordionContent className="space-y-6">
                {/* Schema Parameters */}
                <div className="space-y-3">
                  <Label className="text-sm font-medium">Schema Parameters</Label>
                  <div className="grid md:grid-cols-3 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="max_keys" className="text-sm text-muted-foreground">Max Schema Keys</Label>
                      <Input
                        id="max_keys"
                        type="number"
                        value={config.max_keys_schema}
                        onChange={(e) => handleConfigChange('max_keys_schema', parseInt(e.target.value))}
                        min={1}
                        max={500}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="batch_size" className="text-sm text-muted-foreground">Document Batch Size</Label>
                      <Input
                        id="batch_size"
                        type="number"
                        value={config.documents_batch_size}
                        onChange={(e) => handleConfigChange('documents_batch_size', parseInt(e.target.value))}
                        min={1}
                        max={20}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="seed" className="text-sm text-muted-foreground">Randomization Seed</Label>
                      <Input
                        id="seed"
                        type="number"
                        value={config.document_randomization_seed}
                        onChange={(e) => handleConfigChange('document_randomization_seed', parseInt(e.target.value))}
                      />
                    </div>
                  </div>
                </div>

                {/* Observation Unit */}
                <div className="space-y-3 pt-4 border-t">
                  <Label className="text-sm font-medium">Observation Unit</Label>
                  <p className="text-sm text-muted-foreground">
                    Define what constitutes a single row in your output table.
                  </p>
                  <RadioGroup
                    value={observationUnitMode}
                    onValueChange={(value) => setObservationUnitMode(value as 'auto' | 'name_only' | 'full')}
                    className="space-y-3"
                  >
                    <div className="flex items-start space-x-3">
                      <RadioGroupItem value="auto" id="obs-auto" className="mt-1" />
                      <div className="space-y-1">
                        <Label htmlFor="obs-auto" className="font-medium cursor-pointer">
                          Auto-discover (default)
                        </Label>
                        <p className="text-sm text-muted-foreground">
                          The system will automatically determine the observation unit from your query and documents.
                        </p>
                      </div>
                    </div>
                    <div className="flex items-start space-x-3">
                      <RadioGroupItem value="name_only" id="obs-name" className="mt-1" />
                      <div className="space-y-1 flex-1">
                        <Label htmlFor="obs-name" className="font-medium cursor-pointer">
                          Specify name only
                        </Label>
                        <p className="text-sm text-muted-foreground">
                          Provide the unit name; the system will discover the definition automatically.
                        </p>
                        {observationUnitMode === 'name_only' && (
                          <Input
                            placeholder="e.g., Research Paper, Model-Benchmark Evaluation"
                            value={observationUnitName}
                            onChange={(e) => setObservationUnitName(e.target.value)}
                            className="mt-2"
                          />
                        )}
                      </div>
                    </div>
                    <div className="flex items-start space-x-3">
                      <RadioGroupItem value="full" id="obs-full" className="mt-1" />
                      <div className="space-y-1 flex-1">
                        <Label htmlFor="obs-full" className="font-medium cursor-pointer">
                          Specify name and definition
                        </Label>
                        <p className="text-sm text-muted-foreground">
                          Provide both the unit name and definition for full control.
                        </p>
                        {observationUnitMode === 'full' && (
                          <div className="space-y-2 mt-2">
                            <Input
                              placeholder="Unit name (e.g., Research Paper)"
                              value={observationUnitName}
                              onChange={(e) => setObservationUnitName(e.target.value)}
                            />
                            <Input
                              placeholder="Unit definition (e.g., Each row represents a single research paper)"
                              value={observationUnitDefinition}
                              onChange={(e) => setObservationUnitDefinition(e.target.value)}
                            />
                          </div>
                        )}
                      </div>
                    </div>
                  </RadioGroup>
                </div>

                {/* Initial Schema */}
                <div className="space-y-3 pt-4 border-t">
                  <Label className="text-sm font-medium">Initial Schema</Label>
                  <p className="text-sm text-muted-foreground">
                    Optionally provide an initial schema to guide the discovery process. The LLM will start with these columns and expand as needed.
                  </p>
                  <InitialSchemaEditor onSchemaChange={handleInitialSchemaChange} />
                </div>
              </AccordionContent>
            </AccordionItem>

            {/* Schema Creation LLM */}
            <AccordionItem value="schema-llm">
              <AccordionTrigger className="hover:no-underline">
                <div className="flex items-center gap-2">
                  <Settings className="h-4 w-4" />
                  <span className="font-semibold">Schema Creation LLM</span>
                  <Badge className="ml-2">{config.schema_creation_backend.provider.toUpperCase()}</Badge>
                  <Badge variant="outline">{config.schema_creation_backend.model}</Badge>
                </div>
              </AccordionTrigger>
              <AccordionContent>
                <p className="text-sm text-muted-foreground mb-4">
                  LLM used for discovering schema structure and column definitions. Token limits are auto-detected from model specifications.
                </p>
                <div className="grid md:grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <Label>Provider</Label>
                    <Select
                      value={config.schema_creation_backend.provider}
                      onValueChange={(value) => handleSchemaBackendChange('provider', value)}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {configuredProviders.map((provider) => (
                          <SelectItem key={provider} value={provider}>
                            {LLM_PROVIDER_NAMES[provider as LLMProviderKey]}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="md:col-span-2 space-y-2">
                    <Label>Model</Label>
                    <ModelSelector
                      provider={config.schema_creation_backend.provider as LLMProviderKey}
                      value={config.schema_creation_backend.model}
                      onChange={(modelId) => handleSchemaBackendChange('model', modelId)}
                      showDetails={true}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Temperature</Label>
                    <Input
                      type="number"
                      value={config.schema_creation_backend.temperature}
                      onChange={(e) => handleSchemaBackendChange('temperature', parseFloat(e.target.value))}
                      min={0}
                      max={2}
                      step={0.1}
                    />
                  </div>
                </div>
              </AccordionContent>
            </AccordionItem>

            {/* Value Extraction LLM */}
            <AccordionItem value="value-llm">
              <AccordionTrigger className="hover:no-underline">
                <div className="flex items-center gap-2">
                  <Settings className="h-4 w-4" />
                  <span className="font-semibold">Value Extraction LLM</span>
                  <Badge variant="secondary" className="ml-2">{config.value_extraction_backend.provider.toUpperCase()}</Badge>
                  <Badge variant="outline">{config.value_extraction_backend.model}</Badge>
                </div>
              </AccordionTrigger>
              <AccordionContent>
                <p className="text-sm text-muted-foreground mb-4">
                  LLM used for extracting actual data values from documents
                </p>
                <div className="grid md:grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <Label>Provider</Label>
                    <Select
                      value={config.value_extraction_backend.provider}
                      onValueChange={(value) => handleValueBackendChange('provider', value)}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {configuredProviders.map((provider) => (
                          <SelectItem key={provider} value={provider}>
                            {LLM_PROVIDER_NAMES[provider as LLMProviderKey]}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="md:col-span-2 space-y-2">
                    <Label>Model</Label>
                    <ModelSelector
                      provider={config.value_extraction_backend.provider as LLMProviderKey}
                      value={config.value_extraction_backend.model}
                      onChange={(modelId) => handleValueBackendChange('model', modelId)}
                      showDetails={true}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Max Output Tokens</Label>
                    <Input
                      type="number"
                      value={config.value_extraction_backend.max_output_tokens}
                      onChange={(e) => handleValueBackendChange('max_output_tokens', parseInt(e.target.value))}
                      min={512}
                      max={32768}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Temperature</Label>
                    <Input
                      type="number"
                      value={config.value_extraction_backend.temperature}
                      onChange={(e) => handleValueBackendChange('temperature', parseFloat(e.target.value))}
                      min={0}
                      max={2}
                      step={0.1}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Context Window Size</Label>
                    <Input
                      type="number"
                      value={config.value_extraction_backend.context_window_size || ''}
                      onChange={(e) => handleValueBackendChange('context_window_size', e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="Optional"
                    />
                  </div>
                </div>
              </AccordionContent>
            </AccordionItem>

            {/* Retriever Settings */}
            <AccordionItem value="retriever">
              <AccordionTrigger className="hover:no-underline">
                <div className="flex items-center gap-2">
                  <span className="font-semibold">Retriever Settings</span>
                  <Badge variant="secondary" className="ml-2">{config.retriever?.model_name || 'Default'}</Badge>
                </div>
              </AccordionTrigger>
              <AccordionContent>
                <div className="grid md:grid-cols-3 gap-4">
                  <div className="md:col-span-2 space-y-2">
                    <Label>Model Name</Label>
                    <Input
                      value={config.retriever?.model_name || ''}
                      onChange={(e) => handleRetrieverChange('model_name', e.target.value)}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Passage Characters</Label>
                    <Input
                      type="number"
                      value={config.retriever?.passage_chars || 512}
                      onChange={(e) => handleRetrieverChange('passage_chars', parseInt(e.target.value))}
                      min={128}
                      max={2048}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Overlap</Label>
                    <Input
                      type="number"
                      value={config.retriever?.overlap || 64}
                      onChange={(e) => handleRetrieverChange('overlap', parseInt(e.target.value))}
                      min={0}
                      max={256}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Retrieval K</Label>
                    <Input
                      type="number"
                      value={config.retriever?.k || 15}
                      onChange={(e) => handleRetrieverChange('k', parseInt(e.target.value))}
                      min={1}
                      max={50}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Dynamic K Threshold</Label>
                    <Input
                      type="number"
                      value={config.retriever?.dynamic_k_threshold || 0.65}
                      onChange={(e) => handleRetrieverChange('dynamic_k_threshold', parseFloat(e.target.value))}
                      min={0}
                      max={1}
                      step={0.05}
                    />
                  </div>
                </div>
              </AccordionContent>
            </AccordionItem>
          </Accordion>

          {/* Cost Estimate - Always visible */}
          <Card className="border-2 border-emerald-200 dark:border-emerald-800 bg-emerald-50/50 dark:bg-emerald-950/20">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg flex items-center gap-2">
                <DollarSign className="h-5 w-5 text-emerald-600" />
                Estimated Cost
                {costEstimateLoading && (
                  <Loader2 className="h-4 w-4 animate-spin text-muted-foreground ml-2" />
                )}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {costEstimateLoading ? (
                <div className="text-sm text-muted-foreground">Calculating estimate...</div>
              ) : costEstimate ? (
                <div className="space-y-4">
                  {/* Total Cost Display */}
                  <div className="flex items-center justify-between p-3 bg-white dark:bg-gray-900 rounded-lg border">
                    <span className="font-medium">Total Estimated Cost</span>
                    <span className="text-2xl font-bold text-emerald-600 dark:text-emerald-400">
                      ${costEstimate.total_cost_usd.toFixed(4)}
                    </span>
                  </div>

                  {/* Phase Breakdown */}
                  <div className="grid md:grid-cols-2 gap-3">
                    {/* Schema Discovery */}
                    <div className="p-3 bg-white dark:bg-gray-900 rounded-lg border">
                      <div className="text-sm font-medium text-muted-foreground mb-2">Schema Discovery</div>
                      <div className="space-y-1 text-sm">
                        <div className="flex justify-between">
                          <span>API Calls:</span>
                          <span className="font-mono">{costEstimate.schema_discovery.api_calls}</span>
                        </div>
                        <div className="flex justify-between">
                          <span>Input Tokens:</span>
                          <span className="font-mono">{costEstimate.schema_discovery.input_tokens.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between">
                          <span>Output Tokens:</span>
                          <span className="font-mono">{costEstimate.schema_discovery.output_tokens.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between pt-1 border-t">
                          <span className="font-medium">Cost:</span>
                          <span className="font-mono font-medium">${costEstimate.schema_discovery.cost_usd.toFixed(4)}</span>
                        </div>
                      </div>
                    </div>

                    {/* Value Extraction */}
                    <div className={`p-3 bg-white dark:bg-gray-900 rounded-lg border ${config.skip_value_extraction ? 'opacity-50' : ''}`}>
                      <div className="text-sm font-medium text-muted-foreground mb-2">
                        Value Extraction
                        {config.skip_value_extraction && <Badge variant="secondary" className="ml-2 text-xs">Skipped</Badge>}
                      </div>
                      <div className="space-y-1 text-sm">
                        <div className="flex justify-between">
                          <span>API Calls:</span>
                          <span className="font-mono">{costEstimate.value_extraction.api_calls}</span>
                        </div>
                        <div className="flex justify-between">
                          <span>Input Tokens:</span>
                          <span className="font-mono">{costEstimate.value_extraction.input_tokens.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between">
                          <span>Output Tokens:</span>
                          <span className="font-mono">{costEstimate.value_extraction.output_tokens.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between pt-1 border-t">
                          <span className="font-medium">Cost:</span>
                          <span className="font-mono font-medium">${costEstimate.value_extraction.cost_usd.toFixed(4)}</span>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Document Stats */}
                  {costEstimate.document_stats.num_documents > 0 && (
                    <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
                      <div className="flex items-center gap-1">
                        <FileText className="h-3 w-3" />
                        <span>{costEstimate.document_stats.num_documents} documents</span>
                      </div>
                      <div className="flex items-center gap-1">
                        <TrendingUp className="h-3 w-3" />
                        <span>~{costEstimate.document_stats.avg_tokens_per_document.toLocaleString()} tokens/doc avg</span>
                      </div>
                      <div>
                        <span>Total: {costEstimate.document_stats.total_tokens.toLocaleString()} tokens</span>
                      </div>
                    </div>
                  )}

                  {/* Warnings */}
                  {costEstimate.warnings.length > 0 && (
                    <div className="space-y-2">
                      {costEstimate.warnings.map((warning, idx) => (
                        <Alert key={idx} variant="default" className="py-2">
                          <AlertTriangle className="h-4 w-4" />
                          <AlertDescription className="text-sm">{warning}</AlertDescription>
                        </Alert>
                      ))}
                    </div>
                  )}

                  {/* Disclaimer */}
                  <p className="text-xs text-muted-foreground italic">
                    * This is an estimate based on current configuration. Actual costs may vary depending on LLM responses and document complexity.
                  </p>

                  {/* How we calculate - Expandable */}
                  <Collapsible>
                    <CollapsibleTrigger className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors">
                      <HelpCircle className="h-3.5 w-3.5" />
                      <span>How is this calculated?</span>
                      <ChevronDown className="h-3 w-3" />
                    </CollapsibleTrigger>
                    <CollapsibleContent className="mt-3 p-3 bg-muted/50 rounded-md text-xs space-y-2">
                      <div className="font-medium text-foreground">Cost Formula</div>
                      <div className="font-mono text-[11px] bg-background p-2 rounded border">
                        Cost = (Input Tokens × Input Price) + (Output Tokens × Output Price)
                      </div>
                      
                      <div className="font-medium text-foreground pt-2">Schema Discovery (iterative)</div>
                      <p className="text-muted-foreground">Processes documents in batches, building schema incrementally:</p>
                      <ul className="list-disc list-inside space-y-1 text-muted-foreground">
                        <li><span className="text-foreground">API Calls</span> = ⌈documents ÷ batch_size⌉ + 1 (for observation unit discovery)</li>
                        <li><span className="text-foreground">Input per batch</span> = system_prompt + query + current_schema + passages_from_batch</li>
                        <li className="ml-6 text-[11px]">passages = k × ~250 tokens × batch_size (k = {config.retriever?.k || 15} passages per doc)</li>
                        <li><span className="text-foreground">Output</span> = ~300 tokens avg (JSON with new/updated columns)</li>
                        <li className="ml-6 text-[11px] pt-1 border-t">Model: <span className="font-mono">{config.schema_creation_backend.model || 'default'}</span></li>
                      </ul>

                      <div className="font-medium text-foreground pt-2">Value Extraction (per document)</div>
                      <p className="text-muted-foreground">Processes each document individually:</p>
                      <ul className="list-disc list-inside space-y-1 text-muted-foreground">
                        <li><span className="text-foreground">API Calls</span> = documents × (1 ID + n extractions)</li>
                        <li className="ml-6 text-[11px]">where n = observation units per doc (varies by your data)</li>
                        <li><span className="text-foreground">Input per doc</span> = system_prompt + column_definitions + passages_from_this_doc</li>
                        <li className="ml-6 text-[11px]">passages = k × ~250 tokens (k = {config.retriever?.k || 15})</li>
                        <li><span className="text-foreground">Output</span> = ~40 tokens × columns × 0.7 fill rate</li>
                        <li className="ml-6 text-[11px] pt-1 border-t">Model: <span className="font-mono">{config.value_extraction_backend.model || 'default'}</span></li>
                      </ul>

                      <div className="text-muted-foreground pt-2 border-t mt-2">
                        <div>Schema pricing: <span className="font-mono">{config.schema_creation_backend.provider}/{config.schema_creation_backend.model || 'default'}</span></div>
                        <div>Extraction pricing: <span className="font-mono">{config.value_extraction_backend.provider}/{config.value_extraction_backend.model || 'default'}</span></div>
                      </div>
                    </CollapsibleContent>
                  </Collapsible>
                </div>
              ) : (
                <div className="space-y-4">
                  {/* No documents selected message */}
                  <div className="flex items-center gap-2 p-4 bg-muted/50 rounded-lg border border-dashed">
                    <FileText className="h-5 w-5 text-muted-foreground" />
                    <div className="flex-1">
                      <p className="text-sm font-medium">No documents selected</p>
                      <p className="text-xs text-muted-foreground">
                        {documentSource === 'cloud' 
                          ? 'Select a dataset or upload files to see cost estimate'
                          : 'Upload files to see cost estimate'}
                      </p>
                    </div>
                  </div>

                  {/* How we calculate - Always available */}
                  <Collapsible>
                    <CollapsibleTrigger className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors">
                      <HelpCircle className="h-3.5 w-3.5" />
                      <span>How is this calculated?</span>
                      <ChevronDown className="h-3 w-3" />
                    </CollapsibleTrigger>
                    <CollapsibleContent className="mt-3 p-3 bg-muted/50 rounded-md text-xs space-y-2">
                      <div className="font-medium text-foreground">Cost Formula</div>
                      <div className="font-mono text-[11px] bg-background p-2 rounded border">
                        Cost = (Input Tokens × Input Price) + (Output Tokens × Output Price)
                      </div>
                      
                      <div className="font-medium text-foreground pt-2">Schema Discovery (iterative)</div>
                      <p className="text-muted-foreground">Processes documents in batches, building schema incrementally:</p>
                      <ul className="list-disc list-inside space-y-1 text-muted-foreground">
                        <li><span className="text-foreground">API Calls</span> = ⌈documents ÷ batch_size⌉ + 1 (for observation unit discovery)</li>
                        <li><span className="text-foreground">Input per batch</span> = system_prompt + query + current_schema + passages_from_batch</li>
                        <li className="ml-6 text-[11px]">passages = k × ~250 tokens × batch_size (k = {config.retriever?.k || 15} passages per doc)</li>
                        <li><span className="text-foreground">Output</span> = ~300 tokens avg (JSON with new/updated columns)</li>
                        <li className="ml-6 text-[11px] pt-1 border-t">Model: <span className="font-mono">{config.schema_creation_backend.model || 'default'}</span></li>
                      </ul>

                      <div className="font-medium text-foreground pt-2">Value Extraction (per document)</div>
                      <p className="text-muted-foreground">Processes each document individually:</p>
                      <ul className="list-disc list-inside space-y-1 text-muted-foreground">
                        <li><span className="text-foreground">API Calls</span> = documents × (1 ID + n extractions)</li>
                        <li className="ml-6 text-[11px]">where n = observation units per doc (varies by your data)</li>
                        <li><span className="text-foreground">Input per doc</span> = system_prompt + column_definitions + passages_from_this_doc</li>
                        <li className="ml-6 text-[11px]">passages = k × ~250 tokens (k = {config.retriever?.k || 15})</li>
                        <li><span className="text-foreground">Output</span> = ~40 tokens × columns × 0.7 fill rate</li>
                        <li className="ml-6 text-[11px] pt-1 border-t">Model: <span className="font-mono">{config.value_extraction_backend.model || 'default'}</span></li>
                      </ul>

                      <div className="text-muted-foreground pt-2 border-t mt-2">
                        <div>Schema pricing: <span className="font-mono">{config.schema_creation_backend.provider}/{config.schema_creation_backend.model || 'default'}</span></div>
                        <div>Extraction pricing: <span className="font-mono">{config.value_extraction_backend.provider}/{config.value_extraction_backend.model || 'default'}</span></div>
                      </div>
                    </CollapsibleContent>
                  </Collapsible>
                </div>
              )}
            </CardContent>
          </Card>

          {costEstimateError && (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>{costEstimateError}</AlertDescription>
            </Alert>
          )}

          {/* Actions */}
          <div className="flex justify-between items-center pt-4 border-t">
            <Button variant="outline" onClick={() => navigate('/')}>
              <ArrowLeft className="mr-2 h-4 w-4" />
              Back to Home
            </Button>

            <Button
              size="lg"
              onClick={handleSubmit}
              disabled={!isFormValid || loading || providersLoading || datasetsLoading}
            >
              {(loading || isUploading) ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Sparkles className="mr-2 h-4 w-4" />
              )}
              {isUploading ? 'Uploading files...' : loading ? 'Starting QBSD...' : 'Start QBSD'}
            </Button>
          </div>
        </CardContent>
      </Card>

      {error && (
        <Alert variant="destructive" className="mt-6">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
    </div>
  );
};

export default QBSDConfigPage;
