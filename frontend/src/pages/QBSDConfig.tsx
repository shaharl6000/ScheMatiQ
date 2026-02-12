import { useState, useEffect, useCallback, useMemo } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Sparkles, Settings, ArrowLeft, Loader2, ChevronDown, Upload, Trash2, FileText, DollarSign, AlertTriangle, TrendingUp, HelpCircle, RotateCcw } from 'lucide-react';
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
import { WelcomeDialog } from '@/components/WelcomeDialog/WelcomeDialog';
import { ConsentDialog, getSavedConsent } from '@/components/ConsentDialog/ConsentDialog';
import { InfoTooltip } from '@/components/InfoTooltip/InfoTooltip';

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
import { Checkbox } from '@/components/ui/checkbox';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';

import { qbsdAPI, cloudAPI, loadAPI, configAPI } from '../services/api';
import { useFileUpload } from '../hooks/useFileUpload';
import { formatFileSize } from '../utils/apiHelpers';
import { QBSDConfig, LLMConfig, RetrieverConfig, InitialSchemaColumn, InitialObservationUnit, CostEstimate } from '../types';
import {
  DEFAULT_MAX_DOCUMENTS,
  DEFAULT_MAX_KEYS_SCHEMA,
  DEFAULT_DOCUMENTS_BATCH_SIZE,
  DEFAULT_DOCUMENT_RANDOMIZATION_SEED,
} from '../constants';

const DEFAULT_CONFIG: QBSDConfig = {
  query: '',
  docs_path: [],
  max_keys_schema: DEFAULT_MAX_KEYS_SCHEMA,
  documents_batch_size: DEFAULT_DOCUMENTS_BATCH_SIZE,
  schema_creation_backend: {
    provider: 'gemini',
    model: 'gemini-2.5-flash',
    temperature: 0,
  },
  value_extraction_backend: {
    provider: 'gemini',
    model: 'gemini-2.5-flash-lite',
    temperature: 0,
  },
  output_path: 'outputs/visualization_output.json',
  document_randomization_seed: DEFAULT_DOCUMENT_RANDOMIZATION_SEED,
  skip_value_extraction: false,
};

const QBSDConfigPage = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Previous session file names (restored from navigation state)
  const [previousUploadedFiles, setPreviousUploadedFiles] = useState<string[]>([]);

  // Accordion state (single Advanced Settings accordion)
  const [advancedOpen, setAdvancedOpen] = useState<string>('');

  // LLM editing states (dev mode smart defaults display)
  const [editingSchemaLlm, setEditingSchemaLlm] = useState(false);
  const [editingValueLlm, setEditingValueLlm] = useState(false);

  // Cost estimate expand state
  const [costExpanded, setCostExpanded] = useState(false);

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
  const [documentSource, setDocumentSource] = useState<'upload' | 'cloud'>('upload');
  const [uploadedFiles, setUploadedFiles] = useState<File[]>([]);
  const [isUploading, setIsUploading] = useState(false);

  // Welcome dialog state
  const [welcomeDialogOpen, setWelcomeDialogOpen] = useState(false);

  // Consent dialog state
  const [consentDialogOpen, setConsentDialogOpen] = useState(false);
  const [dataCollectionEnabled, setDataCollectionEnabled] = useState(false);

  // Cost estimate state
  const [costEstimate, setCostEstimate] = useState<CostEstimate | null>(null);
  const [costEstimateLoading, setCostEstimateLoading] = useState(false);
  const [costEstimateError, setCostEstimateError] = useState<string | null>(null);

  // Document limit state (Public Release)
  const [maxDocuments, setMaxDocuments] = useState(DEFAULT_MAX_DOCUMENTS);
  const [developerMode, setDeveloperMode] = useState(false);
  const [limitBypassEnabled, setLimitBypassEnabled] = useState(false);
  const [allowLlmConfig, setAllowLlmConfig] = useState(false);

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

      // Fetch config to check if LLM config is allowed
      const cfg = await configAPI.getConfig().catch(() => ({
        allow_llm_config: true,
        server_has_api_keys: false,
      }));

      // In release mode, only Gemini is needed (LLM config is locked)
      if (!cfg.allow_llm_config) {
        // Release mode: allow access if user has Gemini key OR server has keys
        if (!providers.includes('gemini') && !cfg.server_has_api_keys) {
          navigate('/');
          return;
        }
        setConfiguredProviders(['gemini'] as LLMProvider[]);
        setProvidersLoading(false);
        return;
      }

      // Developer mode: full provider selection
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
        if (datasetsArray.length > 0) {
          setConfig(prev => {
            const currentPaths = Array.isArray(prev.docs_path) ? prev.docs_path : [prev.docs_path];
            const validPaths = currentPaths.filter((path): path is string => path != null && datasetsArray.some(d => d.name === path));
            return { ...prev, docs_path: validPaths };
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

  // Fetch document limit config on mount
  useEffect(() => {
    configAPI.getConfig()
      .then(cfg => {
        setMaxDocuments(cfg.max_documents);
        setDeveloperMode(cfg.developer_mode);
        setAllowLlmConfig(cfg.allow_llm_config);
        setDataCollectionEnabled(cfg.data_collection_enabled ?? false);
      })
      .catch(() => console.log('Using default document limit'));
  }, []);

  // Restore state when navigating back from Visualization screen
  useEffect(() => {
    const state = location.state as {
      config?: QBSDConfig;
      previousSessionId?: string;
      uploadedFileNames?: string[];
    } | null;

    if (!state?.config) return;

    const restoredConfig = state.config;

    // Restore configuration values
    setConfig(prev => ({
      ...prev,
      query: restoredConfig.query || '',
      docs_path: restoredConfig.docs_path || [],
      max_keys_schema: restoredConfig.max_keys_schema ?? DEFAULT_MAX_KEYS_SCHEMA,
      documents_batch_size: restoredConfig.documents_batch_size ?? DEFAULT_DOCUMENTS_BATCH_SIZE,
      document_randomization_seed: restoredConfig.document_randomization_seed ?? DEFAULT_DOCUMENT_RANDOMIZATION_SEED,
      skip_value_extraction: restoredConfig.skip_value_extraction ?? false,
      schema_creation_backend: restoredConfig.schema_creation_backend || prev.schema_creation_backend,
      value_extraction_backend: restoredConfig.value_extraction_backend || prev.value_extraction_backend,
      retriever: restoredConfig.retriever || prev.retriever,
      previous_session_id: state.previousSessionId,
    }));

    // Restore document source
    if (restoredConfig.upload_pending || (state.uploadedFileNames && state.uploadedFileNames.length > 0)) {
      setDocumentSource('upload');
      setPreviousUploadedFiles(state.uploadedFileNames || []);
    } else if (restoredConfig.docs_path && (
      (typeof restoredConfig.docs_path === 'string' && restoredConfig.docs_path) ||
      (Array.isArray(restoredConfig.docs_path) && restoredConfig.docs_path.length > 0)
    )) {
      setDocumentSource('cloud');
    }

    // Restore Observation Unit state
    if (restoredConfig.initial_observation_unit) {
      const unit = restoredConfig.initial_observation_unit;
      if (unit.definition) {
        setObservationUnitMode('full');
        setObservationUnitName(unit.name);
        setObservationUnitDefinition(unit.definition);
      } else {
        setObservationUnitMode('name_only');
        setObservationUnitName(unit.name);
      }
    }

    // Open advanced settings if any non-default values were restored
    const hasAdvancedChanges =
      restoredConfig.max_keys_schema !== DEFAULT_MAX_KEYS_SCHEMA ||
      restoredConfig.documents_batch_size !== DEFAULT_DOCUMENTS_BATCH_SIZE ||
      restoredConfig.document_randomization_seed !== DEFAULT_DOCUMENT_RANDOMIZATION_SEED ||
      restoredConfig.initial_observation_unit ||
      (restoredConfig.schema_creation_backend &&
        (restoredConfig.schema_creation_backend.provider !== DEFAULT_CONFIG.schema_creation_backend.provider ||
        restoredConfig.schema_creation_backend.model !== DEFAULT_CONFIG.schema_creation_backend.model ||
        restoredConfig.schema_creation_backend.temperature !== DEFAULT_CONFIG.schema_creation_backend.temperature)) ||
      (restoredConfig.value_extraction_backend &&
        (restoredConfig.value_extraction_backend.provider !== DEFAULT_CONFIG.value_extraction_backend.provider ||
        restoredConfig.value_extraction_backend.model !== DEFAULT_CONFIG.value_extraction_backend.model ||
        restoredConfig.value_extraction_backend.temperature !== DEFAULT_CONFIG.value_extraction_backend.temperature)) ||
      restoredConfig.retriever;

    if (hasAdvancedChanges) {
      setAdvancedOpen('advanced-settings');
    }

    // Clear navigation state to prevent re-restoration on refresh
    window.history.replaceState({}, document.title);
  }, []); // Run only once on mount

  const handleReset = () => {
    setConfig(DEFAULT_CONFIG);
    setUploadedFiles([]);
    setPreviousUploadedFiles([]);
    setDocumentSource('upload');
    setInitialSchemaPath(undefined);
    setInitialSchemaData(undefined);
    setObservationUnitMode('auto');
    setObservationUnitName('');
    setObservationUnitDefinition('');
    setCostEstimate(null);
    setAdvancedOpen('');
    setEditingSchemaLlm(false);
    setEditingValueLlm(false);
    setCostExpanded(false);
    setError(null);
    window.history.replaceState({}, document.title);
  };

  const [config, setConfig] = useState<QBSDConfig>(DEFAULT_CONFIG);

  // Computed values for document limit (must be after config state is defined)
  const effectiveMaxDocs = (developerMode && limitBypassEnabled) ? Infinity : maxDocuments;
  const isOverLimit = uploadedFiles.length > effectiveMaxDocs;

  // Cloud file count calculation
  const cloudFileCount = useMemo(() => {
    const selectedPaths = Array.isArray(config.docs_path) ? config.docs_path : [];
    return selectedPaths.reduce((total, name) => {
      const ds = datasets.find(d => d.name === name);
      return total + (ds?.file_count || 0);
    }, 0);
  }, [config.docs_path, datasets]);
  const isCloudOverLimit = cloudFileCount > effectiveMaxDocs;

  const handleConfigChange = (field: string, value: any) => {
    setConfig(prev => ({
      ...prev,
      [field]: value,
    }));
  };

  const handleSchemaBackendChange = (field: keyof LLMConfig, value: any) => {
    setConfig(prev => {
      const updates: Partial<LLMConfig> = { [field]: value };

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

  // Debounced cost estimate fetching (developer mode only)
  useEffect(() => {
    if (!developerMode) return;

    if (!config.schema_creation_backend.provider || !config.schema_creation_backend.model) return;

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
        let uploadedFileInfo: Array<{ name: string; size: number }> | undefined;
        if (documentSource === 'upload' && uploadedFiles.length > 0) {
          const filesToEstimate = uploadedFiles.slice(0, effectiveMaxDocs);
          uploadedFileInfo = filesToEstimate.map(file => ({
            name: file.name,
            size: file.size
          }));
        }

        const estimate = await qbsdAPI.estimateCostPreview(config, uploadedFileInfo);
        setCostEstimate(estimate);
      } catch (err: any) {
        console.error('Failed to fetch cost estimate:', err);
        if (err.response?.status !== 501) {
          setCostEstimateError(err.response?.data?.detail || 'Failed to estimate cost');
        }
        setCostEstimate(null);
      } finally {
        setCostEstimateLoading(false);
      }
    };

    const timeoutId = setTimeout(fetchCostEstimate, 500);
    return () => clearTimeout(timeoutId);
  }, [
    config.schema_creation_backend.provider,
    config.schema_creation_backend.model,
    config.value_extraction_backend.provider,
    config.value_extraction_backend.model,
    config.docs_path,
    config.documents_batch_size,
    config.skip_value_extraction,
    config.retriever?.k,
    documentSource,
    uploadedFiles.length,
    effectiveMaxDocs,
    developerMode,
  ]);

  const handleSubmit = async (optOutDataCollection = false) => {
    setLoading(true);
    setError(null);

    try {
      const schemaApiKey = await getApiKeyForProvider(
        config.schema_creation_backend.provider as LLMProvider
      );
      const valueApiKey = await getApiKeyForProvider(
        config.value_extraction_backend.provider as LLMProvider
      );

      let initialObservationUnit: InitialObservationUnit | undefined;
      if (observationUnitMode === 'name_only' && observationUnitName.trim()) {
        initialObservationUnit = { name: observationUnitName.trim() };
      } else if (observationUnitMode === 'full' && observationUnitName.trim()) {
        initialObservationUnit = {
          name: observationUnitName.trim(),
          definition: observationUnitDefinition.trim() || undefined,
        };
      }

      const hasNewUploads = uploadedFiles.length > 0;
      const hasRestoredFiles = previousUploadedFiles.length > 0;
      const configWithKeys: QBSDConfig = {
        ...config,
        docs_path: documentSource === 'upload' ? null : config.docs_path,
        upload_pending: documentSource === 'upload' && (hasNewUploads || hasRestoredFiles),
        previous_session_id: (documentSource === 'upload' && hasRestoredFiles && !hasNewUploads)
          ? config.previous_session_id
          : undefined,
        schema_creation_backend: {
          ...config.schema_creation_backend,
          api_key: schemaApiKey || undefined,
        },
        value_extraction_backend: {
          ...config.value_extraction_backend,
          api_key: valueApiKey || undefined,
        },
        initial_schema: initialSchemaData,
        initial_schema_path: !initialSchemaData ? initialSchemaPath : undefined,
        initial_observation_unit: initialObservationUnit,
        opt_out_data_collection: optOutDataCollection,
      };

      // Step 1: Create session
      const result = await qbsdAPI.configure(configWithKeys);
      const sessionId = result.session_id;

      // Step 2: If files were uploaded, add them to the session
      if (documentSource === 'upload' && uploadedFiles.length > 0) {
        setIsUploading(true);
        try {
          const uploadResult = await loadAPI.addDocuments(sessionId, uploadedFiles, limitBypassEnabled);
          if (uploadResult.warnings && uploadResult.warnings.length > 0) {
            console.warn('Upload warnings:', uploadResult.warnings);
          }
        } catch (uploadErr: any) {
          console.error('File upload failed:', uploadErr);
          setError('Session created but some files failed to upload. You can add documents later.');
        } finally {
          setIsUploading(false);
        }
      }

      // Step 3: Start QBSD execution
      let navState: { autoStarted?: boolean; serverBusy?: boolean; capacityMessage?: string } = {};
      try {
        await qbsdAPI.run(sessionId);
        navState = { autoStarted: true };
      } catch (runErr: any) {
        const status = runErr?.response?.status;
        const detail = runErr?.response?.data?.detail;
        if (status === 503) {
          navState = { serverBusy: true, capacityMessage: detail || 'The server is currently busy processing other requests. Please try again in a few minutes.' };
        }
      }

      // Step 4: Navigate to visualization
      navigate(`/visualize/${sessionId}?mode=qbsd`, { state: navState });
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to configure QBSD');
    } finally {
      setLoading(false);
    }
  };

  // Consent-aware start: show dialog in release mode if not previously accepted
  const handleStartClick = () => {
    if (!dataCollectionEnabled || developerMode) {
      handleSubmit(false);
      return;
    }
    const { consentGiven, savedOptOut } = getSavedConsent();
    if (consentGiven) {
      handleSubmit(savedOptOut);
      return;
    }
    setConsentDialogOpen(true);
  };

  const handleConsentConfirm = (optOut: boolean) => {
    handleSubmit(optOut);
  };

  const selectedPaths = Array.isArray(config.docs_path) ? config.docs_path.filter(Boolean) : [config.docs_path].filter(Boolean);
  const hasQuery = config.query.trim() !== '';
  const hasCloudDocuments = documentSource === 'cloud' && selectedPaths.length > 0;
  const hasUploadedFiles = documentSource === 'upload' && (uploadedFiles.length > 0 || previousUploadedFiles.length > 0);
  const hasDocuments = hasCloudDocuments || hasUploadedFiles;
  const isFormValid = hasQuery || hasDocuments;

  // Track whether form has been modified from defaults
  const isDirty = useMemo(() => {
    if (config.query !== '') return true;
    if (config.max_keys_schema !== DEFAULT_MAX_KEYS_SCHEMA) return true;
    if (config.documents_batch_size !== DEFAULT_DOCUMENTS_BATCH_SIZE) return true;
    if (config.document_randomization_seed !== DEFAULT_DOCUMENT_RANDOMIZATION_SEED) return true;
    if (config.skip_value_extraction) return true;
    if (config.schema_creation_backend.provider !== DEFAULT_CONFIG.schema_creation_backend.provider ||
        config.schema_creation_backend.model !== DEFAULT_CONFIG.schema_creation_backend.model ||
        config.schema_creation_backend.temperature !== DEFAULT_CONFIG.schema_creation_backend.temperature) return true;
    if (config.value_extraction_backend.provider !== DEFAULT_CONFIG.value_extraction_backend.provider ||
        config.value_extraction_backend.model !== DEFAULT_CONFIG.value_extraction_backend.model ||
        config.value_extraction_backend.temperature !== DEFAULT_CONFIG.value_extraction_backend.temperature) return true;
    if (config.retriever) return true;
    if (uploadedFiles.length > 0) return true;
    if (previousUploadedFiles.length > 0) return true;
    if (selectedPaths.length > 0) return true;
    if (observationUnitMode !== 'auto') return true;
    if (initialSchemaPath || initialSchemaData) return true;
    if (limitBypassEnabled) return true;
    return false;
  }, [config, uploadedFiles.length, previousUploadedFiles.length, selectedPaths.length, observationUnitMode, initialSchemaPath, initialSchemaData, limitBypassEnabled]);

  // Summary badge text for advanced settings accordion
  const advancedConfigSummary = useMemo(() => {
    const parts: string[] = [];
    if (initialSchemaData) parts.push(`${initialSchemaData.length} col${initialSchemaData.length !== 1 ? 's' : ''}`);
    else if (initialSchemaPath) parts.push('Schema file');
    if (observationUnitMode !== 'auto') parts.push('Custom unit');
    if (config.max_keys_schema !== DEFAULT_MAX_KEYS_SCHEMA ||
        config.documents_batch_size !== DEFAULT_DOCUMENTS_BATCH_SIZE ||
        config.document_randomization_seed !== DEFAULT_DOCUMENT_RANDOMIZATION_SEED) {
      parts.push('Custom params');
    }
    if (allowLlmConfig && (
      config.schema_creation_backend.provider !== DEFAULT_CONFIG.schema_creation_backend.provider ||
      config.schema_creation_backend.model !== DEFAULT_CONFIG.schema_creation_backend.model
    )) parts.push('Custom LLM');
    if (config.retriever) parts.push('Custom retriever');
    if (limitBypassEnabled) parts.push('Bypass enabled');
    return parts;
  }, [initialSchemaData, initialSchemaPath, observationUnitMode, config.max_keys_schema, config.documents_batch_size, config.document_randomization_seed, config.schema_creation_backend.provider, config.schema_creation_backend.model, config.retriever, allowLlmConfig, limitBypassEnabled]);

  return (
    <div className="max-w-4xl mx-auto">
      <WelcomeDialog
        forceOpen={welcomeDialogOpen}
        onOpenChange={setWelcomeDialogOpen}
      />
      <ConsentDialog
        open={consentDialogOpen}
        onOpenChange={setConsentDialogOpen}
        onConfirm={handleConsentConfirm}
      />

      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <h1 className="text-3xl font-bold tracking-tight">
            Configure QBSD
          </h1>
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setWelcomeDialogOpen(true)}
            aria-label="Show welcome guide"
            className="text-muted-foreground"
          >
            <HelpCircle className="h-5 w-5" />
          </Button>
        </div>
        {isDirty && (
          <Button variant="ghost" onClick={handleReset} className="text-muted-foreground">
            <RotateCcw className="mr-2 h-4 w-4" />
            Reset
          </Button>
        )}
      </div>
      <p className="text-muted-foreground mb-6">
        Configure your query and documents to start extracting structured data.
      </p>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-primary" />
            Basic Configuration
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Flexible Input Banner */}
          <Alert>
            <AlertDescription>
              Provide a query, documents, or both to get started.
            </AlertDescription>
          </Alert>

          {/* Research Query */}
          <div className="space-y-2">
            <Label htmlFor="query">Research Query</Label>
            <Textarea
              id="query"
              rows={3}
              value={config.query}
              onChange={(e) => handleConfigChange('query', e.target.value)}
              placeholder="e.g., Given a protein sequence, can it be determined whether or not it contains a nuclear export signal (NES)? If it does, how strong is the NES, and what is the confidence in that assessment?"
              className="resize-none"
              onKeyDown={(e) => {
                if (e.key === 'Tab' && config.query === '') {
                  e.preventDefault();
                  handleConfigChange('query', 'Given a protein sequence, can it be determined whether or not it contains a nuclear export signal (NES)? If it does, how strong is the NES, and what is the confidence in that assessment?');
                }
              }}
            />
          </div>

          {/* Document Source */}
          <div className="space-y-3">
            <Label>Documents</Label>
            <RadioGroup
              value={documentSource}
              onValueChange={(v) => setDocumentSource(v as 'upload' | 'cloud')}
              className="flex items-center gap-4"
            >
              <div className="flex items-center gap-2">
                <RadioGroupItem value="upload" id="doc-upload" />
                <Label htmlFor="doc-upload" className="cursor-pointer font-normal">Upload files</Label>
              </div>
              <div className="flex items-center gap-2">
                <RadioGroupItem value="cloud" id="doc-cloud" />
                <Label htmlFor="doc-cloud" className="cursor-pointer font-normal">Cloud datasets</Label>
              </div>
            </RadioGroup>

            {/* Upload Files */}
            {documentSource === 'upload' && (
              <div className="space-y-2">
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
                  <Alert variant="destructive">
                    <AlertDescription>{dragError}</AlertDescription>
                  </Alert>
                )}

                {/* Restored files from previous session */}
                {previousUploadedFiles.length > 0 && uploadedFiles.length === 0 && (
                  <div className="text-sm text-muted-foreground">
                    {previousUploadedFiles.length} file{previousUploadedFiles.length > 1 ? 's' : ''} from previous session
                  </div>
                )}

                {uploadedFiles.length > 0 && (
                  <div className="space-y-2">
                    <div className="text-sm text-muted-foreground">
                      {uploadedFiles.length} file{uploadedFiles.length > 1 ? 's' : ''} ({formatFileSize(totalUploadSize)})
                    </div>

                    {uploadedFiles.length <= 5 ? (
                      <div className="space-y-1">
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
                      </div>
                    ) : (
                      <Collapsible>
                        <CollapsibleTrigger className="group flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors">
                          <ChevronDown className="h-4 w-4 transition-transform duration-200 group-data-[state=open]:rotate-180" />
                          <span>Show all files</span>
                        </CollapsibleTrigger>
                        <CollapsibleContent className="space-y-1 mt-2">
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
                        </CollapsibleContent>
                      </Collapsible>
                    )}

                    {/* Document limit warning */}
                    {!limitBypassEnabled && isOverLimit && (
                      <Alert className="border-amber-500 bg-amber-50 dark:bg-amber-950/20">
                        <AlertTriangle className="h-4 w-4 text-amber-600" />
                        <AlertDescription className="text-amber-700 dark:text-amber-400">
                          You've selected {uploadedFiles.length} documents, but analysis is limited to {maxDocuments} to ensure fast results and reasonable costs. A representative sample will be used.
                        </AlertDescription>
                      </Alert>
                    )}
                  </div>
                )}
              </div>
            )}

            {/* Cloud Datasets */}
            {documentSource === 'cloud' && (
              <div className="space-y-2">
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

                {/* Cloud dataset document limit warning */}
                {!limitBypassEnabled && isCloudOverLimit && (
                  <Alert className="border-amber-500 bg-amber-50 dark:bg-amber-950/20">
                    <AlertTriangle className="h-4 w-4 text-amber-600" />
                    <AlertDescription className="text-amber-700 dark:text-amber-400">
                      Your selection contains {cloudFileCount} documents, but analysis is limited to {maxDocuments} to ensure fast results and reasonable costs. A representative sample will be used.
                    </AlertDescription>
                  </Alert>
                )}
              </div>
            )}
          </div>

          {/* Schema Only Mode Checkbox */}
          <div className="flex items-center gap-2">
            <Checkbox
              id="schema-only"
              checked={config.skip_value_extraction || false}
              onCheckedChange={(checked) => handleConfigChange('skip_value_extraction', checked)}
            />
            <Label htmlFor="schema-only" className="text-sm cursor-pointer inline-flex items-center gap-1.5">
              Schema only mode
              <InfoTooltip text="Skip value extraction — discover only the table structure. Faster and lower cost." />
            </Label>
          </div>

          {/* Advanced Settings - Single Accordion */}
          <Accordion
            type="single"
            collapsible
            value={advancedOpen}
            onValueChange={setAdvancedOpen}
          >
            <AccordionItem value="advanced-settings">
              <AccordionTrigger className="hover:no-underline">
                <div className="flex items-center gap-2">
                  <Settings className="h-4 w-4" />
                  <span className="font-semibold">Advanced Settings</span>
                  {advancedConfigSummary.length > 0 && (
                    <Badge variant="secondary" className="ml-2">
                      {advancedConfigSummary.join(', ')}
                    </Badge>
                  )}
                </div>
              </AccordionTrigger>
              <AccordionContent className="space-y-4 pt-2">
                {/* Schema Parameters */}
                <div className="space-y-3">
                  <Label className="text-sm font-medium">Schema Parameters</Label>
                  <div className="grid md:grid-cols-3 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="max_keys" className="text-sm text-muted-foreground inline-flex items-center gap-1.5">
                        Max Schema Keys
                        <InfoTooltip text="Maximum number of columns in your table. Higher numbers capture more detail but increase processing time and cost." />
                      </Label>
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
                      <Label htmlFor="batch_size" className="text-sm text-muted-foreground inline-flex items-center gap-1.5">
                        Document Batch Size
                        <InfoTooltip text="How many documents to process before refining the schema. The default works well for most cases." />
                      </Label>
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

                <hr />

                {/* Observation Unit */}
                <div className="space-y-3">
                  <Label className="text-sm font-medium inline-flex items-center gap-1.5">
                    Observation Unit
                    <InfoTooltip text="What each row in your table represents (e.g., 'a research paper' or 'a patient'). Usually auto-detected, but you can customize it if needed." />
                  </Label>
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

                <hr />

                {/* Initial Schema */}
                <div className="space-y-3">
                  <Label className="text-sm font-medium inline-flex items-center gap-1.5">
                    Initial Schema
                    <InfoTooltip text="Provide column names upfront to guide the extraction. Optional — the tool discovers columns automatically if you leave this blank." />
                  </Label>
                  <p className="text-sm text-muted-foreground">
                    Optionally provide an initial schema to guide the discovery process. The LLM will start with these columns and expand as needed.
                  </p>
                  <InitialSchemaEditor onSchemaChange={handleInitialSchemaChange} />
                </div>

                {/* Developer Mode: LLM Configuration */}
                {allowLlmConfig && (
                  <>
                    <hr />

                    {/* Schema Creation LLM */}
                    <div className="space-y-3">
                      {editingSchemaLlm ? (
                        <>
                          <div className="flex items-center justify-between">
                            <Label className="text-sm font-medium">Schema Creation LLM</Label>
                            <Button variant="ghost" size="sm" onClick={() => setEditingSchemaLlm(false)}>
                              Done
                            </Button>
                          </div>
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
                        </>
                      ) : (
                        <div className="flex items-center justify-between">
                          <div className="text-sm">
                            <span className="font-medium">Schema LLM:</span>{' '}
                            <span className="text-muted-foreground">
                              {LLM_PROVIDER_NAMES[config.schema_creation_backend.provider as LLMProviderKey]} / {config.schema_creation_backend.model}
                              {config.schema_creation_backend.temperature !== 0 && ` (temp: ${config.schema_creation_backend.temperature})`}
                            </span>
                          </div>
                          <Button variant="ghost" size="sm" onClick={() => setEditingSchemaLlm(true)}>
                            Edit
                          </Button>
                        </div>
                      )}
                    </div>

                    <hr />

                    {/* Value Extraction LLM */}
                    <div className="space-y-3">
                      {editingValueLlm ? (
                        <>
                          <div className="flex items-center justify-between">
                            <Label className="text-sm font-medium">Value Extraction LLM</Label>
                            <Button variant="ghost" size="sm" onClick={() => setEditingValueLlm(false)}>
                              Done
                            </Button>
                          </div>
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
                          </div>
                        </>
                      ) : (
                        <div className="flex items-center justify-between">
                          <div className="text-sm">
                            <span className="font-medium">Value LLM:</span>{' '}
                            <span className="text-muted-foreground">
                              {LLM_PROVIDER_NAMES[config.value_extraction_backend.provider as LLMProviderKey]} / {config.value_extraction_backend.model}
                              {config.value_extraction_backend.temperature !== 0 && ` (temp: ${config.value_extraction_backend.temperature})`}
                            </span>
                          </div>
                          <Button variant="ghost" size="sm" onClick={() => setEditingValueLlm(true)}>
                            Edit
                          </Button>
                        </div>
                      )}
                    </div>
                  </>
                )}

                {/* Developer Mode: Retriever Settings */}
                {developerMode && (
                  <>
                    <hr />
                    <div className="space-y-3">
                      <Label className="text-sm font-medium">Retriever Settings</Label>
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
                    </div>
                  </>
                )}

                {/* Developer Mode: Bypass Document Limit */}
                {developerMode && (
                  <>
                    <hr />
                    <div className="flex items-center justify-between p-3 border rounded-lg bg-amber-50 border-amber-200 dark:bg-amber-950/20 dark:border-amber-800">
                      <div>
                        <Label className="text-sm font-medium">Bypass Document Limit</Label>
                        <p className="text-xs text-muted-foreground">
                          Disable the {maxDocuments}-document limit for testing.
                        </p>
                      </div>
                      <Switch checked={limitBypassEnabled} onCheckedChange={setLimitBypassEnabled} />
                    </div>
                  </>
                )}
              </AccordionContent>
            </AccordionItem>
          </Accordion>

          {/* Cost Estimate - Developer mode only, collapsed by default */}
          {developerMode && (
            <div className="space-y-2">
              <div className="flex items-center justify-between p-3 border rounded-lg">
                <div className="flex items-center gap-2">
                  <DollarSign className="h-4 w-4 text-emerald-600" />
                  <span className="text-sm font-medium">
                    {costEstimateLoading ? (
                      <span className="inline-flex items-center gap-1.5">
                        <Loader2 className="h-3 w-3 animate-spin" />
                        Estimating cost...
                      </span>
                    ) : costEstimate ? (
                      `Estimated cost: $${costEstimate.total_cost_usd.toFixed(4)}`
                    ) : (
                      'Upload docs to see cost estimate'
                    )}
                  </span>
                </div>
                {costEstimate && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setCostExpanded(!costExpanded)}
                  >
                    {costExpanded ? 'Hide breakdown' : 'View breakdown'}
                  </Button>
                )}
              </div>

              {costExpanded && costEstimate && (
                <div className="p-4 border rounded-lg space-y-4 bg-muted/20">
                  {/* Phase Breakdown */}
                  <div className="grid md:grid-cols-2 gap-3">
                    {/* Schema Discovery */}
                    <div className="p-3 bg-background rounded-lg border">
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
                    <div className={`p-3 bg-background rounded-lg border ${config.skip_value_extraction ? 'opacity-50' : ''}`}>
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
              )}

              {costEstimateError && (
                <p className="text-sm text-destructive">{costEstimateError}</p>
              )}
            </div>
          )}

          {/* Actions */}
          <div className="flex justify-between items-center pt-4 border-t">
            <Button variant="outline" onClick={() => navigate('/')}>
              <ArrowLeft className="mr-2 h-4 w-4" />
              Back to Home
            </Button>

            <div className="flex items-center gap-3">
              {!isFormValid && (
                <p className="text-sm text-muted-foreground">
                  Add a query or documents to continue
                </p>
              )}
              <Button
                size="lg"
                onClick={handleStartClick}
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
