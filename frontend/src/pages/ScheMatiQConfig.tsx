import { useState, useEffect, useCallback, useMemo } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useNavigationGuard } from '../hooks/useNavigationGuard';
import { useNavigationGuardContext } from '../contexts/NavigationGuardContext';
import { NavigationConfirmDialog } from '@/components/ui/NavigationConfirmDialog';
import { Sparkles, Settings, ArrowLeft, Loader2, ChevronDown, Upload, Trash2, FileText, DollarSign, AlertTriangle, HelpCircle, RotateCcw, FileUp, Table } from 'lucide-react';
import {
  getConfiguredProviders,
  getApiKeyForProvider,
  LLMProvider,
} from '@/utils/apiKeyStorage';
import {
  getDefaultModelForProvider,
  getAvailableProviders,
  LLMProviderKey,
  DEFAULT_SCHEMA_MODEL,
  DEFAULT_RELEASE_EXTRACTION_MODEL,
} from '@/constants/llmModels';

import { WelcomeDialog } from '@/components/WelcomeDialog/WelcomeDialog';
import { ConsentDialog, getSavedConsent } from '@/components/ConsentDialog/ConsentDialog';

import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuLabel,
} from '@/components/ui/dropdown-menu';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import {
  AdvancedSettingsFields,
  RETRIEVER_DEFAULTS,
  type AdvancedSettingsValue,
} from '@/components/AdvancedSettings/AdvancedSettingsFields';
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from '@/components/ui/sheet';

import { ApiKeySection } from '@/components/ApiKeySection';
import { schematiqAPI, cloudAPI, loadAPI, configAPI } from '../services/api';
import { useFileUpload } from '../hooks/useFileUpload';
import { formatFileSize } from '../utils/apiHelpers';
import { ScheMatiQConfig, LLMConfig, RetrieverConfig, InitialSchemaColumn, InitialObservationUnit, CostEstimate } from '../types';
import {
  DEFAULT_MAX_DOCUMENTS,
  DEFAULT_MAX_KEYS_SCHEMA,
  DEFAULT_DOCUMENTS_BATCH_SIZE,
  DEFAULT_DOCUMENT_RANDOMIZATION_SEED,
  DEFAULT_CONVERGENCE_THRESHOLD,
} from '../constants';

const DEFAULT_CONFIG: ScheMatiQConfig = {
  query: '',
  docs_path: [],
  max_keys_schema: DEFAULT_MAX_KEYS_SCHEMA,
  documents_batch_size: DEFAULT_DOCUMENTS_BATCH_SIZE,
  schema_creation_backend: {
    provider: 'gemini',
    model: DEFAULT_SCHEMA_MODEL,
    temperature: 0,
  },
  value_extraction_backend: {
    provider: 'gemini',
    model: DEFAULT_RELEASE_EXTRACTION_MODEL,
    temperature: 0,
  },
  output_path: 'outputs/visualization_output.json',
  document_randomization_seed: DEFAULT_DOCUMENT_RANDOMIZATION_SEED,
  skip_value_extraction: false,
};

const ScheMatiQConfigPage = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Previous session file names (restored from navigation state)
  const [previousUploadedFiles, setPreviousUploadedFiles] = useState<string[]>([]);

  // Settings sheet state
  const [settingsOpen, setSettingsOpen] = useState(false);

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
  const [serverHasApiKeys, setServerHasApiKeys] = useState(true);

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
      'application/json': ['.json'],
    },
    maxSize: 25 * 1024 * 1024, // 25MB per file
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

      setServerHasApiKeys(cfg.server_has_api_keys);

      // In release mode, only Gemini is needed (LLM config is locked)
      if (!cfg.allow_llm_config) {
        // Release mode: allow access if user has Gemini key OR server has keys
        if (!providers.includes('gemini') && !cfg.server_has_api_keys) {
          setConfiguredProviders([]);
          setProvidersLoading(false);
          return;
        }
        setConfiguredProviders(['gemini'] as LLMProvider[]);
        setProvidersLoading(false);
        return;
      }

      // Developer mode: full provider selection
      const availableProviders = getAvailableProviders(providers);

      // No providers and no server keys — show API key section inline
      if (availableProviders.length === 0 && !cfg.server_has_api_keys) {
        setConfiguredProviders([]);
        setProvidersLoading(false);
        return;
      }

      // If no client-side providers but server has keys, default to gemini
      if (availableProviders.length === 0 && cfg.server_has_api_keys) {
        setConfiguredProviders(['gemini'] as LLMProvider[]);
        setProvidersLoading(false);
        return;
      }

      setConfiguredProviders(availableProviders as LLMProvider[]);

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
            const droppedCount = currentPaths.filter(p => p != null).length - validPaths.length;
            if (droppedCount > 0) {
              console.warn(`${droppedCount} previously selected dataset(s) are no longer available`);
            }
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
      .catch(() => {});
  }, []);

  // Restore state when navigating back from Visualization screen
  useEffect(() => {
    let state = location.state as {
      config?: ScheMatiQConfig;
      previousSessionId?: string;
      uploadedFileNames?: string[];
    } | null;

    // Fallback: check sessionStorage (browser back button)
    if (!state?.config) {
      for (let i = 0; i < sessionStorage.length; i++) {
        const key = sessionStorage.key(i);
        if (key?.startsWith('schematiq_config_')) {
          try {
            const saved = JSON.parse(sessionStorage.getItem(key)!);
            if (saved?.config) {
              state = saved;
              sessionStorage.removeItem(key); // consume it
              break;
            }
          } catch { /* ignore parse errors */ }
        }
      }
    }

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
      convergence_threshold: restoredConfig.convergence_threshold,
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
      (restoredConfig.max_keys_schema ?? DEFAULT_MAX_KEYS_SCHEMA) !== DEFAULT_MAX_KEYS_SCHEMA ||
      (restoredConfig.documents_batch_size ?? DEFAULT_DOCUMENTS_BATCH_SIZE) !== DEFAULT_DOCUMENTS_BATCH_SIZE ||
      (restoredConfig.document_randomization_seed ?? DEFAULT_DOCUMENT_RANDOMIZATION_SEED) !== DEFAULT_DOCUMENT_RANDOMIZATION_SEED ||
      (restoredConfig.convergence_threshold != null && restoredConfig.convergence_threshold !== DEFAULT_CONVERGENCE_THRESHOLD) ||
      restoredConfig.initial_observation_unit ||
      (restoredConfig.schema_creation_backend &&
        ((restoredConfig.schema_creation_backend.provider ?? DEFAULT_CONFIG.schema_creation_backend.provider) !== DEFAULT_CONFIG.schema_creation_backend.provider ||
        (restoredConfig.schema_creation_backend.model ?? DEFAULT_CONFIG.schema_creation_backend.model) !== DEFAULT_CONFIG.schema_creation_backend.model ||
        (restoredConfig.schema_creation_backend.temperature ?? DEFAULT_CONFIG.schema_creation_backend.temperature) !== DEFAULT_CONFIG.schema_creation_backend.temperature)) ||
      (restoredConfig.value_extraction_backend &&
        ((restoredConfig.value_extraction_backend.provider ?? DEFAULT_CONFIG.value_extraction_backend.provider) !== DEFAULT_CONFIG.value_extraction_backend.provider ||
        (restoredConfig.value_extraction_backend.model ?? DEFAULT_CONFIG.value_extraction_backend.model) !== DEFAULT_CONFIG.value_extraction_backend.model ||
        (restoredConfig.value_extraction_backend.temperature ?? DEFAULT_CONFIG.value_extraction_backend.temperature) !== DEFAULT_CONFIG.value_extraction_backend.temperature)) ||
      restoredConfig.retriever;

    if (hasAdvancedChanges) {
      setSettingsOpen(true);
    }
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
    setSettingsOpen(false);
    setCostExpanded(false);
    setError(null);
  };

  const [config, setConfig] = useState<ScheMatiQConfig>(DEFAULT_CONFIG);

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

  // Adapter: map this screen's config + local state into the shared
  // AdvancedSettingsFields model, and route the model's changes back to the
  // existing setters/handlers so submit, restore, and cost-estimate logic
  // stay unchanged.
  const advancedValue: AdvancedSettingsValue = useMemo(() => ({
    skipValueExtraction: config.skip_value_extraction ?? false,
    maxKeysSchema: config.max_keys_schema,
    documentsBatchSize: config.documents_batch_size,
    seed: config.document_randomization_seed,
    convergenceThreshold: config.convergence_threshold ?? null,
    observationUnitMode,
    observationUnitName,
    observationUnitDefinition,
    reviewObservationUnit: config.review_observation_unit ?? false,
    schemaProvider: config.schema_creation_backend.provider,
    schemaModel: config.schema_creation_backend.model,
    schemaTemperature: config.schema_creation_backend.temperature,
    valueProvider: config.value_extraction_backend.provider,
    valueModel: config.value_extraction_backend.model,
    valueTemperature: config.value_extraction_backend.temperature,
    retrieverModelName: config.retriever?.model_name ?? RETRIEVER_DEFAULTS.model_name,
    retrieverPassageChars: config.retriever?.passage_chars ?? RETRIEVER_DEFAULTS.passage_chars,
    retrieverOverlap: config.retriever?.overlap ?? RETRIEVER_DEFAULTS.overlap,
    retrieverK: config.retriever?.k ?? RETRIEVER_DEFAULTS.k,
    retrieverDynamicK: config.retriever?.dynamic_k_threshold ?? RETRIEVER_DEFAULTS.dynamic_k_threshold,
    bypassLimit: limitBypassEnabled,
    initialSchemaPath,
    initialSchemaData,
  }), [config, observationUnitMode, observationUnitName, observationUnitDefinition, limitBypassEnabled, initialSchemaPath, initialSchemaData]);

  const applyAdvancedSettings = (patch: Partial<AdvancedSettingsValue>) => {
    if (patch.skipValueExtraction !== undefined) handleConfigChange('skip_value_extraction', patch.skipValueExtraction);
    if (patch.maxKeysSchema !== undefined) handleConfigChange('max_keys_schema', patch.maxKeysSchema);
    if (patch.documentsBatchSize !== undefined) handleConfigChange('documents_batch_size', patch.documentsBatchSize);
    if (patch.seed !== undefined) handleConfigChange('document_randomization_seed', patch.seed);
    if ('convergenceThreshold' in patch) handleConfigChange('convergence_threshold', patch.convergenceThreshold ?? undefined);
    if (patch.reviewObservationUnit !== undefined) handleConfigChange('review_observation_unit', patch.reviewObservationUnit);
    if (patch.observationUnitMode !== undefined) setObservationUnitMode(patch.observationUnitMode);
    if (patch.observationUnitName !== undefined) setObservationUnitName(patch.observationUnitName);
    if (patch.observationUnitDefinition !== undefined) setObservationUnitDefinition(patch.observationUnitDefinition);
    if (patch.schemaProvider !== undefined) handleSchemaBackendChange('provider', patch.schemaProvider);
    if (patch.schemaModel !== undefined) handleSchemaBackendChange('model', patch.schemaModel);
    if (patch.schemaTemperature !== undefined) handleSchemaBackendChange('temperature', patch.schemaTemperature);
    if (patch.valueProvider !== undefined) handleValueBackendChange('provider', patch.valueProvider);
    if (patch.valueModel !== undefined) handleValueBackendChange('model', patch.valueModel);
    if (patch.valueTemperature !== undefined) handleValueBackendChange('temperature', patch.valueTemperature);
    if (patch.retrieverModelName !== undefined) handleRetrieverChange('model_name', patch.retrieverModelName);
    if (patch.retrieverPassageChars !== undefined) handleRetrieverChange('passage_chars', patch.retrieverPassageChars);
    if (patch.retrieverOverlap !== undefined) handleRetrieverChange('overlap', patch.retrieverOverlap);
    if (patch.retrieverK !== undefined) handleRetrieverChange('k', patch.retrieverK);
    if (patch.retrieverDynamicK !== undefined) handleRetrieverChange('dynamic_k_threshold', patch.retrieverDynamicK);
    if (patch.bypassLimit !== undefined) setLimitBypassEnabled(patch.bypassLimit);
    if ('initialSchemaPath' in patch || 'initialSchemaData' in patch) {
      setInitialSchemaPath(patch.initialSchemaPath);
      setInitialSchemaData(patch.initialSchemaData ?? undefined);
    }
  };

  // InitialSchema state kept for navigation-state restoration; UI hidden for simplicity

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

        const estimate = await schematiqAPI.estimateCostPreview(config, uploadedFileInfo);
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
      const configWithKeys: ScheMatiQConfig = {
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
      const result = await schematiqAPI.configure(configWithKeys);
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

      // Step 3: Start ScheMatiQ execution
      let navState: { autoStarted?: boolean; serverBusy?: boolean; capacityMessage?: string } = {};
      try {
        await schematiqAPI.run(sessionId);
        navState = { autoStarted: true };
      } catch (runErr: any) {
        const status = runErr?.response?.status;
        const detail = runErr?.response?.data?.detail;
        if (status === 503) {
          navState = { serverBusy: true, capacityMessage: detail || 'The server is currently busy processing other requests. Please try again in a few minutes.' };
        }
      }

      // Step 4: Save config to sessionStorage for browser-back restoration
      sessionStorage.setItem(`schematiq_config_${sessionId}`, JSON.stringify({
        config: { ...config, upload_pending: uploadedFiles.length > 0 },
        previousSessionId: sessionId,
        uploadedFileNames: uploadedFiles.map(f => f.name),
      }));

      // Step 5: Navigate to visualization (replace to keep history clean during edit cycles)
      navigate(`/visualize/${sessionId}?mode=schematiq`, { replace: true, state: navState });
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to configure ScheMatiQ');
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
  const hasApiKeys = configuredProviders.length > 0 || serverHasApiKeys;
  const isFormValid = hasApiKeys && (hasQuery || hasDocuments);

  // Track whether form has been modified from defaults
  const isDirty = useMemo(() => {
    if (config.query !== '') return true;
    if (config.max_keys_schema !== DEFAULT_MAX_KEYS_SCHEMA) return true;
    if (config.documents_batch_size !== DEFAULT_DOCUMENTS_BATCH_SIZE) return true;
    if (config.document_randomization_seed !== DEFAULT_DOCUMENT_RANDOMIZATION_SEED) return true;
    if (config.convergence_threshold != null && config.convergence_threshold !== DEFAULT_CONVERGENCE_THRESHOLD) return true;
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

  // Block navigation when form has unsaved changes (not during submission)
  const blocker = useNavigationGuard(isDirty && !loading);

  // Register guard in context so the header banner also respects it
  const { registerGuard } = useNavigationGuardContext();
  useEffect(() => {
    return registerGuard(blocker.requestNavigation);
  }, [blocker.requestNavigation, registerGuard]);

  // Summary badge text for advanced settings accordion
  const advancedConfigSummary = useMemo(() => {
    const parts: string[] = [];
    if (initialSchemaData) parts.push(`${initialSchemaData.length} col${initialSchemaData.length !== 1 ? 's' : ''}`);
    else if (initialSchemaPath) parts.push('Schema file');
    if (observationUnitMode !== 'auto') parts.push('Custom unit');
    if (config.max_keys_schema !== DEFAULT_MAX_KEYS_SCHEMA ||
        config.documents_batch_size !== DEFAULT_DOCUMENTS_BATCH_SIZE ||
        config.document_randomization_seed !== DEFAULT_DOCUMENT_RANDOMIZATION_SEED ||
        (config.convergence_threshold != null && config.convergence_threshold !== DEFAULT_CONVERGENCE_THRESHOLD)) {
      parts.push('Custom params');
    }
    if (allowLlmConfig && (
      config.schema_creation_backend.provider !== DEFAULT_CONFIG.schema_creation_backend.provider ||
      config.schema_creation_backend.model !== DEFAULT_CONFIG.schema_creation_backend.model
    )) parts.push('Custom LLM');
    if (config.retriever) parts.push('Custom retriever');
    if (limitBypassEnabled) parts.push('Bypass enabled');
    return parts;
  }, [initialSchemaData, initialSchemaPath, observationUnitMode, config.max_keys_schema, config.documents_batch_size, config.document_randomization_seed, config.convergence_threshold, config.schema_creation_backend.provider, config.schema_creation_backend.model, config.retriever, allowLlmConfig, limitBypassEnabled]);

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
      <NavigationConfirmDialog
        blocker={blocker}
        title="Unsaved changes"
        description="You have unsaved changes. Are you sure you want to leave?"
      />

      {/* Hero Section */}
      <div className="text-center mb-0">
        <h1 className="text-4xl font-bold tracking-tight mb-2 bg-gradient-to-r from-primary via-blue-500 to-blue-400 bg-clip-text text-transparent">
          From Documents to Data
        </h1>
        <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
          Turn a collection of documents into structured datasets — automatically.
        </p>
      </div>

      {/* API Key Section — shown when no server keys or in developer mode */}
      {(developerMode || !serverHasApiKeys) && configuredProviders.length === 0 && !providersLoading && (
        <ApiKeySection onConfigurationChange={(providers) => {
          setConfiguredProviders(providers);
        }} />
      )}

      {/* Toolbar */}
      <div className="flex items-center justify-end mb-1">
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setWelcomeDialogOpen(true)}
            aria-label="Show welcome guide"
            className="text-muted-foreground"
          >
            <HelpCircle className="h-5 w-5" />
          </Button>
          {isDirty && (
            <Button variant="ghost" onClick={handleReset} className="text-muted-foreground">
              <RotateCcw className="mr-2 h-4 w-4" />
              Reset
            </Button>
          )}
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setSettingsOpen(true)}
            aria-label="Advanced settings"
            className="text-muted-foreground relative"
          >
            <Settings className="h-5 w-5" />
            {advancedConfigSummary.length > 0 && (
              <span className="absolute -top-0.5 -right-0.5 flex h-4 w-4 items-center justify-center rounded-full bg-primary text-[10px] text-primary-foreground font-medium">
                {advancedConfigSummary.length}
              </span>
            )}
          </Button>
        </div>
      </div>

      <div className="max-w-3xl mx-auto">
        <div className="space-y-8">
          {/* Step 1: Ask a Question */}
          <section className="space-y-3">
            <div className="flex items-center gap-3">
              <div className="flex-shrink-0 w-7 h-7 rounded-full bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-300 flex items-center justify-center text-sm font-semibold">
                1
              </div>
              <h2 className="text-xl font-semibold">Ask a Question</h2>
            </div>
            <div className="ml-10">
              <Textarea
                id="query"
                rows={3}
                value={config.query}
                onChange={(e) => handleConfigChange('query', e.target.value)}
                placeholder='e.g. "Given a protein sequence, can it be determined whether or not it contains a nuclear export signal (NES)? If it does, how strong is the NES, and what is the confidence in that assessment?"'
                className="resize-none"
                onKeyDown={(e) => {
                  if (e.key === 'Tab' && config.query === '') {
                    e.preventDefault();
                    handleConfigChange('query', 'Given a protein sequence, can it be determined whether or not it contains a nuclear export signal (NES)? If it does, how strong is the NES, and what is the confidence in that assessment?');
                  }
                }}
              />
            </div>
          </section>

          {/* Step 2: Upload Documents */}
          <section className="space-y-3">
            <div className="flex items-center gap-3">
              <div className="flex-shrink-0 w-7 h-7 rounded-full bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-300 flex items-center justify-center text-sm font-semibold">
                2
              </div>
              <h2 className="text-xl font-semibold">Upload Documents</h2>
            </div>
            <div className="ml-10 space-y-3">
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
                    Supports: .txt, .md, .pdf, .doc, .docx, .rtf, .json (max 25MB each)
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
                        selectedPaths.length <= 3
                          ? selectedPaths.join(', ')
                          : `${selectedPaths.slice(0, 2).join(', ')} +${selectedPaths.length - 2} more`
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
          </section>

          {/* Start Button */}
          <div className="pt-4 border-t flex flex-col items-center gap-2">
            <Button
              size="lg"
              className="w-full max-w-xs"
              onClick={handleStartClick}
              disabled={!isFormValid || loading || providersLoading || (documentSource === 'cloud' && datasetsLoading)}
            >
              {(loading || isUploading) ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Sparkles className="mr-2 h-4 w-4" />
              )}
              {isUploading ? 'Uploading files...' : loading ? 'Starting...' : 'Get Your Table'}
            </Button>

            {developerMode && costEstimate && !costEstimateLoading && (
              <span className="text-sm text-muted-foreground">
                Estimated: ~${costEstimate.total_cost_usd.toFixed(4)}
              </span>
            )}
          </div>

          {/* Load existing project link */}
          <p className="text-center mt-3 text-sm text-muted-foreground">
            <button
              onClick={() => navigate('/load')}
              className="text-primary hover:underline font-medium"
            >
              Or load an existing project
            </button>
          </p>

          {/* Resource Links */}
          <div className="flex items-center justify-center gap-3 flex-wrap font-['Google_Sans',sans-serif] mt-10">
            <a
              href="https://youtube.com/watch?v=VILym_Ch0hg&feature=youtu.be"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-5 py-1.5 rounded-full text-white text-sm transition-all duration-300 shadow-sm hover:shadow-lg hover:-translate-y-1"
              style={{ background: 'linear-gradient(135deg, #ff4444 0%, #cc0000 100%)' }}
            >
              <span className="flex items-center justify-center w-4 h-4">
                <i className="fa-brands fa-youtube text-sm"></i>
              </span>
              <span>Demonstration Video</span>
            </a>
            <a
              href="https://arxiv.org/pdf/2604.09237"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-5 py-1.5 rounded-full bg-[#363636] hover:bg-[#2b2b2b] text-white text-sm transition-all duration-300 shadow-sm hover:shadow-lg hover:-translate-y-1"
            >
              <span className="flex items-center justify-center w-4 h-4">
                <i className="ai ai-arxiv text-base"></i>
              </span>
              <span>arXiv</span>
            </a>
            <a
              href="https://github.com/shaharl6000/ScheMatiQ"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-5 py-1.5 rounded-full bg-[#363636] hover:bg-[#2b2b2b] text-white text-sm transition-all duration-300 shadow-sm hover:shadow-lg hover:-translate-y-1"
            >
              <span className="flex items-center justify-center w-4 h-4">
                <i className="fab fa-github text-base"></i>
              </span>
              <span>Code</span>
            </a>
            <a
              href="https://x.com/EliyaHabba/status/2043690798257250662"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-5 py-1.5 rounded-full bg-[#363636] hover:bg-[#2b2b2b] text-white text-sm transition-all duration-300 shadow-sm hover:shadow-lg hover:-translate-y-1"
            >
              <span className="flex items-center justify-center w-4 h-4">
                <i className="fa-brands fa-x-twitter text-xs"></i>
              </span>
              <span>Twitter</span>
            </a>
          </div>

          {/* How It Works */}
          <div className="text-center mt-12">
            <h2 className="text-2xl font-semibold mb-8">How It Works</h2>
            <div className="grid sm:grid-cols-3 gap-8">
              <div className="flex flex-col items-center">
                <div className="p-4 rounded-full bg-primary/10 mb-4">
                  <FileUp className="h-8 w-8 text-primary" />
                </div>
                <h3 className="font-semibold mb-2">Input</h3>
                <p className="text-sm text-muted-foreground">
                  Research question + document corpus
                </p>
              </div>
              <div className="flex flex-col items-center">
                <div className="p-4 rounded-full bg-primary/10 mb-4">
                  <Sparkles className="h-8 w-8 text-primary" />
                </div>
                <h3 className="font-semibold mb-2">Schema Discovery</h3>
                <p className="text-sm text-muted-foreground">
                  Automatically find relevant data fields
                </p>
              </div>
              <div className="flex flex-col items-center">
                <div className="p-4 rounded-full bg-primary/10 mb-4">
                  <Table className="h-8 w-8 text-primary" />
                </div>
                <h3 className="font-semibold mb-2">Structured Output</h3>
                <p className="text-sm text-muted-foreground">
                  Review, edit, and export your dataset
                </p>
              </div>
            </div>
          </div>

        </div>
      </div>

      {/* Settings Sheet */}
      <Sheet open={settingsOpen} onOpenChange={setSettingsOpen}>
        <SheetContent className="overflow-y-auto">
          <SheetHeader>
            <SheetTitle>Advanced Settings</SheetTitle>
            <SheetDescription>Fine-tune how the schema is discovered and data is extracted.</SheetDescription>
          </SheetHeader>
          <div className="space-y-4">
                <AdvancedSettingsFields
                  value={advancedValue}
                  onChange={applyAdvancedSettings}
                  developerMode={developerMode}
                  allowLlmConfig={allowLlmConfig}
                  providers={configuredProviders as LLMProviderKey[]}
                  maxDocuments={maxDocuments}
                />
          </div>

              {/* Cost Estimate - Developer mode only */}
              {developerMode && (
                <div className="mt-6 pt-4 border-t space-y-2">
                  <div className="flex items-center gap-2">
                    <DollarSign className="h-4 w-4 text-emerald-600" />
                    <span className="text-sm font-medium">
                      {costEstimateLoading ? (
                        <span className="inline-flex items-center gap-1.5">
                          <Loader2 className="h-3 w-3 animate-spin" />
                          Estimating...
                        </span>
                      ) : costEstimate ? (
                        `~$${costEstimate.total_cost_usd.toFixed(4)}`
                      ) : (
                        'Add docs for estimate'
                      )}
                    </span>
                  </div>

                  {costEstimate && (
                    <Collapsible>
                      <CollapsibleTrigger className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors">
                        <ChevronDown className="h-3 w-3" />
                        <span>View breakdown</span>
                      </CollapsibleTrigger>
                      <CollapsibleContent className="mt-2 space-y-3">
                        {/* Schema Discovery */}
                        <div className="p-2 bg-muted/30 rounded border text-xs">
                          <div className="font-medium text-muted-foreground mb-1">Schema Discovery</div>
                          <div className="space-y-0.5">
                            <div className="flex justify-between">
                              <span>API Calls:</span>
                              <span className="font-mono">{costEstimate.schema_discovery.api_calls}</span>
                            </div>
                            <div className="flex justify-between">
                              <span>Input:</span>
                              <span className="font-mono">{costEstimate.schema_discovery.input_tokens.toLocaleString()}</span>
                            </div>
                            <div className="flex justify-between">
                              <span>Output:</span>
                              <span className="font-mono">{costEstimate.schema_discovery.output_tokens.toLocaleString()}</span>
                            </div>
                            <div className="flex justify-between pt-0.5 border-t font-medium">
                              <span>Cost:</span>
                              <span className="font-mono">${costEstimate.schema_discovery.cost_usd.toFixed(4)}</span>
                            </div>
                          </div>
                        </div>

                        {/* Value Extraction */}
                        <div className={`p-2 bg-muted/30 rounded border text-xs ${config.skip_value_extraction ? 'opacity-50' : ''}`}>
                          <div className="font-medium text-muted-foreground mb-1">
                            Value Extraction
                            {config.skip_value_extraction && <Badge variant="secondary" className="ml-1 text-[10px]">Skipped</Badge>}
                          </div>
                          <div className="space-y-0.5">
                            <div className="flex justify-between">
                              <span>API Calls:</span>
                              <span className="font-mono">{costEstimate.value_extraction.api_calls}</span>
                            </div>
                            <div className="flex justify-between">
                              <span>Input:</span>
                              <span className="font-mono">{costEstimate.value_extraction.input_tokens.toLocaleString()}</span>
                            </div>
                            <div className="flex justify-between">
                              <span>Output:</span>
                              <span className="font-mono">{costEstimate.value_extraction.output_tokens.toLocaleString()}</span>
                            </div>
                            <div className="flex justify-between pt-0.5 border-t font-medium">
                              <span>Cost:</span>
                              <span className="font-mono">${costEstimate.value_extraction.cost_usd.toFixed(4)}</span>
                            </div>
                          </div>
                        </div>

                        {/* Document Stats */}
                        {costEstimate.document_stats.num_documents > 0 && (
                          <div className="text-[11px] text-muted-foreground space-y-0.5">
                            <div>{costEstimate.document_stats.num_documents} docs, ~{costEstimate.document_stats.avg_tokens_per_document.toLocaleString()} tok/doc</div>
                          </div>
                        )}

                        {/* Warnings */}
                        {costEstimate.warnings.length > 0 && (
                          <div className="space-y-1">
                            {costEstimate.warnings.map((warning, idx) => (
                              <p key={idx} className="text-[11px] text-amber-600">{warning}</p>
                            ))}
                          </div>
                        )}

                        <p className="text-[11px] text-muted-foreground italic">
                          * Estimate may vary with actual usage.
                        </p>
                      </CollapsibleContent>
                    </Collapsible>
                  )}

                  {costEstimateError && (
                    <p className="text-xs text-destructive">{costEstimateError}</p>
                  )}
                </div>
              )}
        </SheetContent>
      </Sheet>

      {error && (
        <Alert variant="destructive" className="mt-6">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
    </div>
  );
};

export default ScheMatiQConfigPage;
