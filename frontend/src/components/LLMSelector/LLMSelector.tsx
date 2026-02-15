import React, { useState, useEffect } from 'react';
import { Brain, Sparkles, Zap, AlertCircle } from 'lucide-react';

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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { LLMConfig } from '../../types';
import {
  getConfiguredProviders,
  LLMProvider,
} from '@/utils/apiKeyStorage';
import {
  LLMProviderKey,
  LLMModelDefinition,
  getModelsForProvider,
  getDefaultModelForProvider,
  getModelByProviderAndId,
  getAvailableProviders,
  getCostBadgeVariant,
  getSpeedBadgeVariant,
  formatCostLevel,
  formatSpeedLevel,
  LLM_PROVIDER_NAMES,
} from '@/constants/llmModels';
import { configAPI } from '../../services/api';


interface LLMSelectorProps {
  open: boolean;
  onClose: () => void;
  onConfirm: (config: LLMConfig) => void;
  title?: string;
  description?: string;
  preservedConfig?: LLMConfig | null;
  loading?: boolean;
  defaultModel?: string;  // Override default model for this context
}

const DEFAULT_CONFIG: LLMConfig = {
  provider: 'gemini',
  model: 'gemini-2.5-flash-lite',

  temperature: 0,
};

/**
 * Get an icon for a model based on its cost tier
 */
function getModelIcon(model: LLMModelDefinition): React.ReactNode {
  switch (model.cost) {
    case 'low':
      return <Zap className="h-4 w-4 text-green-500" />;
    case 'high':
      return <Sparkles className="h-4 w-4 text-amber-500" />;
    default:
      return <Brain className="h-4 w-4 text-blue-500" />;
  }
}

const LLMSelector: React.FC<LLMSelectorProps> = ({
  open,
  onClose,
  onConfirm,
  title = 'Select AI Model for Document Processing',
  description = 'Choose the AI model that will extract information from your uploaded documents.',
  preservedConfig,
  loading = false,
  defaultModel,
}) => {
  const [selectedConfig, setSelectedConfig] = useState<LLMConfig>(DEFAULT_CONFIG);
  const [usePreservedConfig, setUsePreservedConfig] = useState(false);
  const [configuredProviders, setConfiguredProviders] = useState<LLMProviderKey[]>([]);
  const [selectedProvider, setSelectedProvider] = useState<LLMProviderKey>('gemini');
  const [providersLoading, setProvidersLoading] = useState(true);
  const [serverHasApiKeys, setServerHasApiKeys] = useState(false);

  // Load configured providers when dialog opens
  useEffect(() => {
    const loadProviders = async () => {
      if (!open) return;

      setProvidersLoading(true);
      const providers = await getConfiguredProviders();
      const cfg = await configAPI.getConfig().catch(() => ({ server_has_api_keys: false }));
      setServerHasApiKeys(cfg.server_has_api_keys ?? false);

      // Filter to only providers with models
      const availableProviders = getAvailableProviders(providers);
      setConfiguredProviders(availableProviders);

      // Set initial provider
      if (preservedConfig?.provider && availableProviders.includes(preservedConfig.provider as LLMProviderKey)) {
        setSelectedProvider(preservedConfig.provider as LLMProviderKey);
        setSelectedConfig(preservedConfig);
        setUsePreservedConfig(true);
      } else if (availableProviders.length > 0) {
        const defaultProvider = availableProviders[0];
        setSelectedProvider(defaultProvider);
        // Use passed defaultModel if provided, otherwise use provider default
        const modelToUse = defaultModel || getDefaultModelForProvider(defaultProvider);
        setSelectedConfig({
          provider: defaultProvider,
          model: modelToUse,
        
          temperature: 0,
        });
        setUsePreservedConfig(false);
      } else if (cfg.server_has_api_keys) {
        // No client-side keys but server has keys — default to gemini
        setSelectedProvider('gemini');
        const modelToUse = defaultModel || getDefaultModelForProvider('gemini');
        setSelectedConfig({
          provider: 'gemini',
          model: modelToUse,
          max_output_tokens: 1024,
          temperature: 0,
        });
        setUsePreservedConfig(false);
      }

      setProvidersLoading(false);
    };

    loadProviders();
  }, [open, preservedConfig]);

  const handleProviderChange = (provider: LLMProviderKey) => {
    setSelectedProvider(provider);
    const defaultModel = getDefaultModelForProvider(provider);
    setSelectedConfig({
      provider,
      model: defaultModel,
    
      temperature: 0,
    });
    setUsePreservedConfig(false);
  };

  const handleModelChange = (modelId: string) => {
    setSelectedConfig({
      ...selectedConfig,
      provider: selectedProvider,
      model: modelId,
    });
    setUsePreservedConfig(false);
  };

  const handleConfirm = () => {
    onConfirm(selectedConfig);
  };

  const availableModels = getModelsForProvider(selectedProvider);
  const selectedModel = getModelByProviderAndId(selectedProvider, selectedConfig.model);

  return (
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Brain className="h-5 w-5 text-primary" />
            {title}
          </DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          {/* Preserved Config Alert */}
          {preservedConfig && (
            <Alert variant="info">
              <AlertDescription className="flex items-center justify-between">
                <span>
                  <strong>Original Configuration:</strong> {preservedConfig.provider} {preservedConfig.model}
                  {usePreservedConfig && ' (using for consistency)'}
                </span>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    if (usePreservedConfig) {
                      // Switch to selecting a new model
                      setUsePreservedConfig(false);
                    } else {
                      // Switch back to preserved config
                      if (preservedConfig.provider) {
                        setSelectedProvider(preservedConfig.provider as LLMProviderKey);
                        setSelectedConfig(preservedConfig);
                        setUsePreservedConfig(true);
                      }
                    }
                  }}
                >
                  {usePreservedConfig ? 'Change Model' : 'Use Original'}
                </Button>
              </AlertDescription>
            </Alert>
          )}

          {/* No Providers Warning */}
          {!providersLoading && configuredProviders.length === 0 && !serverHasApiKeys && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>
                No API keys configured. Please add an API key on the home page.
              </AlertDescription>
            </Alert>
          )}

          {/* Provider Selection */}
          <div className="space-y-2">
            <Label>Provider</Label>
            <Select
              value={selectedProvider}
              onValueChange={(value) => handleProviderChange(value as LLMProviderKey)}
              disabled={usePreservedConfig || (configuredProviders.length === 0 && !serverHasApiKeys) || providersLoading}
            >
              <SelectTrigger>
                <SelectValue placeholder={providersLoading ? "Loading..." : "Select provider"} />
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

          {/* Model Selection */}
          <div className="space-y-2">
            <Label>Model</Label>
            <Select
              value={selectedConfig.model}
              onValueChange={handleModelChange}
              disabled={usePreservedConfig || availableModels.length === 0 || providersLoading}
            >
              <SelectTrigger>
                <SelectValue placeholder="Select model">
                  {selectedModel && (
                    <div className="flex items-center gap-2">
                      {getModelIcon(selectedModel)}
                      <span>{selectedModel.label}</span>
                    </div>
                  )}
                </SelectValue>
              </SelectTrigger>
              <SelectContent>
                {availableModels.map((model) => (
                  <SelectItem key={model.id} value={model.id}>
                    <div className="flex items-center gap-2">
                      {getModelIcon(model)}
                      <span>{model.label}</span>
                      {model.isDefault && (
                        <Badge variant="outline" className="ml-1 text-xs">
                          default
                        </Badge>
                      )}
                    </div>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {!usePreservedConfig && selectedModel && (
              <p className="text-sm text-muted-foreground">
                {selectedModel.description}
              </p>
            )}
          </div>

          {/* Model Details */}
          {selectedModel && (
            <>
              <Separator />
              <div className="space-y-3">
                <h4 className="text-sm font-medium">Model Details</h4>
                <div className="flex gap-4 text-sm">
                  <span><strong>Provider:</strong> {LLM_PROVIDER_NAMES[selectedProvider]}</span>
                  <span><strong>Model:</strong> {selectedConfig.model}</span>
                </div>
                <div className="flex gap-2">
                  <Badge variant={getCostBadgeVariant(selectedModel.cost)}>
                    Cost: {formatCostLevel(selectedModel.cost)}
                  </Badge>
                  <Badge variant={getSpeedBadgeVariant(selectedModel.speed)}>
                    Speed: {formatSpeedLevel(selectedModel.speed)}
                  </Badge>
                </div>

                {preservedConfig && !usePreservedConfig && (
                  <Alert variant="warning">
                    <AlertDescription>
                      You're choosing a different model than originally used. Results may vary from the original extraction.
                    </AlertDescription>
                  </Alert>
                )}
              </div>
            </>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={loading}>
            Cancel
          </Button>
          <Button
            onClick={handleConfirm}
            disabled={loading || (configuredProviders.length === 0 && !serverHasApiKeys) || !selectedModel}
          >
            {selectedModel && getModelIcon(selectedModel)}
            <span className="ml-2">{loading ? 'Starting...' : 'Start Processing'}</span>
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default LLMSelector;
