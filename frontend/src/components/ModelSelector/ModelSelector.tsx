import React from 'react';
import { Brain, Sparkles, Zap } from 'lucide-react';

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import {
  LLMProviderKey,
  LLMModelDefinition,
  getModelsForProvider,
  getModelByProviderAndId,
  getCostBadgeVariant,
  getSpeedBadgeVariant,
  formatCostLevel,
  formatSpeedLevel,
} from '@/constants/llmModels';

interface ModelSelectorProps {
  provider: LLMProviderKey;
  value: string;
  onChange: (modelId: string) => void;
  disabled?: boolean;
  showDetails?: boolean;
  placeholder?: string;
}

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

/**
 * Reusable model selector dropdown component
 *
 * Shows models filtered by the selected provider with cost/speed badges
 */
export const ModelSelector: React.FC<ModelSelectorProps> = ({
  provider,
  value,
  onChange,
  disabled = false,
  showDetails = true,
  placeholder = 'Select a model',
}) => {
  const models = getModelsForProvider(provider);
  const selectedModel = getModelByProviderAndId(provider, value);

  if (models.length === 0) {
    return (
      <Select disabled>
        <SelectTrigger>
          <SelectValue placeholder="No models available for this provider" />
        </SelectTrigger>
      </Select>
    );
  }

  return (
    <div className="space-y-2">
      <Select value={value} onValueChange={onChange} disabled={disabled}>
        <SelectTrigger>
          <SelectValue placeholder={placeholder}>
            {selectedModel && (
              <div className="flex items-center gap-2">
                {getModelIcon(selectedModel)}
                <span>{selectedModel.label}</span>
              </div>
            )}
          </SelectValue>
        </SelectTrigger>
        <SelectContent>
          {models.map((model) => (
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

      {showDetails && selectedModel && (
        <>
          <div className="flex gap-2">
            <Badge variant={getCostBadgeVariant(selectedModel.cost)}>
              Cost: {formatCostLevel(selectedModel.cost)}
            </Badge>
            <Badge variant={getSpeedBadgeVariant(selectedModel.speed)}>
              Speed: {formatSpeedLevel(selectedModel.speed)}
            </Badge>
          </div>
          <p className="text-sm text-muted-foreground">
            {selectedModel.description}
          </p>
        </>
      )}
    </div>
  );
};

export default ModelSelector;
