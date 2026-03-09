/**
 * Centralized LLM model definitions and helper functions
 *
 * To add a new provider or model, simply update the LLM_MODELS array below.
 * The rest of the application will automatically pick up the changes.
 */

// Type definitions
export type LLMProviderKey = 'openai' | 'gemini' | 'together';
export type CostLevel = 'low' | 'medium' | 'high';
export type SpeedLevel = 'very_fast' | 'fast' | 'moderate' | 'slow';

export interface LLMModelDefinition {
  id: string;                    // Model identifier used in API calls
  provider: LLMProviderKey;      // Provider key
  label: string;                 // Human-readable display name
  description: string;           // Short description of the model
  cost: CostLevel;               // Cost tier
  speed: SpeedLevel;             // Speed tier
  isDefault?: boolean;           // Default model for this provider
  contextWindow?: number;        // Context window size (optional)
}

// Provider display names
export const LLM_PROVIDER_NAMES: Record<LLMProviderKey, string> = {
  openai: 'OpenAI',
  gemini: 'Google Gemini',
  together: 'Together AI',
};

/**
 * All available LLM models organized by provider
 *
 * To add new models:
 * 1. Add the model definition to this array
 * 2. Set isDefault: true for the recommended default model per provider
 */
export const LLM_MODELS: LLMModelDefinition[] = [
  // OpenAI Models
  {
    id: 'gpt-4.1',
    provider: 'openai',
    label: 'GPT-4.1',
    description: 'Flagship model with best performance',
    cost: 'high',
    speed: 'moderate',
    isDefault: true,
  },
  {
    id: 'gpt-4.1-mini',
    provider: 'openai',
    label: 'GPT-4.1 Mini',
    description: 'Smaller, faster, 83% cheaper than GPT-4o',
    cost: 'medium',
    speed: 'fast',
  },
  {
    id: 'gpt-4.1-nano',
    provider: 'openai',
    label: 'GPT-4.1 Nano',
    description: 'Fastest and most cost-effective',
    cost: 'low',
    speed: 'very_fast',
  },
  {
    id: 'gpt-4o-mini',
    provider: 'openai',
    label: 'GPT-4o Mini',
    description: 'Older but reliable option',
    cost: 'low',
    speed: 'fast',
  },

  // Gemini Models
  {
    id: 'gemini-2.5-flash',
    provider: 'gemini',
    label: 'Gemini 2.5 Flash',
    description: 'Best price-performance ratio',
    cost: 'medium',
    speed: 'fast',
    isDefault: true,
    contextWindow: 1000000,
  },
  {
    id: 'gemini-2.5-flash-lite',
    provider: 'gemini',
    label: 'Gemini 2.5 Flash Lite',
    description: 'Cost-efficient, high throughput',
    cost: 'low',
    speed: 'very_fast',
    contextWindow: 1000000,
  },
  {
    id: 'gemini-2.5-pro',
    provider: 'gemini',
    label: 'Gemini 2.5 Pro',
    description: 'Most capable Gemini 2.5 model',
    cost: 'high',
    speed: 'moderate',
    contextWindow: 1000000,
  },
  {
    id: 'gemini-3-flash-preview',
    provider: 'gemini',
    label: 'Gemini 3 Flash (Preview)',
    description: 'Fast frontier-class performance',
    cost: 'medium',
    speed: 'fast',
    contextWindow: 1000000,
  },
  {
    id: 'gemini-3-pro-preview',
    provider: 'gemini',
    label: 'Gemini 3 Pro (Preview)',
    description: 'Most intelligent reasoning model',
    cost: 'high',
    speed: 'moderate',
    contextWindow: 1000000,
  },
  {
    id: 'gemini-3.1-flash-lite-preview',
    provider: 'gemini',
    label: 'Gemini 3.1 Flash Lite (Preview)',
    description: '2.5x faster than 2.5 Flash, best value',
    cost: 'low',
    speed: 'very_fast',
    contextWindow: 1000000,
  },
  {
    id: 'gemini-3.1-pro-preview',
    provider: 'gemini',
    label: 'Gemini 3.1 Pro (Preview)',
    description: 'Most capable reasoning model',
    cost: 'high',
    speed: 'moderate',
    contextWindow: 1000000,
  },
  // Together AI Models
  // Add models here when available
];

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Get all models for a specific provider
 */
export function getModelsForProvider(provider: LLMProviderKey): LLMModelDefinition[] {
  return LLM_MODELS.filter(m => m.provider === provider);
}

/**
 * Get the default model ID for a provider
 * Returns the model marked as isDefault, or the first model if none is marked
 */
export function getDefaultModelForProvider(provider: LLMProviderKey): string {
  const models = getModelsForProvider(provider);
  const defaultModel = models.find(m => m.isDefault);
  return defaultModel?.id || models[0]?.id || '';
}

/**
 * Find a model by its ID (across all providers)
 */
export function getModelById(modelId: string): LLMModelDefinition | undefined {
  return LLM_MODELS.find(m => m.id === modelId);
}

/**
 * Find a model by provider and model ID
 */
export function getModelByProviderAndId(
  provider: LLMProviderKey,
  modelId: string
): LLMModelDefinition | undefined {
  return LLM_MODELS.find(m => m.provider === provider && m.id === modelId);
}

/**
 * Check if a provider has any models defined
 */
export function hasModelsForProvider(provider: LLMProviderKey): boolean {
  return getModelsForProvider(provider).length > 0;
}

/**
 * Filter configured providers to only those with available models
 * Use this to hide providers like Together AI that don't have models in the allowed list
 */
export function getAvailableProviders(
  configuredProviders: string[]
): LLMProviderKey[] {
  return configuredProviders.filter(
    p => hasModelsForProvider(p as LLMProviderKey)
  ) as LLMProviderKey[];
}

// ============================================================================
// UI Helper Functions
// ============================================================================

/**
 * Get badge variant for cost level
 */
export function getCostBadgeVariant(cost: CostLevel): 'success' | 'warning' | 'destructive' | 'secondary' {
  switch (cost) {
    case 'low': return 'success';
    case 'medium': return 'warning';
    case 'high': return 'destructive';
    default: return 'secondary';
  }
}

/**
 * Get badge variant for speed level
 */
export function getSpeedBadgeVariant(speed: SpeedLevel): 'success' | 'info' | 'warning' | 'secondary' {
  switch (speed) {
    case 'very_fast': return 'success';
    case 'fast': return 'info';
    case 'moderate': return 'warning';
    case 'slow': return 'warning';
    default: return 'secondary';
  }
}

/**
 * Format cost level for display
 */
export function formatCostLevel(cost: CostLevel): string {
  return cost.charAt(0).toUpperCase() + cost.slice(1);
}

/**
 * Format speed level for display
 */
export function formatSpeedLevel(speed: SpeedLevel): string {
  return speed.split('_').map(word =>
    word.charAt(0).toUpperCase() + word.slice(1)
  ).join(' ');
}
