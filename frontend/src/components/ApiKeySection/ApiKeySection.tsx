import { useState, useEffect, useCallback } from 'react';
import { Key, ChevronDown, ChevronUp, AlertTriangle } from 'lucide-react';
import { ApiKeyInput } from '@/components/ApiKeyInput';
import {
  getConfiguredProviders,
  migrateGeminiKeys,
  LLMProvider,
} from '@/utils/apiKeyStorage';
import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible';

interface ApiKeySectionProps {
  onConfigurationChange?: (configuredProviders: LLMProvider[]) => void;
}

export const ApiKeySection = ({ onConfigurationChange }: ApiKeySectionProps) => {
  // State for each provider's key
  const [openaiKey, setOpenaiKey] = useState('');
  const [togetherKey, setTogetherKey] = useState('');
  const [geminiKey, setGeminiKey] = useState('');

  // Track configured providers
  const [configuredProviders, setConfiguredProviders] = useState<LLMProvider[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isExpanded, setIsExpanded] = useState(false);

  // Load initial state and migrate old Gemini keys
  useEffect(() => {
    const loadState = async () => {
      setIsLoading(true);

      // Migrate old Gemini multi-key storage format
      await migrateGeminiKeys();

      const providers = await getConfiguredProviders();
      setConfiguredProviders(providers);

      // Always start collapsed
      setIsExpanded(false);

      setIsLoading(false);
    };
    loadState();
  }, []);

  // Update configured providers when keys change
  const updateConfiguredProviders = useCallback(async () => {
    const providers = await getConfiguredProviders();
    setConfiguredProviders(providers);
    onConfigurationChange?.(providers);
  }, [onConfigurationChange]);

  useEffect(() => {
    // Small delay to allow storage to update
    const timeout = setTimeout(updateConfiguredProviders, 100);
    return () => clearTimeout(timeout);
  }, [openaiKey, togetherKey, geminiKey, updateConfiguredProviders]);

  const configuredCount = configuredProviders.length;

  const getBadgeVariant = () => {
    if (configuredCount === 0) return 'destructive';
    if (configuredCount === 3) return 'success';
    return 'info';
  };

  const getBadgeText = () => {
    if (configuredCount === 0) return 'None configured';
    return `${configuredCount} configured`;
  };

  if (isLoading) {
    return (
      <Card className="mb-6">
        <div className="flex items-center justify-between px-4 py-3">
          <div className="flex items-center gap-2">
            <Key className="h-4 w-4 text-muted-foreground" />
            <div className="h-4 w-20 bg-muted animate-pulse rounded" />
          </div>
          <div className="h-4 w-20 bg-muted animate-pulse rounded" />
        </div>
      </Card>
    );
  }

  const hasNoKeys = configuredCount === 0;

  return (
    <Card className={`mb-6 ${hasNoKeys ? 'border-amber-500/50' : ''}`}>
      <Collapsible open={isExpanded} onOpenChange={setIsExpanded}>
        <CollapsibleTrigger asChild>
          <Button
            variant="ghost"
            className="w-full flex items-center justify-between px-4 py-3 h-auto hover:bg-muted/50"
          >
            <div className="flex items-center gap-2">
              {hasNoKeys ? (
                <AlertTriangle className="h-4 w-4 text-amber-500" />
              ) : (
                <Key className="h-4 w-4 text-muted-foreground" />
              )}
              <span className="text-sm font-medium">API Keys</span>
              {hasNoKeys && (
                <span className="text-xs text-amber-600 dark:text-amber-400">
                  (required for AI features)
                </span>
              )}
            </div>
            <div className="flex items-center gap-2">
              <Badge variant={getBadgeVariant()} className="text-xs">{getBadgeText()}</Badge>
              {isExpanded ? (
                <ChevronUp className="h-4 w-4 text-muted-foreground" />
              ) : (
                <ChevronDown className="h-4 w-4 text-muted-foreground" />
              )}
            </div>
          </Button>
        </CollapsibleTrigger>

        <CollapsibleContent>
          <CardContent className="pt-0">
            <div className="grid md:grid-cols-2 gap-6">
              {/* OpenAI */}
              <div className="space-y-1">
                <ApiKeyInput
                  provider="openai"
                  value={openaiKey}
                  onChange={setOpenaiKey}
                />
              </div>

              {/* Together AI */}
              <div className="space-y-1">
                <ApiKeyInput
                  provider="together"
                  value={togetherKey}
                  onChange={setTogetherKey}
                />
              </div>

              {/* Gemini - full width */}
              <div className="md:col-span-2 space-y-1">
                <ApiKeyInput
                  provider="gemini"
                  value={geminiKey}
                  onChange={setGeminiKey}
                />
              </div>
            </div>

            <p className="text-xs text-muted-foreground mt-4 pt-4 border-t">
              Keys are encrypted and stored in your browser. If no keys are provided,
              the server will attempt to use its environment variables.
            </p>
          </CardContent>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  );
};

export default ApiKeySection;
