import { useState, useEffect } from 'react';
import { Eye, EyeOff, Key, CheckCircle } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import {
  encryptAndStore,
  retrieveAndDecrypt,
} from '@/utils/apiKeyStorage';

interface ApiKeyInputProps {
  provider: string;
  value: string;
  onChange: (value: string) => void;
}

export const ApiKeyInput = ({
  provider,
  value,
  onChange,
}: ApiKeyInputProps) => {
  const [showKey, setShowKey] = useState(false);
  const [isSaved, setIsSaved] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  // Load saved key on mount and when provider changes
  useEffect(() => {
    const loadSavedKey = async () => {
      setIsLoading(true);
      const savedKey = await retrieveAndDecrypt(provider);
      if (savedKey) {
        onChange(savedKey);
        setIsSaved(true);
      } else {
        // Clear the value if no saved key for this provider
        onChange('');
        setIsSaved(false);
      }
      setIsLoading(false);
    };
    loadSavedKey();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [provider]);

  // Save key when it changes (or clear from storage if empty)
  const handleKeyChange = async (newValue: string) => {
    onChange(newValue);
    // encryptAndStore handles empty values by clearing the stored key
    await encryptAndStore(provider, newValue);
    setIsSaved(!!newValue);
  };

  const getPlaceholder = () => {
    const placeholders: Record<string, string> = {
      openai: 'sk-...',
      together: 'Enter your Together AI API key',
      gemini: 'Enter your Gemini API key',
    };
    return placeholders[provider] || `Enter your ${provider} API key`;
  };

  const getLabel = () => {
    const labels: Record<string, string> = {
      openai: 'OpenAI API Key',
      together: 'Together AI API Key',
      gemini: 'Gemini API Key',
    };
    return labels[provider] || `${provider} API Key`;
  };

  if (isLoading) {
    return (
      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <Key className="h-4 w-4 text-muted-foreground" />
          <div className="h-4 w-32 bg-muted animate-pulse rounded" />
        </div>
        <div className="h-10 bg-muted animate-pulse rounded-md" />
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <Label className="flex items-center gap-2 text-sm font-medium">
          <Key className="h-4 w-4" />
          {getLabel()}
        </Label>
        {isSaved && value && (
          <span className="text-xs text-green-600 dark:text-green-400 flex items-center gap-1">
            <CheckCircle className="h-3 w-3" />
            Saved locally
          </span>
        )}
      </div>

      <div className="relative">
        <Input
          type={showKey ? 'text' : 'password'}
          value={value}
          onChange={(e) => handleKeyChange(e.target.value)}
          placeholder={getPlaceholder()}
          className="pr-10"
        />
        <Button
          type="button"
          variant="ghost"
          size="sm"
          className="absolute right-1 top-1/2 -translate-y-1/2 h-8 w-8 p-0"
          onClick={() => setShowKey(!showKey)}
        >
          {showKey ? (
            <EyeOff className="h-4 w-4 text-muted-foreground" />
          ) : (
            <Eye className="h-4 w-4 text-muted-foreground" />
          )}
        </Button>
      </div>

      <p className="text-xs text-muted-foreground">
        {value
          ? 'Key stored encrypted in browser. Falls back to server env if empty.'
          : 'Optional - will use server environment variable if not provided.'}
      </p>
    </div>
  );
};

export default ApiKeyInput;
