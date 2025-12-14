import React, { useState, useEffect } from 'react';
import { Brain, Sparkles, Zap } from 'lucide-react';

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

interface LLMSelectorProps {
  open: boolean;
  onClose: () => void;
  onConfirm: (config: LLMConfig) => void;
  title?: string;
  description?: string;
  preservedConfig?: LLMConfig | null;
  loading?: boolean;
}

const LLM_OPTIONS = [
  {
    provider: 'gemini',
    model: 'gemini-2.5-flash',
    label: 'Gemini 2.5 Flash',
    description: 'Balanced performance and accuracy',
    icon: <Brain className="h-4 w-4" />,
    recommended: 'schema_creation',
    cost: 'medium',
    speed: 'fast',
  },
  {
    provider: 'gemini',
    model: 'gemini-2.5-flash-lite',
    label: 'Gemini 2.5 Flash Lite',
    description: 'Fast and cost-effective',
    icon: <Zap className="h-4 w-4" />,
    recommended: 'value_extraction',
    cost: 'low',
    speed: 'very_fast',
  },
  {
    provider: 'gemini',
    model: 'gemini-1.5-pro',
    label: 'Gemini 1.5 Pro',
    description: 'High accuracy for complex tasks',
    icon: <Sparkles className="h-4 w-4" />,
    recommended: 'complex_extraction',
    cost: 'high',
    speed: 'moderate',
  },
];

const DEFAULT_CONFIG: LLMConfig = {
  provider: 'gemini',
  model: 'gemini-2.5-flash-lite',
  max_output_tokens: 1024,
  temperature: 0.3,
};

const LLMSelector: React.FC<LLMSelectorProps> = ({
  open,
  onClose,
  onConfirm,
  title = 'Select AI Model for Document Processing',
  description = 'Choose the AI model that will extract information from your uploaded documents.',
  preservedConfig,
  loading = false,
}) => {
  const [selectedConfig, setSelectedConfig] = useState<LLMConfig>(DEFAULT_CONFIG);
  const [usePreservedConfig, setUsePreservedConfig] = useState(false);

  useEffect(() => {
    if (preservedConfig) {
      setSelectedConfig(preservedConfig);
      setUsePreservedConfig(true);
    } else {
      setSelectedConfig(DEFAULT_CONFIG);
      setUsePreservedConfig(false);
    }
  }, [preservedConfig]);

  const handleModelChange = (value: string) => {
    const [provider, ...modelParts] = value.split('-');
    const model = modelParts.join('-');
    const option = LLM_OPTIONS.find(opt => opt.provider === provider && opt.model === model);
    if (option) {
      setSelectedConfig({
        provider,
        model,
        max_output_tokens: 1024,
        temperature: 0.3,
      });
      setUsePreservedConfig(false);
    }
  };

  const handleConfirm = () => {
    onConfirm(selectedConfig);
  };

  const getSelectedOption = () => {
    return LLM_OPTIONS.find(
      opt => opt.provider === selectedConfig.provider && opt.model === selectedConfig.model
    );
  };

  const getCostVariant = (cost: string): "default" | "secondary" | "destructive" | "outline" | "success" | "warning" | "info" => {
    switch (cost) {
      case 'low': return 'success';
      case 'medium': return 'warning';
      case 'high': return 'destructive';
      default: return 'secondary';
    }
  };

  const getSpeedVariant = (speed: string): "default" | "secondary" | "destructive" | "outline" | "success" | "warning" | "info" => {
    switch (speed) {
      case 'very_fast': return 'success';
      case 'fast': return 'info';
      case 'moderate': return 'warning';
      default: return 'secondary';
    }
  };

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
                  onClick={() => setUsePreservedConfig(!usePreservedConfig)}
                >
                  {usePreservedConfig ? 'Change Model' : 'Use Original'}
                </Button>
              </AlertDescription>
            </Alert>
          )}

          <div className="space-y-2">
            <Label>AI Model</Label>
            <Select
              value={`${selectedConfig.provider}-${selectedConfig.model}`}
              onValueChange={handleModelChange}
              disabled={usePreservedConfig}
            >
              <SelectTrigger>
                <SelectValue placeholder="Select a model" />
              </SelectTrigger>
              <SelectContent>
                {LLM_OPTIONS.map((option) => (
                  <SelectItem
                    key={`${option.provider}-${option.model}`}
                    value={`${option.provider}-${option.model}`}
                  >
                    <div className="flex items-center gap-2">
                      {option.icon}
                      <span>{option.label}</span>
                    </div>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {!usePreservedConfig && getSelectedOption() && (
              <p className="text-sm text-muted-foreground">
                {getSelectedOption()?.description}
              </p>
            )}
          </div>

          {getSelectedOption() && (
            <>
              <Separator />
              <div className="space-y-3">
                <h4 className="text-sm font-medium">Model Details</h4>
                <div className="flex gap-4 text-sm">
                  <span><strong>Provider:</strong> {selectedConfig.provider}</span>
                  <span><strong>Model:</strong> {selectedConfig.model}</span>
                </div>
                <div className="flex gap-2">
                  <Badge variant={getCostVariant(getSelectedOption()?.cost || 'medium')}>
                    Cost: {getSelectedOption()?.cost}
                  </Badge>
                  <Badge variant={getSpeedVariant(getSelectedOption()?.speed || 'moderate')}>
                    Speed: {getSelectedOption()?.speed}
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
          <Button onClick={handleConfirm} disabled={loading}>
            {getSelectedOption()?.icon}
            <span className="ml-2">{loading ? 'Starting...' : 'Start Processing'}</span>
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default LLMSelector;
