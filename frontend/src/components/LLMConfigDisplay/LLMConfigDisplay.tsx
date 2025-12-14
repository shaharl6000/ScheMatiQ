import React from 'react';
import { Brain, Zap, HardDrive, Thermometer } from 'lucide-react';

import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

interface LLMConfigDisplayProps {
  config: {
    provider?: string;
    model?: string;
    max_output_tokens?: number;
    temperature?: number;
  } | null;
  title?: string;
  variant?: 'card' | 'inline' | 'compact';
  showDetails?: boolean;
}

const LLMConfigDisplay: React.FC<LLMConfigDisplayProps> = ({
  config,
  title = "AI Model Configuration",
  variant = 'card',
  showDetails = true,
}) => {
  if (!config) {
    return null;
  }

  const getProviderIcon = (provider: string) => {
    switch (provider?.toLowerCase()) {
      case 'gemini':
        return <Brain className="h-5 w-5 text-primary" />;
      case 'openai':
        return <Brain className="h-5 w-5 text-green-500" />;
      default:
        return <Brain className="h-5 w-5 text-muted-foreground" />;
    }
  };

  const getModelDisplayName = (provider: string, model: string) => {
    if (provider?.toLowerCase() === 'gemini') {
      return model?.replace('gemini-', 'Gemini ') || model;
    }
    return model;
  };

  const getCostLevel = (model: string) => {
    if (model?.includes('lite') || model?.includes('mini')) return 'Low';
    if (model?.includes('pro') || model?.includes('4')) return 'High';
    return 'Medium';
  };

  const getCostVariant = (level: string): "default" | "secondary" | "destructive" | "outline" | "success" | "warning" | "info" => {
    switch (level.toLowerCase()) {
      case 'low': return 'success';
      case 'medium': return 'warning';
      case 'high': return 'destructive';
      default: return 'secondary';
    }
  };

  const renderContent = () => (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        {getProviderIcon(config.provider || '')}
        <span className={variant === 'compact' ? 'text-sm font-medium' : 'font-semibold'}>
          {getModelDisplayName(config.provider || '', config.model || 'Unknown Model')}
        </span>
      </div>

      {showDetails && (
        <div className="flex flex-wrap gap-2">
          <Badge variant="outline" className="gap-1">
            <Brain className="h-3 w-3" />
            {config.provider || 'Unknown'}
          </Badge>

          {config.max_output_tokens && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Badge variant="outline" className="gap-1 cursor-help">
                  <HardDrive className="h-3 w-3" />
                  {config.max_output_tokens} output tokens
                </Badge>
              </TooltipTrigger>
              <TooltipContent>
                Maximum tokens the model can generate in its response
              </TooltipContent>
            </Tooltip>
          )}

          {config.temperature !== undefined && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Badge variant="outline" className="gap-1 cursor-help">
                  <Thermometer className="h-3 w-3" />
                  Temp: {config.temperature}
                </Badge>
              </TooltipTrigger>
              <TooltipContent>
                Temperature controls randomness (0.0 = deterministic, 1.0 = very random)
              </TooltipContent>
            </Tooltip>
          )}

          <Badge variant={getCostVariant(getCostLevel(config.model || ''))} className="gap-1">
            <Zap className="h-3 w-3" />
            Cost: {getCostLevel(config.model || '')}
          </Badge>
        </div>
      )}

      {!showDetails && variant === 'compact' && (
        <p className="text-xs text-muted-foreground">
          {config.provider} • {getCostLevel(config.model || '')} Cost
        </p>
      )}
    </div>
  );

  if (variant === 'card') {
    return (
      <Card>
        <CardContent className="pt-6">
          <h4 className="font-semibold mb-3">{title}</h4>
          {renderContent()}
        </CardContent>
      </Card>
    );
  }

  if (variant === 'inline') {
    return (
      <div>
        <p className="text-sm text-muted-foreground mb-2">{title}</p>
        {renderContent()}
      </div>
    );
  }

  // Compact variant
  return <div>{renderContent()}</div>;
};

export default LLMConfigDisplay;
