import React from 'react';
import { Brain, Zap, HardDrive } from 'lucide-react';

import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

// Temperature is deliberately not displayed. It is not user-configurable (every
// call site sends 0), and Gemini 3.x ignores the parameter outright, so showing
// "Temp: 0" next to "0.0 = deterministic" told users their runs were
// reproducible when they are not.
// See https://ai.google.dev/gemini-api/docs/latest-model
interface LLMConfigDisplayProps {
  config: {
    provider?: string;
    model?: string;
    max_output_tokens?: number;
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
