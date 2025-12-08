import React from 'react';
import {
  Box,
  Typography,
  Chip,
  Tooltip,
  Card,
  CardContent,
  Stack,
} from '@mui/material';
import { Psychology, Speed, Memory, Thermostat } from '@mui/icons-material';

interface LLMConfigDisplayProps {
  config: {
    provider?: string;
    model?: string;
    max_tokens?: number;
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
        return <Psychology color="primary" />;
      case 'openai':
        return <Psychology color="secondary" />;
      default:
        return <Psychology />;
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

  const getCostColor = (level: string) => {
    switch (level.toLowerCase()) {
      case 'low': return 'success';
      case 'medium': return 'warning';
      case 'high': return 'error';
      default: return 'default';
    }
  };

  const renderContent = () => (
    <Stack spacing={showDetails ? 2 : 1}>
      <Box display="flex" alignItems="center" gap={1}>
        {getProviderIcon(config.provider || '')}
        <Typography variant={variant === 'compact' ? 'body2' : 'subtitle1'} fontWeight="medium">
          {getModelDisplayName(config.provider || '', config.model || 'Unknown Model')}
        </Typography>
      </Box>

      {showDetails && (
        <Stack direction="row" spacing={1} flexWrap="wrap">
          <Chip
            size="small"
            icon={<Psychology />}
            label={`Provider: ${config.provider || 'Unknown'}`}
            variant="outlined"
          />
          
          {config.max_tokens && (
            <Tooltip title="Maximum tokens per request">
              <Chip
                size="small"
                icon={<Memory />}
                label={`${config.max_tokens} tokens`}
                variant="outlined"
              />
            </Tooltip>
          )}
          
          {config.temperature !== undefined && (
            <Tooltip title="Temperature controls randomness (0.0 = deterministic, 1.0 = very random)">
              <Chip
                size="small"
                icon={<Thermostat />}
                label={`Temp: ${config.temperature}`}
                variant="outlined"
              />
            </Tooltip>
          )}
          
          <Chip
            size="small"
            icon={<Speed />}
            label={`Cost: ${getCostLevel(config.model || '')}`}
            color={getCostColor(getCostLevel(config.model || ''))}
            variant="outlined"
          />
        </Stack>
      )}
      
      {!showDetails && variant === 'compact' && (
        <Typography variant="caption" color="text.secondary">
          {config.provider} • {getCostLevel(config.model || '')} Cost
        </Typography>
      )}
    </Stack>
  );

  if (variant === 'card') {
    return (
      <Card variant="outlined">
        <CardContent>
          <Typography variant="h6" gutterBottom>
            {title}
          </Typography>
          {renderContent()}
        </CardContent>
      </Card>
    );
  }

  if (variant === 'inline') {
    return (
      <Box>
        <Typography variant="subtitle2" gutterBottom color="text.secondary">
          {title}
        </Typography>
        {renderContent()}
      </Box>
    );
  }

  // Compact variant
  return (
    <Box>
      {renderContent()}
    </Box>
  );
};

export default LLMConfigDisplay;