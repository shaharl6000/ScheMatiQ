import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Typography,
  Box,
  Alert,
  Chip,
  FormHelperText,
  Divider,
} from '@mui/material';
import { AutoFixHigh, Psychology, Speed } from '@mui/icons-material';
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
    icon: <Psychology />,
    recommended: 'schema_creation',
    cost: 'medium',
    speed: 'fast',
  },
  {
    provider: 'gemini',
    model: 'gemini-2.5-flash-lite',
    label: 'Gemini 2.5 Flash Lite',
    description: 'Fast and cost-effective',
    icon: <Speed />,
    recommended: 'value_extraction',
    cost: 'low',
    speed: 'very_fast',
  },
  {
    provider: 'gemini',
    model: 'gemini-1.5-pro',
    label: 'Gemini 1.5 Pro',
    description: 'High accuracy for complex tasks',
    icon: <AutoFixHigh />,
    recommended: 'complex_extraction',
    cost: 'high',
    speed: 'moderate',
  },
];

const DEFAULT_CONFIG: LLMConfig = {
  provider: 'gemini',
  model: 'gemini-2.5-flash-lite',
  max_tokens: 1024,
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

  const handleModelChange = (provider: string, model: string) => {
    const option = LLM_OPTIONS.find(opt => opt.provider === provider && opt.model === model);
    if (option) {
      setSelectedConfig({
        provider,
        model,
        max_tokens: 1024,
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

  const getCostColor = (cost: string) => {
    switch (cost) {
      case 'low': return 'success';
      case 'medium': return 'warning';
      case 'high': return 'error';
      default: return 'default';
    }
  };

  const getSpeedColor = (speed: string) => {
    switch (speed) {
      case 'very_fast': return 'success';
      case 'fast': return 'info';
      case 'moderate': return 'warning';
      default: return 'default';
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>
        <Box display="flex" alignItems="center" gap={1}>
          <Psychology color="primary" />
          {title}
        </Box>
      </DialogTitle>
      
      <DialogContent>
        <Typography variant="body2" color="text.secondary" gutterBottom>
          {description}
        </Typography>

        {preservedConfig && (
          <Alert 
            severity="info" 
            sx={{ mb: 2 }}
            action={
              <Button 
                size="small" 
                onClick={() => setUsePreservedConfig(!usePreservedConfig)}
              >
                {usePreservedConfig ? 'Change Model' : 'Use Original'}
              </Button>
            }
          >
            <Typography variant="body2">
              <strong>Original Configuration Detected:</strong> This schema was created using{' '}
              {preservedConfig.provider} {preservedConfig.model}. 
              {usePreservedConfig ? ' Using original model for consistency.' : ''}
            </Typography>
          </Alert>
        )}

        <FormControl fullWidth margin="normal">
          <InputLabel>AI Model</InputLabel>
          <Select
            value={`${selectedConfig.provider}-${selectedConfig.model}`}
            label="AI Model"
            disabled={usePreservedConfig}
            onChange={(e) => {
              const [provider, ...modelParts] = e.target.value.split('-');
              const model = modelParts.join('-');
              handleModelChange(provider, model);
            }}
          >
            {LLM_OPTIONS.map((option) => (
              <MenuItem 
                key={`${option.provider}-${option.model}`} 
                value={`${option.provider}-${option.model}`}
              >
                <Box display="flex" alignItems="center" gap={2} width="100%">
                  {option.icon}
                  <Box flex={1}>
                    <Typography variant="body1">{option.label}</Typography>
                    <Typography variant="caption" color="text.secondary">
                      {option.description}
                    </Typography>
                  </Box>
                  <Box display="flex" gap={0.5}>
                    <Chip 
                      label={option.cost} 
                      size="small" 
                      color={getCostColor(option.cost)}
                      variant="outlined" 
                    />
                    <Chip 
                      label={option.speed} 
                      size="small" 
                      color={getSpeedColor(option.speed)}
                      variant="outlined" 
                    />
                  </Box>
                </Box>
              </MenuItem>
            ))}
          </Select>
          {!usePreservedConfig && (
            <FormHelperText>
              {getSelectedOption()?.description}
            </FormHelperText>
          )}
        </FormControl>

        {getSelectedOption() && (
          <>
            <Divider sx={{ my: 2 }} />
            <Box>
              <Typography variant="subtitle2" gutterBottom>Model Details:</Typography>
              <Box display="flex" gap={2} alignItems="center" mb={1}>
                <Typography variant="body2">
                  <strong>Provider:</strong> {selectedConfig.provider}
                </Typography>
                <Typography variant="body2">
                  <strong>Model:</strong> {selectedConfig.model}
                </Typography>
              </Box>
              <Box display="flex" gap={1} mb={2}>
                <Chip 
                  label={`Cost: ${getSelectedOption()?.cost}`} 
                  size="small" 
                  color={getCostColor(getSelectedOption()?.cost || 'medium')}
                />
                <Chip 
                  label={`Speed: ${getSelectedOption()?.speed}`} 
                  size="small" 
                  color={getSpeedColor(getSelectedOption()?.speed || 'moderate')}
                />
              </Box>
              
              {preservedConfig && !usePreservedConfig && (
                <Alert severity="warning" sx={{ mt: 2 }}>
                  You're choosing a different model than originally used. Results may vary from the original extraction.
                </Alert>
              )}
            </Box>
          </>
        )}
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose} disabled={loading}>
          Cancel
        </Button>
        <Button 
          onClick={handleConfirm} 
          variant="contained" 
          disabled={loading}
          startIcon={getSelectedOption()?.icon}
        >
          {loading ? 'Starting...' : 'Start Processing'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default LLMSelector;