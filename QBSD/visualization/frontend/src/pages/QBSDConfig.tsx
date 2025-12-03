import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  Paper,
  TextField,
  Button,
  Grid,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  CircularProgress,
} from '@mui/material';
import { ExpandMore, AutoAwesome, Settings } from '@mui/icons-material';

import { qbsdAPI } from '../services/api';
import { QBSDConfig, LLMConfig, RetrieverConfig } from '../types';

const QBSDConfigPage: React.FC = () => {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  const [config, setConfig] = useState<QBSDConfig>({
    query: '',
    docs_path: '',
    max_keys_schema: 100,
    documents_batch_size: 4,
    backend: {
      provider: 'gemini',
      model: 'gemini-2.5-flash',
      max_tokens: 8192,
      temperature: 0.2,
      max_context_tokens: 1000000,
    },
    retriever: {
      type: 'embedding',
      model_name: 'all-MiniLM-L6-v2',
      passage_chars: 512,
      overlap: 64,
      k: 15,
      enable_dynamic_k: true,
      dynamic_k_threshold: 0.65,
      dynamic_k_minimum: 3,
    },
    output_path: 'outputs/visualization_output.json',
    document_randomization_seed: 42,
  });

  const handleConfigChange = (field: string, value: any) => {
    setConfig(prev => ({
      ...prev,
      [field]: value,
    }));
  };

  const handleBackendChange = (field: keyof LLMConfig, value: any) => {
    setConfig(prev => ({
      ...prev,
      backend: {
        ...prev.backend,
        [field]: value,
      },
    }));
  };

  const handleRetrieverChange = (field: keyof RetrieverConfig, value: any) => {
    setConfig(prev => ({
      ...prev,
      retriever: {
        ...prev.retriever!,
        [field]: value,
      },
    }));
  };

  const handleSubmit = async () => {
    setLoading(true);
    setError(null);

    try {
      const result = await qbsdAPI.configure(config);
      
      // Navigate to monitoring page
      navigate(`/visualize/${result.session_id}?mode=qbsd`);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to configure QBSD');
    } finally {
      setLoading(false);
    }
  };

  const isFormValid = config.query.trim() !== '' && config.docs_path.toString().trim() !== '';

  return (
    <Box sx={{ maxWidth: 1000, mx: 'auto', mt: 4 }}>
      <Typography variant="h4" gutterBottom>
        Configure QBSD
      </Typography>
      <Typography variant="body1" color="text.secondary" paragraph>
        Set up your Query-Based Schema Discovery parameters to run AI-powered data extraction.
      </Typography>

      <Paper sx={{ p: 4, mt: 4 }}>
        <Grid container spacing={3}>
          {/* Basic Configuration */}
          <Grid item xs={12}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
              <AutoAwesome sx={{ mr: 1 }} />
              Basic Configuration
            </Typography>
          </Grid>

          <Grid item xs={12}>
            <TextField
              label="Research Query"
              multiline
              rows={3}
              fullWidth
              required
              value={config.query}
              onChange={(e) => handleConfigChange('query', e.target.value)}
              placeholder="e.g., Given a protein sequence, can it be determined whether or not it contains a nuclear export signal (NES)?"
              helperText="The research question that will guide schema discovery"
            />
          </Grid>

          <Grid item xs={12} md={8}>
            <TextField
              label="Document Paths"
              fullWidth
              required
              value={Array.isArray(config.docs_path) ? config.docs_path.join(', ') : config.docs_path}
              onChange={(e) => {
                const paths = e.target.value.split(',').map(p => p.trim()).filter(p => p);
                handleConfigChange('docs_path', paths.length === 1 ? paths[0] : paths);
              }}
              placeholder="../src/full_text, ../src/abstracts"
              helperText="Comma-separated paths to document directories"
            />
          </Grid>

          <Grid item xs={12} md={4}>
            <TextField
              label="Max Schema Keys"
              type="number"
              fullWidth
              value={config.max_keys_schema}
              onChange={(e) => handleConfigChange('max_keys_schema', parseInt(e.target.value))}
              inputProps={{ min: 1, max: 500 }}
              helperText="Maximum number of columns"
            />
          </Grid>

          <Grid item xs={12} md={6}>
            <TextField
              label="Document Batch Size"
              type="number"
              fullWidth
              value={config.documents_batch_size}
              onChange={(e) => handleConfigChange('documents_batch_size', parseInt(e.target.value))}
              inputProps={{ min: 1, max: 20 }}
              helperText="Documents processed per iteration"
            />
          </Grid>

          <Grid item xs={12} md={6}>
            <TextField
              label="Randomization Seed"
              type="number"
              fullWidth
              value={config.document_randomization_seed}
              onChange={(e) => handleConfigChange('document_randomization_seed', parseInt(e.target.value))}
              helperText="For reproducible document ordering"
            />
          </Grid>

          {/* LLM Backend Configuration */}
          <Grid item xs={12}>
            <Accordion sx={{ mt: 2 }}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <Settings sx={{ mr: 1 }} />
                  <Typography variant="h6">LLM Backend Settings</Typography>
                  <Chip 
                    label={config.backend.provider.toUpperCase()} 
                    size="small" 
                    color="primary" 
                    sx={{ ml: 2 }}
                  />
                </Box>
              </AccordionSummary>
              <AccordionDetails>
                <Grid container spacing={2}>
                  <Grid item xs={12} md={4}>
                    <FormControl fullWidth>
                      <InputLabel>Provider</InputLabel>
                      <Select
                        value={config.backend.provider}
                        onChange={(e) => handleBackendChange('provider', e.target.value)}
                      >
                        <MenuItem value="gemini">Google Gemini</MenuItem>
                        <MenuItem value="openai">OpenAI</MenuItem>
                        <MenuItem value="together">Together AI</MenuItem>
                      </Select>
                    </FormControl>
                  </Grid>

                  <Grid item xs={12} md={8}>
                    <TextField
                      label="Model"
                      fullWidth
                      value={config.backend.model}
                      onChange={(e) => handleBackendChange('model', e.target.value)}
                      placeholder="e.g., gemini-2.5-flash, gpt-4, etc."
                    />
                  </Grid>

                  <Grid item xs={12} md={4}>
                    <TextField
                      label="Max Tokens"
                      type="number"
                      fullWidth
                      value={config.backend.max_tokens}
                      onChange={(e) => handleBackendChange('max_tokens', parseInt(e.target.value))}
                      inputProps={{ min: 512, max: 32768 }}
                    />
                  </Grid>

                  <Grid item xs={12} md={4}>
                    <TextField
                      label="Temperature"
                      type="number"
                      fullWidth
                      value={config.backend.temperature}
                      onChange={(e) => handleBackendChange('temperature', parseFloat(e.target.value))}
                      inputProps={{ min: 0, max: 2, step: 0.1 }}
                    />
                  </Grid>

                  <Grid item xs={12} md={4}>
                    <TextField
                      label="Max Context Tokens"
                      type="number"
                      fullWidth
                      value={config.backend.max_context_tokens || ''}
                      onChange={(e) => handleBackendChange('max_context_tokens', e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="Optional"
                    />
                  </Grid>
                </Grid>
              </AccordionDetails>
            </Accordion>
          </Grid>

          {/* Retriever Configuration */}
          <Grid item xs={12}>
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography variant="h6">Retriever Settings</Typography>
                <Chip 
                  label={config.retriever?.model_name || 'Default'} 
                  size="small" 
                  color="secondary" 
                  sx={{ ml: 2 }}
                />
              </AccordionSummary>
              <AccordionDetails>
                <Grid container spacing={2}>
                  <Grid item xs={12} md={6}>
                    <TextField
                      label="Model Name"
                      fullWidth
                      value={config.retriever?.model_name || ''}
                      onChange={(e) => handleRetrieverChange('model_name', e.target.value)}
                    />
                  </Grid>

                  <Grid item xs={12} md={6}>
                    <TextField
                      label="Passage Characters"
                      type="number"
                      fullWidth
                      value={config.retriever?.passage_chars || 512}
                      onChange={(e) => handleRetrieverChange('passage_chars', parseInt(e.target.value))}
                      inputProps={{ min: 128, max: 2048 }}
                    />
                  </Grid>

                  <Grid item xs={12} md={4}>
                    <TextField
                      label="Overlap"
                      type="number"
                      fullWidth
                      value={config.retriever?.overlap || 64}
                      onChange={(e) => handleRetrieverChange('overlap', parseInt(e.target.value))}
                      inputProps={{ min: 0, max: 256 }}
                    />
                  </Grid>

                  <Grid item xs={12} md={4}>
                    <TextField
                      label="Retrieval K"
                      type="number"
                      fullWidth
                      value={config.retriever?.k || 15}
                      onChange={(e) => handleRetrieverChange('k', parseInt(e.target.value))}
                      inputProps={{ min: 1, max: 50 }}
                    />
                  </Grid>

                  <Grid item xs={12} md={4}>
                    <TextField
                      label="Dynamic K Threshold"
                      type="number"
                      fullWidth
                      value={config.retriever?.dynamic_k_threshold || 0.65}
                      onChange={(e) => handleRetrieverChange('dynamic_k_threshold', parseFloat(e.target.value))}
                      inputProps={{ min: 0, max: 1, step: 0.05 }}
                    />
                  </Grid>
                </Grid>
              </AccordionDetails>
            </Accordion>
          </Grid>

          {/* Submit */}
          <Grid item xs={12}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 3 }}>
              <Button onClick={() => navigate('/')} variant="outlined">
                Back to Home
              </Button>
              
              <Button
                variant="contained"
                size="large"
                onClick={handleSubmit}
                disabled={!isFormValid || loading}
                startIcon={loading ? <CircularProgress size={20} /> : <AutoAwesome />}
              >
                {loading ? 'Starting QBSD...' : 'Start QBSD'}
              </Button>
            </Box>
          </Grid>
        </Grid>
      </Paper>

      {error && (
        <Alert severity="error" sx={{ mt: 3 }}>
          {error}
        </Alert>
      )}
    </Box>
  );
};

export default QBSDConfigPage;