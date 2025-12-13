import React from 'react';
import {
  Box,
  Typography,
  Chip,
  Tooltip,
  Card,
  CardContent,
  Stack,
  Divider,
} from '@mui/material';
import { Psychology, Schedule, Info } from '@mui/icons-material';
import LLMConfigDisplay from '../LLMConfigDisplay';

interface ConfigurationInfoProps {
  session?: any; // Use any to avoid complex type conflicts
  compact?: boolean;
}

const ConfigurationInfo: React.FC<ConfigurationInfoProps> = ({
  session,
  compact = false,
}) => {
  if (!session?.metadata) {
    return null;
  }

  const { metadata } = session;
  const llmConfig = metadata?.extracted_schema?.llm_configuration;
  const schemaBackend = llmConfig?.schema_creation_backend;
  const extractionBackend = llmConfig?.value_extraction_backend;
  
  const formatDate = (dateString?: string) => {
    if (!dateString) return 'Unknown';
    try {
      return new Date(dateString).toLocaleDateString();
    } catch {
      return 'Unknown';
    }
  };

  if (compact) {
    return (
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap' }}>
        {schemaBackend && (
          <Tooltip title={`Schema created with ${schemaBackend.provider} ${schemaBackend.model}`}>
            <Chip
              size="small"
              icon={<Psychology />}
              label={`Schema: ${schemaBackend.model?.replace('gemini-', '') || 'Unknown'}`}
              variant="outlined"
              color="primary"
            />
          </Tooltip>
        )}
        
        {extractionBackend && (
          <Tooltip title={`Value extraction with ${extractionBackend.provider} ${extractionBackend.model}`}>
            <Chip
              size="small"
              icon={<Psychology />}
              label={`Extraction: ${extractionBackend.model?.replace('gemini-', '') || 'Unknown'}`}
              variant="outlined"
              color="secondary"
            />
          </Tooltip>
        )}
        
        {metadata.created && (
          <Tooltip title={`Created: ${formatDate(metadata.created)}`}>
            <Chip
              size="small"
              icon={<Schedule />}
              label={formatDate(metadata.created)}
              variant="outlined"
            />
          </Tooltip>
        )}
      </Box>
    );
  }

  return (
    <Card variant="outlined" sx={{ mb: 2 }}>
      <CardContent>
        <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Info color="primary" />
          Session Configuration
        </Typography>
        
        <Stack spacing={2}>
          {/* Basic Session Info */}
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              Session Information
            </Typography>
            <Stack direction="row" spacing={1} flexWrap="wrap">
              <Chip 
                label={`Type: ${session.type === 'load' ? 'Load Existing' : 'QBSD Pipeline'}`}
                size="small"
                color="primary"
              />
              {metadata.created && (
                <Chip
                  label={`Created: ${formatDate(metadata.created)}`}
                  size="small"
                  variant="outlined"
                />
              )}
              {metadata.last_modified && (
                <Chip
                  label={`Modified: ${formatDate(metadata.last_modified)}`}
                  size="small"
                  variant="outlined"
                />
              )}
            </Stack>
          </Box>

          {/* Query Information */}
          {session.schema_query && (
            <Box>
              <Typography variant="subtitle2" gutterBottom>
                Research Query
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ 
                p: 1.5, 
                bgcolor: 'grey.50', 
                borderRadius: 1,
                fontStyle: 'italic'
              }}>
                "{session.schema_query}"
              </Typography>
            </Box>
          )}

          {/* AI Model Configuration */}
          {(schemaBackend || extractionBackend) && (
            <>
              <Divider />
              <Typography variant="subtitle2" gutterBottom>
                AI Model Configuration
              </Typography>
              
              <Stack spacing={2}>
                {schemaBackend && (
                  <LLMConfigDisplay
                    config={schemaBackend}
                    title="Schema Creation Model"
                    variant="inline"
                    showDetails={true}
                  />
                )}
                
                {extractionBackend && (
                  <LLMConfigDisplay
                    config={extractionBackend}
                    title="Value Extraction Model"
                    variant="inline"
                    showDetails={true}
                  />
                )}
              </Stack>
            </>
          )}

          {/* Data Source */}
          {metadata.source && (
            <Box>
              <Typography variant="subtitle2" gutterBottom>
                Data Source
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {metadata.source}
              </Typography>
            </Box>
          )}
        </Stack>
      </CardContent>
    </Card>
  );
};

export default ConfigurationInfo;