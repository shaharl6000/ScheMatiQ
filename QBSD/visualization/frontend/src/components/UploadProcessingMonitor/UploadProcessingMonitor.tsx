import React from 'react';
import {
  Box,
  Typography,
  Paper,
  LinearProgress,
  Chip,
  Alert,
  List,
  ListItem,
  ListItemText,
  Card,
  CardContent,
  Grid,
  Button,
} from '@mui/material';
import {
  CheckCircle,
  Schedule,
  PlayArrow,
  Error,
  TableView,
  Schema,
  Visibility,
} from '@mui/icons-material';
import { ProcessingStatus } from '../../types';
import LLMConfigDisplay from '../LLMConfigDisplay';

interface UploadProcessingMonitorProps {
  sessionId?: string | null;
  status: ProcessingStatus | null;
  loading: boolean;
  error?: string | null;
  onNavigateToResults?: () => void;
  llmConfig?: any; // LLM configuration used for processing
}

const UploadProcessingMonitor: React.FC<UploadProcessingMonitorProps> = ({
  sessionId,
  status,
  loading,
  error,
  onNavigateToResults,
  llmConfig,
}) => {
  const getStatusColor = (statusValue?: string) => {
    switch (statusValue) {
      case 'completed': return 'success';
      case 'processing_documents': return 'warning';
      case 'error': return 'error';
      default: return 'default';
    }
  };

  const getStatusIcon = (statusValue?: string) => {
    switch (statusValue) {
      case 'completed': return <CheckCircle color="success" />;
      case 'processing_documents': return <PlayArrow color="warning" />;
      case 'error': return <Error color="error" />;
      default: return <Schedule color="action" />;
    }
  };

  const formatStatus = (statusValue?: string) => {
    switch (statusValue) {
      case 'processing_documents': return 'Processing Documents';
      case 'completed': return 'Processing Complete';
      case 'error': return 'Processing Failed';
      default: return 'Preparing...';
    }
  };

  if (loading && !status) {
    return (
      <Box>
        <Typography variant="h6" gutterBottom>
          Starting Document Processing
        </Typography>
        <LinearProgress sx={{ mb: 2 }} />
        <Typography variant="body2" color="text.secondary">
          Initializing AI processing pipeline...
        </Typography>
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Document Processing Status
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      {status && (
        <>
          {/* Status Overview */}
          <Grid container spacing={3} sx={{ mb: 4 }}>
            <Grid item xs={12} md={6}>
              <Card>
                <CardContent>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                    <Typography variant="h6">Processing Status</Typography>
                    <Chip 
                      label={formatStatus(status.status)}
                      color={getStatusColor(status.status)}
                      icon={getStatusIcon(status.status)}
                    />
                  </Box>
                  
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Documents: {status.processed_documents}/{status.total_documents}
                  </Typography>
                  
                  <LinearProgress 
                    variant="determinate" 
                    value={status.progress * 100}
                    sx={{ mb: 2 }}
                  />
                  
                  <Typography variant="body2">
                    Progress: {(status.progress * 100).toFixed(1)}%
                  </Typography>
                </CardContent>
              </Card>
            </Grid>

            <Grid item xs={12} md={6}>
              <Card>
                <CardContent>
                  <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                    <TableView sx={{ mr: 1 }} />
                    Data Statistics
                  </Typography>
                  
                  <List dense>
                    <ListItem>
                      <ListItemText
                        primary="Original Rows"
                        secondary={status.original_row_count?.toLocaleString() || '0'}
                      />
                    </ListItem>
                    <ListItem>
                      <ListItemText
                        primary="Additional Rows Added"
                        secondary={status.additional_rows_added?.toLocaleString() || '0'}
                      />
                    </ListItem>
                    <ListItem>
                      <ListItemText
                        primary="Total Documents"
                        secondary={status.total_documents?.toLocaleString() || '0'}
                      />
                    </ListItem>
                  </List>
                </CardContent>
              </Card>
            </Grid>
          </Grid>

          {/* Processing Details */}
          {status.status === 'processing_documents' && (
            <Paper sx={{ p: 3, mb: 3 }}>
              <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                <Schema sx={{ mr: 1 }} />
                AI Processing in Progress
              </Typography>
              
              <Typography variant="body2" color="text.secondary" paragraph>
                The system is analyzing your uploaded documents using the extracted schema. 
                Each document is being processed to extract structured data according to the 
                discovered column definitions.
              </Typography>

              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <Chip 
                  label={`${status.processed_documents} of ${status.total_documents} documents processed`}
                  size="small"
                  color="primary"
                />
                <Chip 
                  label={`${status.additional_rows_added} new rows extracted`}
                  size="small"
                  color="secondary"
                />
              </Box>

              {status.processed_documents > 0 && (
                <Typography variant="body2" color="success.main">
                  ✓ Processing is active and making progress
                </Typography>
              )}

              {/* Display LLM Configuration used for processing */}
              {llmConfig && (
                <Box sx={{ mt: 3, pt: 2, borderTop: '1px solid', borderColor: 'divider' }}>
                  <LLMConfigDisplay
                    config={llmConfig}
                    title="AI Model Used for Processing"
                    variant="inline"
                    showDetails={true}
                  />
                </Box>
              )}
            </Paper>
          )}

          {/* Processing Stats */}
          {status.processing_stats && Object.keys(status.processing_stats).length > 0 && (
            <Paper sx={{ p: 3, mb: 3 }}>
              <Typography variant="h6" gutterBottom>
                Processing Statistics
              </Typography>
              
              <Grid container spacing={2}>
                {Object.entries(status.processing_stats).map(([key, value]) => (
                  <Grid item xs={12} sm={6} md={4} key={key}>
                    <Box sx={{ textAlign: 'center', p: 2, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
                      <Typography variant="h6" color="primary">
                        {typeof value === 'number' ? value.toLocaleString() : String(value)}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        {key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                      </Typography>
                    </Box>
                  </Grid>
                ))}
              </Grid>
            </Paper>
          )}

          {/* Completion Message */}
          {status.status === 'completed' && (
            <Alert severity="success" sx={{ mb: 3 }}>
              <Typography variant="body1" gutterBottom>
                🎉 Document processing completed successfully!
              </Typography>
              <Typography variant="body2" gutterBottom>
                Added {status.additional_rows_added} new rows to your dataset from {status.total_documents} documents.
              </Typography>
              {onNavigateToResults && (
                <Box sx={{ mt: 2 }}>
                  <Button
                    variant="contained"
                    onClick={onNavigateToResults}
                    startIcon={<Visibility />}
                    size="large"
                  >
                    View Results
                  </Button>
                </Box>
              )}
            </Alert>
          )}

          {/* Technical Details */}
          <Paper sx={{ p: 2, backgroundColor: 'grey.50' }}>
            <Typography variant="body2" color="text.secondary">
              <strong>Session ID:</strong> {status.session_id}<br/>
              <strong>Last Updated:</strong> {new Date(status.last_modified).toLocaleString()}
            </Typography>
          </Paper>
        </>
      )}
    </Box>
  );
};

export default UploadProcessingMonitor;