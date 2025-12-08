import React, { useState, useEffect } from 'react';
import {
  Card,
  CardContent,
  Typography,
  LinearProgress,
  Box,
  Chip,
  IconButton,
  Collapse,
  Alert,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  CircularProgress,
} from '@mui/material';
import {
  ExpandMore,
  ExpandLess,
  Refresh,
  CheckCircle,
  Error as ErrorIcon,
  Info,
  Cancel,
} from '@mui/icons-material';

import { ReprocessingStatus } from '../../types';
import { schemaAPI } from '../../services/api';

interface ReprocessingMonitorProps {
  sessionId: string;
  status?: ReprocessingStatus | null;
  onStatusUpdate?: (status: ReprocessingStatus | null) => void;
}

const ReprocessingMonitor: React.FC<ReprocessingMonitorProps> = ({
  sessionId,
  status,
  onStatusUpdate
}) => {
  const [expanded, setExpanded] = useState(true);
  const [localStatus, setLocalStatus] = useState<ReprocessingStatus | null>(status || null);
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    setLocalStatus(status || null);
  }, [status]);

  // Auto-refresh status when processing
  useEffect(() => {
    if (!localStatus || localStatus.status !== 'processing') return;

    const interval = setInterval(async () => {
      try {
        const updatedStatus = await schemaAPI.getReprocessingStatus(sessionId);
        setLocalStatus(updatedStatus.status === 'processing' ? updatedStatus : null);
        if (onStatusUpdate) {
          onStatusUpdate(updatedStatus.status === 'processing' ? updatedStatus : null);
        }
      } catch (error) {
        console.error('Failed to refresh reprocessing status:', error);
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [localStatus?.status, sessionId, onStatusUpdate]);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      const updatedStatus = await schemaAPI.getReprocessingStatus(sessionId);
      setLocalStatus(updatedStatus.status === 'processing' ? updatedStatus : null);
      if (onStatusUpdate) {
        onStatusUpdate(updatedStatus.status === 'processing' ? updatedStatus : null);
      }
    } catch (error) {
      console.error('Failed to refresh status:', error);
    } finally {
      setRefreshing(false);
    }
  };

  if (!localStatus || localStatus.status === 'idle') {
    return null;
  }

  const getStatusIcon = () => {
    switch (localStatus.status) {
      case 'processing':
        return <CircularProgress size={16} />;
      case 'completed':
        return <CheckCircle color="success" />;
      case 'failed':
        return <ErrorIcon color="error" />;
      default:
        return <Info />;
    }
  };

  const getStatusColor = (): 'default' | 'primary' | 'secondary' | 'error' | 'info' | 'success' | 'warning' => {
    switch (localStatus.status) {
      case 'processing':
        return 'primary';
      case 'completed':
        return 'success';
      case 'failed':
        return 'error';
      default:
        return 'info';
    }
  };

  const formatEstimatedTime = (estimated?: string): string => {
    if (!estimated) return 'Unknown';
    
    try {
      const estimatedDate = new Date(estimated);
      const now = new Date();
      const diffMs = estimatedDate.getTime() - now.getTime();
      
      if (diffMs <= 0) return 'Soon';
      
      const diffMinutes = Math.ceil(diffMs / (1000 * 60));
      if (diffMinutes < 60) return `~${diffMinutes}m`;
      
      const diffHours = Math.ceil(diffMinutes / 60);
      return `~${diffHours}h`;
    } catch {
      return 'Unknown';
    }
  };

  return (
    <Card 
      variant="outlined" 
      sx={{ 
        mb: 2,
        border: localStatus.status === 'processing' ? 2 : 1,
        borderColor: localStatus.status === 'processing' ? 'primary.main' : 'divider'
      }}
    >
      <CardContent sx={{ pb: 1 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexGrow: 1 }}>
            {getStatusIcon()}
            <Typography variant="h6" sx={{ fontWeight: 500 }}>
              Document Reprocessing
            </Typography>
            <Chip 
              label={localStatus.status.charAt(0).toUpperCase() + localStatus.status.slice(1)}
              color={getStatusColor()}
              size="small"
            />
          </Box>
          
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <IconButton 
              size="small"
              onClick={handleRefresh}
              disabled={refreshing}
              title="Refresh status"
            >
              {refreshing ? <CircularProgress size={16} /> : <Refresh />}
            </IconButton>
            
            <IconButton
              size="small"
              onClick={() => setExpanded(!expanded)}
              title={expanded ? "Collapse" : "Expand"}
            >
              {expanded ? <ExpandLess /> : <ExpandMore />}
            </IconButton>
          </Box>
        </Box>

        <Collapse in={expanded}>
          <Box sx={{ mt: 2 }}>
            {/* Progress Bar */}
            {localStatus.status === 'processing' && (
              <>
                <Typography variant="body2" color="text.secondary" gutterBottom>
                  {localStatus.current_step}
                </Typography>
                
                <LinearProgress 
                  variant="determinate" 
                  value={localStatus.progress * 100}
                  sx={{ mb: 1, height: 8, borderRadius: 4 }}
                />
                
                <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                  <Typography variant="caption" color="text.secondary">
                    {Math.round(localStatus.progress * 100)}% complete
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    ETA: {formatEstimatedTime(localStatus.estimated_completion)}
                  </Typography>
                </Box>
              </>
            )}

            {/* Document Progress */}
            <Box sx={{ mb: 2 }}>
              <Typography variant="body2" gutterBottom>
                <strong>Progress:</strong> {localStatus.processed_documents} of {localStatus.total_documents} documents
              </Typography>
              
              {localStatus.total_documents > 0 && (
                <LinearProgress 
                  variant="determinate"
                  value={(localStatus.processed_documents / localStatus.total_documents) * 100}
                  sx={{ height: 4, borderRadius: 2 }}
                />
              )}
            </Box>

            {/* Affected Columns */}
            {localStatus.affected_columns.length > 0 && (
              <Box sx={{ mb: 2 }}>
                <Typography variant="body2" gutterBottom>
                  <strong>Affected Columns:</strong>
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                  {localStatus.affected_columns.map((column, index) => (
                    <Chip
                      key={index}
                      label={column}
                      size="small"
                      variant="outlined"
                      color="primary"
                    />
                  ))}
                </Box>
              </Box>
            )}

            {/* Status Messages */}
            {localStatus.status === 'completed' && (
              <Alert severity="success" sx={{ mt: 1 }}>
                <Typography variant="body2">
                  Reprocessing completed successfully! All documents have been processed with the updated schema.
                </Typography>
              </Alert>
            )}

            {localStatus.status === 'failed' && (
              <Alert severity="error" sx={{ mt: 1 }}>
                <Typography variant="body2">
                  Reprocessing failed. Please check the system logs for more details or try again.
                </Typography>
              </Alert>
            )}

            {localStatus.status === 'processing' && (
              <Alert severity="info" sx={{ mt: 1 }}>
                <Typography variant="body2">
                  Reprocessing is in progress. You can continue using other parts of the application while this completes in the background.
                </Typography>
              </Alert>
            )}
          </Box>
        </Collapse>
      </CardContent>
    </Card>
  );
};

export default ReprocessingMonitor;