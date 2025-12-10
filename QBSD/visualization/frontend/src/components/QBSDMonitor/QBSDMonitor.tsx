import React, { useState, useEffect } from 'react';
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
  ListItemIcon,
  Button,
  Card,
  CardContent,
  Grid,
} from '@mui/material';
import {
  PlayArrow,
  Stop,
  CheckCircle,
  Error,
  Info,
  Warning,
  Timeline,
} from '@mui/icons-material';
import { useQuery, useQueryClient } from 'react-query';

import { qbsdAPI } from '../../services/api';
import { webSocketService } from '../../services/websocket';
import { WebSocketMessage, ProgressData, SchemaCompletionData, RowCompletionData, LogData } from '../../types';

interface QBSDMonitorProps {
  sessionId: string;
}

interface LogEntry {
  timestamp: string;
  level: 'info' | 'warning' | 'error' | 'success';
  message: string;
  details?: any;
}

const QBSDMonitor: React.FC<QBSDMonitorProps> = ({ sessionId }) => {
  const queryClient = useQueryClient();
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [isConnected, setIsConnected] = useState(false);

  // Phase tracking state
  const [currentPhase, setCurrentPhase] = useState<'idle' | 'schema' | 'extraction' | 'completed'>('idle');
  const [schemaProgress, setSchemaProgress] = useState({
    iteration: 0,
    maxIterations: 5,
    columnsDiscovered: 0,
    isComplete: false
  });
  const [extractionProgress, setExtractionProgress] = useState({
    processedDocs: 0,
    totalDocs: 0,
    isComplete: false
  });

  // Fetch QBSD status
  const { data: status, isLoading } = useQuery(
    ['qbsd-status', sessionId],
    () => qbsdAPI.getStatus(sessionId),
    {
      refetchInterval: 2000, // Refetch every 2 seconds
    }
  );

  // WebSocket connection for real-time updates
  useEffect(() => {
    const handleMessage = (message: WebSocketMessage) => {
      if (message.type === 'connected') {
        setIsConnected(true);
        addLog('info', 'Connected to real-time monitoring');
      } else if (message.type === 'progress') {
        const progressData = message.data as ProgressData;
        addLog('info', progressData?.current_step || 'Processing...', message.data);

        // Detect phase from step name
        const stepName = progressData?.current_step || '';
        if (stepName.toLowerCase().includes('schema')) {
          setCurrentPhase('schema');
          // Extract iteration info from details if available
          const details = progressData?.details as Record<string, unknown> | undefined;
          if (details?.iteration) {
            setSchemaProgress(prev => ({
              ...prev,
              iteration: details.iteration as number,
              maxIterations: (details.max_iterations as number) || 5,
              columnsDiscovered: (details.columns_discovered as number) || prev.columnsDiscovered
            }));
          }
        } else if (stepName.toLowerCase().includes('value extraction') || stepName.toLowerCase().includes('extracting')) {
          setCurrentPhase('extraction');
        } else if (stepName.toLowerCase().includes('finaliz')) {
          setCurrentPhase('completed');
          setExtractionProgress(prev => ({ ...prev, isComplete: true }));
        } else if (stepName.toLowerCase().includes('initializ') || stepName.toLowerCase().includes('loading') || stepName.toLowerCase().includes('setting up')) {
          setCurrentPhase('idle');
        }

        // Invalidate queries to refresh data
        queryClient.invalidateQueries(['qbsd-status', sessionId]);
      } else if (message.type === 'error') {
        addLog('error', message.message || 'An error occurred', message.data);
      } else if (message.type === 'completed') {
        addLog('success', 'QBSD execution completed successfully!', message.data);
        setCurrentPhase('completed');
        setSchemaProgress(prev => ({ ...prev, isComplete: true }));
        setExtractionProgress(prev => ({ ...prev, isComplete: true }));
        queryClient.invalidateQueries(['qbsd-status', sessionId]);
      } else if (message.type === 'schema_completed') {
        const schemaData = message.data as SchemaCompletionData;
        addLog('success', `Schema discovery finished! Discovered ${schemaData?.total_columns || 'several'} columns.`, message.data);
        console.log('📨 Received schema_completed message, invalidating queries');

        // Update schema progress
        setSchemaProgress(prev => ({
          ...prev,
          columnsDiscovered: schemaData?.total_columns || prev.columnsDiscovered,
          isComplete: true
        }));
        setCurrentPhase('extraction');

        // Invalidate all relevant queries to refresh UI
        queryClient.invalidateQueries(['qbsd-status', sessionId]);
        queryClient.invalidateQueries(['session', sessionId, 'qbsd']); // Match the exact query key
        // Small delay then force refresh
        setTimeout(() => {
          queryClient.refetchQueries(['session', sessionId, 'qbsd']);
        }, 500);
      } else if (message.type === 'row_completed') {
        const rowData = message.data as RowCompletionData;
        addLog('info', `Document ${rowData?.row_index}/${rowData?.total_rows} finished processing`, message.data);

        // Update extraction progress
        setExtractionProgress(prev => ({
          ...prev,
          processedDocs: rowData?.row_index || prev.processedDocs,
          totalDocs: rowData?.total_rows || prev.totalDocs,
          isComplete: (rowData?.row_index || 0) >= (rowData?.total_rows || 1)
        }));

        queryClient.invalidateQueries(['data', sessionId]);
      } else if (message.type === 'log') {
        const logData = message.data as LogData;
        addLog(logData?.level || 'info', logData?.message || 'Log message', message.data);
      }
    };

    webSocketService.connect(sessionId, 'progress');
    const cleanup = webSocketService.addMessageHandler(handleMessage);

    return () => {
      cleanup();
      webSocketService.disconnect();
      setIsConnected(false);
    };
  }, [sessionId, queryClient]);

  const addLog = (level: LogEntry['level'], message: string, details?: any) => {
    setLogs(prev => [
      {
        timestamp: new Date().toISOString(),
        level,
        message,
        details,
      },
      ...prev.slice(0, 99) // Keep only last 100 logs
    ]);
  };

  const handleStart = async () => {
    try {
      // Reset progress state for new run
      setCurrentPhase('idle');
      setSchemaProgress({
        iteration: 0,
        maxIterations: 5,
        columnsDiscovered: 0,
        isComplete: false
      });
      setExtractionProgress({
        processedDocs: 0,
        totalDocs: 0,
        isComplete: false
      });

      await qbsdAPI.run(sessionId);
      addLog('info', 'QBSD execution started');
    } catch (error: any) {
      addLog('error', `Failed to start QBSD: ${error.message}`);
    }
  };

  const handleStop = async () => {
    try {
      await qbsdAPI.stop(sessionId);
      addLog('warning', 'QBSD execution stopped by user');
    } catch (error: any) {
      addLog('error', `Failed to stop QBSD: ${error.message}`);
    }
  };

  const getStatusColor = (status?: string) => {
    switch (status) {
      case 'completed': return 'success';
      case 'schema_ready': return 'info';
      case 'processing': return 'warning';
      case 'error': return 'error';
      default: return 'default';
    }
  };

  const getLogIcon = (level: LogEntry['level']) => {
    switch (level) {
      case 'success': return <CheckCircle color="success" />;
      case 'error': return <Error color="error" />;
      case 'warning': return <Warning color="warning" />;
      default: return <Info color="info" />;
    }
  };

  if (isLoading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
        <Typography>Loading QBSD status...</Typography>
      </Box>
    );
  }

  return (
    <Box>
      {/* Status Overview */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                <Typography variant="h6">Execution Status</Typography>
                <Chip 
                  label={status?.status || 'Unknown'}
                  color={getStatusColor(status?.status)}
                />
              </Box>
              
              {/* Two-Phase Progress Display */}
              <Box sx={{ mb: 2 }}>
                {/* Phase 1: Schema Discovery */}
                <Box sx={{ mb: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 0.5 }}>
                    <Typography
                      variant="body2"
                      sx={{
                        fontWeight: currentPhase === 'schema' ? 'bold' : 'normal',
                        color: schemaProgress.isComplete ? 'success.main' : 'text.primary'
                      }}
                    >
                      {schemaProgress.isComplete ? '\u2713 ' : ''}Phase 1: Schema Discovery
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {schemaProgress.isComplete
                        ? `${schemaProgress.columnsDiscovered} columns`
                        : currentPhase === 'idle'
                          ? 'Waiting...'
                          : 'Discovering...'}
                    </Typography>
                  </Box>
                  <LinearProgress
                    variant={currentPhase === 'schema' && !schemaProgress.isComplete ? 'indeterminate' : 'determinate'}
                    value={schemaProgress.isComplete ? 100 : 0}
                    color={schemaProgress.isComplete ? 'success' : 'primary'}
                    sx={{ height: 8, borderRadius: 4 }}
                  />
                </Box>

                {/* Phase 2: Value Extraction */}
                <Box sx={{ mb: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 0.5 }}>
                    <Typography
                      variant="body2"
                      sx={{
                        fontWeight: currentPhase === 'extraction' ? 'bold' : 'normal',
                        color: extractionProgress.isComplete ? 'success.main' :
                               (currentPhase === 'idle' || currentPhase === 'schema') ? 'text.disabled' : 'text.primary'
                      }}
                    >
                      {extractionProgress.isComplete ? '\u2713 ' : ''}Phase 2: Value Extraction
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {(currentPhase === 'idle' || currentPhase === 'schema')
                        ? 'Waiting for schema...'
                        : extractionProgress.totalDocs > 0
                          ? `${extractionProgress.processedDocs}/${extractionProgress.totalDocs} documents`
                          : 'Starting...'}
                    </Typography>
                  </Box>
                  <LinearProgress
                    variant={currentPhase === 'extraction' && extractionProgress.totalDocs === 0 ? 'indeterminate' : 'determinate'}
                    value={extractionProgress.totalDocs > 0
                      ? (extractionProgress.processedDocs / extractionProgress.totalDocs) * 100
                      : 0}
                    color={extractionProgress.isComplete ? 'success' :
                           (currentPhase === 'idle' || currentPhase === 'schema') ? 'inherit' : 'primary'}
                    sx={{
                      height: 8,
                      borderRadius: 4,
                      opacity: (currentPhase === 'idle' || currentPhase === 'schema') ? 0.3 : 1
                    }}
                  />
                </Box>

                {/* Current Step Detail */}
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                  {status?.current_step || 'Ready to start...'}
                </Typography>

                {status?.estimated_time_remaining && (
                  <Typography variant="caption" color="text.secondary">
                    Est. time remaining: {Math.ceil(status.estimated_time_remaining / 60)} minutes
                  </Typography>
                )}
              </Box>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Controls
              </Typography>
              
              <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
                <Button
                  startIcon={<PlayArrow />}
                  variant="contained"
                  onClick={handleStart}
                  disabled={status?.status === 'processing'}
                  size="small"
                >
                  Start
                </Button>
                
                <Button
                  startIcon={<Stop />}
                  variant="outlined"
                  color="error"
                  onClick={handleStop}
                  disabled={status?.status !== 'processing'}
                  size="small"
                >
                  Stop
                </Button>
              </Box>

              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Box 
                  sx={{ 
                    width: 8, 
                    height: 8, 
                    borderRadius: '50%', 
                    bgcolor: isConnected ? 'success.main' : 'error.main' 
                  }} 
                />
                <Typography variant="body2" color="text.secondary">
                  WebSocket: {isConnected ? 'Connected' : 'Disconnected'}
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Error Display */}
      {status?.error_message && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {status.error_message}
        </Alert>
      )}

      {/* Real-time Logs */}
      <Paper sx={{ p: 3 }}>
        <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
          <Timeline sx={{ mr: 1 }} />
          Real-time Logs
        </Typography>
        
        <Box sx={{ maxHeight: 400, overflow: 'auto', border: '1px solid #ddd', borderRadius: 1 }}>
          {logs.length === 0 ? (
            <Box sx={{ p: 2, textAlign: 'center', color: 'text.secondary' }}>
              No logs yet. Logs will appear here when QBSD starts running.
            </Box>
          ) : (
            <List dense>
              {logs.map((log, index) => (
                <ListItem key={index} sx={{ borderBottom: '1px solid #f0f0f0' }}>
                  <ListItemIcon sx={{ minWidth: 40 }}>
                    {getLogIcon(log.level)}
                  </ListItemIcon>
                  <ListItemText
                    primary={log.message}
                    secondary={
                      <Box>
                        <Typography variant="caption" display="block">
                          {new Date(log.timestamp).toLocaleTimeString()}
                        </Typography>
                        {log.details && (
                          <Typography variant="caption" color="text.secondary">
                            {JSON.stringify(log.details)}
                          </Typography>
                        )}
                      </Box>
                    }
                  />
                </ListItem>
              ))}
            </List>
          )}
        </Box>
      </Paper>

      {/* Instructions */}
      <Alert severity="info" sx={{ mt: 3 }}>
        <Typography variant="body2">
          <strong>Instructions:</strong> Use the Start button to begin QBSD execution. 
          You can monitor progress in real-time through this interface. Once completed, 
          switch to other tabs to view the discovered schema and extracted data.
        </Typography>
      </Alert>
    </Box>
  );
};

export default QBSDMonitor;