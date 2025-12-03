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
  Pause,
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
import { QBSDStatus, WebSocketMessage } from '../../types';

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
        addLog('info', `Progress: ${message.data?.current_step || 'Processing...'}`, message.data);
        // Invalidate queries to refresh data
        queryClient.invalidateQueries(['qbsd-status', sessionId]);
      } else if (message.type === 'error') {
        addLog('error', message.message || 'An error occurred', message.data);
      } else if (message.type === 'completed') {
        addLog('success', 'QBSD execution completed successfully!', message.data);
        queryClient.invalidateQueries(['qbsd-status', sessionId]);
      } else if (message.type === 'log') {
        addLog(message.data?.level || 'info', message.data?.message || 'Log message', message.data);
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
              
              {status && (
                <Box>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    {status.current_step} ({status.steps_completed}/{status.total_steps})
                  </Typography>
                  
                  <LinearProgress 
                    variant="determinate" 
                    value={status.progress * 100}
                    sx={{ mb: 2 }}
                  />
                  
                  <Typography variant="body2">
                    Progress: {(status.progress * 100).toFixed(1)}%
                  </Typography>

                  {status.estimated_time_remaining && (
                    <Typography variant="body2" color="text.secondary">
                      Est. time remaining: {Math.ceil(status.estimated_time_remaining / 60)} minutes
                    </Typography>
                  )}
                </Box>
              )}
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