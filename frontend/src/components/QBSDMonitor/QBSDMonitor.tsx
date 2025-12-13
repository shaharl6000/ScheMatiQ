import React, { useState, useEffect } from 'react';
import {
  Play,
  Square,
  CheckCircle2,
  AlertCircle,
  Info,
  AlertTriangle,
  Activity,
  Loader2,
} from 'lucide-react';
import { useQuery, useQueryClient } from 'react-query';

import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Progress } from '@/components/ui/progress';
import { ScrollArea } from '@/components/ui/scroll-area';

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
  const [isStarting, setIsStarting] = useState(false);

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
      refetchInterval: 2000,
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

        const stepName = progressData?.current_step || '';
        if (stepName.toLowerCase().includes('schema')) {
          setCurrentPhase('schema');
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

        setSchemaProgress(prev => ({
          ...prev,
          columnsDiscovered: schemaData?.total_columns || prev.columnsDiscovered,
          isComplete: true
        }));
        setCurrentPhase('extraction');

        queryClient.invalidateQueries(['qbsd-status', sessionId]);
        queryClient.invalidateQueries(['session', sessionId, 'qbsd']);
        setTimeout(() => {
          queryClient.refetchQueries(['session', sessionId, 'qbsd']);
        }, 500);
      } else if (message.type === 'row_completed') {
        const rowData = message.data as RowCompletionData;
        addLog('info', `Document ${rowData?.row_index}/${rowData?.total_rows} finished processing`, message.data);

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
      ...prev.slice(0, 99)
    ]);
  };

  const handleStart = async () => {
    if (isStarting || status?.status === 'processing') {
      return;
    }

    setIsStarting(true);

    try {
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
    } finally {
      setIsStarting(false);
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

  const getStatusVariant = (statusValue?: string): "default" | "secondary" | "destructive" | "outline" | "success" | "warning" | "info" => {
    switch (statusValue) {
      case 'completed': return 'success';
      case 'schema_ready': return 'info';
      case 'processing': return 'warning';
      case 'error': return 'destructive';
      default: return 'default';
    }
  };

  const getLogIcon = (level: LogEntry['level']) => {
    switch (level) {
      case 'success': return <CheckCircle2 className="h-4 w-4 text-green-500" />;
      case 'error': return <AlertCircle className="h-4 w-4 text-destructive" />;
      case 'warning': return <AlertTriangle className="h-4 w-4 text-yellow-500" />;
      default: return <Info className="h-4 w-4 text-blue-500" />;
    }
  };

  if (isLoading) {
    return (
      <div className="flex justify-center mt-8">
        <p className="text-muted-foreground">Loading QBSD status...</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Status Overview */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card>
          <CardContent className="pt-6">
            <div className="flex justify-between items-center mb-4">
              <h3 className="font-semibold">Execution Status</h3>
              <Badge variant={getStatusVariant(status?.status)}>
                {status?.status || 'Unknown'}
              </Badge>
            </div>

            {/* Two-Phase Progress Display */}
            <div className="space-y-4">
              {/* Phase 1: Schema Discovery */}
              <div>
                <div className="flex justify-between items-center mb-1">
                  <span className={`text-sm ${currentPhase === 'schema' ? 'font-bold' : ''} ${schemaProgress.isComplete ? 'text-green-600' : ''}`}>
                    {schemaProgress.isComplete ? '✓ ' : ''}Phase 1: Schema Discovery
                  </span>
                  <span className="text-xs text-muted-foreground">
                    {schemaProgress.isComplete
                      ? `${schemaProgress.columnsDiscovered} columns`
                      : currentPhase === 'idle'
                        ? 'Waiting...'
                        : 'Discovering...'}
                  </span>
                </div>
                <Progress
                  value={schemaProgress.isComplete ? 100 : (currentPhase === 'schema' ? undefined : 0)}
                  className="h-2"
                />
              </div>

              {/* Phase 2: Value Extraction */}
              <div>
                <div className="flex justify-between items-center mb-1">
                  <span className={`text-sm ${currentPhase === 'extraction' ? 'font-bold' : ''} ${extractionProgress.isComplete ? 'text-green-600' : (currentPhase === 'idle' || currentPhase === 'schema') ? 'text-muted-foreground' : ''}`}>
                    {extractionProgress.isComplete ? '✓ ' : ''}Phase 2: Value Extraction
                  </span>
                  <span className="text-xs text-muted-foreground">
                    {(currentPhase === 'idle' || currentPhase === 'schema')
                      ? 'Waiting for schema...'
                      : extractionProgress.totalDocs > 0
                        ? `${extractionProgress.processedDocs}/${extractionProgress.totalDocs} documents`
                        : 'Starting...'}
                  </span>
                </div>
                <Progress
                  value={extractionProgress.totalDocs > 0
                    ? (extractionProgress.processedDocs / extractionProgress.totalDocs) * 100
                    : 0}
                  className={`h-2 ${(currentPhase === 'idle' || currentPhase === 'schema') ? 'opacity-30' : ''}`}
                />
              </div>

              {/* Current Step Detail */}
              <p className="text-sm text-muted-foreground">
                {status?.current_step || 'Ready to start...'}
              </p>

              {status?.estimated_time_remaining && (
                <p className="text-xs text-muted-foreground">
                  Est. time remaining: {Math.ceil(status.estimated_time_remaining / 60)} minutes
                </p>
              )}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-6">
            <h3 className="font-semibold mb-4">Controls</h3>

            <div className="flex gap-2 mb-4">
              <Button
                onClick={handleStart}
                disabled={status?.status === 'processing' || isStarting}
                size="sm"
              >
                <Play className="h-4 w-4 mr-1" />
                {isStarting ? 'Starting...' : 'Start'}
              </Button>

              <Button
                variant="outline"
                onClick={handleStop}
                disabled={status?.status !== 'processing'}
                size="sm"
              >
                <Square className="h-4 w-4 mr-1" />
                Stop
              </Button>
            </div>

            <div className="flex items-center gap-2">
              <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`} />
              <span className="text-sm text-muted-foreground">
                WebSocket: {isConnected ? 'Connected' : 'Disconnected'}
              </span>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Error Display */}
      {status?.error_message && (
        <Alert variant="destructive">
          <AlertDescription>{status.error_message}</AlertDescription>
        </Alert>
      )}

      {/* Real-time Logs */}
      <Card>
        <CardContent className="pt-6">
          <h3 className="font-semibold flex items-center gap-2 mb-4">
            <Activity className="h-5 w-5" />
            Real-time Logs
          </h3>

          <ScrollArea className="h-[400px] border rounded-md">
            {logs.length === 0 ? (
              <div className="p-4 text-center text-muted-foreground">
                No logs yet. Logs will appear here when QBSD starts running.
              </div>
            ) : (
              <div className="divide-y">
                {logs.map((log, index) => (
                  <div key={index} className="p-3 flex gap-3">
                    <div className="mt-0.5">{getLogIcon(log.level)}</div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm">{log.message}</p>
                      <p className="text-xs text-muted-foreground">
                        {new Date(log.timestamp).toLocaleTimeString()}
                      </p>
                      {log.details && (
                        <p className="text-xs text-muted-foreground truncate">
                          {JSON.stringify(log.details)}
                        </p>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </ScrollArea>
        </CardContent>
      </Card>

      {/* Instructions */}
      <Alert variant="info">
        <Info className="h-4 w-4" />
        <AlertDescription>
          <strong>Instructions:</strong> Use the Start button to begin QBSD execution.
          You can monitor progress in real-time through this interface. Once completed,
          switch to other tabs to view the discovered schema and extracted data.
        </AlertDescription>
      </Alert>
    </div>
  );
};

export default QBSDMonitor;
