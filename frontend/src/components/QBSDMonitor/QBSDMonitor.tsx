import React, { useState, useEffect } from 'react';
import {
  Play,
  Square,
  CheckCircle2,
  AlertCircle,
  Info,
  AlertTriangle,
  Activity,
  Wifi,
  WifiOff,
  Loader2,
  XCircle,
  ChevronDown,
} from 'lucide-react';
import { useQuery, useQueryClient } from 'react-query';

import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Progress } from '@/components/ui/progress';
import { ScrollArea } from '@/components/ui/scroll-area';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible';

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

// Processing state that changes IMMEDIATELY on user action
type ProcessingState = 'idle' | 'starting' | 'schema' | 'extraction' | 'completed' | 'error';

const QBSDMonitor: React.FC<QBSDMonitorProps> = ({ sessionId }) => {
  const queryClient = useQueryClient();
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [connectionStatus, setConnectionStatus] = useState<'connecting' | 'connected' | 'disconnected' | 'reconnecting'>('connecting');
  const [logsOpen, setLogsOpen] = useState(false);

  // Main processing state - changes IMMEDIATELY on Start click
  const [processingState, setProcessingState] = useState<ProcessingState>('idle');
  const [currentStepMessage, setCurrentStepMessage] = useState<string>('');
  const [errorMessage, setErrorMessage] = useState<string>('');

  // Phase tracking state
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

  // Sync processingState with backend status (for page refreshes)
  useEffect(() => {
    if (status?.status === 'processing' && processingState === 'idle') {
      setProcessingState('starting');
    } else if (status?.status === 'completed' && processingState !== 'completed') {
      setProcessingState('completed');
    } else if (status?.status === 'error') {
      setProcessingState('error');
      setErrorMessage(status.error_message || 'An error occurred');
    }
  }, [status?.status]);

  // WebSocket connection polling fallback
  useEffect(() => {
    const interval = setInterval(() => {
      const isConnected = webSocketService.isConnected();
      if (isConnected && connectionStatus !== 'connected') {
        setConnectionStatus('connected');
      } else if (!isConnected && connectionStatus === 'connected') {
        setConnectionStatus('disconnected');
      }
    }, 1000);
    return () => clearInterval(interval);
  }, [connectionStatus]);

  // WebSocket connection for real-time updates
  useEffect(() => {
    const handleMessage = (message: WebSocketMessage) => {
      if (message.type === 'connected') {
        setConnectionStatus('connected');
        addLog('success', 'Connected to real-time monitoring');
      } else if (message.type === 'disconnected') {
        setConnectionStatus('disconnected');
        addLog('warning', 'Disconnected from server');
      } else if (message.type === 'reconnecting') {
        setConnectionStatus('reconnecting');
        addLog('info', message.message || 'Attempting to reconnect...');
      } else if (message.type === 'progress') {
        const progressData = message.data as ProgressData;
        const stepName = progressData?.current_step || 'Processing...';
        setCurrentStepMessage(stepName);
        addLog('info', stepName, message.data);

        // Update processing state based on step name
        if (stepName.toLowerCase().includes('schema')) {
          setProcessingState('schema');
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
          setProcessingState('extraction');
        } else if (stepName.toLowerCase().includes('finaliz')) {
          setProcessingState('completed');
          setExtractionProgress(prev => ({ ...prev, isComplete: true }));
        } else if (processingState === 'starting') {
          // Keep in starting state until we get a specific phase
        }

        queryClient.invalidateQueries(['qbsd-status', sessionId]);
      } else if (message.type === 'error') {
        setProcessingState('error');
        setErrorMessage(message.message || 'An error occurred');
        addLog('error', message.message || 'An error occurred', message.data);
      } else if (message.type === 'completed') {
        addLog('success', 'QBSD execution completed successfully!', message.data);
        setProcessingState('completed');
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
        setProcessingState('extraction');

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
      setConnectionStatus('disconnected');
    };
  }, [sessionId, queryClient]);

  const addLog = (level: LogEntry['level'], message: string, details?: any) => {
    const now = new Date();
    setLogs(prev => [
      {
        timestamp: now.toISOString(),
        level,
        message,
        details,
      },
      ...prev.slice(0, 99)
    ]);
  };

  const handleStart = async () => {
    if (processingState !== 'idle' && processingState !== 'error' && processingState !== 'completed') {
      return;
    }

    // IMMEDIATELY set to starting state - don't wait for anything
    setProcessingState('starting');
    setCurrentStepMessage('Initializing...');
    setErrorMessage('');

    // Reset progress
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

    try {
      await qbsdAPI.run(sessionId);
      addLog('info', 'QBSD execution started');
      // Stay in 'starting' state until we receive progress updates
    } catch (error: any) {
      setProcessingState('error');
      setErrorMessage(error.message || 'Failed to start QBSD');
      addLog('error', `Failed to start QBSD: ${error.message}`);
    }
  };

  const handleStop = async () => {
    try {
      await qbsdAPI.stop(sessionId);
      setProcessingState('idle');
      addLog('warning', 'QBSD execution stopped by user');
    } catch (error: any) {
      addLog('error', `Failed to stop QBSD: ${error.message}`);
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

  const isProcessing = processingState === 'starting' || processingState === 'schema' || processingState === 'extraction';

  if (isLoading) {
    return (
      <div className="flex justify-center items-center min-h-[400px]">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="h-12 w-12 animate-spin text-primary" />
          <p className="text-muted-foreground">Loading QBSD status...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Connection Status Badge - Top Right */}
      <div className="flex justify-end">
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-muted/50 border">
          {connectionStatus === 'connected' && (
            <>
              <div className="w-2 h-2 rounded-full bg-green-500" />
              <Wifi className="h-3.5 w-3.5 text-green-500" />
              <span className="text-xs text-green-600 font-medium">Connected</span>
            </>
          )}
          {connectionStatus === 'connecting' && (
            <>
              <div className="w-2 h-2 rounded-full bg-yellow-500 animate-pulse" />
              <Loader2 className="h-3.5 w-3.5 text-yellow-500 animate-spin" />
              <span className="text-xs text-yellow-600 font-medium">Connecting...</span>
            </>
          )}
          {connectionStatus === 'reconnecting' && (
            <>
              <div className="w-2 h-2 rounded-full bg-yellow-500 animate-pulse" />
              <Loader2 className="h-3.5 w-3.5 text-yellow-500 animate-spin" />
              <span className="text-xs text-yellow-600 font-medium">Reconnecting...</span>
            </>
          )}
          {connectionStatus === 'disconnected' && (
            <>
              <div className="w-2 h-2 rounded-full bg-red-500" />
              <WifiOff className="h-3.5 w-3.5 text-red-500" />
              <span className="text-xs text-red-600 font-medium">Disconnected</span>
            </>
          )}
        </div>
      </div>

      {/* HERO PROCESSING SECTION */}
      <Card className="border-2">
        <CardContent className="py-12 flex flex-col items-center justify-center min-h-[250px]">
          {/* IDLE STATE */}
          {processingState === 'idle' && (
            <>
              <div className="w-20 h-20 rounded-full bg-muted flex items-center justify-center mb-6">
                <Play className="h-10 w-10 text-muted-foreground" />
              </div>
              <p className="text-2xl font-semibold text-muted-foreground mb-2">Ready to Start</p>
              <p className="text-sm text-muted-foreground mb-6">
                Click the button below to begin QBSD execution
              </p>
              <Button size="lg" onClick={handleStart} className="px-8">
                <Play className="h-5 w-5 mr-2" />
                Start QBSD
              </Button>
            </>
          )}

          {/* PROCESSING STATES (starting, schema, extraction) */}
          {isProcessing && (
            <>
              <div className="relative mb-6">
                <Loader2 className="h-20 w-20 animate-spin text-primary" />
                <div className="absolute inset-0 flex items-center justify-center">
                  <div className="h-12 w-12 rounded-full bg-primary/10 animate-pulse" />
                </div>
              </div>
              <p className="text-2xl font-semibold mb-2">
                {processingState === 'starting' && 'Starting...'}
                {processingState === 'schema' && 'Discovering Schema...'}
                {processingState === 'extraction' && 'Extracting Values...'}
              </p>
              <p className="text-muted-foreground mb-6 text-center max-w-md">
                {currentStepMessage || 'Processing your documents...'}
              </p>
              <Button variant="outline" onClick={handleStop}>
                <Square className="h-4 w-4 mr-2" />
                Stop
              </Button>
            </>
          )}

          {/* COMPLETED STATE */}
          {processingState === 'completed' && (
            <>
              <div className="w-20 h-20 rounded-full bg-green-100 flex items-center justify-center mb-6">
                <CheckCircle2 className="h-12 w-12 text-green-600" />
              </div>
              <p className="text-2xl font-semibold text-green-600 mb-2">Completed Successfully!</p>
              <p className="text-muted-foreground mb-6 text-center max-w-md">
                {schemaProgress.columnsDiscovered > 0 && extractionProgress.totalDocs > 0
                  ? `Discovered ${schemaProgress.columnsDiscovered} columns from ${extractionProgress.totalDocs} documents`
                  : 'Schema discovery and value extraction finished'}
              </p>
              <Button variant="outline" onClick={handleStart}>
                <Play className="h-4 w-4 mr-2" />
                Run Again
              </Button>
            </>
          )}

          {/* ERROR STATE */}
          {processingState === 'error' && (
            <>
              <div className="w-20 h-20 rounded-full bg-red-100 flex items-center justify-center mb-6">
                <XCircle className="h-12 w-12 text-red-600" />
              </div>
              <p className="text-2xl font-semibold text-red-600 mb-2">Error Occurred</p>
              <p className="text-muted-foreground mb-6 text-center max-w-md">
                {errorMessage || 'An unexpected error occurred'}
              </p>
              <Button onClick={handleStart}>
                <Play className="h-4 w-4 mr-2" />
                Try Again
              </Button>
            </>
          )}
        </CardContent>
      </Card>

      {/* Phase Progress Cards - Side by Side */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Phase 1: Schema Discovery */}
        <Card className={`transition-all ${processingState === 'schema' ? 'border-primary border-2 shadow-md' : ''}`}>
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="font-medium flex items-center gap-2">
                {schemaProgress.isComplete ? (
                  <CheckCircle2 className="h-5 w-5 text-green-500" />
                ) : processingState === 'schema' ? (
                  <Loader2 className="h-5 w-5 animate-spin text-primary" />
                ) : (
                  <div className="h-5 w-5 rounded-full border-2 border-muted-foreground/30" />
                )}
                Phase 1: Schema
              </span>
              <Badge variant={schemaProgress.isComplete ? 'success' : processingState === 'schema' ? 'default' : 'secondary'}>
                {schemaProgress.isComplete
                  ? 'Complete'
                  : processingState === 'schema'
                    ? 'In Progress'
                    : 'Pending'}
              </Badge>
            </div>
            <Progress
              value={schemaProgress.isComplete ? 100 : (processingState === 'schema' ? Math.max(10, (schemaProgress.iteration / schemaProgress.maxIterations) * 100) : 0)}
              className={`h-2 ${processingState === 'schema' && !schemaProgress.isComplete ? 'animate-pulse' : ''}`}
            />
            <p className="text-xs text-muted-foreground mt-2">
              {schemaProgress.isComplete
                ? `${schemaProgress.columnsDiscovered} columns discovered`
                : processingState === 'schema' && schemaProgress.iteration > 0
                  ? `Iteration ${schemaProgress.iteration}/${schemaProgress.maxIterations}`
                  : processingState === 'schema'
                    ? 'Analyzing documents...'
                    : 'Waiting to start'}
            </p>
          </CardContent>
        </Card>

        {/* Phase 2: Value Extraction */}
        <Card className={`transition-all ${processingState === 'extraction' ? 'border-primary border-2 shadow-md' : ''}`}>
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="font-medium flex items-center gap-2">
                {extractionProgress.isComplete ? (
                  <CheckCircle2 className="h-5 w-5 text-green-500" />
                ) : processingState === 'extraction' ? (
                  <Loader2 className="h-5 w-5 animate-spin text-primary" />
                ) : (
                  <div className="h-5 w-5 rounded-full border-2 border-muted-foreground/30" />
                )}
                Phase 2: Extraction
              </span>
              <Badge variant={extractionProgress.isComplete ? 'success' : processingState === 'extraction' ? 'default' : 'secondary'}>
                {extractionProgress.isComplete
                  ? 'Complete'
                  : processingState === 'extraction'
                    ? 'In Progress'
                    : 'Pending'}
              </Badge>
            </div>
            <Progress
              value={extractionProgress.isComplete
                ? 100
                : extractionProgress.totalDocs > 0
                  ? (extractionProgress.processedDocs / extractionProgress.totalDocs) * 100
                  : processingState === 'extraction' ? 10 : 0}
              className={`h-2 ${processingState === 'extraction' && !extractionProgress.isComplete ? 'animate-pulse' : ''}`}
            />
            <p className="text-xs text-muted-foreground mt-2">
              {extractionProgress.isComplete
                ? `${extractionProgress.totalDocs} documents processed`
                : processingState === 'extraction' && extractionProgress.totalDocs > 0
                  ? `${extractionProgress.processedDocs}/${extractionProgress.totalDocs} documents`
                  : processingState === 'extraction'
                    ? 'Starting extraction...'
                    : 'Waiting for schema'}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Error Display */}
      {status?.error_message && processingState !== 'error' && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>{status.error_message}</AlertDescription>
        </Alert>
      )}

      {/* Collapsible Activity Log */}
      <Collapsible open={logsOpen} onOpenChange={setLogsOpen}>
        <CollapsibleTrigger className="flex items-center gap-2 w-full p-3 hover:bg-muted/50 rounded-lg border transition-colors">
          <ChevronDown className={`h-4 w-4 transition-transform duration-200 ${logsOpen ? '' : '-rotate-90'}`} />
          <Activity className="h-4 w-4" />
          <span className="font-medium">Activity Log</span>
          <Badge variant="secondary" className="ml-auto">
            {logs.length}
          </Badge>
        </CollapsibleTrigger>
        <CollapsibleContent>
          <Card className="mt-2">
            <CardContent className="pt-4">
              <ScrollArea className="h-[300px]">
                {logs.length === 0 ? (
                  <div className="p-4 text-center text-muted-foreground">
                    No logs yet. Logs will appear here when QBSD starts running.
                  </div>
                ) : (
                  <div className="space-y-1">
                    {logs.map((log, index) => (
                      <div key={index} className="p-2 flex gap-3 hover:bg-muted/50 rounded">
                        <div className="mt-0.5">{getLogIcon(log.level)}</div>
                        <div className="flex-1 min-w-0">
                          <p className="text-sm">{log.message}</p>
                          <p className="text-xs text-muted-foreground">
                            {new Date(log.timestamp).toLocaleTimeString()}
                          </p>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </ScrollArea>
            </CardContent>
          </Card>
        </CollapsibleContent>
      </Collapsible>
    </div>
  );
};

export default QBSDMonitor;
