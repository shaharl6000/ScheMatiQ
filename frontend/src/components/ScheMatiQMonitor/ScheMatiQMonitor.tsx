import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Play,
  Square,
  CheckCircle2,
  AlertCircle,
  Info,
  AlertTriangle,
  Activity,
  Loader2,
  XCircle,
  ChevronDown,
  Clock,
  Layers,
  ArrowRight,
  Plus,
  X,
  Pencil,
} from 'lucide-react';
import { useQuery, useQueryClient } from 'react-query';

import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Progress } from '@/components/ui/progress';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible';

import { schematiqAPI, observationUnitAPI, loadAPI } from '../../services/api';
import { webSocketService } from '../../services/websocket';
import { ScheMatiQStatus, WebSocketMessage, ProgressData, SchemaCompletionData, RowCompletionData, LogData, StoppedData, ObservationUnitReadyData } from '../../types';

interface ScheMatiQMonitorProps {
  sessionId: string;
  autoStarted?: boolean;
  initialCapacityMessage?: string;
}

interface LogEntry {
  timestamp: string;
  level: 'info' | 'warning' | 'error' | 'success';
  message: string;
  details?: any;
}

// Processing state that changes IMMEDIATELY on user action
type ProcessingState = 'idle' | 'starting' | 'schema' | 'extraction' | 'completed' | 'error' | 'stopped' | 'observation_unit_review';

const ScheMatiQMonitor: React.FC<ScheMatiQMonitorProps> = ({ sessionId, autoStarted = false, initialCapacityMessage = '' }) => {
  const queryClient = useQueryClient();
  const cachedStatus = queryClient.getQueryData<ScheMatiQStatus>(['schematiq-status', sessionId]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [connectionStatus, setConnectionStatus] = useState<'connecting' | 'connected' | 'disconnected' | 'reconnecting'>('connecting');
  const [logsOpen, setLogsOpen] = useState(false);

  // Main processing state - changes IMMEDIATELY on Start click
  const [processingState, setProcessingState] = useState<ProcessingState>(() => {
    if (initialCapacityMessage) return 'idle';
    if (cachedStatus) {
      if (cachedStatus.schema_completed && cachedStatus.status === 'processing') return 'extraction';
      if (cachedStatus.status === 'processing') return 'schema';
      if (cachedStatus.status === 'completed') return 'completed';
      if (cachedStatus.status === 'stopped') return 'stopped';
      if (cachedStatus.status === 'error') return 'error';
      if (cachedStatus.status === 'observation_unit_review') return 'observation_unit_review';
    }
    return autoStarted ? 'starting' : 'idle';
  });
  const [currentStepMessage, setCurrentStepMessage] = useState<string>(() => {
    if (cachedStatus?.status === 'processing') {
      return cachedStatus.schema_completed ? 'Extracting values' : 'Discovering schema...';
    }
    if (autoStarted && !initialCapacityMessage) return 'Initializing...';
    return '';
  });
  const [errorMessage, setErrorMessage] = useState<string>('');
  const [capacityMessage, setCapacityMessage] = useState<string>(initialCapacityMessage);
  const [quotaExceeded, setQuotaExceeded] = useState(false);

  // Phase tracking state
  const [schemaProgress, setSchemaProgress] = useState(() => ({
    iteration: 0,
    maxIterations: 5,
    columnsDiscovered: cachedStatus?.columns_discovered || 0,
    isComplete: cachedStatus?.schema_completed || false,
  }));
  const [extractionProgress, setExtractionProgress] = useState(() => ({
    processedDocs: cachedStatus?.processed_documents || 0,
    totalDocs: cachedStatus?.total_documents || 0,
    isComplete: cachedStatus?.status === 'completed',
  }));

  // Stopped state info
  const [stoppedInfo, setStoppedInfo] = useState<{
    schemaSaved: boolean;
    dataRowsSaved: number;
  } | null>(null);

  // Stop button loading state - shows immediate feedback when user clicks stop
  const [isStopping, setIsStopping] = useState(false);

  // Observation unit review state
  const [reviewObsUnit, setReviewObsUnit] = useState<ObservationUnitReadyData | null>(null);
  const [editName, setEditName] = useState('');
  const [editDefinition, setEditDefinition] = useState('');
  const [editExamples, setEditExamples] = useState<string[]>([]);
  const [newExample, setNewExample] = useState('');
  const [isResuming, setIsResuming] = useState(false);
  const [obsUnitEdited, setObsUnitEdited] = useState(false);

  // Track ScheMatiQ start time for elapsed display
  const startTimeRef = useRef<number | null>(null);

  // Track last logged phase to avoid repeating phase-transition messages
  const lastLoggedPhaseRef = useRef<string | null>(null);

  // Fetch ScheMatiQ status - disable polling when WebSocket is connected
  const { data: status, isLoading } = useQuery(
    ['schematiq-status', sessionId],
    () => schematiqAPI.getStatus(sessionId),
    {
      refetchInterval: () => {
        // Don't poll if WebSocket is connected - rely on real-time updates
        if (webSocketService.isConnected()) return false;
        return 2000; // Fallback polling if WebSocket disconnects
      },
    }
  );

  // Sync processingState with backend status (for page refreshes and tab switch remounts)
  useEffect(() => {
    if (status?.status === 'processing' && processingState === 'idle') {
      setProcessingState('starting');
    } else if (status?.status === 'completed' && processingState !== 'completed') {
      setProcessingState('completed');
    } else if (status?.status === 'stopped' && processingState !== 'stopped') {
      setProcessingState('stopped');
    } else if (status?.status === 'observation_unit_review' && processingState !== 'observation_unit_review') {
      setProcessingState('observation_unit_review');
      // Load observation unit from session if we don't have it (page refresh case)
      if (!reviewObsUnit) {
        loadAPI.getSession(sessionId).then(session => {
          if (session?.observation_unit) {
            const obsData: ObservationUnitReadyData = {
              name: session.observation_unit.name,
              definition: session.observation_unit.definition,
              example_names: session.observation_unit.example_names || [],
            };
            setReviewObsUnit(obsData);
            setEditName(obsData.name);
            setEditDefinition(obsData.definition);
            setEditExamples(obsData.example_names || []);
            setObsUnitEdited(false);
          }
        }).catch(() => { /* ignore - will retry on next poll */ });
      }
    } else if (status?.status === 'error') {
      setProcessingState('error');
      setErrorMessage(status.error_message || 'An error occurred');
    }

    // Recover phase state from polling (handles tab switch remount)
    if (status?.schema_completed && !schemaProgress.isComplete) {
      setSchemaProgress(prev => ({
        ...prev,
        isComplete: true,
        columnsDiscovered: status.columns_discovered || prev.columnsDiscovered,
      }));
      // If currently processing, we must be in extraction phase
      if (status.status === 'processing') {
        setProcessingState('extraction');
      }
    }

    // Recover extraction progress from polled status
    if (status?.total_documents && status.total_documents > 0) {
      setExtractionProgress(prev => ({
        ...prev,
        totalDocs: status.total_documents || prev.totalDocs,
        processedDocs: status.processed_documents || prev.processedDocs,
        isComplete: prev.isComplete || status.status === 'completed',
      }));
    }
  }, [status?.status, status?.schema_completed, status?.columns_discovered, status?.total_documents, status?.processed_documents]);

  // WebSocket connection status is now updated via message handlers below
  // (removed redundant 1-second polling interval)

  // WebSocket connection for real-time updates
  useEffect(() => {
    const handleMessage = async (message: WebSocketMessage) => {
      if (message.type === 'connected') {
        setConnectionStatus('connected');
        // Don't log — connection status is shown by the indicator
      } else if (message.type === 'disconnected') {
        setConnectionStatus('disconnected');
        // Don't log — connection status is shown by the indicator
      } else if (message.type === 'reconnecting') {
        setConnectionStatus('reconnecting');
        // Only log if it's a repeated attempt (not the first automatic one)
        const msg = message.message || '';
        if (msg.includes('2/') || msg.includes('3/') || msg.includes('4/') || msg.includes('5/')) {
          addLog('warning', 'Reconnecting to server...');
        }
      } else if (message.type === 'progress') {
        const progressData = message.data as ProgressData;
        const stepName = progressData?.current_step || 'Processing...';
        setCurrentStepMessage(stepName);

        // Only log phase transitions once, not every progress tick
        const lower = stepName.toLowerCase();
        if (lower.includes('schema') && !lower.includes('complete')) {
          setProcessingState('schema');
          if (lastLoggedPhaseRef.current !== 'schema') {
            lastLoggedPhaseRef.current = 'schema';
            addLog('info', 'Starting schema discovery...');
          }
          const details = progressData?.details as Record<string, unknown> | undefined;
          if (details?.iteration) {
            setSchemaProgress(prev => ({
              ...prev,
              iteration: details.iteration as number,
              maxIterations: (details.max_iterations as number) || 5,
              columnsDiscovered: (details.columns_discovered as number) || prev.columnsDiscovered
            }));
          }
        } else if (lower.includes('value extraction') || lower.includes('extracting')) {
          setProcessingState('extraction');
          if (lastLoggedPhaseRef.current !== 'extraction') {
            lastLoggedPhaseRef.current = 'extraction';
            addLog('info', 'Starting value extraction...');
          }
        } else if (lower.includes('finaliz')) {
          setProcessingState('completed');
          setExtractionProgress(prev => ({ ...prev, isComplete: true }));
        }

        queryClient.invalidateQueries(['schematiq-status', sessionId]);
      } else if (message.type === 'error') {
        setProcessingState('error');
        setErrorMessage(message.message || 'An error occurred');
        addLog('error', message.message || 'An error occurred', message.data);
      } else if (message.type === 'quota_exceeded') {
        setProcessingState('idle');
        setQuotaExceeded(true);
        addLog('warning', message.message || 'API usage limit reached', message.data);
      } else if (message.type === 'schema_progress') {
        const data = message.data as unknown as Record<string, any>;
        const iteration = data.iteration as number;
        const maxIterations = data.max_iterations as number;
        const newCols = data.new_columns as string[] | undefined;
        setSchemaProgress(prev => ({
          ...prev,
          columnsDiscovered: data.columns_discovered || prev.columnsDiscovered,
          iteration: iteration || prev.iteration,
          maxIterations: maxIterations || prev.maxIterations,
        }));

        // Log batch results with column names
        if (newCols && newCols.length > 0) {
          const colList = newCols.length <= 5
            ? newCols.join(', ')
            : `${newCols.slice(0, 5).join(', ')} and ${newCols.length - 5} more`;
          addLog('success', `Batch ${iteration}/${maxIterations}: Found ${newCols.length} new column${newCols.length > 1 ? 's' : ''} \u2014 ${colList}`);
        } else if (iteration) {
          addLog('info', `Batch ${iteration}/${maxIterations}: No new columns found (schema stable)`);
        }
      } else if (message.type === 'completed') {
        const data = message.data as any;
        const elapsed = data?.elapsed_seconds;
        const elapsedStr = elapsed ? ` Finished in ${formatElapsed(elapsed)}.` : '';
        addLog('success', `All done!${elapsedStr}`, message.data);
        setProcessingState('completed');
        setSchemaProgress(prev => ({ ...prev, isComplete: true }));
        setExtractionProgress(prev => ({
          ...prev,
          isComplete: true,
          totalDocs: data?.total_documents || prev.totalDocs,
          processedDocs: data?.total_documents || prev.processedDocs,
        }));
        queryClient.invalidateQueries(['schematiq-status', sessionId]);
      } else if (message.type === 'schema_completed') {
        const schemaData = message.data as SchemaCompletionData;
        addLog('success', `Schema ready! Found ${schemaData?.total_columns || 'several'} columns`, message.data);

        setSchemaProgress(prev => ({
          ...prev,
          columnsDiscovered: schemaData?.total_columns || prev.columnsDiscovered,
          isComplete: true
        }));
        setProcessingState('extraction');

        queryClient.invalidateQueries(['schematiq-status', sessionId]);
        queryClient.invalidateQueries(['session', sessionId, 'schematiq']);
        setTimeout(() => {
          queryClient.refetchQueries(['session', sessionId, 'schematiq']);
        }, 500);
      } else if (message.type === 'row_completed') {
        const rowData = message.data as RowCompletionData;
        const names = rowData?.document_names;
        if (names && names.length === 1) {
          addLog('info', `Processed ${names[0]} (${rowData?.row_index}/${rowData?.total_rows})`);
        } else if (names && names.length > 1) {
          addLog('info', `Processed ${names.length} documents (${rowData?.row_index}/${rowData?.total_rows})`);
        } else {
          addLog('info', `Processed document ${rowData?.row_index}/${rowData?.total_rows}`);
        }

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
      } else if (message.type === 'observation_unit_ready') {
        const obsData = message.data as ObservationUnitReadyData;
        addLog('info', `Observation unit discovered: "${obsData?.name}". Review before continuing.`);
        setProcessingState('observation_unit_review');
        if (obsData) {
          setReviewObsUnit(obsData);
          setEditName(obsData.name);
          setEditDefinition(obsData.definition);
          setEditExamples(obsData.example_names || []);
          setObsUnitEdited(false);
        }
        queryClient.invalidateQueries(['qbsd-status', sessionId]);
      } else if (message.type === 'stopped') {
        const stoppedData = message.data as StoppedData;
        const schemaSaved = stoppedData?.schema_saved || false;
        const rows = stoppedData?.data_rows_saved || 0;
        if (schemaSaved) {
          addLog('warning', `Stopped by user. Schema saved${rows > 0 ? `, ${rows} row${rows > 1 ? 's' : ''} extracted` : ''}.`);
        } else {
          addLog('warning', 'Stopped by user before schema discovery completed.');
        }

        // Refetch session data FIRST to ensure columns are loaded before UI updates
        await queryClient.refetchQueries(['session', sessionId, 'schematiq']);
        await queryClient.refetchQueries(['schematiq-status', sessionId]);

        // THEN update processing state (after data is available)
        setProcessingState('stopped');
        setIsStopping(false);  // Reset stop button state
        setStoppedInfo({
          schemaSaved: schemaSaved,
          dataRowsSaved: rows
        });
        // Update schema progress if we have partial schema
        if (schemaSaved) {
          setSchemaProgress(prev => ({ ...prev, isComplete: true }));
        }
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

  // Auto-dismiss capacity message after 30 seconds
  useEffect(() => {
    if (!capacityMessage) return;
    const timer = setTimeout(() => setCapacityMessage(''), 30000);
    return () => clearTimeout(timer);
  }, [capacityMessage]);

  const addLog = (level: LogEntry['level'], message: string, details?: any) => {
    setLogs(prev => {
      // Skip duplicate if most recent log has the same message
      if (prev.length > 0 && prev[0].message === message) {
        return prev;
      }
      return [
        {
          timestamp: new Date().toISOString(),
          level,
          message,
          details,
        },
        ...prev.slice(0, 99),
      ];
    });
  };

  const formatElapsed = (seconds: number): string => {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
  };

  const handleStart = async () => {
    if (processingState !== 'idle' && processingState !== 'error' && processingState !== 'completed' && processingState !== 'stopped') {
      return;
    }

    // IMMEDIATELY set to starting state - don't wait for anything
    setProcessingState('starting');
    setCurrentStepMessage('Initializing...');
    setErrorMessage('');
    setCapacityMessage('');
    setQuotaExceeded(false);
    setStoppedInfo(null);
    startTimeRef.current = Date.now();
    lastLoggedPhaseRef.current = null;

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
      await schematiqAPI.run(sessionId);
      addLog('info', 'ScheMatiQ execution started');
      // Stay in 'starting' state until we receive progress updates
    } catch (error: any) {
      const status = error?.response?.status;
      const detail = error?.response?.data?.detail;

      if (status === 429) {
        // Quota exceeded — show orange banner
        setProcessingState('idle');
        setQuotaExceeded(true);
        addLog('warning', 'API usage limit reached');
      } else if (status === 503) {
        // Server busy — show friendly amber banner, not error state
        setProcessingState('idle');
        setCapacityMessage(detail || 'The server is currently busy processing other requests. Please try again in a few minutes.');
        addLog('warning', 'Server busy — please retry shortly');
      } else {
        setProcessingState('error');
        const message = detail || error.message || 'Failed to start ScheMatiQ';
        setErrorMessage(message);
        addLog('error', `Failed to start ScheMatiQ: ${message}`);
      }
    }
  };

  const handleStop = async () => {
    setIsStopping(true);
    try {
      await schematiqAPI.stop(sessionId);
      // API returned — stop flag is set. Transition immediately.
      setProcessingState('stopped');
      setIsStopping(false);
      addLog('warning', 'Stop requested — processing will stop at the next checkpoint.');
    } catch (error: any) {
      addLog('error', `Failed to stop ScheMatiQ: ${error.message}`);
      setIsStopping(false);
    }
  };

  const handleAddExample = useCallback(() => {
    const trimmed = newExample.trim();
    if (!trimmed || editExamples.includes(trimmed)) return;
    if (editExamples.length >= 20) return;
    setEditExamples(prev => [...prev, trimmed]);
    setNewExample('');
    setObsUnitEdited(true);
  }, [newExample, editExamples]);

  const handleRemoveExample = useCallback((index: number) => {
    setEditExamples(prev => prev.filter((_, i) => i !== index));
    setObsUnitEdited(true);
  }, []);

  const handleResume = async (skipEdit = false) => {
    setIsResuming(true);
    try {
      // If the user edited the observation unit, save changes first
      if (!skipEdit && obsUnitEdited && editName.trim() && editDefinition.trim()) {
        await observationUnitAPI.updateDefinition(sessionId, {
          name: editName.trim(),
          definition: editDefinition.trim(),
          example_names: editExamples.length > 0 ? editExamples : undefined,
        });
        addLog('success', `Observation unit updated to "${editName.trim()}"`);
      }

      // Resume the pipeline
      await qbsdAPI.resume(sessionId);
      addLog('info', 'Resuming schema generation...');

      // Transition to starting/schema state
      setProcessingState('starting');
      setCurrentStepMessage('Resuming pipeline...');
      startTimeRef.current = Date.now();
      lastLoggedPhaseRef.current = null;

      // Reset progress for the new run
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
    } catch (error: any) {
      const detail = error?.response?.data?.detail;
      const message = detail || error.message || 'Failed to resume';
      setErrorMessage(message);
      setProcessingState('error');
      addLog('error', `Failed to resume: ${message}`);
    } finally {
      setIsResuming(false);
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

  if (isLoading && !autoStarted) {
    return (
      <div className="flex justify-center items-center min-h-[400px]">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="h-12 w-12 animate-spin text-primary" />
          <p className="text-muted-foreground">Loading ScheMatiQ status...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* HERO PROCESSING SECTION */}
      <Card className="border-2">
        <CardContent className="py-6 flex flex-col items-center justify-center min-h-[160px]">
          {/* IDLE STATE */}
          {processingState === 'idle' && (
            <>
              {quotaExceeded ? (
                <>
                  <div className="w-14 h-14 rounded-full bg-orange-100 flex items-center justify-center mb-4">
                    <AlertTriangle className="h-7 w-7 text-orange-600" />
                  </div>
                  <p className="text-xl font-semibold text-orange-600 mb-1">Service Temporarily Unavailable</p>
                  <p className="text-sm text-muted-foreground mb-3 text-center max-w-md">
                    The system has reached its processing capacity and is unable to start new sessions at this time.
                  </p>
                  <p className="text-xs text-muted-foreground text-center max-w-sm">
                    Please try again later or contact us for assistance.
                  </p>
                </>
              ) : capacityMessage ? (
                <>
                  <div className="w-14 h-14 rounded-full bg-amber-100 flex items-center justify-center mb-4">
                    <Clock className="h-7 w-7 text-amber-600" />
                  </div>
                  <p className="text-xl font-semibold text-amber-600 mb-1">Server Busy</p>
                  <p className="text-sm text-muted-foreground mb-4 text-center max-w-md">
                    {capacityMessage}
                  </p>
                  <Button size="lg" onClick={handleStart} className="px-8">
                    <Play className="h-5 w-5 mr-2" />
                    Try Again
                  </Button>
                </>
              ) : (
                <>
                  <div className="w-14 h-14 rounded-full bg-muted flex items-center justify-center mb-4">
                    <Play className="h-7 w-7 text-muted-foreground" />
                  </div>
                  <p className="text-xl font-semibold text-muted-foreground mb-1">Ready to Start</p>
                  <p className="text-sm text-muted-foreground mb-4">
                    Click the button below to begin ScheMatiQ execution
                  </p>
                  <Button size="lg" onClick={handleStart} className="px-8">
                    <Play className="h-5 w-5 mr-2" />
                    Start ScheMatiQ
                  </Button>
                </>
              )}
            </>
          )}

          {/* PROCESSING STATES (starting, schema, extraction) */}
          {isProcessing && (
            <>
              <div className="relative mb-4">
                <Loader2 className="h-14 w-14 animate-spin text-primary" />
                <div className="absolute inset-0 flex items-center justify-center">
                  <div className="h-8 w-8 rounded-full bg-primary/10 animate-pulse" />
                </div>
              </div>
              <p className="text-xl font-semibold mb-1">
                {processingState === 'starting' && 'Starting...'}
                {processingState === 'schema' && 'Discovering Schema...'}
                {processingState === 'extraction' && 'Extracting Values...'}
              </p>
              <p className="text-muted-foreground mb-4 text-center max-w-md">
                {currentStepMessage || 'Processing your documents...'}
              </p>
              <Button variant="outline" onClick={handleStop} disabled={isStopping}>
                {isStopping ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    Stopping...
                  </>
                ) : (
                  <>
                    <Square className="h-4 w-4 mr-2" />
                    Stop
                  </>
                )}
              </Button>
            </>
          )}

          {/* COMPLETED STATE */}
          {processingState === 'completed' && (
            <>
              <div className="w-14 h-14 rounded-full bg-green-100 flex items-center justify-center mb-4">
                <CheckCircle2 className="h-8 w-8 text-green-600" />
              </div>
              <p className="text-xl font-semibold text-green-600 mb-1">Completed Successfully!</p>
              <p className="text-muted-foreground text-center max-w-md">
                {schemaProgress.columnsDiscovered > 0 && extractionProgress.totalDocs > 0
                  ? `Discovered ${schemaProgress.columnsDiscovered} columns from ${extractionProgress.totalDocs} documents`
                  : 'Schema discovery and value extraction finished'}
              </p>
            </>
          )}

          {/* ERROR STATE */}
          {processingState === 'error' && (
            <>
              <div className="w-14 h-14 rounded-full bg-red-100 flex items-center justify-center mb-4">
                <XCircle className="h-8 w-8 text-red-600" />
              </div>
              <p className="text-xl font-semibold text-red-600 mb-1">Error Occurred</p>
              <p className="text-muted-foreground mb-4 text-center max-w-md">
                {errorMessage || 'An unexpected error occurred'}
              </p>
              <Button onClick={handleStart}>
                <Play className="h-4 w-4 mr-2" />
                Try Again
              </Button>
            </>
          )}

          {/* STOPPED STATE */}
          {processingState === 'stopped' && (
            <>
              <div className="w-14 h-14 rounded-full bg-yellow-100 flex items-center justify-center mb-4">
                <Square className="h-8 w-8 text-yellow-600" />
              </div>
              <p className="text-xl font-semibold text-yellow-600 mb-1">Processing Stopped</p>
              {stoppedInfo ? (
                <>
                  <p className="text-muted-foreground mb-2 text-center max-w-md">
                    {stoppedInfo.schemaSaved
                      ? `Schema discovered. ${stoppedInfo.dataRowsSaved > 0
                          ? `${stoppedInfo.dataRowsSaved} data rows extracted.`
                          : 'No data extracted yet.'}`
                      : 'Stopped before schema discovery completed.'}
                  </p>
                  {(stoppedInfo.schemaSaved || stoppedInfo.dataRowsSaved > 0) && (
                    <p className="text-sm text-muted-foreground">
                      You can view and export partial results in the Data and Schema tabs.
                    </p>
                  )}
                </>
              ) : (
                <p className="text-muted-foreground mb-2 text-center max-w-md">
                  Wrapping up current operation...
                </p>
              )}
            </>
          )}

          {/* OBSERVATION UNIT REVIEW STATE */}
          {processingState === 'observation_unit_review' && reviewObsUnit && (
            <div className="w-full max-w-lg">
              <div className="flex flex-col items-center mb-4">
                <div className="w-14 h-14 rounded-full bg-purple-100 flex items-center justify-center mb-3">
                  <Layers className="h-7 w-7 text-purple-600" />
                </div>
                <p className="text-xl font-semibold text-purple-700 mb-1">Review Observation Unit</p>
                <p className="text-sm text-muted-foreground text-center">
                  The observation unit defines what each row in your table represents.
                  Review and optionally edit before schema generation.
                </p>
              </div>

              <div className="space-y-3 text-left">
                {/* Name */}
                <div className="space-y-1">
                  <Label htmlFor="obs-name" className="text-sm font-medium">Name</Label>
                  <Input
                    id="obs-name"
                    value={editName}
                    onChange={(e) => { setEditName(e.target.value); setObsUnitEdited(true); }}
                    placeholder="e.g., Model, Protein, Study"
                  />
                </div>

                {/* Definition */}
                <div className="space-y-1">
                  <Label htmlFor="obs-definition" className="text-sm font-medium">Definition</Label>
                  <Textarea
                    id="obs-definition"
                    value={editDefinition}
                    onChange={(e) => { setEditDefinition(e.target.value); setObsUnitEdited(true); }}
                    placeholder="Describe what constitutes a single row..."
                    rows={3}
                    className="resize-none"
                  />
                  <p className="text-xs text-muted-foreground">
                    {editDefinition.length}/500 characters
                  </p>
                </div>

                {/* Example Names */}
                <div className="space-y-1">
                  <Label className="text-sm font-medium">Example Names</Label>
                  {editExamples.length > 0 && (
                    <div className="flex flex-wrap gap-1.5 mb-1.5">
                      {editExamples.map((ex, i) => (
                        <Badge key={i} variant="secondary" className="gap-1 pr-1">
                          {ex}
                          <button
                            onClick={() => handleRemoveExample(i)}
                            className="ml-0.5 hover:text-destructive rounded-full"
                          >
                            <X className="h-3 w-3" />
                          </button>
                        </Badge>
                      ))}
                    </div>
                  )}
                  <div className="flex gap-2">
                    <Input
                      value={newExample}
                      onChange={(e) => setNewExample(e.target.value)}
                      placeholder="Add an example..."
                      className="flex-1"
                      onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); handleAddExample(); } }}
                    />
                    <Button variant="outline" size="sm" onClick={handleAddExample} disabled={!newExample.trim()}>
                      <Plus className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              </div>

              {/* Action Buttons */}
              <div className="flex flex-col gap-2 mt-5">
                <Button
                  size="lg"
                  onClick={() => handleResume(false)}
                  disabled={isResuming || !editName.trim() || !editDefinition.trim()}
                  className="w-full"
                >
                  {isResuming ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      Resuming...
                    </>
                  ) : (
                    <>
                      <ArrowRight className="h-4 w-4 mr-2" />
                      {obsUnitEdited ? 'Save & Continue to Schema Generation' : 'Continue to Schema Generation'}
                    </>
                  )}
                </Button>
                {obsUnitEdited && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      // Reset edits to original values
                      setEditName(reviewObsUnit.name);
                      setEditDefinition(reviewObsUnit.definition);
                      setEditExamples(reviewObsUnit.example_names || []);
                      setObsUnitEdited(false);
                    }}
                    className="text-muted-foreground"
                  >
                    Discard changes
                  </Button>
                )}
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Observation Unit Info + Edit Button (shown after review step, during or after processing) */}
      {reviewObsUnit && processingState !== 'observation_unit_review' && processingState !== 'idle' && (
        <Card className="bg-purple-50/50 border-purple-200">
          <CardContent className="py-3 px-4 flex items-center justify-between">
            <div className="flex items-center gap-2 min-w-0">
              <Layers className="h-4 w-4 text-purple-600 shrink-0" />
              <div className="min-w-0">
                <span className="text-xs text-purple-600 font-medium">Observation Unit</span>
                <p className="text-sm font-semibold text-purple-900 truncate">{editName || reviewObsUnit.name}</p>
              </div>
            </div>
            <Button
              variant="ghost"
              size="sm"
              className="text-purple-600 hover:text-purple-800 hover:bg-purple-100 shrink-0"
              disabled={isResuming}
              onClick={async () => {
                // If pipeline is running, stop it first
                if (isProcessing) {
                  try {
                    await qbsdAPI.stop(sessionId);
                    addLog('info', 'Stopped pipeline to edit observation unit');
                  } catch (e) {
                    // Ignore stop errors — might already be stopped
                  }
                }
                // Load fresh observation unit from session
                try {
                  const session = await loadAPI.getSession(sessionId);
                  if (session?.observation_unit) {
                    const obsData: ObservationUnitReadyData = {
                      name: session.observation_unit.name,
                      definition: session.observation_unit.definition,
                      example_names: session.observation_unit.example_names || [],
                    };
                    setReviewObsUnit(obsData);
                    setEditName(obsData.name);
                    setEditDefinition(obsData.definition);
                    setEditExamples(obsData.example_names || []);
                    setObsUnitEdited(false);
                  }
                } catch (e) {
                  // Fall back to the cached observation unit data
                }
                setProcessingState('observation_unit_review');
              }}
            >
              <Pencil className="h-3.5 w-3.5 mr-1.5" />
              Edit & Rediscover
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Phase Progress Cards - Side by Side */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Phase 1: Schema Discovery */}
        {/* Schema is complete if explicitly marked OR if overall process completed */}
        {(() => {
          const schemaIsComplete = schemaProgress.isComplete || processingState === 'completed';
          return (
            <Card className={`transition-all ${processingState === 'schema' ? 'border-primary border-2 shadow-md' : ''}`}>
              <CardContent className="pt-4 pb-4">
                <div className="flex items-center justify-between mb-3">
                  <span className="font-medium flex items-center gap-2">
                    {schemaIsComplete ? (
                      <CheckCircle2 className="h-5 w-5 text-green-500" />
                    ) : processingState === 'schema' ? (
                      <Loader2 className="h-5 w-5 animate-spin text-primary" />
                    ) : (
                      <div className="h-5 w-5 rounded-full border-2 border-muted-foreground/30" />
                    )}
                    Phase 1: Schema
                  </span>
                  <Badge variant={schemaIsComplete ? 'success' : processingState === 'schema' ? 'default' : 'secondary'}>
                    {schemaIsComplete
                      ? 'Complete'
                      : processingState === 'schema'
                        ? 'In Progress'
                        : 'Pending'}
                  </Badge>
                </div>
                <Progress
                  value={schemaIsComplete ? 100 : (processingState === 'schema' ? Math.max(10, (schemaProgress.iteration / schemaProgress.maxIterations) * 100) : 0)}
                  className={`h-2 ${processingState === 'schema' && !schemaIsComplete ? 'animate-pulse' : ''}`}
                />
                <p className="text-xs text-muted-foreground mt-2">
                  {schemaIsComplete
                    ? `${schemaProgress.columnsDiscovered} columns discovered`
                    : processingState === 'schema' && schemaProgress.iteration > 0
                      ? `Iteration ${schemaProgress.iteration}/${schemaProgress.maxIterations}`
                      : processingState === 'schema'
                        ? 'Analyzing documents...'
                        : 'Waiting to start'}
                </p>
              </CardContent>
            </Card>
          );
        })()}

        {/* Phase 2: Value Extraction */}
        {/* Extraction is complete if explicitly marked OR if overall process completed */}
        {(() => {
          const extractionIsComplete = extractionProgress.isComplete || processingState === 'completed';
          return (
            <Card className={`transition-all ${processingState === 'extraction' ? 'border-primary border-2 shadow-md' : ''}`}>
              <CardContent className="pt-4 pb-4">
                <div className="flex items-center justify-between mb-3">
                  <span className="font-medium flex items-center gap-2">
                    {extractionIsComplete ? (
                      <CheckCircle2 className="h-5 w-5 text-green-500" />
                    ) : processingState === 'extraction' ? (
                      <Loader2 className="h-5 w-5 animate-spin text-primary" />
                    ) : (
                      <div className="h-5 w-5 rounded-full border-2 border-muted-foreground/30" />
                    )}
                    Phase 2: Extraction
                  </span>
                  <Badge variant={extractionIsComplete ? 'success' : processingState === 'extraction' ? 'default' : 'secondary'}>
                    {extractionIsComplete
                      ? 'Complete'
                      : processingState === 'extraction'
                        ? 'In Progress'
                        : 'Pending'}
                  </Badge>
                </div>
                <Progress
                  value={extractionIsComplete
                    ? 100
                    : extractionProgress.totalDocs > 0
                      ? (extractionProgress.processedDocs / extractionProgress.totalDocs) * 100
                      : processingState === 'extraction' ? 10 : 0}
                  className={`h-2 ${processingState === 'extraction' && !extractionIsComplete ? 'animate-pulse' : ''}`}
                />
                <p className="text-xs text-muted-foreground mt-2">
                  {extractionIsComplete
                    ? `${extractionProgress.totalDocs} documents processed`
                    : processingState === 'extraction' && extractionProgress.totalDocs > 0
                      ? `${extractionProgress.processedDocs}/${extractionProgress.totalDocs} documents`
                      : processingState === 'extraction'
                        ? 'Starting extraction...'
                        : 'Waiting for schema'}
                </p>
              </CardContent>
            </Card>
          );
        })()}
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
                    {isProcessing
                      ? 'New activity will appear here as it happens.'
                      : 'No logs yet. Logs will appear here when ScheMatiQ starts running.'}
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

export default ScheMatiQMonitor;
