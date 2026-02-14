import React, { useState, useEffect, useRef } from 'react';
import { debug } from '@/utils/debug';
import {
  Play,
  Square,
  CheckCircle2,
  AlertCircle,
  Loader2,
  XCircle,
  ChevronDown,
  ArrowLeft,
  Activity,
} from 'lucide-react';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
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

import { schemaAPI } from '../../services/api';
import { webSocketService } from '../../services/websocket';
import { WebSocketMessage, ColumnInfo } from '../../types';

interface ContinueDiscoveryMonitorProps {
  sessionId: string;
  operationId: string;
  initialColumns: string[];
  onComplete: (newColumns: ColumnInfo[]) => void;
  onCancel: () => void;
  onError: (error: string) => void;
}

interface LogEntry {
  timestamp: string;
  level: 'info' | 'warning' | 'error' | 'success';
  message: string;
}

type Phase = 'discovery' | 'review' | 'extraction';
type Status = 'running' | 'completed' | 'failed' | 'stopped';

const ContinueDiscoveryMonitor: React.FC<ContinueDiscoveryMonitorProps> = ({
  sessionId,
  operationId,
  initialColumns,
  onComplete,
  onCancel,
  onError,
}) => {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [logsOpen, setLogsOpen] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState<'connecting' | 'connected' | 'disconnected'>('connecting');

  // Processing state
  const [phase, setPhase] = useState<Phase>('discovery');
  const [status, setStatus] = useState<Status>('running');
  const [currentMessage, setCurrentMessage] = useState('Initializing schema discovery...');
  const [isStopping, setIsStopping] = useState(false);

  // Discovery phase progress
  const [discoveryProgress, setDiscoveryProgress] = useState({
    iteration: 0,
    maxIterations: 0,  // Will be set from backend
    columnsDiscovered: 0,
    progress: 0,
  });

  // Extraction phase progress
  const [extractionProgress, setExtractionProgress] = useState({
    processedDocs: 0,
    totalDocs: 0,
    progress: 0,
  });

  // New columns found
  const [newColumns, setNewColumns] = useState<ColumnInfo[]>([]);

  const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Poll for status updates (fallback if WebSocket not working)
  useEffect(() => {
    const pollStatus = async () => {
      try {
        const statusData = await schemaAPI.continueDiscovery.getStatus(sessionId, operationId);

        setPhase(statusData.phase as Phase);

        if (statusData.phase === 'discovery') {
          setDiscoveryProgress({
            iteration: statusData.current_batch,
            maxIterations: statusData.total_batches || 0,
            columnsDiscovered: statusData.new_columns?.length || 0,
            progress: statusData.progress * 100,
          });
          setCurrentMessage(`Analyzing documents... (${Math.round(statusData.progress * 100)}%)`);
        } else if (statusData.phase === 'extraction') {
          setExtractionProgress({
            processedDocs: statusData.processed_documents,
            totalDocs: statusData.total_documents,
            progress: statusData.progress * 100,
          });
          setCurrentMessage(`Extracting values... (${statusData.processed_documents}/${statusData.total_documents} documents)`);
        }

        if (statusData.status === 'completed') {
          setStatus('completed');
          if (statusData.phase === 'extraction') {
            // Transform new columns to ColumnInfo format
            const columns: ColumnInfo[] = statusData.new_columns.map(nc => ({
              name: nc.name,
              definition: nc.definition,
              rationale: nc.rationale,
              allowed_values: nc.allowed_values,
              source_document: nc.source_document,
              discovery_iteration: nc.discovery_iteration,
            }));
            setNewColumns(columns);
            addLog('success', 'Extraction completed successfully!');
            debug.log('ContinueDiscoveryMonitor: onComplete called with columns:', columns);
            onComplete(columns);
          } else if (statusData.phase === 'discovery') {
            // Transform new columns and call onComplete even for discovery phase
            // so columns appear in Schema tab immediately (with null values)
            const columns: ColumnInfo[] = statusData.new_columns.map(nc => ({
              name: nc.name,
              definition: nc.definition,
              rationale: nc.rationale,
              allowed_values: nc.allowed_values,
              source_document: nc.source_document,
              discovery_iteration: nc.discovery_iteration,
            }));
            setNewColumns(columns);
            setCurrentMessage(`Discovery complete! Found ${statusData.new_columns.length} new columns.`);
            addLog('success', `Discovered ${statusData.new_columns.length} new columns`);
            debug.log('ContinueDiscoveryMonitor: onComplete called with columns (discovery):', columns);
            onComplete(columns);
          }
          if (pollIntervalRef.current) {
            clearInterval(pollIntervalRef.current);
            pollIntervalRef.current = null;
          }
        } else if (statusData.status === 'failed') {
          setStatus('failed');
          addLog('error', statusData.error || 'Operation failed');
          onError(statusData.error || 'Operation failed');
          if (pollIntervalRef.current) {
            clearInterval(pollIntervalRef.current);
            pollIntervalRef.current = null;
          }
        } else if (statusData.status === 'stopped') {
          setStatus('stopped');
          addLog('warning', 'Operation stopped by user');
          if (pollIntervalRef.current) {
            clearInterval(pollIntervalRef.current);
            pollIntervalRef.current = null;
          }
        }
      } catch (err) {
        console.error('Failed to poll status:', err);
      }
    };

    pollStatus();
    pollIntervalRef.current = setInterval(pollStatus, 2000);

    return () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
      }
    };
  }, [sessionId, operationId]);

  // WebSocket for real-time updates
  useEffect(() => {
    const handleMessage = (message: WebSocketMessage) => {
      if (message.type === 'connected') {
        setConnectionStatus('connected');
        // Don't log — connection status is shown by the indicator
      } else if (message.type === 'disconnected') {
        setConnectionStatus('disconnected');
        // Don't log — connection status is shown by the indicator
      } else if (message.type === 'continue_discovery_progress') {
        const data = message.data as any;
        if (data?.phase === 'discovery') {
          setPhase('discovery');
          setDiscoveryProgress({
            iteration: data.iteration || 0,
            maxIterations: data.max_iterations || 0,
            columnsDiscovered: data.current_columns || 0,
            progress: (data.progress || 0) * 100,
          });
          setCurrentMessage(data.message || 'Discovering schema...');
          // Only log meaningful phase transitions, not every progress tick
          if (data.iteration && data.max_iterations) {
            addLog('info', `Batch ${data.iteration}/${data.max_iterations}: Analyzing documents...`);
          }
        } else if (data?.phase === 'extraction') {
          setPhase('extraction');
          setExtractionProgress({
            processedDocs: data.processed || 0,
            totalDocs: data.total || 0,
            progress: (data.progress || 0) * 100,
          });
          setCurrentMessage(data.message || 'Extracting values...');
        }
      } else if (message.type === 'continue_discovery_completed') {
        setStatus('completed');
        addLog('success', 'All done!');
      } else if (message.type === 'continue_discovery_stopped') {
        setStatus('stopped');
        setIsStopping(false);
        addLog('warning', 'Stopped by user.');
      } else if (message.type === 'log') {
        const data = message.data as any;
        addLog(data?.level || 'info', data?.message || 'Log message');
      }
    };

    webSocketService.connect(sessionId, 'progress');
    const cleanup = webSocketService.addMessageHandler(handleMessage);

    return () => {
      cleanup();
    };
  }, [sessionId]);

  const addLog = (level: LogEntry['level'], message: string) => {
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
        },
        ...prev.slice(0, 99),
      ];
    });
  };

  const handleStop = async () => {
    setIsStopping(true);
    try {
      await schemaAPI.continueDiscovery.stop(sessionId, operationId);
      // API returned — stop flag is set. Transition immediately.
      setStatus('stopped');
      setIsStopping(false);
      addLog('warning', 'Stop requested — processing will stop at the next checkpoint.');
    } catch (err: any) {
      addLog('error', 'Failed to stop operation');
      setIsStopping(false);
    }
  };

  const getStatusIcon = () => {
    if (status === 'completed') return <CheckCircle2 className="h-16 w-16 text-green-500" />;
    if (status === 'failed') return <XCircle className="h-16 w-16 text-red-500" />;
    if (status === 'stopped') return <Square className="h-16 w-16 text-yellow-500" />;
    return <Loader2 className="h-16 w-16 text-primary animate-spin" />;
  };

  const getStatusMessage = () => {
    if (status === 'completed') return 'Operation Complete';
    if (status === 'failed') return 'Operation Failed';
    if (status === 'stopped') return 'Operation Stopped';
    return phase === 'discovery' ? 'Discovering Schema...' : 'Extracting Values...';
  };

  const getLogLevelColor = (level: LogEntry['level']) => {
    switch (level) {
      case 'success': return 'text-green-600';
      case 'warning': return 'text-yellow-600';
      case 'error': return 'text-red-600';
      default: return 'text-blue-600';
    }
  };

  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="sm" onClick={onCancel}>
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back
          </Button>
          <div>
            <h2 className="text-2xl font-bold">Continue Schema Discovery</h2>
            <p className="text-muted-foreground">
              Starting with {initialColumns.length} existing columns
            </p>
          </div>
        </div>
        <Badge variant={connectionStatus === 'connected' ? 'default' : 'secondary'}>
          {connectionStatus === 'connected' ? 'Live' : 'Polling'}
        </Badge>
      </div>

      {/* Status Hero */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-col items-center justify-center py-8">
            {getStatusIcon()}
            <h3 className="text-xl font-semibold mt-4">{getStatusMessage()}</h3>
            <p className="text-muted-foreground mt-2">{currentMessage}</p>
          </div>
        </CardContent>
      </Card>

      {/* Phase Progress Cards */}
      <div className="grid md:grid-cols-2 gap-4">
        {/* Discovery Phase */}
        <Card className={phase === 'discovery' && status === 'running' ? 'border-2 border-primary' : ''}>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              {phase === 'discovery' && status === 'running' ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : discoveryProgress.columnsDiscovered > 0 || status !== 'running' ? (
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              ) : (
                <div className="h-4 w-4 rounded-full border-2" />
              )}
              Phase 1: Schema Discovery
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Progress value={discoveryProgress.progress} className="mb-2" />
            <div className="flex justify-between text-sm text-muted-foreground">
              <span>
                {discoveryProgress.maxIterations > 0
                  ? `Batch ${discoveryProgress.iteration}/${discoveryProgress.maxIterations}`
                  : 'Starting...'}
              </span>
              <span>{discoveryProgress.columnsDiscovered} columns</span>
            </div>
          </CardContent>
        </Card>

        {/* Extraction Phase */}
        <Card className={phase === 'extraction' && status === 'running' ? 'border-2 border-primary' : ''}>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              {phase === 'extraction' && status === 'running' ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : status === 'completed' ? (
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              ) : (
                <div className="h-4 w-4 rounded-full border-2" />
              )}
              Phase 2: Value Extraction
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Progress value={extractionProgress.progress} className="mb-2" />
            <div className="flex justify-between text-sm text-muted-foreground">
              <span>{extractionProgress.processedDocs}/{extractionProgress.totalDocs} documents</span>
              <span>{Math.round(extractionProgress.progress)}%</span>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Activity Log */}
      <Collapsible open={logsOpen} onOpenChange={setLogsOpen}>
        <CollapsibleTrigger asChild>
          <Button variant="ghost" className="w-full justify-between">
            <div className="flex items-center gap-2">
              <Activity className="h-4 w-4" />
              <span>Activity Log ({logs.length})</span>
            </div>
            <ChevronDown className={`h-4 w-4 transition-transform ${logsOpen ? 'rotate-180' : ''}`} />
          </Button>
        </CollapsibleTrigger>
        <CollapsibleContent>
          <Card className="mt-2">
            <ScrollArea className="h-[200px] p-4">
              {logs.length === 0 ? (
                <p className="text-muted-foreground text-sm">No activity yet...</p>
              ) : (
                <div className="space-y-2">
                  {logs.map((log, i) => (
                    <div key={i} className="flex gap-2 text-sm">
                      <span className="text-muted-foreground whitespace-nowrap">
                        {new Date(log.timestamp).toLocaleTimeString()}
                      </span>
                      <span className={getLogLevelColor(log.level)}>{log.message}</span>
                    </div>
                  ))}
                </div>
              )}
            </ScrollArea>
          </Card>
        </CollapsibleContent>
      </Collapsible>

      {/* Actions */}
      <div className="flex justify-end gap-2">
        {status === 'running' && (
          <Button variant="destructive" onClick={handleStop} disabled={isStopping}>
            {isStopping ? (
              <Loader2 className="h-4 w-4 animate-spin mr-2" />
            ) : (
              <Square className="h-4 w-4 mr-2" />
            )}
            Stop
          </Button>
        )}
        {(status === 'completed' || status === 'stopped' || status === 'failed') && (
          <Button onClick={onCancel}>
            Close
          </Button>
        )}
      </div>
    </div>
  );
};

export default ContinueDiscoveryMonitor;
