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
  RefreshCw,
  ChevronLeft,
  Table,
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

import { schematiqAPI, observationUnitAPI, loadAPI, schemaAPI, configAPI } from '../../services/api';
import { webSocketService } from '../../services/websocket';
import { ScheMatiQStatus, WebSocketMessage, ProgressData, SchemaCompletionData, RowCompletionData, LogData, StoppedData, ObservationUnitReadyData, ReextractionRequest } from '../../types';
import { getApiKeyForProvider, getConfiguredProviders } from '../../utils/apiKeyStorage';
import {
  LLMProviderKey,
  getDefaultModelForProvider,
  getAvailableProviders,
} from '@/constants/llmModels';
import { extractApiErrorMessage } from '../../utils/apiHelpers';

interface ScheMatiQMonitorProps {
  sessionId: string;
  autoStarted?: boolean;
  initialCapacityMessage?: string;
  /** Notify parent when deferred extraction starts (schema-only → full table fill). */
  onExtractionStarted?: (columns: string[], operationId?: string) => void;
  /** Keep WebSocket alive before resume so extraction events stream incrementally. */
  onResumeStarted?: () => void;
}

/** Format observation unit for monitor logs and summary surfaces. */
function formatObservationUnitSummary(obs: {
  name: string;
  definition: string;
  example_names?: string[];
}): string {
  let summary = `"${obs.name}": ${obs.definition}`;
  if (obs.example_names && obs.example_names.length > 0) {
    summary += ` (examples: ${obs.example_names.join(', ')})`;
  }
  return summary;
}

interface LogEntry {
  timestamp: string;
  level: 'info' | 'warning' | 'error' | 'success';
  message: string;
  details?: any;
}

type ReviewEntryMode = 'initial' | 'mid_run' | 'post_run';

// Processing state that changes IMMEDIATELY on user action
type ProcessingState = 'idle' | 'starting' | 'schema' | 'extraction' | 'completed' | 'error' | 'stopped' | 'observation_unit_review';

interface LlmStats {
  total_calls: number;
  current_cost_usd: number;
  estimated_cost_usd: number;
  estimated_calls: number;
}

const ScheMatiQMonitor: React.FC<ScheMatiQMonitorProps> = ({
  sessionId,
  autoStarted = false,
  initialCapacityMessage = '',
  onExtractionStarted,
  onResumeStarted,
}) => {
  const queryClient = useQueryClient();
  const llmStatsCacheKey = ['schematiq-llm-stats', sessionId];
  const cachedStatus = queryClient.getQueryData<ScheMatiQStatus>(['schematiq-status', sessionId]);
  const cachedLlmStats = queryClient.getQueryData<LlmStats>(llmStatsCacheKey);
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
  const [reviewSaveError, setReviewSaveError] = useState<string>('');
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
  // How the user entered observation-unit review (initial pause vs mid-run vs post-run edit)
  const [reviewEntryMode, setReviewEntryMode] = useState<ReviewEntryMode | null>(null);
  const [processingStateBeforeEdit, setProcessingStateBeforeEdit] = useState<ProcessingState | null>(null);
  const [editBaseline, setEditBaseline] = useState<ObservationUnitReadyData | null>(null);

  // LLM Stats state
  const [llmStats, setLlmStats] = useState<LlmStats | null>(cachedLlmStats || null);

  // Whether value extraction was deliberately skipped (schema-only mode)
  const [schemaOnly, setSchemaOnly] = useState<boolean>(cachedStatus?.schema_only ?? false);
  const [isDeferredExtracting, setIsDeferredExtracting] = useState(false);
  const [deferredExtractError, setDeferredExtractError] = useState('');
  const [activeReextractionId, setActiveReextractionId] = useState<string | null>(null);

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
    if (status?.schema_only !== undefined) {
      setSchemaOnly(status.schema_only);
    }
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
            setEditBaseline(obsData);
            setReviewEntryMode('initial');
            setProcessingStateBeforeEdit(null);
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

    // Recover/persist LLM stats from status endpoint (tab switches/remounts).
    if (status?.llm_stats) {
      const stats = status.llm_stats as LlmStats;
      setLlmStats(stats);
      queryClient.setQueryData(llmStatsCacheKey, stats);
    }
  }, [status?.status, status?.schema_completed, status?.columns_discovered, status?.total_documents, status?.processed_documents]);

  // Recover final llm stats for already-completed sessions (after remount/page refresh).
  useEffect(() => {
    if (llmStats || status?.status !== 'completed') return;
    loadAPI.getSession(sessionId).then((session) => {
      const savedStats = session?.metadata?.processing_stats?.llm_stats as LlmStats | undefined;
      if (savedStats) {
        setLlmStats(savedStats);
        queryClient.setQueryData(llmStatsCacheKey, savedStats);
      }
    }).catch(() => {
      // ignore; live websocket updates still cover active runs
    });
  }, [llmStats, status?.status, sessionId, queryClient]);

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
        const details = progressData?.details as Record<string, unknown> | undefined;

        // Keep llm stats updated for every step, not just schema step.
        if (details?.llm_stats) {
          const stats = details.llm_stats as LlmStats;
          setLlmStats(stats);
          queryClient.setQueryData(llmStatsCacheKey, stats);
        }

        // Only log phase transitions once, not every progress tick
        const lower = stepName.toLowerCase();
        if (lower.includes('schema') && !lower.includes('complete')) {
          setProcessingState('schema');
          if (lastLoggedPhaseRef.current !== 'schema') {
            lastLoggedPhaseRef.current = 'schema';
            addLog('info', 'Starting schema discovery...');
          }
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
          if (!schemaOnly) {
            setExtractionProgress(prev => ({ ...prev, isComplete: true }));
          }
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
        if (data?.llm_stats) {
          const stats = data.llm_stats as LlmStats;
          setLlmStats(stats);
          queryClient.setQueryData(llmStatsCacheKey, stats);
          addLog('success', `LLM usage: ${stats.total_calls} calls, $${stats.current_cost_usd.toFixed(4)} total`);
        }
        const elapsed = data?.elapsed_seconds;
        const elapsedStr = elapsed ? ` Finished in ${formatElapsed(elapsed)}.` : '';
        const isSchemaOnly = !!data?.schema_only;
        if (isSchemaOnly) setSchemaOnly(true);
        addLog('success', `All done!${elapsedStr}`, message.data);
        setProcessingState('completed');
        setSchemaProgress(prev => ({ ...prev, isComplete: true }));
        if (!isSchemaOnly) {
          setExtractionProgress(prev => ({
            ...prev,
            isComplete: true,
            totalDocs: data?.total_documents || prev.totalDocs,
            processedDocs: data?.total_documents || prev.processedDocs,
          }));
        }
        queryClient.invalidateQueries(['schematiq-status', sessionId]);
      } else if (message.type === 'schema_completed') {
        const schemaData = message.data as SchemaCompletionData;
        addLog('success', `Schema ready! Found ${schemaData?.total_columns || 'several'} columns`, message.data);

        setSchemaProgress(prev => ({
          ...prev,
          columnsDiscovered: schemaData?.total_columns || prev.columnsDiscovered,
          isComplete: true
        }));
        // Don't advance to extraction phase if this is a schema-only run;
        // the completed event will arrive shortly and set processingState to 'completed'.
        if (!schemaOnly) {
          setProcessingState('extraction');
        }

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
        addLog(
          'info',
          obsData
            ? `Observation unit discovered: ${formatObservationUnitSummary(obsData)}. Review before continuing.`
            : 'Observation unit discovered. Review before continuing.',
        );
        setProcessingState('observation_unit_review');
        if (obsData) {
          setReviewObsUnit(obsData);
          setEditName(obsData.name);
          setEditDefinition(obsData.definition);
          setEditExamples(obsData.example_names || []);
          setObsUnitEdited(false);
          setEditBaseline(obsData);
          setReviewEntryMode('initial');
          setProcessingStateBeforeEdit(null);
        }
        queryClient.invalidateQueries(['schematiq-status', sessionId]);
      } else if (message.type === 'reextraction_started') {
        const data = message.data as { operation_id?: string; columns?: string[]; total_documents?: number };
        setSchemaOnly(false);
        setProcessingState('extraction');
        setDeferredExtractError('');
        if (data?.operation_id) {
          setActiveReextractionId(data.operation_id);
        }
        const total = data?.total_documents ?? 0;
        if (total > 0) {
          setExtractionProgress({
            processedDocs: 0,
            totalDocs: total,
            isComplete: false,
          });
          setCurrentStepMessage(`Extracting values from ${total} document(s)...`);
        }
        addLog('info', 'Extracting table data from your uploaded documents...');
      } else if (message.type === 'document_started') {
        const data = message.data as {
          document_name?: string;
          document_index?: number;
          total_documents?: number;
        };
        setProcessingState('extraction');
        setSchemaOnly(false);
        const total = data?.total_documents ?? 0;
        const index = data?.document_index ?? 0;
        if (total > 0) {
          setExtractionProgress(prev => ({
            ...prev,
            totalDocs: total,
            processedDocs: Math.max(prev.processedDocs, index - 1),
            isComplete: false,
          }));
          setCurrentStepMessage(
            `Processing ${data?.document_name || 'document'} (${index}/${total})...`,
          );
        }
      } else if (message.type === 'reextraction_progress') {
        const data = message.data as {
          processed_documents?: number;
          total_documents?: number;
          current_row?: string;
        };
        setProcessingState('extraction');
        setSchemaOnly(false);
        const total = data?.total_documents ?? 0;
        const processed = total > 0
          ? Math.min(data?.processed_documents ?? 0, total)
          : (data?.processed_documents ?? 0);
        setExtractionProgress(prev => ({
          ...prev,
          processedDocs: processed || prev.processedDocs,
          totalDocs: total || prev.totalDocs,
          isComplete: false,
        }));
        if (total > 0) {
          setCurrentStepMessage(
            data?.current_row
              ? `Extracting row "${data.current_row}" (document ${processed}/${total})...`
              : `Extracting values (document ${processed}/${total})...`,
          );
        }
      } else if (message.type === 'reextraction_completed') {
        const data = message.data as { columns?: string[] };
        setActiveReextractionId(null);
        setSchemaOnly(false);
        setIsStopping(false);
        setProcessingState('completed');
        setExtractionProgress(prev => ({
          ...prev,
          isComplete: true,
          processedDocs: prev.totalDocs || prev.processedDocs,
        }));
        setCurrentStepMessage('Extraction complete.');
        addLog('success', `Extraction complete${data?.columns?.length ? ` (${data.columns.length} columns)` : ''}.`);
        queryClient.invalidateQueries(['schematiq-status', sessionId]);
        queryClient.invalidateQueries(['session', sessionId, 'schematiq']);
        queryClient.invalidateQueries(['data', sessionId]);
      } else if (message.type === 'reextraction_failed') {
        const data = message.data as { error?: string };
        setActiveReextractionId(null);
        setIsStopping(false);
        setDeferredExtractError(data?.error || 'Extraction failed');
        setSchemaOnly(true);
        setProcessingState('completed');
        setCurrentStepMessage('');
        addLog('error', data?.error || 'Extraction failed');
      } else if (message.type === 'reextraction_stopped') {
        const data = message.data as { processed_documents?: number; total_documents?: number };
        setActiveReextractionId(null);
        setIsStopping(false);
        setSchemaOnly(false);
        setProcessingState('completed');
        const processed = data?.processed_documents ?? 0;
        setCurrentStepMessage(
          processed > 0
            ? `Extraction stopped (${processed} document(s) processed).`
            : 'Extraction stopped.',
        );
        addLog('warning', 'Extraction stopped. Partial results may be available on the Data tab.');
        queryClient.invalidateQueries(['schematiq-status', sessionId]);
        queryClient.invalidateQueries(['session', sessionId, 'schematiq']);
        queryClient.invalidateQueries(['data', sessionId]);
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

  // Poll re-extraction status when WebSocket updates are missed
  useEffect(() => {
    if (!activeReextractionId) return;

    let cancelled = false;

    const applyTerminalStatus = (status: string, error?: string) => {
      setActiveReextractionId(null);
      setIsStopping(false);
      if (status === 'completed') {
        setSchemaOnly(false);
        setProcessingState('completed');
        setExtractionProgress(prev => ({
          ...prev,
          isComplete: true,
          processedDocs: prev.totalDocs || prev.processedDocs,
        }));
        setCurrentStepMessage('Extraction complete.');
        queryClient.invalidateQueries(['schematiq-status', sessionId]);
        queryClient.invalidateQueries(['session', sessionId, 'schematiq']);
        queryClient.invalidateQueries(['data', sessionId]);
      } else if (status === 'stopped') {
        setSchemaOnly(false);
        setProcessingState('completed');
        setCurrentStepMessage('Extraction stopped.');
        queryClient.invalidateQueries(['schematiq-status', sessionId]);
        queryClient.invalidateQueries(['session', sessionId, 'schematiq']);
        queryClient.invalidateQueries(['data', sessionId]);
      } else if (status === 'failed') {
        setSchemaOnly(true);
        setProcessingState('completed');
        setDeferredExtractError(error || 'Extraction failed');
        setCurrentStepMessage('');
      }
    };

    const poll = async () => {
      try {
        const st = await schemaAPI.getReextractionStatus(sessionId, activeReextractionId);
        if (cancelled) return;

        if (st.total_documents > 0) {
          setProcessingState('extraction');
          const total = st.total_documents;
          const processed = Math.min(st.processed_documents, total);
          setExtractionProgress({
            processedDocs: processed,
            totalDocs: total,
            isComplete: st.status === 'completed',
          });
          if (st.status === 'running' || st.status === 'starting') {
            setCurrentStepMessage(
              `Extracting values (${st.processed_documents}/${st.total_documents})...`,
            );
          }
        }

        if (st.status === 'completed' || st.status === 'failed' || st.status === 'stopped') {
          applyTerminalStatus(st.status, st.error);
        }
      } catch {
        // Operation may have been cleaned up after completion
      }
    };

    poll();
    const intervalId = setInterval(poll, 2500);
    return () => {
      cancelled = true;
      clearInterval(intervalId);
    };
  }, [activeReextractionId, sessionId, queryClient]);

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

  const handleExtractTableData = async () => {
    setIsDeferredExtracting(true);
    setDeferredExtractError('');
    try {
      const session = await loadAPI.getSession(sessionId);
      const columns = (session.columns || [])
        .filter((c) => c.name && !c.name.toLowerCase().endsWith('_excerpt'))
        .map((c) => c.name);
      if (columns.length === 0) {
        setDeferredExtractError('No schema columns found. Wait for schema discovery to finish or open the Schema tab.');
        return;
      }

      const availability = await schemaAPI.precheckDocuments(sessionId, {
        operation_type: 'reextraction',
      });
      if (!availability.can_proceed) {
        setDeferredExtractError(
          'No source documents found for this session. Your files should still be on the server from the initial upload — try refreshing, or add documents on the Data tab.',
        );
        return;
      }

      const cfg = await configAPI.getConfig().catch(() => ({ allow_llm_config: true }));
      const configured = await getConfiguredProviders();
      const available = getAvailableProviders(configured);
      const provider: LLMProviderKey = !cfg.allow_llm_config ? 'gemini' : (available[0] ?? 'gemini');
      const model = getDefaultModelForProvider(provider);
      const apiKey = await getApiKeyForProvider(provider);

      const request: ReextractionRequest = { columns };
      if (apiKey) {
        request.llm_config = { provider, model, api_key: apiKey, temperature: 0 };
      }

      const response = await schemaAPI.startReextraction(sessionId, request);
      const docCount = response.rows_to_process || response.estimated_papers || 0;
      if (docCount === 0) {
        setDeferredExtractError(
          'No source documents available to process. Upload documents on the Data tab and try again.',
        );
        return;
      }

      // Ensure progress WebSocket is connected before background work starts
      if (!webSocketService.isConnected()) {
        webSocketService.connect(sessionId, 'progress');
      }

      setSchemaOnly(false);
      setActiveReextractionId(response.operation_id);
      setProcessingState('extraction');
      setCurrentStepMessage(`Extracting values from ${docCount} document(s)...`);
      setExtractionProgress({
        processedDocs: 0,
        totalDocs: docCount,
        isComplete: false,
      });
      if (onExtractionStarted) {
        onExtractionStarted(response.columns, response.operation_id);
      }
      addLog(
        'info',
        `Extracting ${response.columns.length} column(s) across ${docCount} document(s). Open the Data tab for live rows.`,
      );
    } catch (err: unknown) {
      const message = extractApiErrorMessage(err, 'Failed to start extraction');
      setDeferredExtractError(
        (err as { response?: { status?: number } })?.response?.status === 503
          ? (message || 'Server is busy. Try again in a few minutes.')
          : message,
      );
      addLog(
        'error',
        (err as { response?: { status?: number } })?.response?.status === 503
          ? (message || 'Server is busy')
          : message,
      );
    } finally {
      setIsDeferredExtracting(false);
    }
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
    setLlmStats(null);
    queryClient.removeQueries(llmStatsCacheKey, { exact: true });
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
    } catch (error: unknown) {
      const status = (error as { response?: { status?: number } })?.response?.status;

      if (status === 429) {
        // Quota exceeded — show orange banner
        setProcessingState('idle');
        setQuotaExceeded(true);
        addLog('warning', 'API usage limit reached');
      } else if (status === 503) {
        // Server busy — show friendly amber banner, not error state
        setProcessingState('idle');
        const message = extractApiErrorMessage(error, 'The server is currently busy processing other requests. Please try again in a few minutes.');
        setCapacityMessage(message);
        addLog('warning', 'Server busy — please retry shortly');
      } else {
        setProcessingState('error');
        const message = extractApiErrorMessage(error, 'Failed to start ScheMatiQ');
        setErrorMessage(message);
        addLog('error', `Failed to start ScheMatiQ: ${message}`);
      }
    }
  };

  const handleStop = async () => {
    setIsStopping(true);
    try {
      if (activeReextractionId) {
        await schemaAPI.stopReextraction(sessionId, activeReextractionId);
        addLog('warning', 'Stop requested — extraction will stop at the next checkpoint.');
        // UI updates on reextraction_stopped WebSocket (or status poll fallback)
      } else {
        await schematiqAPI.stop(sessionId);
        setProcessingState('stopped');
        setIsStopping(false);
        addLog('warning', 'Stop requested — processing will stop at the next checkpoint.');
      }
    } catch (error: unknown) {
      const message = extractApiErrorMessage(error, 'Failed to stop');
      addLog('error', message);
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

  const examplesEqual = (a: string[], b: string[]) =>
    a.length === b.length && a.every((val, i) => val === b[i]);

  const hasObsUnitChanges = useCallback(() => {
    const baseline = editBaseline || reviewObsUnit;
    if (!baseline) return false;
    return (
      editName.trim() !== baseline.name.trim() ||
      editDefinition.trim() !== baseline.definition.trim() ||
      !examplesEqual(editExamples, baseline.example_names || [])
    );
  }, [editBaseline, reviewObsUnit, editName, editDefinition, editExamples]);

  const closeMidRunEditor = useCallback((restoreState?: ProcessingState | null) => {
    const baseline = editBaseline || reviewObsUnit;
    if (baseline) {
      setEditName(baseline.name);
      setEditDefinition(baseline.definition);
      setEditExamples(baseline.example_names || []);
    }
    setObsUnitEdited(false);
    setReviewEntryMode(null);
    setEditBaseline(null);
    setProcessingStateBeforeEdit(null);
    if (restoreState) {
      setProcessingState(restoreState);
    }
  }, [editBaseline, reviewObsUnit]);

  const handleCloseReviewPanel = () => {
    if (reviewEntryMode === 'mid_run') {
      closeMidRunEditor(processingStateBeforeEdit || 'schema');
      addLog('info', 'Returned to monitor — pipeline continues.');
      return;
    }
    if (reviewEntryMode === 'post_run') {
      const baseline = editBaseline || reviewObsUnit;
      if (baseline) {
        setEditName(baseline.name);
        setEditDefinition(baseline.definition);
        setEditExamples(baseline.example_names || []);
      }
      setObsUnitEdited(false);
      setReviewEntryMode(null);
      setEditBaseline(null);
      setProcessingState(processingStateBeforeEdit || 'completed');
      setProcessingStateBeforeEdit(null);
    }
  };

  const handleDiscardObsUnitChanges = () => {
    const baseline = editBaseline || reviewObsUnit;
    if (baseline) {
      setEditName(baseline.name);
      setEditDefinition(baseline.definition);
      setEditExamples(baseline.example_names || []);
    }
    setObsUnitEdited(false);
  };

  const handleResume = async (skipEdit = false) => {
    setIsResuming(true);
    setReviewSaveError('');
    try {
      let confirmedObsUnit: ObservationUnitReadyData | null = reviewObsUnit
        ? {
            name: editName.trim() || reviewObsUnit.name,
            definition: editDefinition.trim() || reviewObsUnit.definition,
            example_names: editExamples.length > 0 ? editExamples : (reviewObsUnit.example_names || []),
          }
        : null;

      const shouldPersistEdit = !skipEdit && hasObsUnitChanges() && editName.trim() && editDefinition.trim();

      // If the user edited the observation unit, save changes first
      if (shouldPersistEdit) {
        const response = await observationUnitAPI.updateDefinition(sessionId, {
          name: editName.trim(),
          definition: editDefinition.trim(),
          example_names: editExamples.length > 0 ? editExamples : undefined,
        });
        confirmedObsUnit = {
          name: response.observation_unit.name,
          definition: response.observation_unit.definition,
          example_names: response.observation_unit.example_names || [],
        };
        setReviewObsUnit(confirmedObsUnit);
        setEditName(confirmedObsUnit.name);
        setEditDefinition(confirmedObsUnit.definition);
        setEditExamples(confirmedObsUnit.example_names);
        addLog('success', `Observation unit updated: ${formatObservationUnitSummary(confirmedObsUnit)}`);
      }

      const isRediscover =
        (reviewEntryMode === 'mid_run' || reviewEntryMode === 'post_run') && shouldPersistEdit;
      if (isRediscover) {
        addLog('info', 'Stopping current run and rediscovering schema with updated observation unit...');
        setReviewEntryMode(null);
        setEditBaseline(null);
        setProcessingStateBeforeEdit(null);
      }

      // Keep WebSocket connected before backend resumes extraction
      onResumeStarted?.();

      // Resume waits for any in-flight pipeline to stop, then restarts (backend resume_qbsd)
      await schematiqAPI.resume(sessionId);
      if (confirmedObsUnit) {
        addLog('info', `Resuming with observation unit: ${formatObservationUnitSummary(confirmedObsUnit)}`);
      } else {
        addLog('info', 'Resuming schema generation...');
      }

      // Transition to starting/schema state
      setReviewEntryMode(null);
      setEditBaseline(null);
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
    } catch (error: unknown) {
      const message = extractApiErrorMessage(error, 'Failed to resume');
      setReviewSaveError(message);
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

  // While mid-run review panel is open, backend phase is not `observation_unit_review` — keep phase cards live
  const phaseDisplayState: ProcessingState =
    processingState === 'observation_unit_review' && reviewEntryMode === 'mid_run'
      ? (schemaProgress.isComplete || status?.schema_completed
          ? 'extraction'
          : (processingStateBeforeEdit || 'schema'))
      : processingState === 'observation_unit_review' && reviewEntryMode === 'post_run'
        ? (processingStateBeforeEdit || 'completed')
        : processingState;

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
              <p className="text-xl font-semibold text-green-600 mb-1">
                {schemaOnly ? 'Schema Discovery Complete' : 'Completed Successfully!'}
              </p>
              <p className="text-muted-foreground text-center max-w-md">
                {schemaOnly
                  ? `Discovered ${schemaProgress.columnsDiscovered || status?.columns_discovered || 0} columns. Value extraction was skipped — extract your table below when ready.`
                  : schemaProgress.columnsDiscovered > 0 && extractionProgress.totalDocs > 0
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
          {processingState === 'observation_unit_review' && reviewObsUnit && (() => {
            const hasChanges = hasObsUnitChanges();
            const schemaAlreadyDiscovered = schemaProgress.isComplete || !!status?.schema_completed;
            const isMidRunReview = reviewEntryMode === 'mid_run';
            const isPostRunReview = reviewEntryMode === 'post_run';
            const isInitialReview = reviewEntryMode === 'initial';
            const trimmedDefinition = editDefinition.trim();
            const formValid = !!editName.trim()
              && trimmedDefinition.length >= 10
              && trimmedDefinition.length <= 500
              && editName.trim().length <= 100;
            const primaryLabel = schemaAlreadyDiscovered
              ? 'Save & Rediscover'
              : (isInitialReview && !hasChanges
                ? 'Continue to Schema Generation'
                : 'Save & Continue to Schema Generation');
            const primaryEnabled = isResuming
              ? false
              : isInitialReview
                ? formValid
                : formValid && hasChanges;
            const discardEnabled = !isResuming && hasChanges;
            const showCloseControl = isMidRunReview || isPostRunReview;

            return (
            <div className="w-full max-w-lg relative">
              {showCloseControl && (
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="absolute left-0 top-0 h-8 w-8 text-muted-foreground hover:text-foreground"
                  onClick={handleCloseReviewPanel}
                  disabled={isResuming}
                  aria-label="Back to monitor"
                >
                  <ChevronLeft className="h-5 w-5" />
                </Button>
              )}

              <div className="flex flex-col items-center mb-4 pt-1">
                <div className="w-14 h-14 rounded-full bg-purple-100 flex items-center justify-center mb-3">
                  <Layers className="h-7 w-7 text-purple-600" />
                </div>
                <p className="text-xl font-semibold text-purple-700 mb-1">Review Observation Unit</p>
                <p className="text-sm text-muted-foreground text-center">
                  {isMidRunReview
                    ? 'Review or edit the observation unit. The pipeline keeps running until you save a change.'
                    : isPostRunReview
                      ? 'Edit the observation unit to rediscover the schema with your changes.'
                      : 'The observation unit defines what each row in your table represents. Review and optionally edit before schema generation.'}
                </p>
              </div>

              <div className="space-y-3 text-left">
                {/* Name */}
                <div className="space-y-1">
                  <Label htmlFor="obs-name" className="text-sm font-medium">Name</Label>
                  <Input
                    id="obs-name"
                    value={editName}
                    onChange={(e) => { setEditName(e.target.value); setObsUnitEdited(true); setReviewSaveError(''); }}
                    placeholder="e.g., Model, Protein, Study"
                  />
                </div>

                {/* Definition */}
                <div className="space-y-1">
                  <Label htmlFor="obs-definition" className="text-sm font-medium">Definition</Label>
                  <Textarea
                    id="obs-definition"
                    value={editDefinition}
                    onChange={(e) => { setEditDefinition(e.target.value); setObsUnitEdited(true); setReviewSaveError(''); }}
                    placeholder="Describe what constitutes a single row..."
                    rows={3}
                    className="resize-none"
                  />
                  <p className="text-xs text-muted-foreground">
                    {trimmedDefinition.length}/500 characters (minimum 10)
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

              {/* Action Buttons — always mounted; only enabled state and label change */}
              {reviewSaveError && (
                <Alert variant="destructive" className="mt-4">
                  <AlertDescription>{reviewSaveError}</AlertDescription>
                </Alert>
              )}
              <div className="flex flex-col gap-2 mt-5">
                <Button
                  size="lg"
                  onClick={() => handleResume(false)}
                  disabled={!primaryEnabled}
                  className="w-full"
                >
                  {isResuming ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      {(isMidRunReview || isPostRunReview) && hasChanges ? 'Rediscovering...' : 'Resuming...'}
                    </>
                  ) : (
                    <>
                      <ArrowRight className="h-4 w-4 mr-2" />
                      {primaryLabel}
                    </>
                  )}
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={handleDiscardObsUnitChanges}
                  disabled={!discardEnabled}
                  className="text-muted-foreground"
                >
                  Discard changes
                </Button>
              </div>
            </div>
            );
          })()}
        </CardContent>
      </Card>

      {/* Observation Unit Info + Edit Button (shown after review step, during or after processing) */}
      {reviewObsUnit && processingState !== 'observation_unit_review' && processingState !== 'idle' && (
        <Card className="bg-purple-50/50 border-purple-200">
          <CardContent className="py-3 px-4 flex items-center justify-between">
            <div className="flex items-center gap-2 min-w-0">
              <Layers className="h-4 w-4 text-purple-600 shrink-0 mt-0.5" />
              <div className="min-w-0">
                <span className="text-xs text-purple-600 font-medium">Observation Unit</span>
                <p className="text-sm font-semibold text-purple-900">{editName || reviewObsUnit.name}</p>
                <p className="text-xs text-purple-800/80 mt-0.5 line-clamp-2">
                  {editDefinition || reviewObsUnit.definition}
                </p>
                {(editExamples.length > 0 || (reviewObsUnit.example_names?.length ?? 0) > 0) && (
                  <p className="text-xs text-purple-700/70 mt-0.5 truncate">
                    Examples: {(editExamples.length > 0 ? editExamples : reviewObsUnit.example_names || []).join(', ')}
                  </p>
                )}
              </div>
            </div>
            <Button
              variant="ghost"
              size="sm"
              className="text-purple-600 hover:text-purple-800 hover:bg-purple-100 shrink-0"
              disabled={isResuming}
              onClick={async () => {
                // Enter edit mode without stopping the pipeline — stop only happens on confirmed change
                setProcessingStateBeforeEdit(processingState);
                setReviewEntryMode(isProcessing ? 'mid_run' : 'post_run');
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
                    setEditBaseline(obsData);
                  }
                } catch (e) {
                  // Fall back to the cached observation unit data
                  if (reviewObsUnit) {
                    setEditBaseline({
                      name: reviewObsUnit.name,
                      definition: reviewObsUnit.definition,
                      example_names: reviewObsUnit.example_names || [],
                    });
                  }
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

      {/* Deferred extraction (schema-only mode) */}
      {schemaOnly && processingState === 'completed' && (
        <Card className="border-2 border-primary/20 bg-primary/5">
          <CardContent className="pt-5 pb-5">
            <div className="flex flex-col sm:flex-row sm:items-center gap-4">
              <div className="flex items-start gap-3 flex-1 min-w-0">
                <div className="w-10 h-10 rounded-full bg-primary/10 flex items-center justify-center shrink-0">
                  <Table className="h-5 w-5 text-primary" />
                </div>
                <div className="min-w-0">
                  <p className="font-semibold text-foreground">Extract table data</p>
                  <p className="text-sm text-muted-foreground mt-0.5">
                    Your documents are already uploaded for this session. Run value extraction to fill the table using the discovered schema — no need to start over.
                  </p>
                </div>
              </div>
              <Button
                size="lg"
                className="shrink-0 w-full sm:w-auto"
                onClick={handleExtractTableData}
                disabled={isDeferredExtracting}
              >
                {isDeferredExtracting ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    Starting...
                  </>
                ) : (
                  <>
                    <RefreshCw className="h-4 w-4 mr-2" />
                    Extract Table Data
                  </>
                )}
              </Button>
            </div>
            {deferredExtractError && (
              <Alert variant="destructive" className="mt-4">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>{deferredExtractError}</AlertDescription>
              </Alert>
            )}
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
            <Card className={`transition-all ${phaseDisplayState === 'schema' || phaseDisplayState === 'starting' ? 'border-primary border-2 shadow-md' : ''}`}>
              <CardContent className="pt-4 pb-4">
                <div className="flex items-center justify-between mb-3">
                  <span className="font-medium flex items-center gap-2">
                    {schemaIsComplete ? (
                      <CheckCircle2 className="h-5 w-5 text-green-500" />
                    ) : phaseDisplayState === 'schema' || phaseDisplayState === 'starting' ? (
                      <Loader2 className="h-5 w-5 animate-spin text-primary" />
                    ) : (
                      <div className="h-5 w-5 rounded-full border-2 border-muted-foreground/30" />
                    )}
                    Phase 1: Schema
                  </span>
                  <Badge variant={schemaIsComplete ? 'success' : phaseDisplayState === 'schema' || phaseDisplayState === 'starting' ? 'default' : 'secondary'}>
                    {schemaIsComplete
                      ? 'Complete'
                      : phaseDisplayState === 'schema' || phaseDisplayState === 'starting'
                        ? 'In Progress'
                        : 'Pending'}
                  </Badge>
                </div>
                <Progress
                  value={schemaIsComplete ? 100 : (phaseDisplayState === 'schema' || phaseDisplayState === 'starting' ? Math.max(10, (schemaProgress.iteration / schemaProgress.maxIterations) * 100) : 0)}
                  className={`h-2 ${(phaseDisplayState === 'schema' || phaseDisplayState === 'starting') && !schemaIsComplete ? 'animate-pulse' : ''}`}
                />
                <p className="text-xs text-muted-foreground mt-2">
                  {schemaIsComplete
                    ? `${schemaProgress.columnsDiscovered} columns discovered`
                    : (phaseDisplayState === 'schema' || phaseDisplayState === 'starting') && schemaProgress.iteration > 0
                      ? `Iteration ${schemaProgress.iteration}/${schemaProgress.maxIterations}`
                      : phaseDisplayState === 'schema' || phaseDisplayState === 'starting'
                        ? 'Analyzing documents...'
                        : 'Waiting to start'}
                </p>
              </CardContent>
            </Card>
          );
        })()}

        {/* Phase 2: Value Extraction */}
        {(() => {
          const extractionIsComplete =
            extractionProgress.isComplete ||
            (!schemaOnly &&
              extractionProgress.totalDocs > 0 &&
              extractionProgress.processedDocs >= extractionProgress.totalDocs);
          return (
            <Card className={`transition-all ${!schemaOnly && phaseDisplayState === 'extraction' ? 'border-primary border-2 shadow-md' : ''} ${schemaOnly ? 'opacity-60' : ''}`}>
              <CardContent className="pt-4 pb-4">
                <div className="flex items-center justify-between mb-3">
                  <span className="font-medium flex items-center gap-2">
                    {schemaOnly ? (
                      <div className="h-5 w-5 rounded-full border-2 border-muted-foreground/30 flex items-center justify-center">
                        <div className="h-0.5 w-2.5 bg-muted-foreground/50 rounded" />
                      </div>
                    ) : extractionIsComplete ? (
                      <CheckCircle2 className="h-5 w-5 text-green-500" />
                    ) : phaseDisplayState === 'extraction' ? (
                      <Loader2 className="h-5 w-5 animate-spin text-primary" />
                    ) : (
                      <div className="h-5 w-5 rounded-full border-2 border-muted-foreground/30" />
                    )}
                    Phase 2: Extraction
                  </span>
                  <Badge variant={schemaOnly ? 'secondary' : extractionIsComplete ? 'success' : phaseDisplayState === 'extraction' ? 'default' : 'secondary'}>
                    {schemaOnly
                      ? 'Skipped'
                      : extractionIsComplete
                        ? 'Complete'
                        : phaseDisplayState === 'extraction'
                          ? 'In Progress'
                          : 'Pending'}
                  </Badge>
                </div>
                <Progress
                  value={schemaOnly
                    ? 0
                    : extractionIsComplete
                      ? 100
                      : extractionProgress.totalDocs > 0
                        ? (extractionProgress.processedDocs / extractionProgress.totalDocs) * 100
                        : phaseDisplayState === 'extraction' ? 10 : 0}
                  className={`h-2 ${!schemaOnly && phaseDisplayState === 'extraction' && !extractionIsComplete ? 'animate-pulse' : ''}`}
                />
                <p className="text-xs text-muted-foreground mt-2">
                  {schemaOnly
                    ? 'Extraction was skipped'
                    : extractionIsComplete
                      ? `${extractionProgress.totalDocs} documents processed`
                      : phaseDisplayState === 'extraction' && extractionProgress.totalDocs > 0
                        ? `${extractionProgress.processedDocs}/${extractionProgress.totalDocs} documents`
                        : phaseDisplayState === 'extraction'
                          ? 'Starting extraction...'
                          : 'Waiting for schema'}
                </p>
              </CardContent>
            </Card>
          );
        })()}
      </div>

      {/* LLM Usage & Cost Stats */}
      {llmStats && (
        <Card className="border-2 border-blue-100 bg-blue-50/30">
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center gap-2 mb-3">
              <Activity className="h-5 w-5 text-blue-600" />
              <span className="font-medium text-blue-900">LLM Usage & Cost</span>
              <Badge variant="outline" className="ml-auto bg-white text-blue-700 border-blue-200">
                Live Estimate
              </Badge>
            </div>
            
            <div className="grid grid-cols-2 gap-6">
              <div>
                <p className="text-xs text-muted-foreground uppercase tracking-wider font-semibold mb-1">API Calls</p>
                <div className="flex items-baseline gap-2">
                  <span className="text-2xl font-bold text-slate-800">{llmStats.total_calls}</span>
                  <span className="text-sm text-muted-foreground">
                    / ~{llmStats.estimated_calls} est.
                  </span>
                </div>
                <Progress 
                  value={Math.min(100, (llmStats.total_calls / (llmStats.estimated_calls || 1)) * 100)} 
                  className="h-1.5 mt-2 bg-blue-100" 
                  indicatorClassName="bg-blue-500"
                />
              </div>
              
              <div>
                <p className="text-xs text-muted-foreground uppercase tracking-wider font-semibold mb-1">Cost (USD)</p>
                <div className="flex items-baseline gap-2">
                  <span className="text-2xl font-bold text-slate-800">${llmStats.current_cost_usd.toFixed(4)}</span>
                  <span className="text-sm text-muted-foreground">
                    / ~${llmStats.estimated_cost_usd.toFixed(4)} est.
                  </span>
                </div>
                <Progress 
                  value={Math.min(100, (llmStats.current_cost_usd / (llmStats.estimated_cost_usd || 0.01)) * 100)} 
                  className="h-1.5 mt-2 bg-blue-100" 
                  indicatorClassName={llmStats.current_cost_usd > llmStats.estimated_cost_usd ? "bg-amber-500" : "bg-blue-500"}
                />
              </div>
            </div>
          </CardContent>
        </Card>
      )}

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
