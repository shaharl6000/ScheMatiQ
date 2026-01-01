import React, { useState, useEffect } from 'react';
import { useParams, useSearchParams, useNavigate, Link } from 'react-router-dom';
import {
  ArrowLeft,
  Table2,
  Database,
  BarChart3,
  Download,
  RefreshCw,
  CheckCircle2,
  Play,
  XCircle,
  Loader2,
  X,
  FileText,
  Square,
} from 'lucide-react';
import { useQuery, useQueryClient } from 'react-query';

import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

import { loadAPI, qbsdAPI, cloudAPI } from '../services/api';
import { getApiKeyForProvider, LLMProvider } from '../utils/apiKeyStorage';
import { VisualizationSession, CellValue, CellExtractedData } from '../types';
import {
  PROCESSING_REFRESH_INTERVAL,
  NEW_ROW_HIGHLIGHT_DURATION,
  WS_RECONNECT_ATTEMPTS,
  WS_RECONNECT_DELAY_BASE,
  WS_RECONNECT_MAX_DELAY,
  API_BASE_URL,
  WS_BASE_URL
} from '../constants/index';

// Component imports
import DataTable from '../components/DataTable/DataTable';
import SchemaViewer from '../components/SchemaViewer/SchemaViewer';
import StatsDashboard from '../components/StatsDashboard/StatsDashboard';
import QBSDMonitor from '../components/QBSDMonitor/QBSDMonitor';
import UploadProcessingMonitor from '../components/UploadProcessingMonitor/UploadProcessingMonitor';
import DocumentUpload from '../components/DocumentUpload/DocumentUpload';
import LLMSelector from '../components/LLMSelector';

const Visualize = () => {
  const { sessionId } = useParams<{ sessionId: string }>();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const mode = searchParams.get('mode') as 'load' | 'qbsd' || 'load';
  const [activeTab, setActiveTab] = useState(mode === 'qbsd' ? 'monitor' : 'data');

  // Enhanced upload document management state
  const [uploadedDocuments, setUploadedDocuments] = useState<File[]>([]);
  const [documentUploadLoading, setDocumentUploadLoading] = useState(false);
  const [documentUploadResult, setDocumentUploadResult] = useState<any>(null);
  const [documentUploadError, setDocumentUploadError] = useState<string | null>(null);
  const [newlyAddedRows, setNewlyAddedRows] = useState<Set<number>>(new Set());
  const [removingDocument, setRemovingDocument] = useState<string | null>(null);

  // Streaming cells state
  const [streamingCells, setStreamingCells] = useState<Map<string, Record<string, CellValue>>>(new Map());

  // Processing columns state (for re-extraction visual indicators)
  const [processingColumns, setProcessingColumns] = useState<Set<string>>(new Set());

  // Current document being processed (for progress display)
  const [currentDocumentProgress, setCurrentDocumentProgress] = useState<{
    documentName: string;
    documentIndex: number;
    totalDocuments: number;
  } | null>(null);

  // LLM selection state
  const [showLLMSelector, setShowLLMSelector] = useState(false);

  // Stop processing state
  const [isStoppingProcessing, setIsStoppingProcessing] = useState(false);

  // Active re-extraction operation tracking
  // const [activeReextractionId, setActiveReextractionId] = useState<string | null>(null);
  // const [isStoppingReextraction, setIsStoppingReextraction] = useState(false);

  // Column order state
  const [columnOrder, setColumnOrder] = useState<string[]>([]);

  // WebSocket state
  const [forceWebSocketConnect, setForceWebSocketConnect] = useState(false);
  const wsRef = React.useRef<WebSocket | null>(null);

  // Load column order from localStorage
  useEffect(() => {
    if (sessionId) {
      const savedOrder = localStorage.getItem(`columnOrder_${sessionId}`);
      if (savedOrder) {
        try {
          setColumnOrder(JSON.parse(savedOrder));
        } catch (e) {
          console.error('Failed to parse saved column order:', e);
        }
      }
    }
  }, [sessionId]);

  const handleColumnReorder = (newOrder: string[]) => {
    setColumnOrder(newOrder);
    if (sessionId) {
      localStorage.setItem(`columnOrder_${sessionId}`, JSON.stringify(newOrder));
    }
  };

  // WebSocket connection
  const connectWebSocketSync = (): Promise<WebSocket> => {
    return new Promise((resolve, reject) => {
      const wsUrl = `${WS_BASE_URL}/ws/progress/${sessionId}`;

      const ws = new WebSocket(wsUrl);

      const timeout = setTimeout(() => {
        if (ws.readyState !== WebSocket.OPEN) {
          ws.close();
          reject(new Error('WebSocket connection timeout'));
        }
      }, 5000);

      ws.onopen = () => {
        clearTimeout(timeout);
        wsRef.current = ws;
        resolve(ws);
      };

      ws.onerror = (error) => {
        clearTimeout(timeout);
        reject(error);
      };

      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          switch (message.type) {
            case 'cell_extracted':
              if (message.data?.row_name && message.data?.column) {
                const cellData = message.data as CellExtractedData;
                setStreamingCells(prev => {
                  const updated = new Map(prev);
                  const rowData = updated.get(cellData.row_name) || {};
                  rowData[cellData.column] = cellData.value;
                  updated.set(cellData.row_name, rowData);
                  return updated;
                });
              }
              break;
            case 'row_completed':
              if (message.data?.row_name) {
                setStreamingCells(prev => {
                  const updated = new Map(prev);
                  updated.delete(message.data.row_name);
                  return updated;
                });
              }
              setNewlyAddedRows(prev => new Set(Array.from(prev).concat(message.data.row_index)));
              queryClient.invalidateQueries(['data', sessionId]);
              queryClient.invalidateQueries(['session', sessionId]);
              setTimeout(() => {
                setNewlyAddedRows(prev => {
                  const newSet = new Set(Array.from(prev));
                  newSet.delete(message.data.row_index);
                  return newSet;
                });
              }, NEW_ROW_HIGHLIGHT_DURATION);
              break;
            case 'completion':
              setStreamingCells(new Map());
              setForceWebSocketConnect(false);
              queryClient.refetchQueries(['session', sessionId, mode]);
              queryClient.refetchQueries(['data', sessionId, mode]);
              setTimeout(() => {
                if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
                  wsRef.current.close(1000, 'Processing completed');
                  wsRef.current = null;
                }
              }, 3000);
              break;
            case 'reextraction_started':
              // Initialize processing columns when re-extraction starts
              if (message.data?.columns && Array.isArray(message.data.columns)) {
                setProcessingColumns(new Set(message.data.columns));
              }
              break;
            case 'document_started':
              // Update current document progress
              if (message.data?.document_name) {
                setCurrentDocumentProgress({
                  documentName: message.data.document_name,
                  documentIndex: message.data.document_index || 0,
                  totalDocuments: message.data.total_documents || 0
                });
              }
              break;
            case 'reextraction_progress':
              if (message.data?.column) {
                setProcessingColumns(prev => {
                  const newSet = new Set(Array.from(prev));
                  newSet.add(message.data.column);
                  return newSet;
                });
              }
              break;
            case 'reextraction_completed':
              console.log('Re-extraction completed:', message.data);
              setProcessingColumns(new Set()); // Clear processing state
              setCurrentDocumentProgress(null); // Clear document progress
              setStreamingCells(new Map());    // Clear streaming cells
              queryClient.invalidateQueries(['session', sessionId, mode]);
              queryClient.invalidateQueries(['data', sessionId, mode]);
              break;
            case 'reextraction_stopped':
              console.log('Re-extraction stopped:', message.data);
              setProcessingColumns(new Set()); // Clear processing state
              setCurrentDocumentProgress(null); // Clear document progress
              setStreamingCells(new Map());    // Clear streaming cells
              queryClient.invalidateQueries(['session', sessionId, mode]);
              queryClient.invalidateQueries(['data', sessionId, mode]);
              break;
          }
        } catch (err) {
          console.error('Error parsing WebSocket message:', err);
        }
      };

      ws.onclose = (event) => {
        if (wsRef.current === ws) {
          wsRef.current = null;
        }
      };
    });
  };

  // Fetch session data
  const { data: session, isLoading: sessionLoading, error: sessionError } = useQuery(
    ['session', sessionId, mode],
    async () => {
      if (mode === 'load') {
        return loadAPI.getSession(sessionId!);
      } else {
        const [fullSession, status, schema] = await Promise.all([
          loadAPI.getSession(sessionId!).catch(() => null),
          qbsdAPI.getStatus(sessionId!),
          qbsdAPI.getSchema(sessionId!)
        ]);

        return {
          id: sessionId!,
          type: 'qbsd' as const,
          status: status.status as any,
          metadata: {
            source: `QBSD Query: ${schema.query || 'Unknown'}`,
            created: fullSession?.metadata?.created || new Date().toISOString(),
            last_modified: fullSession?.metadata?.last_modified || new Date().toISOString(),
            uploaded_documents: fullSession?.metadata?.uploaded_documents,
            processed_documents: fullSession?.metadata?.processed_documents,
            additional_rows_added: fullSession?.metadata?.additional_rows_added,
          },
          schema_query: schema.query,
          columns: schema.schema || [],
          statistics: fullSession?.statistics,
        } as VisualizationSession;
      }
    },
    {
      enabled: !!sessionId,
      refetchInterval: (data) => {
        if (mode === 'qbsd') return PROCESSING_REFRESH_INTERVAL;
        if (data?.status === 'processing_documents') return PROCESSING_REFRESH_INTERVAL;
        return false;
      },
    }
  );

  // Fetch data
  const { data: dataResponse, isLoading: dataLoading } = useQuery(
    ['data', sessionId, mode],
    async () => {
      if (mode === 'load') {
        return loadAPI.getData(sessionId!, 0, 100);
      } else {
        return qbsdAPI.getData(sessionId!, 0, 100);
      }
    },
    {
      enabled: !!sessionId && (
        session?.status === 'completed' ||
        session?.status === 'processing_documents' ||
        session?.status === 'documents_uploaded'
      ),
      refetchInterval: session?.status === 'processing_documents' ? PROCESSING_REFRESH_INTERVAL : false,
      keepPreviousData: true,
    }
  );

  // WebSocket effect for re-extraction and document processing
  // Uses wsRef to maintain connection across effect re-runs
  useEffect(() => {
    if (!sessionId) return;

    const wsUrl = `${WS_BASE_URL}/ws/progress/${sessionId}`;
    let reconnectAttempts = 0;
    let reconnectTimeout: NodeJS.Timeout | null = null;

    const connectWebSocket = () => {
      // Don't create duplicate connections
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        console.log('WebSocket already connected, skipping');
        return;
      }

      try {
        console.log('Creating WebSocket connection for session:', sessionId);
        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;

        ws.onopen = () => {
          console.log('WebSocket connected for session:', sessionId);
          reconnectAttempts = 0;
        };

        ws.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data);
            switch (message.type) {
              case 'cell_extracted':
                if (message.data?.row_name && message.data?.column) {
                  const cellData = message.data as CellExtractedData;
                  setStreamingCells(prev => {
                    const updated = new Map(prev);
                    const rowData = updated.get(cellData.row_name) || {};
                    rowData[cellData.column] = cellData.value;
                    updated.set(cellData.row_name, rowData);
                    return updated;
                  });
                }
                break;
              case 'row_completed':
                if (message.data?.row_name) {
                  setStreamingCells(prev => {
                    const updated = new Map(prev);
                    updated.delete(message.data.row_name);
                    return updated;
                  });
                }
                setNewlyAddedRows(prev => new Set(Array.from(prev).concat(message.data.row_index)));
                queryClient.invalidateQueries(['data', sessionId]);
                queryClient.invalidateQueries(['session', sessionId]);
                setTimeout(() => {
                  setNewlyAddedRows(prev => {
                    const newSet = new Set(Array.from(prev));
                    newSet.delete(message.data.row_index);
                    return newSet;
                  });
                }, NEW_ROW_HIGHLIGHT_DURATION);
                break;
              case 'completion':
                setStreamingCells(new Map());
                setForceWebSocketConnect(false);
                queryClient.refetchQueries(['session', sessionId, mode]);
                queryClient.refetchQueries(['data', sessionId, mode]);
                setTimeout(() => {
                  if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
                    wsRef.current.close(1000, 'Processing completed');
                    wsRef.current = null;
                  }
                }, 3000);
                break;
              case 'reextraction_started':
                // Initialize processing columns when re-extraction starts
                if (message.data?.columns && Array.isArray(message.data.columns)) {
                  setProcessingColumns(new Set(message.data.columns));
                }
                break;
              case 'document_started':
                // Update current document progress
                if (message.data?.document_name) {
                  setCurrentDocumentProgress({
                    documentName: message.data.document_name,
                    documentIndex: message.data.document_index || 0,
                    totalDocuments: message.data.total_documents || 0
                  });
                }
                break;
              case 'reextraction_progress':
                if (message.data?.column) {
                  setProcessingColumns(prev => {
                    const newSet = new Set(Array.from(prev));
                    newSet.add(message.data.column);
                    return newSet;
                  });
                }
                break;
              case 'reextraction_completed':
                console.log('Re-extraction completed:', message.data);
                setProcessingColumns(new Set()); // Clear processing state
                setCurrentDocumentProgress(null); // Clear document progress
                setStreamingCells(new Map());    // Clear streaming cells
                setForceWebSocketConnect(false); // Allow WebSocket to close
                queryClient.invalidateQueries(['session', sessionId, mode]);
                queryClient.invalidateQueries(['data', sessionId, mode]);
                break;
              case 'reextraction_stopped':
                console.log('Re-extraction stopped:', message.data);
                setProcessingColumns(new Set()); // Clear processing state
                setCurrentDocumentProgress(null); // Clear document progress
                setStreamingCells(new Map());    // Clear streaming cells
                setForceWebSocketConnect(false); // Allow WebSocket to close
                queryClient.invalidateQueries(['session', sessionId, mode]);
                queryClient.invalidateQueries(['data', sessionId, mode]);
                break;
            }
          } catch (err) {
            console.error('Error parsing WebSocket message:', err);
          }
        };

        ws.onclose = (event) => {
          console.log('WebSocket closed:', event.code, event.reason);
          wsRef.current = null;
          // Only reconnect if not a clean close and we still need the connection
          if (event.code !== 1000 && reconnectAttempts < WS_RECONNECT_ATTEMPTS) {
            const delay = Math.min(WS_RECONNECT_DELAY_BASE * Math.pow(2, reconnectAttempts), WS_RECONNECT_MAX_DELAY);
            reconnectTimeout = setTimeout(() => {
              reconnectAttempts++;
              connectWebSocket();
            }, delay);
          }
        };

        ws.onerror = (error) => {
          console.error('WebSocket error:', error);
        };
      } catch (error) {
        console.error('Error creating WebSocket:', error);
      }
    };

    // Determine if we should connect
    const shouldConnect = forceWebSocketConnect ||
      session?.status === 'processing_documents' ||
      (mode === 'qbsd' && session?.status === 'processing');

    if (shouldConnect) {
      connectWebSocket();
    }

    return () => {
      // Clear any pending reconnect
      if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
      }
      // Only close if we're unmounting AND not in the middle of re-extraction
      // Check forceWebSocketConnect via closure - if still true, don't close
    };
  }, [sessionId, mode, forceWebSocketConnect, session?.status, queryClient]);

  // Separate effect to close WebSocket when no longer needed
  useEffect(() => {
    if (!forceWebSocketConnect && session?.status !== 'processing_documents' &&
        !(mode === 'qbsd' && session?.status === 'processing')) {
      // No longer need WebSocket, close it
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        console.log('Closing WebSocket - no longer needed');
        wsRef.current.close(1000, 'No longer needed');
        wsRef.current = null;
      }
    }
  }, [forceWebSocketConnect, session?.status, mode]);

  // Schema data update listener
  useEffect(() => {
    const handleSchemaDataUpdate = (event: CustomEvent) => {
      const { sessionId: eventSessionId } = event.detail;
      if (eventSessionId === sessionId) {
        queryClient.invalidateQueries(['session', sessionId, mode]);
        queryClient.invalidateQueries(['data', sessionId, mode]);
      }
    };

    window.addEventListener('schema-data-updated', handleSchemaDataUpdate as EventListener);
    return () => {
      window.removeEventListener('schema-data-updated', handleSchemaDataUpdate as EventListener);
    };
  }, [sessionId, mode, queryClient]);

  const handleRefresh = () => {
    queryClient.invalidateQueries(['session', sessionId]);
    queryClient.invalidateQueries(['data', sessionId]);
  };

  const handleExport = async () => {
    try {
      // Use API_BASE_URL for production deployment
      const baseUrl = API_BASE_URL ? `${API_BASE_URL}/api` : '/api';
      let apiUrl = mode === 'load'
        ? `${baseUrl}/load/export/${sessionId}`
        : `${baseUrl}/qbsd/export/${sessionId}`;

      if (columnOrder.length > 0) {
        const orderParam = encodeURIComponent(columnOrder.join(','));
        apiUrl += `?column_order=${orderParam}`;
      }

      const response = await fetch(apiUrl);
      if (!response.ok) throw new Error('Export failed');

      const contentDisposition = response.headers.get('Content-Disposition');
      let filename = 'exported_data.csv';
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="?([^"]+)"?/);
        if (filenameMatch) filename = filenameMatch[1];
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(link);
    } catch (error) {
      console.error('Export error:', error);
      alert('Export failed. Please try again.');
    }
  };

  const handleDocumentProcessing = async () => {
    setShowLLMSelector(true);
  };

  const handleLLMSelection = async (llmConfig: any) => {
    setShowLLMSelector(false);
    if (!sessionId) {
      setDocumentUploadError('No session available');
      return;
    }

    setDocumentUploadLoading(true);
    setDocumentUploadError(null);

    try {
      // Retrieve API key from storage for the selected provider
      const apiKey = await getApiKeyForProvider(llmConfig.provider as LLMProvider);
      if (!apiKey) {
        setDocumentUploadError(`No API key configured for ${llmConfig.provider}. Please add your API key on the home page.`);
        setDocumentUploadLoading(false);
        return;
      }

      // Include API key in the config
      const configWithKey = {
        ...llmConfig,
        api_key: apiKey,
      };

      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        wsRef.current.close(1000, 'Reconnecting');
        wsRef.current = null;
      }
      await connectWebSocketSync();
      await new Promise(resolve => setTimeout(resolve, 300));

      try {
        await loadAPI.confirmWebSocketReady(sessionId);
      } catch (wsError) {
        console.warn('WebSocket confirmation failed:', wsError);
      }

      setForceWebSocketConnect(true);
      await loadAPI.processDocuments(sessionId, configWithKey);
      queryClient.invalidateQueries(['session', sessionId]);
    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to start processing';
      setDocumentUploadError(errorMessage);
      setForceWebSocketConnect(false);
      if (wsRef.current) {
        wsRef.current.close(1000, 'Error occurred');
        wsRef.current = null;
      }
    } finally {
      setDocumentUploadLoading(false);
    }
  };

  const handleDocumentUpload = async () => {
    if (!sessionId || uploadedDocuments.length === 0) {
      setDocumentUploadError('No session or documents');
      return;
    }

    setDocumentUploadLoading(true);
    setDocumentUploadError(null);

    try {
      const result = await loadAPI.addDocuments(sessionId, uploadedDocuments);
      setDocumentUploadResult(result);
      setUploadedDocuments([]);
      queryClient.invalidateQueries(['session', sessionId]);
    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to upload';
      setDocumentUploadError(errorMessage);
    } finally {
      setDocumentUploadLoading(false);
    }
  };

  const handleRemoveDocument = async (filename: string) => {
    if (!sessionId) return;

    setRemovingDocument(filename);
    setDocumentUploadError(null);

    try {
      await loadAPI.removeDocument(sessionId, filename);
      queryClient.invalidateQueries(['session', sessionId]);
    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to remove document';
      setDocumentUploadError(errorMessage);
    } finally {
      setRemovingDocument(null);
    }
  };

  const handleStopProcessing = async () => {
    if (!sessionId) return;

    setIsStoppingProcessing(true);

    try {
      const result = await loadAPI.stopProcessing(sessionId);
      if (result.stopped) {
        // Refresh session data to get updated status
        queryClient.invalidateQueries(['session', sessionId]);
        queryClient.invalidateQueries(['data', sessionId]);
      }
    } catch (err: any) {
      console.error('Failed to stop processing:', err);
      setDocumentUploadError(err.response?.data?.detail || err.message || 'Failed to stop processing');
    } finally {
      setIsStoppingProcessing(false);
    }
  };

  // Handle re-extraction started - set up WebSocket and processing columns
  const handleReextractionStarted = (columns: string[]) => {
    // Set the columns being processed (for skeleton display in table)
    setProcessingColumns(new Set(columns));
    // Force WebSocket connection to receive real-time updates
    setForceWebSocketConnect(true);
  };

  if (!sessionId) {
    return (
      <Alert variant="destructive">
        <AlertDescription>Invalid session ID</AlertDescription>
      </Alert>
    );
  }

  if (sessionLoading) {
    return (
      <div className="flex justify-center items-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  if (sessionError) {
    return (
      <Alert variant="destructive" className="mt-4">
        <AlertDescription>Failed to load session data</AlertDescription>
      </Alert>
    );
  }

  const isQBSDRunning = mode === 'qbsd' && session?.status === 'processing';
  const isQBSDStopped = mode === 'qbsd' && session?.status === 'stopped';
  const isSchemaReady = ['schema_ready', 'schema_extracted', 'documents_uploaded', 'processing_documents', 'completed', 'stopped'].includes(session?.status || '') ||
    (mode === 'qbsd' && session?.status === 'processing' && (session?.columns?.length ?? 0) > 0);
  const isCompleted = session?.status === 'completed';
  const isEnhancedUploadProcessing = session?.status === 'processing_documents';

  const getStatusBadge = () => {
    const status = session?.status;
    const variants: Record<string, 'default' | 'success' | 'warning' | 'destructive' | 'info'> = {
      completed: 'success',
      stopped: 'warning',
      schema_ready: 'info',
      schema_extracted: 'info',
      documents_uploaded: 'warning',
      processing_documents: 'warning',
      processing: 'warning',
      error: 'destructive',
    };
    const labels: Record<string, string> = {
      stopped: 'Stopped (Partial)',
      schema_ready: 'Schema Ready',
      schema_extracted: 'Schema Extracted',
      documents_uploaded: 'Documents Ready',
      processing_documents: 'Processing Documents',
    };
    return (
      <Badge variant={variants[status || ''] || 'default'}>
        {labels[status || ''] || status || 'Unknown'}
      </Badge>
    );
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between border-b pb-4">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="sm" onClick={() => navigate('/')}>
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back
          </Button>
          <div>
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Link to="/" className="hover:text-foreground">Home</Link>
              <span>/</span>
              <span>{session?.type === 'load' ? 'Load Session' : 'QBSD Session'}</span>
            </div>
            <h1 className="text-lg font-semibold">{session?.metadata.source}</h1>
          </div>
        </div>

        <div className="flex items-center gap-2">
          {getStatusBadge()}
          {(isCompleted || isEnhancedUploadProcessing || isQBSDStopped) && (
            <>
              <Button variant="ghost" size="sm" onClick={handleRefresh}>
                <RefreshCw className="h-4 w-4 mr-2" />
                Refresh
              </Button>
              <Button variant="outline" size="sm" onClick={handleExport}>
                <Download className="h-4 w-4 mr-2" />
                Export{isQBSDStopped ? ' Partial' : ''}
              </Button>
            </>
          )}
        </div>
      </div>

      {/* Tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
        <TabsList>
          <TabsTrigger value="data" disabled={!isCompleted && !isEnhancedUploadProcessing && !isQBSDRunning && !isQBSDStopped} className="gap-2">
            <Table2 className="h-4 w-4" />
            Data
          </TabsTrigger>
          <TabsTrigger value="schema" disabled={!isSchemaReady || !session?.columns?.length} className="gap-2">
            <Database className="h-4 w-4" />
            Schema
          </TabsTrigger>
          <TabsTrigger value="stats" disabled={!isCompleted && !isQBSDStopped} className="gap-2">
            <BarChart3 className="h-4 w-4" />
            Statistics
          </TabsTrigger>
          {mode === 'qbsd' && (
            <TabsTrigger value="monitor" className="gap-2">
              {session?.status === 'processing' ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : session?.status === 'completed' ? (
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              ) : session?.status === 'stopped' ? (
                <Square className="h-4 w-4 text-yellow-500" />
              ) : session?.status === 'error' ? (
                <XCircle className="h-4 w-4 text-red-500" />
              ) : (
                <Play className="h-4 w-4" />
              )}
              QBSD Monitor
            </TabsTrigger>
          )}
          {mode === 'load' && isEnhancedUploadProcessing && (
            <TabsTrigger value="processing" className="gap-2">
              <Loader2 className="h-4 w-4 animate-spin" />
              Processing Monitor
            </TabsTrigger>
          )}
        </TabsList>

        {/* Data Tab */}
        <TabsContent value="data" className="mt-4">
          {(isCompleted || isEnhancedUploadProcessing || isQBSDRunning || isQBSDStopped || session?.status === 'documents_uploaded') && (dataResponse || streamingCells.size > 0) ? (
            <div className="relative">
              <DataTable
                sessionId={sessionId!}
                sessionType={mode}
                newlyAddedRows={newlyAddedRows}
                columnOrder={columnOrder}
                onColumnReorder={handleColumnReorder}
                streamingCells={streamingCells}
                processingColumns={processingColumns}
                currentDocumentProgress={currentDocumentProgress}
              />

              {/* Document Upload Section */}
              {((mode === 'load' && ['documents_uploaded', 'processing_documents', 'completed'].includes(session?.status || '')) ||
                (mode === 'qbsd' && ['completed', 'documents_uploaded', 'processing_documents'].includes(session?.status || ''))) &&
                !sessionLoading && !dataLoading && dataResponse && (
                  <Card className="mt-6">
                    <CardHeader>
                      <CardTitle>
                        {mode === 'qbsd'
                          ? 'Add More Documents'
                          : session?.status === 'documents_uploaded'
                            ? 'Process Your Documents'
                            : 'Add More Documents'}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <p className="text-sm text-muted-foreground">
                        {mode === 'qbsd'
                          ? 'Upload additional documents to extract more data using your discovered schema.'
                          : session?.status === 'documents_uploaded'
                            ? 'You have uploaded documents that are ready to be processed.'
                            : 'Upload additional documents to extract more data using your existing schema.'}
                      </p>

                      {session?.metadata?.uploaded_documents && session.metadata.uploaded_documents.length > 0 && (
                        <div className="space-y-2">
                          <p className="text-sm font-medium">Uploaded documents ({session.metadata.uploaded_documents.length}):</p>
                          <div className="flex flex-wrap gap-2">
                            {session.metadata.uploaded_documents.map((doc, index) => (
                              <div
                                key={`${doc}-${index}`}
                                className="flex items-center gap-1 px-3 py-1.5 bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-full text-sm"
                              >
                                <FileText className="h-3.5 w-3.5 text-blue-600 dark:text-blue-400" />
                                <span className="text-blue-700 dark:text-blue-300 max-w-[200px] truncate" title={doc}>
                                  {doc}
                                </span>
                                {session.status !== 'processing_documents' && (
                                  <button
                                    onClick={() => handleRemoveDocument(doc)}
                                    disabled={removingDocument === doc}
                                    className="ml-1 p-0.5 hover:bg-blue-200 dark:hover:bg-blue-800 rounded-full transition-colors disabled:opacity-50"
                                    title={`Remove ${doc}`}
                                  >
                                    {removingDocument === doc ? (
                                      <Loader2 className="h-3.5 w-3.5 text-blue-600 dark:text-blue-400 animate-spin" />
                                    ) : (
                                      <X className="h-3.5 w-3.5 text-blue-600 dark:text-blue-400 hover:text-red-600 dark:hover:text-red-400" />
                                    )}
                                  </button>
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {documentUploadError && (
                        <Alert variant="destructive">
                          <AlertDescription>{documentUploadError}</AlertDescription>
                        </Alert>
                      )}

                      <DocumentUpload
                        onFilesChange={setUploadedDocuments}
                        uploadedFiles={uploadedDocuments}
                        loading={documentUploadLoading}
                        onUpload={handleDocumentUpload}
                        canUpload={true}
                        uploadResult={documentUploadResult}
                        sessionId={sessionId}
                        onCloudDocumentsAdd={async (dataset, files) => {
                          const result = await cloudAPI.addCloudDocuments(sessionId!, dataset, files);
                          if (result.added_files?.length > 0) {
                            queryClient.invalidateQueries(['session', sessionId]);
                            setDocumentUploadResult({
                              status: 'success',
                              message: `Added ${result.added_files.length} cloud documents`,
                              uploaded_files: result.added_files,
                              warnings: result.errors || []
                            });
                          }
                          if (result.errors?.length) {
                            throw new Error(result.errors.join(', '));
                          }
                        }}
                      />

                      {((session?.metadata?.uploaded_documents?.length || 0) > 0 || documentUploadResult?.uploaded_files?.length > 0) && (
                        <div className="pt-4 border-t">
                          {session?.status === 'processing_documents' && (
                            <div className="flex items-center gap-2 mb-4">
                              <Loader2 className="h-5 w-5 animate-spin" />
                              <span>Processing in progress...</span>
                            </div>
                          )}

                          {session?.status === 'completed' && (session?.metadata?.additional_rows_added ?? 0) > 0 && (
                            <Alert variant="success" className="mb-4">
                              <AlertDescription>
                                Processing completed! Added {session.metadata.additional_rows_added} new rows.
                              </AlertDescription>
                            </Alert>
                          )}

                          {session?.status !== 'completed' && (
                            <Button
                              onClick={handleDocumentProcessing}
                              disabled={session?.status?.includes('processing') || documentUploadLoading}
                            >
                              {documentUploadLoading ? (
                                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                              ) : null}
                              {documentUploadLoading ? 'Starting...' :
                                session?.status?.includes('processing') ? 'Processing...' :
                                  'Select AI Model & Process Documents'}
                            </Button>
                          )}
                        </div>
                      )}
                    </CardContent>
                  </Card>
                )}
            </div>
          ) : (
            <Alert variant="info">
              <AlertDescription>
                {isQBSDRunning ? 'Data will be available when QBSD processing completes' : 'No data available'}
              </AlertDescription>
            </Alert>
          )}
        </TabsContent>

        {/* Schema Tab */}
        <TabsContent value="schema" className="mt-4">
          {session?.columns?.length ? (
            <SchemaViewer
              columns={session.columns}
              query={session.schema_query}
              sessionId={sessionId}
              sessionType={session.type}
              readonly={false}
              processingColumns={processingColumns}
              onColumnsChange={() => {
                queryClient.invalidateQueries(['session', sessionId, mode]);
                queryClient.invalidateQueries(['data', sessionId, mode]);
              }}
              onReextractionStarted={handleReextractionStarted}
              llmConfig={session.metadata?.extracted_schema?.llm_configuration?.schema_creation_backend || null}
            />
          ) : (
            <Alert variant="info">
              <AlertDescription>No schema available</AlertDescription>
            </Alert>
          )}
        </TabsContent>

        {/* Statistics Tab */}
        <TabsContent value="stats" className="mt-4">
          {session?.statistics ? (
            <StatsDashboard
              statistics={session.statistics}
              session={session}
              creationMetadata={session.creation_metadata}
              modificationHistory={session.modification_history}
            />
          ) : isCompleted ? (
            <Alert variant="warning">
              <AlertDescription>Statistics not available</AlertDescription>
            </Alert>
          ) : (
            <Alert variant="info">
              <AlertDescription>Statistics will be available when processing completes</AlertDescription>
            </Alert>
          )}
        </TabsContent>

        {/* QBSD Monitor Tab */}
        {mode === 'qbsd' && (
          <TabsContent value="monitor" className="mt-4">
            <QBSDMonitor sessionId={sessionId} />
          </TabsContent>
        )}

        {/* Processing Monitor Tab */}
        {mode === 'load' && isEnhancedUploadProcessing && (
          <TabsContent value="processing" className="mt-4">
            <UploadProcessingMonitor
              sessionId={sessionId}
              status={{
                session_id: sessionId || '',
                status: session?.status || 'processing_documents',
                total_documents: session?.metadata?.uploaded_documents?.length || 0,
                processed_documents: session?.metadata?.processed_documents || 0,
                original_row_count: session?.metadata?.original_row_count || 0,
                additional_rows_added: session?.metadata?.additional_rows_added || 0,
                processing_stats: session?.metadata?.processing_stats || {},
                progress: session?.metadata?.processed_documents && session?.metadata?.uploaded_documents?.length
                  ? session.metadata.processed_documents / session.metadata.uploaded_documents.length
                  : 0,
                last_modified: session?.metadata?.last_modified || new Date().toISOString(),
              }}
              loading={false}
              onStop={handleStopProcessing}
              isStopping={isStoppingProcessing}
              llmConfig={session?.metadata?.extracted_schema?.llm_configuration?.value_extraction_backend || null}
            />
          </TabsContent>
        )}
      </Tabs>

      {/* LLM Selection Dialog */}
      <LLMSelector
        open={showLLMSelector}
        onClose={() => setShowLLMSelector(false)}
        onConfirm={handleLLMSelection}
        title="Select AI Model for Document Processing"
        description="Choose the AI model that will extract information from your uploaded documents."
        preservedConfig={session?.metadata?.extracted_schema?.llm_configuration?.value_extraction_backend || null}
        loading={documentUploadLoading}
      />
    </div>
  );
};

export default Visualize;
