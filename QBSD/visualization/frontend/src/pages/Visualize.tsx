import React, { useState, useEffect } from 'react';
import { useParams, useSearchParams, useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  AppBar,
  Toolbar,
  Tabs,
  Tab,
  Button,
  Chip,
  Alert,
  CircularProgress,
  Breadcrumbs,
  Link,
  Paper,
  IconButton,
} from '@mui/material';
import {
  ArrowBack,
  TableView,
  Schema,
  Analytics,
  Download,
  Refresh,
  Info,
  CheckCircle,
  PlayArrow,
  Error as ErrorIcon,
  DragIndicator,
  Close,
} from '@mui/icons-material';
import { useQuery, useQueryClient } from 'react-query';

import { uploadAPI, qbsdAPI } from '../services/api';
import { VisualizationSession, CellValue, CellExtractedData } from '../types';
import {
  PROCESSING_REFRESH_INTERVAL, 
  NEW_ROW_HIGHLIGHT_DURATION,
  WS_RECONNECT_ATTEMPTS,
  WS_RECONNECT_DELAY_BASE,
  WS_RECONNECT_MAX_DELAY
} from '../constants/index';

// Component imports (will be created next)
import DataTable from '../components/DataTable/DataTable';
import SchemaViewer from '../components/SchemaViewer/SchemaViewer';
import StatsDashboard from '../components/StatsDashboard/StatsDashboard';
import QBSDMonitor from '../components/QBSDMonitor/QBSDMonitor';
import UploadProcessingMonitor from '../components/UploadProcessingMonitor/UploadProcessingMonitor';
import DocumentUpload from '../components/DocumentUpload/DocumentUpload';
import ConfigurationInfo from '../components/ConfigurationInfo';
import LLMSelector from '../components/LLMSelector';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

const TabPanel: React.FC<TabPanelProps> = ({ children, value, index }) => (
  <div hidden={value !== index} style={{ paddingTop: 16 }}>
    {value === index && children}
  </div>
);

const Visualize: React.FC = () => {
  const { sessionId } = useParams<{ sessionId: string }>();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  
  const mode = searchParams.get('mode') as 'upload' | 'qbsd' || 'upload';
  const [activeTab, setActiveTab] = useState(mode === 'qbsd' ? 4 : 0); // Start with QBSD Monitor (tab index 4) for QBSD mode
  
  // Enhanced upload document management state
  const [uploadedDocuments, setUploadedDocuments] = useState<File[]>([]);
  const [documentUploadLoading, setDocumentUploadLoading] = useState(false);
  const [documentUploadResult, setDocumentUploadResult] = useState<any>(null);
  const [documentUploadError, setDocumentUploadError] = useState<string | null>(null);
  const [newlyAddedRows, setNewlyAddedRows] = useState<Set<number>>(new Set());

  // Streaming cells state - stores cell values as they're extracted in real-time
  // Map<row_name, Record<column_name, value>>
  const [streamingCells, setStreamingCells] = useState<Map<string, Record<string, CellValue>>>(new Map());
  
  // LLM selection state for document processing
  const [showLLMSelector, setShowLLMSelector] = useState(false);
  const [selectedLLMConfig, setSelectedLLMConfig] = useState<any>(null);

  // Column order state for drag-drop reordering
  const [columnOrder, setColumnOrder] = useState<string[]>([]);

  // Draggable processing overlay state
  const [overlayPosition, setOverlayPosition] = useState({ x: 0, y: 0 });
  const [isDragging, setIsDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });

  // Force WebSocket connection when processing starts (before status poll catches up)
  const [forceWebSocketConnect, setForceWebSocketConnect] = useState(false);

  // WebSocket reference for direct connection control
  const wsRef = React.useRef<WebSocket | null>(null);

  // Load column order from localStorage on mount
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

  // Handler for column reorder - saves to localStorage
  const handleColumnReorder = (newOrder: string[]) => {
    setColumnOrder(newOrder);
    if (sessionId) {
      localStorage.setItem(`columnOrder_${sessionId}`, JSON.stringify(newOrder));
    }
  };

  // Handlers for draggable processing overlay
  const handleOverlayMouseDown = (e: React.MouseEvent) => {
    // Only start drag if clicking on the drag handle area
    if ((e.target as HTMLElement).closest('.drag-handle')) {
      setIsDragging(true);
      setDragStart({
        x: e.clientX - overlayPosition.x,
        y: e.clientY - overlayPosition.y
      });
      e.preventDefault();
    }
  };

  const handleOverlayMouseMove = (e: React.MouseEvent) => {
    if (isDragging) {
      setOverlayPosition({
        x: e.clientX - dragStart.x,
        y: e.clientY - dragStart.y
      });
    }
  };

  const handleOverlayMouseUp = () => {
    setIsDragging(false);
  };

  // Function to establish WebSocket connection and return a Promise that resolves when connected
  const connectWebSocketSync = (): Promise<WebSocket> => {
    return new Promise((resolve, reject) => {
      // WebSocket needs to connect directly to the backend server (port 8000)
      // The CRA proxy only handles HTTP, not WebSocket connections
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const backendHost = process.env.REACT_APP_WS_HOST || 'localhost:8000';
      const wsUrl = `${protocol}//${backendHost}/ws/progress/${sessionId}`;

      console.log('🔌 Creating WebSocket connection to:', wsUrl);
      const ws = new WebSocket(wsUrl);

      const timeout = setTimeout(() => {
        if (ws.readyState !== WebSocket.OPEN) {
          ws.close();
          reject(new Error('WebSocket connection timeout'));
        }
      }, 5000); // 5 second timeout

      ws.onopen = () => {
        console.log('🔌 WebSocket CONNECTED (sync) for session:', sessionId);
        clearTimeout(timeout);
        wsRef.current = ws;
        resolve(ws);
      };

      ws.onerror = (error) => {
        console.error('🔌 WebSocket error (sync):', error);
        clearTimeout(timeout);
        reject(error);
      };

      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          console.log('📨 WebSocket message received (sync):', message.type, message);

          switch (message.type) {
            case 'connected':
              console.log('🔌 WebSocket confirmed connection:', message);
              break;

            case 'cell_extracted':
              // Real-time cell value streaming - update streaming cells state
              console.log('📦 CELL_EXTRACTED received:', message.data);
              if (message.data?.row_name && message.data?.column) {
                const cellData = message.data as CellExtractedData;
                console.log(`📦 Updating streaming cells: ${cellData.row_name} / ${cellData.column} = ${JSON.stringify(cellData.value).substring(0, 50)}...`);
                setStreamingCells(prev => {
                  const updated = new Map(prev);
                  const rowData = updated.get(cellData.row_name) || {};
                  rowData[cellData.column] = cellData.value;
                  updated.set(cellData.row_name, rowData);
                  console.log(`📦 Streaming cells now has ${updated.size} rows`);
                  return updated;
                });
              } else {
                console.warn('⚠️ cell_extracted missing row_name or column:', message.data);
              }
              break;

            case 'row_completed':
              // Row is complete - clear streaming data for this row
              if (message.data?.row_name) {
                setStreamingCells(prev => {
                  const updated = new Map(prev);
                  updated.delete(message.data.row_name);
                  return updated;
                });
              }
              // Mark this row as newly added for visual highlighting
              setNewlyAddedRows(prev => new Set(Array.from(prev).concat(message.data.row_index)));
              // Refresh data to show the new row
              queryClient.invalidateQueries(['data', sessionId]);
              queryClient.invalidateQueries(['session', sessionId]);
              // Remove highlight after delay
              setTimeout(() => {
                setNewlyAddedRows(prev => {
                  const newSet = new Set(Array.from(prev));
                  newSet.delete(message.data.row_index);
                  return newSet;
                });
              }, NEW_ROW_HIGHLIGHT_DURATION);
              break;

            case 'completion':
              console.log('🎉 WebSocket: Processing completion received (sync)', message);
              setStreamingCells(new Map());
              setForceWebSocketConnect(false);
              queryClient.refetchQueries(['session', sessionId, mode]);
              queryClient.refetchQueries(['data', sessionId, mode]);
              // Close WebSocket after completion
              setTimeout(() => {
                if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
                  console.log('🔌 Closing sync WebSocket after completion');
                  wsRef.current.close(1000, 'Processing completed');
                  wsRef.current = null;
                }
              }, 3000);
              break;
          }
        } catch (err) {
          console.error('Error parsing WebSocket message (sync):', err);
        }
      };

      ws.onclose = (event) => {
        console.log('🔌 WebSocket closed (sync):', event.code, event.reason);
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
      if (mode === 'upload') {
        return uploadAPI.getSession(sessionId!);
      } else {
        // For QBSD, get full session from session manager (for metadata like uploaded_documents)
        // plus status and schema from QBSD API
        const [fullSession, status, schema] = await Promise.all([
          uploadAPI.getSession(sessionId!).catch(() => null), // May fail if session doesn't exist in upload API
          qbsdAPI.getStatus(sessionId!),
          qbsdAPI.getSchema(sessionId!)
        ]);

        const sessionData = {
          id: sessionId!,
          type: 'qbsd' as const,
          status: status.status as any,
          metadata: {
            source: `QBSD Query: ${schema.query || 'Unknown'}`,
            created: fullSession?.metadata?.created || new Date().toISOString(),
            last_modified: fullSession?.metadata?.last_modified || new Date().toISOString(),
            // Include document-related metadata from full session
            uploaded_documents: fullSession?.metadata?.uploaded_documents,
            processed_documents: fullSession?.metadata?.processed_documents,
            additional_rows_added: fullSession?.metadata?.additional_rows_added,
          },
          schema_query: schema.query,
          columns: schema.schema || [],
          statistics: fullSession?.statistics,
        } as VisualizationSession;

        // Debug logging
        console.log('🔍 Session Data Update:', {
          status: status.status,
          columnsCount: schema.schema?.length || 0,
          hasColumns: !!(schema.schema && schema.schema.length > 0),
          uploadedDocuments: fullSession?.metadata?.uploaded_documents?.length || 0
        });

        return sessionData;
      }
    },
    {
      enabled: !!sessionId,
      // Auto-refresh for QBSD mode OR when processing documents in upload mode
      refetchInterval: (data) => {
        if (mode === 'qbsd') return PROCESSING_REFRESH_INTERVAL;
        // For upload mode, poll during document processing to detect completion
        if (data?.status === 'processing_documents') return PROCESSING_REFRESH_INTERVAL;
        return false;
      },
    }
  );

  // Fetch data
  const { data: dataResponse, isLoading: dataLoading } = useQuery(
    ['data', sessionId, mode],
    async () => {
      if (mode === 'upload') {
        return uploadAPI.getData(sessionId!, 0, 100);
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
      refetchInterval: session?.status === 'processing_documents' ? PROCESSING_REFRESH_INTERVAL : false, // Auto-refresh during processing for faster updates
      keepPreviousData: true, // Keep previous data while fetching new data to prevent disappearing
    }
  );

  // Reset overlay position when processing completes
  useEffect(() => {
    if (session?.status !== 'processing_documents') {
      setOverlayPosition({ x: 0, y: 0 });
    }
  }, [session?.status]);

  // WebSocket integration for real-time updates (both upload and qbsd modes)
  useEffect(() => {
    if (!sessionId) return;

    // WebSocket needs to connect directly to the backend server (port 8000)
    // The CRA proxy only handles HTTP, not WebSocket connections
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const backendHost = process.env.REACT_APP_WS_HOST || 'localhost:8000';
    const wsUrl = `${protocol}//${backendHost}/ws/progress/${sessionId}`;
    
    let ws: WebSocket | null = null;
    let reconnectAttempts = 0;
    const maxReconnectAttempts = WS_RECONNECT_ATTEMPTS;
    
    const connectWebSocket = () => {
      try {
        ws = new WebSocket(wsUrl);
        
        ws.onopen = () => {
          console.log('🔌 WebSocket connected for upload session:', sessionId);
          reconnectAttempts = 0;
        };
        
        ws.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data);
            console.log('📨 WebSocket message received:', message.type, message);

            switch (message.type) {
              case 'cell_extracted':
                // Real-time cell value streaming - update streaming cells state
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
                // Row is complete - clear streaming data for this row (now in database)
                if (message.data?.row_name) {
                  setStreamingCells(prev => {
                    const updated = new Map(prev);
                    updated.delete(message.data.row_name);
                    return updated;
                  });
                }

                // Mark this row as newly added for visual highlighting
                setNewlyAddedRows(prev => new Set(Array.from(prev).concat(message.data.row_index)));

                // Refresh data to show the new row
                queryClient.invalidateQueries(['data', sessionId]);
                queryClient.invalidateQueries(['session', sessionId]);

                // Remove highlight after 5 seconds
                setTimeout(() => {
                  setNewlyAddedRows(prev => {
                    const newSet = new Set(Array.from(prev));
                    newSet.delete(message.data.row_index);
                    return newSet;
                  });
                }, NEW_ROW_HIGHLIGHT_DURATION);
                break;
                
              case 'progress_update':
              case 'completion':
                console.log('🎉 WebSocket: Processing completion received', message);
                // Immediately invalidate and refresh session and data queries
                // Use exact query keys to ensure proper cache updates
                queryClient.invalidateQueries(['session', sessionId, mode]);
                queryClient.invalidateQueries(['data', sessionId, mode]);

                if (message.type === 'completion') {
                  console.log('💾 WebSocket: Completion message - forcing immediate data refresh');

                  // Clear all streaming cells on completion - data is now in the database
                  setStreamingCells(new Map());

                  // Reset force WebSocket connect flag
                  setForceWebSocketConnect(false);

                  // Force immediate refetch with exact query keys to ensure UI updates
                  queryClient.refetchQueries(['session', sessionId, mode]);
                  queryClient.refetchQueries(['data', sessionId, mode]);

                  // Also refetch with partial keys for backward compatibility
                  queryClient.refetchQueries(['session', sessionId]);
                  queryClient.refetchQueries(['data', sessionId]);

                  // Disconnect WebSocket after completion to avoid keeping it open unnecessarily
                  setTimeout(() => {
                    if (ws && ws.readyState === WebSocket.OPEN) {
                      console.log('🔌 WebSocket: Closing connection after processing completion');
                      ws.close(1000, 'Processing completed');
                    }
                  }, 3000); // Longer delay to ensure all refreshes complete
                }
                break;
            }
          } catch (err) {
            console.error('Error parsing WebSocket message:', err);
          }
        };
        
        ws.onerror = (error) => {
          console.error('WebSocket error:', error);
        };
        
        ws.onclose = (event) => {
          console.log('WebSocket closed:', event.code, event.reason);
          ws = null;
          
          // Attempt to reconnect if not a manual close
          if (event.code !== 1000 && reconnectAttempts < maxReconnectAttempts) {
            const delay = Math.min(WS_RECONNECT_DELAY_BASE * Math.pow(2, reconnectAttempts), WS_RECONNECT_MAX_DELAY);
            console.log(`Attempting to reconnect in ${delay}ms (attempt ${reconnectAttempts + 1}/${maxReconnectAttempts})`);
            
            setTimeout(() => {
              reconnectAttempts++;
              connectWebSocket();
            }, delay);
          }
        };
      } catch (error) {
        console.error('Error creating WebSocket connection:', error);
      }
    };
    
    // Connect during document processing and maintain connection for completed status to catch final messages
    // Also connect during QBSD processing to receive real-time cell updates
    // IMPORTANT: Skip if wsRef.current is already connected (from handleLLMSelection)
    const alreadyConnected = wsRef.current && wsRef.current.readyState === WebSocket.OPEN;

    if (forceWebSocketConnect || session?.status === 'processing_documents') {
      if (alreadyConnected) {
        console.log('🔌 WebSocket already connected via sync method, skipping useEffect connection');
      } else {
        console.log('🔌 Connecting WebSocket for document processing (force:', forceWebSocketConnect, ', status:', session?.status, ')');
        connectWebSocket();
      }
    } else if (mode === 'qbsd' && session?.status === 'processing') {
      // QBSD mode: connect during processing to receive cell_extracted events
      if (!alreadyConnected) {
        console.log('🔌 Connecting WebSocket for QBSD processing');
        connectWebSocket();
      }
    } else if (session?.status === 'completed' && (session?.metadata?.additional_rows_added || 0) > 0) {
      // For recently completed sessions with added rows, maintain connection briefly to catch any late messages
      if (!alreadyConnected) {
        console.log('🔌 Connecting WebSocket for recently completed session');
        connectWebSocket();
      }
    }
    
    // Fallback refresh when session transitions to completed to ensure UI updates
    if (session?.status === 'completed') {
      const fallbackRefresh = setTimeout(() => {
        console.log('🔄 Fallback refresh: Force refreshing queries for completed session');
        queryClient.invalidateQueries(['session', sessionId]);
        queryClient.invalidateQueries(['data', sessionId]);
        queryClient.invalidateQueries(['data', sessionId, mode]);
        // Also force immediate refetch
        queryClient.refetchQueries(['session', sessionId]);
        queryClient.refetchQueries(['data', sessionId]);
      }, 500); // Shorter delay for faster UI update
      
      return () => {
        clearTimeout(fallbackRefresh);
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.close(1000, 'Component unmounting');
        }
      };
    }
    
    return () => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close(1000, 'Component unmounting');
      }
    };
  }, [sessionId, mode, session?.status, queryClient, forceWebSocketConnect]);

  // Listen for schema data update events from SchemaViewer
  useEffect(() => {
    const handleSchemaDataUpdate = (event: CustomEvent) => {
      const { sessionId: eventSessionId, operation } = event.detail;
      if (eventSessionId === sessionId) {
        console.log('🔄 Handling schema data update:', operation);
        // Invalidate all relevant queries to force refresh
        queryClient.invalidateQueries(['session', sessionId, mode]);
        queryClient.invalidateQueries(['data', sessionId, mode]);
        queryClient.invalidateQueries(['data', sessionId]);
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
      // Build URL with column order if user has reordered columns
      let apiUrl = mode === 'upload'
        ? `/api/upload/export/${sessionId}`
        : `/api/qbsd/export/${sessionId}`;

      // Add column order as query parameter if available
      if (columnOrder.length > 0) {
        const orderParam = encodeURIComponent(columnOrder.join(','));
        apiUrl += `?column_order=${orderParam}`;
      }

      const response = await fetch(apiUrl);
      
      if (!response.ok) {
        throw new Error('Export failed');
      }
      
      // Get filename from response headers or create default
      const contentDisposition = response.headers.get('Content-Disposition');
      let filename = 'exported_data.csv';
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="?([^"]+)"?/);
        if (filenameMatch) {
          filename = filenameMatch[1];
        }
      }
      
      // Download the file
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      
      // Cleanup
      window.URL.revokeObjectURL(url);
      document.body.removeChild(link);
      
    } catch (error) {
      console.error('Export error:', error);
      // You could show a toast/snackbar here for better UX
      alert('Export failed. Please try again.');
    }
  };

  // Enhanced upload document handlers
  const handleDocumentProcessing = async () => {
    // Show LLM selector for document processing
    setShowLLMSelector(true);
  };

  const handleLLMSelection = async (llmConfig: any) => {
    setSelectedLLMConfig(llmConfig);
    setShowLLMSelector(false);

    if (!sessionId) {
      setDocumentUploadError('No session available for processing');
      return;
    }

    setDocumentUploadLoading(true);
    setDocumentUploadError(null);

    try {
      // CRITICAL: Connect WebSocket FIRST and WAIT for it to be open
      // This ensures we're connected before the first cell_extracted event
      console.log('🔌 Establishing WebSocket connection BEFORE starting processing...');

      // Close any existing connection first
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        wsRef.current.close(1000, 'Reconnecting for new processing');
        wsRef.current = null;
      }

      await connectWebSocketSync();
      console.log('🔌 WebSocket connected locally!');

      // Small delay for backend to fully register the connection
      await new Promise(resolve => setTimeout(resolve, 300));

      // Verify WebSocket is registered on backend before starting processing
      try {
        const wsConfirmation = await uploadAPI.confirmWebSocketReady(sessionId);
        console.log('✅ WebSocket confirmed on backend:', wsConfirmation);
      } catch (wsError) {
        console.warn('⚠️ WebSocket confirmation failed, proceeding anyway (buffering will catch events):', wsError);
        // Don't fail here - the buffering mechanism will catch any early events
      }

      setForceWebSocketConnect(true); // Keep flag for useEffect to not create duplicate

      // NOW start document processing - WebSocket is confirmed ready
      console.log('🚀 Starting document processing...');
      await uploadAPI.processDocuments(sessionId, llmConfig);

      // Refresh session data to show updated status
      queryClient.invalidateQueries(['session', sessionId]);

      console.log('🤖 Document processing started with LLM config:', llmConfig);
    } catch (err: any) {
      console.error('Failed to start document processing:', err);
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to start document processing';
      setDocumentUploadError(errorMessage);
      // Reset force connect and close WebSocket on error
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
      setDocumentUploadError('No session or documents to upload');
      return;
    }

    setDocumentUploadLoading(true);
    setDocumentUploadError(null);
    
    try {
      const result = await uploadAPI.addDocuments(sessionId, uploadedDocuments);
      setDocumentUploadResult(result);
      setUploadedDocuments([]); // Clear uploaded files
      
      // Refresh session data to show updated status
      queryClient.invalidateQueries(['session', sessionId]);
    } catch (err: any) {
      console.error('Failed to upload documents:', err);
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to upload documents';
      setDocumentUploadError(errorMessage);
    } finally {
      setDocumentUploadLoading(false);
    }
  };


  const startDocumentProcessingMonitor = () => {
    const pollStatus = async () => {
      if (!sessionId) return;
      
      try {
        // Refresh both session and data
        queryClient.invalidateQueries(['session', sessionId]);
        queryClient.invalidateQueries(['data', sessionId]);
        
        // Check session status
        const session = await uploadAPI.getSession(sessionId);
        
        if (session.status === 'completed') {
          // Processing completed, stop polling
          return;
        } else if (session.status === 'processing_documents') {
          // Continue polling
          setTimeout(pollStatus, 2000); // Poll every 2 seconds
        } else if (session.status === 'error') {
          setDocumentUploadError('Document processing failed');
          return;
        }
      } catch (err: any) {
        console.error('Error monitoring processing:', err);
        // Retry with longer delay
        setTimeout(pollStatus, 5000);
      }
    };

    pollStatus();
  };

  if (!sessionId) {
    return (
      <Alert severity="error">
        Invalid session ID
      </Alert>
    );
  }

  if (sessionLoading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', mt: 8 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (sessionError) {
    return (
      <Alert severity="error" sx={{ mt: 4 }}>
        Failed to load session data
      </Alert>
    );
  }

  const isQBSDRunning = mode === 'qbsd' && session?.status === 'processing';
  const isSchemaReady = session?.status === 'schema_ready' ||
                       session?.status === 'schema_extracted' ||
                       session?.status === 'documents_uploaded' ||
                       session?.status === 'processing_documents' ||
                       session?.status === 'completed' ||
                       // For QBSD: schema is ready during processing if columns exist
                       (mode === 'qbsd' && session?.status === 'processing' && (session?.columns?.length ?? 0) > 0);
  const isCompleted = session?.status === 'completed';
  const isEnhancedUploadProcessing = session?.status === 'processing_documents';
  
  // Debug logging for tab enablement
  console.log('🎯 Tab Enablement Debug:', {
    sessionStatus: session?.status,
    isSchemaReady,
    hasColumns: !!(session?.columns?.length),
    columnsCount: session?.columns?.length || 0,
    tabWillBeEnabled: isSchemaReady && !!(session?.columns?.length)
  });
  
  // Debug logging
  console.log('DEBUG Frontend:', {
    mode,
    sessionStatus: session?.status,
    isCompleted,
    dataResponse: !!dataResponse,
    dataLoading,
    sessionId
  });

  return (
    <Box>
      {/* Header */}
      <AppBar position="static" color="default" elevation={1}>
        <Toolbar>
          <Button
            startIcon={<ArrowBack />}
            onClick={() => navigate('/')}
            sx={{ mr: 2 }}
          >
            Back
          </Button>
          
          <Box sx={{ flexGrow: 1 }}>
            <Breadcrumbs>
              <Link underline="hover" color="inherit" onClick={() => navigate('/')}>
                Home
              </Link>
              <Typography color="text.primary">
                {session?.type === 'upload' ? 'Upload Session' : 'QBSD Session'}
              </Typography>
            </Breadcrumbs>
            <Typography variant="h6" sx={{ mt: 0.5 }}>
              {session?.metadata.source}
            </Typography>
          </Box>

          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Chip 
              label={
                session?.status === 'schema_ready' ? 'Schema Ready' :
                session?.status === 'schema_extracted' ? 'Schema Extracted' :
                session?.status === 'documents_uploaded' ? 'Documents Ready' :
                session?.status === 'processing_documents' ? 'Processing Documents' :
                session?.status || 'Unknown'
              }
              color={
                session?.status === 'completed' ? 'success' :
                session?.status === 'schema_ready' ? 'info' :
                session?.status === 'schema_extracted' ? 'info' :
                session?.status === 'documents_uploaded' ? 'warning' :
                session?.status === 'processing_documents' ? 'warning' :
                session?.status === 'processing' ? 'warning' :
                session?.status === 'error' ? 'error' : 'default'
              }
              size="small"
            />
            
            {(isCompleted || session?.status === 'processing_documents') && (
              <>
                <Button
                  startIcon={<Refresh />}
                  onClick={handleRefresh}
                  size="small"
                >
                  Refresh
                </Button>
                <Button
                  startIcon={<Download />}
                  onClick={handleExport}
                  size="small"
                  variant="outlined"
                >
                  Export
                </Button>
              </>
            )}
          </Box>
        </Toolbar>
      </AppBar>

      {/* Tabs */}
      <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
        <Tabs 
          value={activeTab} 
          onChange={(_, newValue) => setActiveTab(newValue)}
          variant="scrollable"
          scrollButtons="auto"
        >
          <Tab icon={<TableView />} label="Data" disabled={!isCompleted && !isEnhancedUploadProcessing} />
          <Tab icon={<Schema />} label="Schema" disabled={!isSchemaReady || !session?.columns?.length} />
          <Tab icon={<Analytics />} label="Statistics" disabled={!isCompleted} />
          <Tab icon={<Info />} label="Session Info" />
          {mode === 'qbsd' && (
            <Tab
              icon={
                session?.status === 'processing' ? (
                  <CircularProgress size={16} />
                ) : session?.status === 'completed' ? (
                  <CheckCircle color="success" fontSize="small" />
                ) : session?.status === 'error' ? (
                  <ErrorIcon color="error" fontSize="small" />
                ) : (
                  <PlayArrow fontSize="small" />
                )
              }
              label="QBSD Monitor"
            />
          )}
          {(mode === 'upload' && isEnhancedUploadProcessing) && (
            <Tab icon={<CircularProgress size={16} />} label="Processing Monitor" />
          )}
        </Tabs>
      </Box>

      {/* Tab Panels */}
      <Box sx={{ p: 3 }}>
        <TabPanel value={activeTab} index={0}>
          {/* Data Table - show if data exists */}
          {(isCompleted || isEnhancedUploadProcessing || session?.status === 'documents_uploaded') && dataResponse ? (
            <Box sx={{ position: 'relative' }}>
              {/* Draggable Processing Overlay */}
              {session?.status === 'processing_documents' && (
                <Box
                  onMouseDown={handleOverlayMouseDown}
                  onMouseMove={handleOverlayMouseMove}
                  onMouseUp={handleOverlayMouseUp}
                  onMouseLeave={handleOverlayMouseUp}
                  sx={{
                    position: 'absolute',
                    top: overlayPosition.y,
                    left: overlayPosition.x,
                    right: overlayPosition.x === 0 ? 0 : 'auto',
                    width: overlayPosition.x !== 0 ? 'auto' : undefined,
                    minWidth: 300,
                    maxWidth: '100%',
                    zIndex: 10,
                    backgroundColor: 'rgba(255, 255, 255, 0.98)',
                    backdropFilter: 'blur(4px)',
                    padding: 2,
                    borderRadius: 2,
                    border: '2px solid',
                    borderColor: 'primary.main',
                    boxShadow: isDragging ? 8 : 4,
                    cursor: isDragging ? 'grabbing' : 'default',
                    userSelect: 'none',
                    transition: isDragging ? 'none' : 'box-shadow 0.2s ease'
                  }}
                >
                  {/* Drag Handle */}
                  <Box
                    className="drag-handle"
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      mb: 1,
                      pb: 1,
                      borderBottom: '1px solid',
                      borderColor: 'divider',
                      cursor: 'grab',
                      '&:active': { cursor: 'grabbing' }
                    }}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <DragIndicator sx={{ color: 'text.secondary' }} />
                      <Typography variant="subtitle2" color="text.secondary">
                        Drag to move
                      </Typography>
                    </Box>
                    <IconButton
                      size="small"
                      onClick={() => setOverlayPosition({ x: 0, y: 0 })}
                      title="Reset position"
                    >
                      <Close fontSize="small" />
                    </IconButton>
                  </Box>

                  <Alert severity="info" sx={{ mb: 1.5 }}>
                    <Typography variant="body1" gutterBottom sx={{ fontWeight: 'bold' }}>
                      🤖 Processing Documents with AI
                    </Typography>
                    <Typography variant="body2">
                      Extracting data from your uploaded documents. Cells fill in as values are extracted.
                    </Typography>
                  </Alert>

                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
                    <CircularProgress size={20} />
                    <Typography variant="body2">
                      {session.metadata?.processed_documents || 0} of {session.metadata?.uploaded_documents?.length || 0} documents processed
                    </Typography>
                    {session.metadata?.additional_rows_added && session.metadata.additional_rows_added > 0 && (
                      <Chip
                        label={`+${session.metadata.additional_rows_added} new rows`}
                        color="success"
                        size="small"
                      />
                    )}
                  </Box>
                </Box>
              )}
              
              <DataTable
                sessionId={sessionId!}
                sessionType={mode}
                newlyAddedRows={newlyAddedRows}
                columnOrder={columnOrder}
                onColumnReorder={handleColumnReorder}
                streamingCells={streamingCells}
              />
              
              {/* Document Upload Section - show for upload sessions with schema, or completed/documents_uploaded/processing QBSD sessions */}
              {((mode === 'upload' && (session?.status === 'documents_uploaded' || session?.status === 'processing_documents' || session?.status === 'completed')) ||
                (mode === 'qbsd' && (session?.status === 'completed' || session?.status === 'documents_uploaded' || session?.status === 'processing_documents'))) &&
               !sessionLoading && !dataLoading && dataResponse && (
                <Box sx={{ mt: 4 }}>
                  <Paper sx={{ p: 3 }}>
                    <Typography variant="h6" gutterBottom>
                      {mode === 'qbsd'
                        ? 'Add More Documents'
                        : session?.status === 'documents_uploaded'
                          ? 'Process Your Documents'
                          : 'Add More Documents'}
                    </Typography>
                    <Typography variant="body2" color="text.secondary" paragraph>
                      {mode === 'qbsd'
                        ? 'Upload additional documents to extract more data using your discovered schema.'
                        : session?.status === 'documents_uploaded'
                          ? 'You have uploaded documents that are ready to be processed. Click the button below to extract data using your schema.'
                          : 'Upload additional documents to extract more data using your existing schema.'
                      }
                    </Typography>
                    
                    {/* Show current uploaded documents status */}
                    {session?.metadata?.uploaded_documents && session.metadata.uploaded_documents.length > 0 && (
                      <Alert severity="info" sx={{ mb: 2 }}>
                        <Typography variant="body2">
                          <strong>Uploaded documents:</strong> {session.metadata.uploaded_documents.join(', ')}
                        </Typography>
                      </Alert>
                    )}
                    
                    {documentUploadError && (
                      <Alert severity="error" sx={{ mb: 2 }}>
                        {documentUploadError}
                      </Alert>
                    )}
                    
                    {/* Document upload interface */}
                    <DocumentUpload
                      onFilesChange={setUploadedDocuments}
                      uploadedFiles={uploadedDocuments}
                      loading={documentUploadLoading}
                      onUpload={handleDocumentUpload}
                      canUpload={true}
                      uploadResult={documentUploadResult}
                    />
                    
                    {/* Process documents section */}
                    {((session?.metadata?.uploaded_documents && session.metadata.uploaded_documents.length > 0) || (documentUploadResult && documentUploadResult.uploaded_files.length > 0)) && (
                      <Box sx={{ mt: 3 }}>
                        {/* Processing progress bar */}
                        {session?.status === 'processing_documents' && (
                          <Box sx={{ mb: 3 }}>
                            <Alert severity="info" sx={{ mb: 2 }}>
                              <Typography variant="body2" gutterBottom>
                                🤖 Processing documents with AI...
                              </Typography>
                              <Typography variant="body2">
                                Extracting data from your uploaded documents using the schema. This may take a few minutes.
                              </Typography>
                            </Alert>
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                              <CircularProgress size={20} />
                              <Typography variant="body2">
                                Processing in progress...
                              </Typography>
                            </Box>
                          </Box>
                        )}
                        
                        {/* Processing completion message */}
                        {session?.status === 'completed' && session?.metadata?.additional_rows_added && session.metadata.additional_rows_added > 0 && (
                          <Alert severity="success" sx={{ mb: 3 }}>
                            <Typography variant="body2" gutterBottom>
                              ✅ Processing completed successfully!
                            </Typography>
                            <Typography variant="body2">
                              Added {session.metadata.additional_rows_added} new rows from your uploaded documents.
                              The data table above now includes the extracted information.
                            </Typography>
                          </Alert>
                        )}
                        
                        {/* Process button */}
                        {session?.status !== 'completed' && (
                          <Box>
                            <Button
                              variant="contained"
                              size="large"
                              onClick={handleDocumentProcessing}
                              disabled={session?.status?.includes('processing') || documentUploadLoading}
                              startIcon={documentUploadLoading ? <CircularProgress size={20} /> : undefined}
                            >
                              {documentUploadLoading ? 'Starting Processing...' : 
                               session?.status?.includes('processing') ? 'Processing...' : 
                               'Select AI Model & Process Documents'}
                            </Button>
                            
                            {/* Show preserved LLM config if available */}
                            {session?.metadata?.extracted_schema?.llm_configuration?.value_extraction_backend && (
                              <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                                Previously used: {session.metadata.extracted_schema.llm_configuration.value_extraction_backend.provider} {session.metadata.extracted_schema.llm_configuration.value_extraction_backend.model}
                              </Typography>
                            )}
                          </Box>
                        )}
                        
                        {session?.status !== 'processing_documents' && session?.status !== 'completed' && (
                          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                            Choose an AI model and analyze your uploaded documents to extract data according to your schema.
                          </Typography>
                        )}
                      </Box>
                    )}
                  </Paper>
                </Box>
              )}
            </Box>
          ) : (
            <>
              {/* Show document upload for sessions without data but with schema (enhanced upload flow) */}
              {mode === 'upload' && !sessionLoading && (session?.status === 'documents_uploaded' || session?.status === 'processing_documents') && !dataResponse && (
                <Box sx={{ mt: 4 }}>
                  <Alert severity="info" sx={{ mb: 3 }}>
                    {session?.status === 'documents_uploaded' 
                      ? 'You have uploaded documents that are ready to be processed. Click the button below to start extracting data.'
                      : 'Document processing is in progress. Please wait while we extract data from your uploaded documents.'
                    }
                  </Alert>
                  
                  <Paper sx={{ p: 3 }}>
                    <Typography variant="h6" gutterBottom>
                      {session?.status === 'documents_uploaded' ? 'Process Your Documents' : 'Processing Your Documents'}
                    </Typography>
                    <Typography variant="body2" color="text.secondary" paragraph>
                      {session?.status === 'documents_uploaded'
                        ? 'You have uploaded documents that are ready to be processed. Click the button below to extract data using your schema.'
                        : 'Your documents are being analyzed using the extracted schema. This may take a few minutes depending on the number of documents and their complexity.'
                      }
                    </Typography>
                    
                    {/* Show current uploaded documents status */}
                    {session?.metadata?.uploaded_documents && session.metadata.uploaded_documents.length > 0 && (
                      <Alert severity="info" sx={{ mb: 2 }}>
                        <Typography variant="body2">
                          <strong>Uploaded documents:</strong> {session.metadata.uploaded_documents.join(', ')}
                        </Typography>
                      </Alert>
                    )}
                    
                    {documentUploadError && (
                      <Alert severity="error" sx={{ mb: 2 }}>
                        {documentUploadError}
                      </Alert>
                    )}
                    
                    {/* Processing progress bar */}
                    {session?.status === 'processing_documents' && (
                      <Box sx={{ mb: 3 }}>
                        <Alert severity="info" sx={{ mb: 2 }}>
                          <Typography variant="body2" gutterBottom>
                            🤖 Processing documents with AI...
                          </Typography>
                          <Typography variant="body2">
                            Extracting data from your uploaded documents using the schema. This may take a few minutes.
                          </Typography>
                        </Alert>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                          <CircularProgress size={20} />
                          <Typography variant="body2">
                            Processing in progress...
                          </Typography>
                        </Box>
                      </Box>
                    )}
                    
                    {/* Process button */}
                    <Button
                      variant="contained"
                      size="large"
                      onClick={handleDocumentProcessing}
                      disabled={session?.status?.includes('processing')}
                    >
                      {session?.status?.includes('processing') ? 'Processing...' : 'Process Documents with AI'}
                    </Button>
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                      This will analyze your uploaded documents and extract data according to your schema.
                    </Typography>
                  </Paper>
                </Box>
              )}
              
              {/* No data message - only show if no data AND no document capabilities AND not loading */}
              {!sessionLoading && !(isCompleted || isEnhancedUploadProcessing || session?.status === 'documents_uploaded') && !dataResponse && (
                <Alert severity="info">
                  {isQBSDRunning ? 'Data will be available when QBSD processing completes' : 'No data available'}
                </Alert>
              )}
            </>
          )}
        </TabPanel>

        <TabPanel value={activeTab} index={1}>
          {session?.columns?.length ? (
            <SchemaViewer 
              columns={session.columns}
              query={session.schema_query}
              sessionId={sessionId}
              sessionType={session.type}
              readonly={false}
              onColumnsChange={() => {
                // Refetch session data when schema changes
                queryClient.invalidateQueries(['session', sessionId, mode]);
                // Also invalidate data queries to refresh the data table
                queryClient.invalidateQueries(['data', sessionId]);
              }}
              llmConfig={session.metadata?.extracted_schema?.llm_configuration?.schema_creation_backend || null}
            />
          ) : (
            <Alert severity="info">
              No schema available
            </Alert>
          )}
        </TabPanel>

        <TabPanel value={activeTab} index={2}>
          {session?.statistics ? (
            <StatsDashboard statistics={session.statistics} />
          ) : isCompleted ? (
            <Alert severity="warning">
              Statistics not available
            </Alert>
          ) : (
            <Alert severity="info">
              Statistics will be available when processing completes
            </Alert>
          )}
        </TabPanel>

        <TabPanel value={activeTab} index={3}>
          <ConfigurationInfo session={session} compact={false} />
        </TabPanel>

        {mode === 'qbsd' && (
          <TabPanel value={activeTab} index={4}>
            <QBSDMonitor sessionId={sessionId} />
          </TabPanel>
        )}

        {(mode === 'upload' && isEnhancedUploadProcessing) && (
          <TabPanel value={activeTab} index={5}>
            <UploadProcessingMonitor 
              sessionId={sessionId} 
              status={null}
              loading={false}
              llmConfig={session?.metadata?.extracted_schema?.llm_configuration?.value_extraction_backend || null}
            />
          </TabPanel>
        )}
      </Box>

      {/* LLM Selection Dialog for Document Processing */}
      <LLMSelector
        open={showLLMSelector}
        onClose={() => setShowLLMSelector(false)}
        onConfirm={handleLLMSelection}
        title="Select AI Model for Document Processing"
        description="Choose the AI model that will extract information from your uploaded documents using the existing schema."
        preservedConfig={session?.metadata?.extracted_schema?.llm_configuration?.value_extraction_backend || null}
        loading={documentUploadLoading}
      />
    </Box>
  );
};

export default Visualize;