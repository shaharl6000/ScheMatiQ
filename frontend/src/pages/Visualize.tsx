import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { useParams, useSearchParams, useNavigate, useLocation } from 'react-router-dom';
import { useViewHistory } from '../hooks/useViewHistory';
import { useNavigationGuardContext } from '../contexts/NavigationGuardContext';
import { NavigationConfirmDialog } from '@/components/ui/NavigationConfirmDialog';
import { debug } from '@/utils/debug';
import {
  ArrowLeft,
  Download,
  Save,
  ChevronDown,
  Loader2,
  X,
  FileText,
  Copy,
  HelpCircle,
  Search,
} from 'lucide-react';
import { useQuery, useQueryClient } from 'react-query';

import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useToast } from '@/components/ui/use-toast';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';

import { loadAPI, schematiqAPI, cloudAPI, configAPI, downloadBlob, unitsAPI } from '../services/api';
import { getApiKeyForProvider, LLMProvider } from '../utils/apiKeyStorage';
import { VisualizationSession, CellValue, CellExtractedData } from '../types';
import {
  PROCESSING_REFRESH_INTERVAL,
  NEW_ROW_HIGHLIGHT_DURATION,
  WS_RECONNECT_ATTEMPTS,
  WS_RECONNECT_DELAY_BASE,
  WS_RECONNECT_MAX_DELAY,
  WS_BASE_URL
} from '../constants/index';

// Component imports
import DataTable from '../components/DataTable/DataTable';
import UnitGroupedTable from '../components/DataTable/UnitGroupedTable';
import SchemaViewer from '../components/SchemaViewer/SchemaViewer';
import StatsDashboard from '../components/StatsDashboard/StatsDashboard';
import ScheMatiQMonitor from '../components/ScheMatiQMonitor/ScheMatiQMonitor';
import UploadProcessingMonitor from '../components/UploadProcessingMonitor/UploadProcessingMonitor';
import DocumentUpload from '../components/DocumentUpload/DocumentUpload';
import LLMSelector from '../components/LLMSelector';
import { useViewMode } from '../contexts/ViewModeContext';
import ViewModeToggle from '../components/ViewMode/ViewModeToggle';
import { useUnits } from '../hooks/useUnits';
import TableFeedbackWidget from '../components/TableFeedbackWidget/TableFeedbackWidget';
import { VisualizeGuideDialog } from '../components/VisualizeGuideDialog/VisualizeGuideDialog';

const Visualize = () => {
  const { sessionId } = useParams<{ sessionId: string }>();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();
  const { toast } = useToast();

  const mode = searchParams.get('mode') as 'load' | 'schematiq' || 'load';
  const [activeTab, setActiveTab] = useState(mode === 'schematiq' ? 'monitor' : 'data');

  // Read auto-start state from navigation (set by ScheMatiQConfig after calling run)
  const [autoStartState] = useState(() => {
    const state = location.state as {
      autoStarted?: boolean;
      serverBusy?: boolean;
      capacityMessage?: string;
    } | null;
    // Clear navigation state to prevent re-use on page refresh
    if (state?.autoStarted || state?.serverBusy) {
      window.history.replaceState({}, document.title);
    }
    return {
      autoStarted: state?.autoStarted || false,
      initialCapacityMessage: state?.serverBusy ? (state.capacityMessage || 'The server is currently busy processing other requests. Please try again in a few minutes.') : '',
    };
  });

  // View mode context for unit view toggle
  const { viewMode, setViewMode } = useViewMode();

  // Fetch observation units for the session (to determine if unit view is available)
  const { units: unitListResponse, refresh: refreshUnits } = useUnits(sessionId);

  // Fetch document list for document filtering in standard view
  const { data: documentListResponse } = useQuery(
    ['documentList', sessionId],
    () => unitsAPI.getDocuments(sessionId!),
    { enabled: !!sessionId }
  );

  // Document filter state
  const [selectedDocuments, setSelectedDocuments] = useState<string[]>([]);

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

  // Current column being extracted (for active chip highlight in progress bar)
  const [currentColumn, setCurrentColumn] = useState<string | null>(null);

  // Current document being processed (for progress display)
  const [currentDocumentProgress, setCurrentDocumentProgress] = useState<{
    documentName: string;
    documentIndex: number;
    totalDocuments: number;
  } | null>(null);

  // Document limit state
  const [maxDocuments, setMaxDocuments] = useState<number | undefined>(undefined);

  // Developer mode state (for conditionally showing feedback widget)
  const [developerMode, setDeveloperMode] = useState<boolean>(true);

  // LLM selection state
  const [showLLMSelector, setShowLLMSelector] = useState(false);

  // Visualize guide dialog state
  const [visualizeGuideAutoOpen, setVisualizeGuideAutoOpen] = useState(false);
  const [visualizeGuideForceOpen, setVisualizeGuideForceOpen] = useState(false);
  const hasShownGuideRef = React.useRef(false);

  // Stop processing state
  const [isStoppingProcessing, setIsStoppingProcessing] = useState(false);

  // Active re-extraction operation tracking
  const [activeReextractionId, setActiveReextractionId] = useState<string | null>(null);
  const [isStoppingReextraction, setIsStoppingReextraction] = useState(false);

  // Continue Discovery activity tracking (for navigation guard)
  const [continueDiscoveryActive, setContinueDiscoveryActive] = useState(false);

  // Column order state
  const [columnOrder, setColumnOrder] = useState<string[]>([]);

  // Add More Documents collapsible state
  const [addDocsExpanded, setAddDocsExpanded] = useState(false);

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

  // Fetch config to get document limit and developer mode
  useEffect(() => {
    configAPI.getConfig()
      .then(cfg => {
        setMaxDocuments(cfg.max_documents);
        setDeveloperMode(cfg.developer_mode);
      })
      .catch(() => {});
  }, []);

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
              queryClient.invalidateQueries({ queryKey: ['unitData', sessionId], exact: false });
              setTimeout(() => {
                setNewlyAddedRows(prev => {
                  const newSet = new Set(Array.from(prev));
                  newSet.delete(message.data.row_index);
                  return newSet;
                });
              }, NEW_ROW_HIGHLIGHT_DURATION);
              break;
            case 'completed':
              debug.log('✅ WebSocket completion received, refetching data...');
              setStreamingCells(new Map());
              setCurrentDocumentProgress(null);
              setForceWebSocketConnect(false);
              if (mode === 'schematiq' && !hasShownGuideRef.current) {
                hasShownGuideRef.current = true;
                setVisualizeGuideAutoOpen(true);
              }
              // Use broader query filter to match all data queries (including DataTable's paginated queries)
              queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
              queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
              queryClient.refetchQueries({ queryKey: ['unitData', sessionId], exact: false });
              refreshUnits();
              setTimeout(() => {
                if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
                  wsRef.current.close(1000, 'Processing completed');
                  wsRef.current = null;
                }
              }, 3000);
              break;
            case 'reextraction_started':
              // Initialize processing columns and store operation ID
              if (message.data?.columns && Array.isArray(message.data.columns)) {
                setProcessingColumns(new Set(message.data.columns));
              }
              if (message.data?.operation_id) {
                setActiveReextractionId(message.data.operation_id);
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
                setCurrentColumn(message.data.column);
                setProcessingColumns(prev => {
                  const newSet = new Set(Array.from(prev));
                  newSet.add(message.data.column);
                  return newSet;
                });
              }
              break;
            case 'reextraction_completed':
              debug.log('Re-extraction completed:', message.data);
              setProcessingColumns(new Set()); // Clear processing state
              setCurrentColumn(null);          // Clear current column
              setCurrentDocumentProgress(null); // Clear document progress
              setStreamingCells(new Map());    // Clear streaming cells
              setActiveReextractionId(null);   // Clear operation ID
              setIsStoppingReextraction(false); // Clear stopping state
              queryClient.invalidateQueries(['session', sessionId, mode]);
              queryClient.invalidateQueries(['data', sessionId, mode]);
              queryClient.invalidateQueries({ queryKey: ['unitData', sessionId], exact: false });
              refreshUnits();
              break;
            case 'reextraction_stopped':
              debug.log('Re-extraction stopped:', message.data);
              setProcessingColumns(new Set()); // Clear processing state
              setCurrentColumn(null);          // Clear current column
              setActiveReextractionId(null);   // Clear operation ID
              setIsStoppingReextraction(false); // Clear stopping state
              setCurrentDocumentProgress(null); // Clear document progress
              setStreamingCells(new Map());    // Clear streaming cells
              // Force immediate data refetch to show partial results
              queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
              queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
              queryClient.refetchQueries({ queryKey: ['unitData', sessionId], exact: false });
              refreshUnits();
              break;
            case 'reextraction_failed':
              debug.log('Re-extraction failed:', message.data);
              setProcessingColumns(new Set());
              setCurrentColumn(null);
              setCurrentDocumentProgress(null);
              setStreamingCells(new Map());
              setActiveReextractionId(null);
              setIsStoppingReextraction(false);
              queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
              queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
              break;

            case 'stopped':
              debug.log('ScheMatiQ stopped:', message.data);
              setStreamingCells(new Map());
              setForceWebSocketConnect(false);
              setIsStoppingProcessing(false);
              queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
              queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
              queryClient.refetchQueries({ queryKey: ['unitData', sessionId], exact: false });
              refreshUnits();
              break;

            // Live partial schema during discovery
            case 'schema_progress':
              debug.log('Schema progress:', message.data);
              queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
              break;

            // Continue Discovery events
            case 'continue_discovery_started':
              debug.log('Continue discovery started:', message.data);
              setContinueDiscoveryActive(true);
              break;
            case 'continue_discovery_progress':
              debug.log('Continue discovery progress:', message.data);
              break;
            case 'continue_discovery_completed':
              debug.log('Continue discovery completed:', message.data);
              queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
              queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
              queryClient.refetchQueries({ queryKey: ['unitData', sessionId], exact: false });
              refreshUnits();
              break;
            case 'continue_discovery_stopped':
              debug.log('Continue discovery stopped:', message.data);
              setContinueDiscoveryActive(false);
              break;
            case 'continue_discovery_failed':
              debug.log('Continue discovery failed:', message.data);
              setContinueDiscoveryActive(false);
              break;
            case 'incremental_extraction_started':
              // Initialize processing columns when incremental extraction starts
              if (message.data?.columns && Array.isArray(message.data.columns)) {
                setProcessingColumns(new Set(message.data.columns));
              }
              setContinueDiscoveryActive(true);
              break;
            case 'incremental_extraction_progress':
              if (message.data?.column) {
                setCurrentColumn(message.data.column);
                setProcessingColumns(prev => {
                  const newSet = new Set(Array.from(prev));
                  newSet.add(message.data.column);
                  return newSet;
                });
              }
              break;
            case 'incremental_extraction_completed':
              debug.log('Incremental extraction completed:', message.data);
              setProcessingColumns(new Set()); // Clear processing state
              setCurrentColumn(null);          // Clear current column
              setCurrentDocumentProgress(null); // Clear document progress
              setStreamingCells(new Map());    // Clear streaming cells
              setContinueDiscoveryActive(false);
              queryClient.invalidateQueries(['session', sessionId, mode]);
              queryClient.invalidateQueries(['data', sessionId, mode]);
              queryClient.invalidateQueries({ queryKey: ['unitData', sessionId], exact: false });
              refreshUnits();
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
  const { data: session, isLoading: sessionLoading, error: sessionError, isRefetching: isSessionRefetching } = useQuery(
    ['session', sessionId, mode],
    async () => {
      if (mode === 'load') {
        return loadAPI.getSession(sessionId!);
      } else {
        const [fullSession, status, schema] = await Promise.all([
          loadAPI.getSession(sessionId!).catch(() => null),
          schematiqAPI.getStatus(sessionId!),
          schematiqAPI.getSchema(sessionId!)
        ]);

        return {
          id: sessionId!,
          type: 'schematiq' as const,
          status: status.status as any,
          metadata: {
            source: `ScheMatiQ Query: ${schema.query || 'Unknown'}`,
            created: fullSession?.metadata?.created || new Date().toISOString(),
            last_modified: fullSession?.metadata?.last_modified || new Date().toISOString(),
            uploaded_documents: fullSession?.metadata?.uploaded_documents,
            processed_documents: fullSession?.metadata?.processed_documents,
            additional_rows_added: fullSession?.metadata?.additional_rows_added,
          },
          schema_query: schema.query,
          columns: schema.schema || [],
          statistics: fullSession?.statistics,
          // Get observation_unit from schema (JSON file) or fallback to session
          observation_unit: schema.observation_unit || fullSession?.observation_unit,
        } as VisualizationSession;
      }
    },
    {
      enabled: !!sessionId,
      refetchInterval: (data) => {
        // Don't poll if WebSocket is connected - rely on real-time updates
        if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
          return false;
        }
        // Fallback polling when WebSocket is not connected
        if (mode === 'schematiq') return PROCESSING_REFRESH_INTERVAL;
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
        return schematiqAPI.getData(sessionId!, 0, 100);
      }
    },
    {
      enabled: !!sessionId && (
        session?.status === 'completed' ||
        session?.status === 'stopped' ||
        session?.status === 'processing_documents' ||
        session?.status === 'documents_uploaded'
      ),
      refetchInterval: () => {
        // Don't poll if WebSocket is connected - rely on real-time updates
        if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
          return false;
        }
        // Fallback polling when WebSocket is not connected
        return session?.status === 'processing_documents' ? PROCESSING_REFRESH_INTERVAL : false;
      },
      keepPreviousData: true,
    }
  );

  // Show guide dialog when loading an existing completed session.
  // Use requestAnimationFrame to defer the state update — ensures the dialog
  // has mounted with autoOpen=false before we transition it to true,
  // since the dialog relies on detecting a false→true transition.
  useEffect(() => {
    if (mode === 'load' && session?.status === 'completed' && !hasShownGuideRef.current) {
      hasShownGuideRef.current = true;
      requestAnimationFrame(() => setVisualizeGuideAutoOpen(true));
    }
  }, [mode, session?.status]);

  // Fallback check for observation units in table data
  // This allows showing the "By Unit" toggle even when the API doesn't detect unit names
  // Check for _unit_name metadata field OR columns with "unit"/"observation" in the name
  const hasUnitColumn = useMemo(() => {
    if (!dataResponse?.rows?.length) return false;

    // Check for _unit_name field (observation unit metadata) - same as DataTable's hasObservationUnits
    if (dataResponse.rows.some(row => row._unit_name != null)) return true;

    // Fallback: check column headers in data object
    const firstRow = dataResponse.rows[0];
    const headers = Object.keys(firstRow.data || {});
    return headers.some(header =>
      header.toLowerCase().includes('unit') ||
      header.toLowerCase().includes('observation')
    );
  }, [dataResponse]);

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
        debug.log('WebSocket already connected, skipping');
        return;
      }

      try {
        debug.log('Creating WebSocket connection for session:', sessionId);
        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;

        ws.onopen = () => {
          debug.log('WebSocket connected for session:', sessionId);
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
                queryClient.invalidateQueries({ queryKey: ['unitData', sessionId], exact: false });
                setTimeout(() => {
                  setNewlyAddedRows(prev => {
                    const newSet = new Set(Array.from(prev));
                    newSet.delete(message.data.row_index);
                    return newSet;
                  });
                }, NEW_ROW_HIGHLIGHT_DURATION);
                break;
              case 'completed':
                debug.log('✅ WebSocket completion received, refetching data...');
                setStreamingCells(new Map());
                setCurrentDocumentProgress(null);
                setForceWebSocketConnect(false);
                if (mode === 'schematiq' && !hasShownGuideRef.current) {
                  hasShownGuideRef.current = true;
                  setVisualizeGuideAutoOpen(true);
                }
                // Use broader query filter to match all data queries (including DataTable's paginated queries)
                queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['unitData', sessionId], exact: false });
                refreshUnits();
                setTimeout(() => {
                  if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
                    wsRef.current.close(1000, 'Processing completed');
                    wsRef.current = null;
                  }
                }, 3000);
                break;
              case 'reextraction_started':
                // Initialize processing columns and store operation ID
                if (message.data?.columns && Array.isArray(message.data.columns)) {
                  setProcessingColumns(new Set(message.data.columns));
                }
                if (message.data?.operation_id) {
                  setActiveReextractionId(message.data.operation_id);
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
                  setCurrentColumn(message.data.column);
                  setProcessingColumns(prev => {
                    const newSet = new Set(Array.from(prev));
                    newSet.add(message.data.column);
                    return newSet;
                  });
                }
                break;
              case 'reextraction_completed':
                debug.log('Re-extraction completed:', message.data);
                setProcessingColumns(new Set()); // Clear processing state
                setCurrentColumn(null);          // Clear current column
                setCurrentDocumentProgress(null); // Clear document progress
                setStreamingCells(new Map());    // Clear streaming cells
                setActiveReextractionId(null);   // Clear operation ID
                setIsStoppingReextraction(false); // Clear stopping state
                // Force immediate data refetch (not just invalidate)
                queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['unitData', sessionId], exact: false });
                refreshUnits();
                // Notify feedback widget to reset (new table deserves fresh feedback)
                window.dispatchEvent(new Event('reextraction_completed'));
                // Delay WebSocket close to allow refetch to complete
                setTimeout(() => {
                  setForceWebSocketConnect(false);
                }, 3000);
                break;
              case 'reextraction_stopped':
                debug.log('Re-extraction stopped:', message.data);
                setProcessingColumns(new Set()); // Clear processing state
                setCurrentColumn(null);          // Clear current column
                setCurrentDocumentProgress(null); // Clear document progress
                setStreamingCells(new Map());    // Clear streaming cells
                setActiveReextractionId(null);   // Clear operation ID
                setIsStoppingReextraction(false); // Clear stopping state
                setForceWebSocketConnect(false); // Allow WebSocket to close
                // Force immediate data refetch to show partial results
                queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['unitData', sessionId], exact: false });
                refreshUnits();
                break;
              case 'reextraction_failed':
                debug.log('Re-extraction failed:', message.data);
                setProcessingColumns(new Set());
                setCurrentColumn(null);
                setCurrentDocumentProgress(null);
                setStreamingCells(new Map());
                setActiveReextractionId(null);
                setIsStoppingReextraction(false);
                setForceWebSocketConnect(false);
                // Refetch to show current state
                queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
                break;

              case 'stopped':
                debug.log('ScheMatiQ stopped:', message.data);
                setStreamingCells(new Map());
                setForceWebSocketConnect(false);
                setIsStoppingProcessing(false);
                queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['unitData', sessionId], exact: false });
                refreshUnits();
                break;

              // Live partial schema during discovery
              case 'schema_progress':
                debug.log('Schema progress:', message.data);
                queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
                break;

              // Continue Discovery events
              case 'continue_discovery_started':
                debug.log('Continue discovery started:', message.data);
                setContinueDiscoveryActive(true);
                break;
              case 'continue_discovery_progress':
                debug.log('Continue discovery progress:', message.data);
                break;
              case 'continue_discovery_completed':
                debug.log('Continue discovery completed:', message.data);
                queryClient.refetchQueries({ queryKey: ['session', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['data', sessionId], exact: false });
                queryClient.refetchQueries({ queryKey: ['unitData', sessionId], exact: false });
                refreshUnits();
                // Note: don't clear continueDiscoveryActive yet — extraction may follow
                break;
              case 'continue_discovery_stopped':
                debug.log('Continue discovery stopped:', message.data);
                setContinueDiscoveryActive(false);
                break;
              case 'continue_discovery_failed':
                debug.log('Continue discovery failed:', message.data);
                setContinueDiscoveryActive(false);
                break;
              case 'incremental_extraction_started':
                // Initialize processing columns when incremental extraction starts
                if (message.data?.columns && Array.isArray(message.data.columns)) {
                  setProcessingColumns(new Set(message.data.columns));
                }
                setContinueDiscoveryActive(true);
                break;
              case 'incremental_extraction_progress':
                if (message.data?.column) {
                  setCurrentColumn(message.data.column);
                  setProcessingColumns(prev => {
                    const newSet = new Set(Array.from(prev));
                    newSet.add(message.data.column);
                    return newSet;
                  });
                }
                break;
              case 'incremental_extraction_completed':
                debug.log('Incremental extraction completed:', message.data);
                setProcessingColumns(new Set()); // Clear processing state
                setCurrentColumn(null);          // Clear current column
                setCurrentDocumentProgress(null); // Clear document progress
                setStreamingCells(new Map());    // Clear streaming cells
                setForceWebSocketConnect(false); // Allow WebSocket to close
                setContinueDiscoveryActive(false);
                queryClient.invalidateQueries(['session', sessionId, mode]);
                queryClient.invalidateQueries(['data', sessionId, mode]);
                queryClient.invalidateQueries({ queryKey: ['unitData', sessionId], exact: false });
                refreshUnits();
                break;
            }
          } catch (err) {
            console.error('Error parsing WebSocket message:', err);
          }
        };

        ws.onclose = (event) => {
          debug.log('WebSocket closed:', event.code, event.reason);
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
      (mode === 'schematiq' && session?.status === 'processing');

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
        !(mode === 'schematiq' && session?.status === 'processing')) {
      // No longer need WebSocket, close it
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        debug.log('Closing WebSocket - no longer needed');
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
        queryClient.invalidateQueries({ queryKey: ['unitData', sessionId], exact: false });
        refreshUnits();
      }
    };

    window.addEventListener('schema-data-updated', handleSchemaDataUpdate as EventListener);
    return () => {
      window.removeEventListener('schema-data-updated', handleSchemaDataUpdate as EventListener);
    };
  }, [sessionId, mode, queryClient]);

  const handleDownloadTable = async () => {
    try {
      const exportPath = mode === 'load'
        ? `/load/export/${sessionId}`
        : `/schematiq/export/${sessionId}`;

      const tzOffset = new Date().getTimezoneOffset();
      let queryStr = `?tz_offset=${tzOffset}`;

      if (columnOrder.length > 0) {
        const orderParam = encodeURIComponent(columnOrder.join(','));
        queryStr += `&column_order=${orderParam}`;
      }

      await downloadBlob(`${exportPath}${queryStr}`, 'table_data.csv');
    } catch (error) {
      console.error('Export error:', error);
      alert('Download failed. Please try again.');
    }
  };

  const handleSaveProject = async () => {
    try {
      const tzOffset = new Date().getTimezoneOffset();
      const exportPath = mode === 'load'
        ? `/load/export-complete/${sessionId}?format=json&tz_offset=${tzOffset}`
        : `/schematiq/export-complete/${sessionId}?format=json&tz_offset=${tzOffset}`;

      await downloadBlob(exportPath, 'project.schematiq.json');
    } catch (error) {
      console.error('Export error:', error);
      alert('Save failed. Please try again.');
    }
  };

  const handleDocumentProcessing = async () => {
    // Check if LLM config is allowed (developer mode)
    const cfg = await configAPI.getConfig().catch(() => ({ allow_llm_config: true, server_has_api_keys: false }));
    if (!cfg.allow_llm_config) {
      // Release mode: use default Gemini config, prefer user key, fallback to server key
      const geminiKey = await getApiKeyForProvider('gemini');
      if (!geminiKey && !cfg.server_has_api_keys) {
        setDocumentUploadError('No Gemini API key configured. Please add your API key on the home page.');
        return;
      }
      handleLLMSelection({
        provider: 'gemini',
        model: 'gemini-2.5-flash-lite',
        temperature: 0,
        ...(geminiKey ? { api_key: geminiKey } : {}),
      });
    } else {
      // Developer mode: show LLM selector
      setShowLLMSelector(true);
    }
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
      // Retrieve API key: prefer what was passed in, then check storage
      const apiKey = llmConfig.api_key || await getApiKeyForProvider(llmConfig.provider as LLMProvider);

      if (!apiKey) {
        // No client-side key — check if server can provide one
        const cfg = await configAPI.getConfig().catch(() => ({ server_has_api_keys: false }));
        if (!cfg.server_has_api_keys) {
          setDocumentUploadError(`No API key configured for ${llmConfig.provider}. Please add your API key on the home page.`);
          setDocumentUploadLoading(false);
          return;
        }
      }

      // Include API key in the config (undefined = backend uses server key)
      const configWithKey = {
        ...llmConfig,
        api_key: apiKey || undefined,
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
      const detail = err.response?.data?.detail;
      const errorMessage = err.response?.status === 503
        ? (detail || 'The server is currently busy. Please try again in a few minutes.')
        : (detail || err.message || 'Failed to start processing');
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
      await loadAPI.stopProcessing(sessionId);
      // Don't clear spinner — WebSocket 'stopped' handler will do it
    } catch (err: any) {
      console.error('Failed to stop processing:', err);
      setDocumentUploadError(err.response?.data?.detail || err.message || 'Failed to stop processing');
      setIsStoppingProcessing(false);
    }
  };

  // Handle re-extraction started - set up WebSocket and processing columns
  const handleReextractionStarted = (columns: string[], operationId?: string) => {
    // Set the columns being processed (for skeleton display in table)
    setProcessingColumns(new Set(columns));
    // Store operation ID for stop functionality
    if (operationId) {
      setActiveReextractionId(operationId);
    }
    // Force WebSocket connection to receive real-time updates
    setForceWebSocketConnect(true);
  };

  // Handle stopping re-extraction
  const handleStopReextraction = async () => {
    if (!activeReextractionId || !sessionId) return;

    setIsStoppingReextraction(true);
    try {
      const { schemaAPI } = await import('../services/api');
      await schemaAPI.stopReextraction(sessionId, activeReextractionId);
      // Don't clear spinner — WebSocket 'reextraction_stopped' handler will do it
    } catch (err: any) {
      console.error('Failed to stop re-extraction:', err);
      setIsStoppingReextraction(false);
    }
  };

  const handleBackNavigation = useCallback(async () => {
    if (session && mode === 'schematiq') {
      try {
        // Fetch full configuration used for this session
        const config = await schematiqAPI.getConfig(sessionId!);
        const statePayload = {
          config,
          previousSessionId: sessionId,
          uploadedFileNames: session?.metadata?.uploaded_documents || []
        };

        // Save to sessionStorage so browser back also works
        sessionStorage.setItem(`schematiq_config_${sessionId}`, JSON.stringify(statePayload));

        // Navigate back (replace to keep history clean during edit cycles)
        navigate('/schematiq', { replace: true, state: statePayload });
      } catch (err) {
        console.error('Failed to fetch config for restoration:', err);
        // Fallback: navigate with basic state from session
        const fallbackPayload = {
          config: {
            query: session?.schema_query || '',
            docs_path: session?.metadata.cloud_dataset ? session.metadata.cloud_dataset.split(', ') : [],
          }
        };
        sessionStorage.setItem(`schematiq_config_${sessionId}`, JSON.stringify(fallbackPayload));
        navigate('/schematiq', { replace: true, state: fallbackPayload });
      }
    } else {
      navigate('/');
    }
  }, [session, mode, sessionId, navigate]);

  // --- View history + navigation guard (must be before early returns) ---
  const isAnyProcessingActive = (mode === 'schematiq' && session?.status === 'processing') ||
    !!activeReextractionId ||
    session?.status === 'processing_documents' ||
    continueDiscoveryActive;

  const handleViewRestore = useCallback((entry: { tab: string; viewMode: import('../types/unit').ViewMode }) => {
    setActiveTab(entry.tab);
    setViewMode(entry.viewMode);
  }, [setViewMode]);

  const { pushViewState, blocker: processingBlocker, requestNavigation } =
    useViewHistory(activeTab, viewMode, isAnyProcessingActive, handleViewRestore);

  // Register guard in context so the header banner also respects it
  const { registerGuard } = useNavigationGuardContext();
  useEffect(() => {
    return registerGuard(requestNavigation);
  }, [requestNavigation, registerGuard]);

  // Wrapped tab change handler — records target view in browser history
  const handleTabChange = useCallback((newTab: string) => {
    if (newTab === activeTab) return;
    pushViewState({ tab: newTab, viewMode });
    setActiveTab(newTab);
  }, [activeTab, viewMode, pushViewState]);

  // Wrapped view mode change handler
  const handleViewModeChange = useCallback((newMode: import('../types/unit').ViewMode) => {
    if (newMode === viewMode) return;
    pushViewState({ tab: activeTab, viewMode: newMode });
    setViewMode(newMode);
  }, [activeTab, viewMode, pushViewState, setViewMode]);

  // Back arrow: navigate directly to home/config (skip view history traversal)
  const handleBackClick = useCallback(() => {
    requestNavigation(() => { handleBackNavigation(); });
  }, [requestNavigation, handleBackNavigation]);

  if (!sessionId) {
    return (
      <Alert variant="destructive">
        <AlertDescription>Invalid session ID</AlertDescription>
      </Alert>
    );
  }

  if (sessionLoading && !autoStartState.autoStarted) {
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

  const isScheMatiQRunning = mode === 'schematiq' && session?.status === 'processing';
  const isScheMatiQStopped = mode === 'schematiq' && session?.status === 'stopped';
  const isSchemaReady = ['schema_ready', 'schema_extracted', 'documents_uploaded', 'processing_documents', 'completed', 'stopped'].includes(session?.status || '') ||
    (mode === 'schematiq' && session?.status === 'processing' && (session?.columns?.length ?? 0) > 0);
  const isCompleted = session?.status === 'completed';
  const isEnhancedUploadProcessing = session?.status === 'processing_documents';

  // Debug logging for Data tab disable condition
  debug.log('Data tab state:', {
    sessionStatus: session?.status,
    mode,
    isCompleted,
    isEnhancedUploadProcessing,
    isScheMatiQRunning,
    isScheMatiQStopped,
    dataTabDisabled: !isCompleted && !isEnhancedUploadProcessing && !isScheMatiQRunning && !isScheMatiQStopped && session?.status !== 'documents_uploaded'
  });

  const getStatusBadge = () => {
    const status = session?.status;
    if (!status && autoStartState.autoStarted) {
      return <Badge variant="warning">Processing</Badge>;
    }
    const variants: Record<string, 'default' | 'success' | 'warning' | 'destructive' | 'info'> = {
      completed: 'success',
      stopped: 'warning',
      schema_ready: 'info',
      schema_extracted: 'info',
      documents_uploaded: 'warning',
      processing_documents: 'warning',
      processing: 'warning',
      observation_unit_review: 'info',
      error: 'destructive',
    };
    const labels: Record<string, string> = {
      stopped: 'Stopped (Partial)',
      schema_ready: 'Schema Ready',
      schema_extracted: 'Schema Extracted',
      documents_uploaded: 'Documents Ready',
      processing_documents: 'Processing Documents',
      observation_unit_review: 'Reviewing Observation Unit',
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
      <div className="space-y-3 border-b pb-4">
        <div className="flex items-center justify-between">
          <Button variant="ghost" size="sm" onClick={handleBackClick}>
            <ArrowLeft className="h-4 w-4 mr-2" />
            {mode === 'schematiq' ? 'Back to Configuration' : 'Back to Home'}
          </Button>

          <div className="flex items-center gap-2">
            {getStatusBadge()}
            {(isCompleted || isScheMatiQStopped) && (
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setVisualizeGuideForceOpen(true)}
                aria-label="Show results guide"
                className="text-muted-foreground"
              >
                <HelpCircle className="h-5 w-5" />
              </Button>
            )}
            {(isCompleted || isEnhancedUploadProcessing || isScheMatiQStopped) && (
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="outline" size="sm">
                    <Download className="h-4 w-4 mr-2" />
                    Export{isScheMatiQStopped ? ' Current Results' : ''}
                    <ChevronDown className="h-3 w-3 ml-1" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-72">
                  <DropdownMenuItem onClick={handleDownloadTable}>
                    <Download className="h-4 w-4 mr-2 shrink-0" />
                    <div>
                      <div>Download Table (.csv)</div>
                      <div className="text-xs text-muted-foreground">Clean data for Excel — no metadata</div>
                    </div>
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={handleSaveProject}>
                    <Save className="h-4 w-4 mr-2 shrink-0" />
                    <div>
                      <div>Save Project (.schematiq.json)</div>
                      <div className="text-xs text-muted-foreground">Full project with schema and history — for reloading</div>
                    </div>
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            )}
          </div>
        </div>

        {/* Research Question card */}
        <Card className="bg-blue-50 border-blue-200">
          <CardContent className="py-2.5 px-4">
            <div className="group flex items-start gap-3">
              <Search className="h-4 w-4 text-blue-600 mt-0.5 shrink-0" />
              <div className="min-w-0 flex-1">
                <p className="text-xs text-blue-600 font-medium mb-0.5">Research Question</p>
                {session?.schema_query ? (
                  <p className="text-base font-medium text-blue-900">{session.schema_query}</p>
                ) : (
                  <div className="h-6 w-3/4 bg-blue-200/50 rounded animate-pulse" />
                )}
              </div>
              {session?.schema_query && (
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity text-blue-600 hover:text-blue-800 hover:bg-blue-100 shrink-0"
                  onClick={() => {
                    navigator.clipboard.writeText(session?.schema_query || '');
                    toast({ title: "Query copied to clipboard" });
                  }}
                >
                  <Copy className="h-3 w-3" />
                </Button>
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Tabs */}
      <Tabs value={activeTab} onValueChange={handleTabChange} className="w-full">
        <TabsList>
          <TabsTrigger
            value="data"
            disabled={sessionLoading || (!isCompleted && !isEnhancedUploadProcessing && !isScheMatiQRunning && !isScheMatiQStopped && session?.status !== 'documents_uploaded')}
            title={(!isCompleted && !isEnhancedUploadProcessing && !isScheMatiQRunning && !isScheMatiQStopped && session?.status !== 'documents_uploaded') ? 'Data will appear once processing starts' : undefined}
          >
            Data
          </TabsTrigger>
          <TabsTrigger
            value="schema"
            disabled={sessionLoading || !isSchemaReady || (!session?.columns?.length && !isSessionRefetching)}
            title={(!isSchemaReady || (!session?.columns?.length && !isSessionRefetching)) ? 'Schema will appear once processing starts' : undefined}
          >
            Schema
          </TabsTrigger>
          <TabsTrigger
            value="stats"
            disabled={sessionLoading || (!isCompleted && !isScheMatiQStopped)}
            title={(!isCompleted && !isScheMatiQStopped) ? 'Statistics will appear once processing completes' : undefined}
          >
            Statistics
          </TabsTrigger>
          {mode === 'schematiq' && (
            <TabsTrigger value="monitor" className={session?.status === 'processing' ? 'gap-2' : undefined}>
              {session?.status === 'processing' && (
                <Loader2 className="h-4 w-4 animate-spin" />
              )}
              ScheMatiQ Monitor
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
          {(isCompleted || isEnhancedUploadProcessing || isScheMatiQRunning || isScheMatiQStopped || session?.status === 'documents_uploaded') && (dataResponse || streamingCells.size > 0) ? (
            <div className="relative" data-table-container>
              {/* View mode toggle (only when observation units exist) */}
              {((unitListResponse && unitListResponse.totalUnits > 0) || hasUnitColumn) && (
                <div className="mb-4 flex items-center gap-4">
                  <ViewModeToggle
                    viewMode={viewMode}
                    onViewModeChange={handleViewModeChange}
                    disabled={(!unitListResponse || unitListResponse.totalUnits === 0) && !hasUnitColumn}
                    disabledTooltip="No observation units found in this session"
                    unitCount={unitListResponse?.totalUnits || (hasUnitColumn ? undefined : 0)}
                  />
                </div>
              )}
              {/* Render either standard DataTable or UnitGroupedTable based on view mode */}
              {viewMode === 'by_unit' && ((unitListResponse && unitListResponse.totalUnits > 0) || hasUnitColumn) ? (
                <UnitGroupedTable
                  sessionId={sessionId!}
                  sessionType={mode}
                  columns={session?.columns?.map(col => col.name) || []}
                  columnInfo={session?.columns?.map(col => ({ name: col.name, definition: col.definition, allowed_values: col.allowed_values ?? undefined }))}
                  onDataChange={() => {
                    queryClient.invalidateQueries(['session', sessionId, mode]);
                    queryClient.invalidateQueries(['data', sessionId, mode]);
                    queryClient.invalidateQueries({ queryKey: ['unitData', sessionId], exact: false });
                    refreshUnits();
                  }}
                  processingColumns={processingColumns}
                  currentColumn={currentColumn}
                  currentDocumentProgress={currentDocumentProgress}
                  onStopReextraction={handleStopReextraction}
                  isStoppingReextraction={isStoppingReextraction}
                  isProcessingDocuments={isEnhancedUploadProcessing}
                  onStopProcessing={handleStopProcessing}
                  isStoppingProcessing={isStoppingProcessing}
                  columnOrder={columnOrder}
                />
              ) : (
                <DataTable
                  sessionId={sessionId!}
                  sessionType={mode}
                  newlyAddedRows={newlyAddedRows}
                  columnOrder={columnOrder}
                  onColumnReorder={handleColumnReorder}
                  streamingCells={streamingCells}
                  processingColumns={processingColumns}
                  currentColumn={currentColumn}
                  currentDocumentProgress={currentDocumentProgress}
                  onStopReextraction={handleStopReextraction}
                  isStoppingReextraction={isStoppingReextraction}
                  isProcessingDocuments={isEnhancedUploadProcessing}
                  onStopProcessing={handleStopProcessing}
                  isStoppingProcessing={isStoppingProcessing}
                  onDataChange={() => {
                    queryClient.invalidateQueries(['session', sessionId, mode]);
                    queryClient.invalidateQueries(['data', sessionId, mode]);
                    queryClient.invalidateQueries({ queryKey: ['unitData', sessionId], exact: false });
                    refreshUnits();
                  }}
                  columnInfo={session?.columns?.map(col => ({ name: col.name, definition: col.definition, allowed_values: col.allowed_values ?? undefined }))}
                  viewMode={viewMode}
                  onViewModeChange={handleViewModeChange}
                  hasUnits={((unitListResponse && unitListResponse.totalUnits > 0) || hasUnitColumn) || false}
                  unitCount={unitListResponse?.totalUnits}
                  documentList={documentListResponse?.documents}
                  selectedDocuments={selectedDocuments}
                  onDocumentChange={setSelectedDocuments}
                  documentDataLoading={!documentListResponse}
                />
              )}

              {/* Document Upload Section (collapsible) */}
              {((mode === 'load' && ['documents_uploaded', 'processing_documents', 'completed', 'stopped'].includes(session?.status || '')) ||
                (mode === 'schematiq' && ['completed', 'documents_uploaded', 'processing_documents', 'stopped'].includes(session?.status || ''))) &&
                !sessionLoading && !dataLoading && dataResponse && (
                  <Card className="mt-6">
                    <CardHeader className="cursor-pointer select-none" onClick={() => setAddDocsExpanded(!addDocsExpanded)}>
                      <div className="flex items-center justify-between">
                        <CardTitle className="text-base">
                          {mode === 'schematiq' ? 'Add More Documents'
                            : session?.status === 'documents_uploaded' ? 'Process Your Documents'
                            : 'Add More Documents'}
                        </CardTitle>
                        <ChevronDown className={`h-4 w-4 text-muted-foreground transition-transform ${addDocsExpanded ? 'rotate-180' : ''}`} />
                      </div>
                    </CardHeader>
                    {addDocsExpanded && (
                      <CardContent className="space-y-4">
                        <p className="text-sm text-muted-foreground">
                          {mode === 'schematiq'
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
                          maxDocuments={maxDocuments}
                          existingDocumentCount={session?.metadata?.uploaded_documents?.length || 0}
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
                                    'Process Documents'}
                              </Button>
                            )}
                          </div>
                        )}
                      </CardContent>
                    )}
                  </Card>
                )}
            </div>
          ) : (
            <Alert variant="info">
              <AlertDescription>
                {isScheMatiQRunning ? 'Data will be available when ScheMatiQ processing completes' : 'No data available'}
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
                // Use refetch for immediate update instead of invalidate
                queryClient.refetchQueries({ queryKey: ['session', sessionId, mode] });
                queryClient.refetchQueries({ queryKey: ['data', sessionId, mode] });
                queryClient.refetchQueries({ queryKey: ['unitData', sessionId], exact: false });
                refreshUnits();
              }}
              onReextractionStarted={handleReextractionStarted}
              llmConfig={session.metadata?.extracted_schema?.llm_configuration?.schema_creation_backend || null}
              observationUnit={session.observation_unit}
              onRegenerateSchema={mode === 'schematiq' ? async () => {
                try {
                  await schematiqAPI.resume(sessionId!);
                  
                  // Invalidate session query to refetch fresh status (should now show "processing")
                  queryClient.invalidateQueries(['session', sessionId]);
                  
                  // Force WebSocket connection to receive real-time progress updates
                  setForceWebSocketConnect(true);
                  
                  toast({
                    title: 'Schema Rediscovery Started',
                    description: 'The schema is being rediscovered with the updated observation unit. Switching to the Monitor tab.',
                  });
                  
                  // Switch to monitor tab to show progress
                  setActiveTab('monitor');
                } catch (error: any) {
                  const detail = error?.response?.data?.detail;
                  toast({
                    title: 'Error',
                    description: detail || 'Failed to start schema rediscovery',
                    variant: 'destructive',
                  });
                }
              } : undefined}
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

        {/* ScheMatiQ Monitor Tab */}
        {mode === 'schematiq' && (
          <TabsContent value="monitor" className="mt-4">
            <ScheMatiQMonitor
              sessionId={sessionId}
              autoStarted={autoStartState.autoStarted}
              initialCapacityMessage={autoStartState.initialCapacityMessage}
            />
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

      {/* Table Feedback Widget (release mode only) */}
      {!developerMode && sessionId && (
        <TableFeedbackWidget
          sessionId={sessionId}
          sessionStatus={session?.status || ''}
          activeTab={activeTab}
          tableRowCount={dataResponse?.total_count || 0}
          tableColumnCount={session?.columns?.length || 0}
        />
      )}

      {/* LLM Selection Dialog */}
      <LLMSelector
        open={showLLMSelector}
        onClose={() => setShowLLMSelector(false)}
        onConfirm={handleLLMSelection}
        title="Select AI Model for Document Processing"
        description="Choose the AI model that will extract information from your uploaded documents."
        preservedConfig={session?.metadata?.extracted_schema?.llm_configuration?.value_extraction_backend || null}
        loading={documentUploadLoading}
        defaultModel="gemini-2.5-flash-lite"
      />

      {/* Visualize Guide Dialog */}
      <VisualizeGuideDialog
        autoOpen={visualizeGuideAutoOpen}
        forceOpen={visualizeGuideForceOpen}
        onOpenChange={(open) => {
          if (!open) {
            setVisualizeGuideAutoOpen(false);
            setVisualizeGuideForceOpen(false);
          }
        }}
        onDismiss={() => handleTabChange('data')}
      />

      {/* Navigation guard for active processing */}
      <NavigationConfirmDialog
        blocker={processingBlocker}
        title="Processing in progress"
        description="Schema discovery is still running. You can return to this session later. Are you sure you want to leave?"
      />
    </div>
  );
};

export default Visualize;
