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
} from '@mui/material';
import { 
  ArrowBack, 
  TableView, 
  Schema, 
  Analytics, 
  Download,
  Refresh,
  Info,
} from '@mui/icons-material';
import { useQuery, useQueryClient } from 'react-query';

import { uploadAPI, qbsdAPI } from '../services/api';
import { VisualizationSession } from '../types';
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
  const [activeTab, setActiveTab] = useState(mode === 'qbsd' ? 3 : 0); // Start with monitor for QBSD
  
  // Enhanced upload document management state
  const [uploadedDocuments, setUploadedDocuments] = useState<File[]>([]);
  const [documentUploadLoading, setDocumentUploadLoading] = useState(false);
  const [documentUploadResult, setDocumentUploadResult] = useState<any>(null);
  const [documentUploadError, setDocumentUploadError] = useState<string | null>(null);
  const [newlyAddedRows, setNewlyAddedRows] = useState<Set<number>>(new Set());
  
  // Fetch session data
  const { data: session, isLoading: sessionLoading, error: sessionError } = useQuery(
    ['session', sessionId, mode],
    async () => {
      if (mode === 'upload') {
        return uploadAPI.getSession(sessionId!);
      } else {
        // For QBSD, we need to construct session info from status
        const status = await qbsdAPI.getStatus(sessionId!);
        const schema = await qbsdAPI.getSchema(sessionId!);
        
        const sessionData = {
          id: sessionId!,
          type: 'qbsd' as const,
          status: status.status as any,
          metadata: {
            source: `QBSD Query: ${schema.query || 'Unknown'}`,
            created: new Date().toISOString(),
            last_modified: new Date().toISOString(),
          },
          schema_query: schema.query,
          columns: schema.schema || [],
          statistics: undefined,
        } as VisualizationSession;
        
        // Debug logging
        console.log('🔍 Session Data Update:', {
          status: status.status,
          columnsCount: schema.schema?.length || 0,
          hasColumns: !!(schema.schema && schema.schema.length > 0)
        });
        
        return sessionData;
      }
    },
    {
      enabled: !!sessionId,
      refetchInterval: mode === 'qbsd' ? PROCESSING_REFRESH_INTERVAL : false, // Auto-refresh for QBSD
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

  // WebSocket integration for real-time updates
  useEffect(() => {
    if (!sessionId || mode !== 'upload') return;

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/${sessionId}`;
    
    let ws: WebSocket | null = null;
    let reconnectAttempts = 0;
    const maxReconnectAttempts = WS_RECONNECT_ATTEMPTS;
    
    const connectWebSocket = () => {
      try {
        ws = new WebSocket(wsUrl);
        
        ws.onopen = () => {
          console.log('WebSocket connected for upload session:', sessionId);
          reconnectAttempts = 0;
        };
        
        ws.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data);
            console.log('WebSocket message received:', message);
            
            switch (message.type) {
              case 'row_completed':
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
                // Refresh session data for progress updates
                queryClient.invalidateQueries(['session', sessionId]);
                if (message.type === 'completion') {
                  console.log('💾 WebSocket: Invalidating data queries to refresh table');
                  queryClient.invalidateQueries(['data', sessionId]);
                  queryClient.invalidateQueries(['data', sessionId, mode]);
                  // Disconnect WebSocket after completion to avoid keeping it open unnecessarily
                  setTimeout(() => {
                    if (ws && ws.readyState === WebSocket.OPEN) {
                      console.log('🔌 WebSocket: Closing connection after processing completion');
                      ws.close(1000, 'Processing completed');
                    }
                  }, 2000); // Increased delay to ensure data refresh completes
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
    
    // Connect during document processing and briefly after completion to catch final messages
    if (session?.status === 'processing_documents' || session?.status === 'completed') {
      connectWebSocket();
    }
    
    return () => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close(1000, 'Component unmounting');
      }
    };
  }, [sessionId, mode, session?.status, queryClient]);

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
      const apiUrl = mode === 'upload' 
        ? `/api/upload/export/${sessionId}`
        : `/api/qbsd/export/${sessionId}`;
      
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

  const handleDocumentProcessing = async () => {
    if (!sessionId) return;

    try {
      await uploadAPI.processDocuments(sessionId);
      // Refresh data to show processing status
      queryClient.invalidateQueries(['session', sessionId]);
      queryClient.invalidateQueries(['data', sessionId]);
      
      // Start monitoring processing progress
      startDocumentProcessingMonitor();
    } catch (err: any) {
      console.error('Failed to start document processing:', err);
      setDocumentUploadError(err.response?.data?.detail || 'Failed to start processing');
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
                       session?.status === 'completed';
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
            
            {isCompleted && (
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
            <Tab icon={<CircularProgress size={16} />} label="QBSD Monitor" />
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
              {/* Processing Overlay */}
              {session?.status === 'processing_documents' && (
                <Box
                  sx={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    right: 0,
                    zIndex: 10,
                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                    backdropFilter: 'blur(2px)',
                    padding: 3,
                    borderRadius: 1,
                    border: '2px solid',
                    borderColor: 'primary.main',
                    mb: 2
                  }}
                >
                  <Alert severity="info" sx={{ mb: 2 }}>
                    <Typography variant="body1" gutterBottom sx={{ fontWeight: 'bold' }}>
                      🤖 Processing Documents with AI
                    </Typography>
                    <Typography variant="body2">
                      Extracting data from your uploaded documents. New rows will appear as they are completed.
                    </Typography>
                  </Alert>
                  
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <CircularProgress size={24} />
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
                data={dataResponse}
                sessionId={sessionId}
                sessionType={mode}
                newlyAddedRows={newlyAddedRows}
              />
              
              {/* Document Upload Section - only show below data table when data is loaded and not during initial processing */}
              {mode === 'upload' && !sessionLoading && !dataLoading && dataResponse && (session?.status === 'documents_uploaded' || session?.status === 'processing_documents' || session?.status === 'completed') && (
                <Box sx={{ mt: 4 }}>
                  <Paper sx={{ p: 3 }}>
                    <Typography variant="h6" gutterBottom>
                      {session?.status === 'documents_uploaded' ? 'Process Your Documents' : 'Add More Documents'}
                    </Typography>
                    <Typography variant="body2" color="text.secondary" paragraph>
                      {session?.status === 'documents_uploaded' 
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
                          <Button
                            variant="contained"
                            size="large"
                            onClick={handleDocumentProcessing}
                            disabled={session?.status?.includes('processing')}
                          >
                            {session?.status?.includes('processing') ? 'Processing...' : 'Process Documents with AI'}
                          </Button>
                        )}
                        
                        {session?.status !== 'processing_documents' && session?.status !== 'completed' && (
                          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                            This will analyze your uploaded documents and extract data according to your schema.
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
    </Box>
  );
};

export default Visualize;