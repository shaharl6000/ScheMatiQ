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
} from '@mui/material';
import { 
  ArrowBack, 
  TableView, 
  Schema, 
  Analytics, 
  Download,
  Refresh,
} from '@mui/icons-material';
import { useQuery, useQueryClient } from 'react-query';

import { sessionAPI, uploadAPI, qbsdAPI } from '../services/api';
import { VisualizationSession, PaginatedData } from '../types';

// Component imports (will be created next)
import DataTable from '../components/DataTable/DataTable';
import SchemaViewer from '../components/SchemaViewer/SchemaViewer';
import StatsDashboard from '../components/StatsDashboard/StatsDashboard';
import QBSDMonitor from '../components/QBSDMonitor/QBSDMonitor';

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
        
        return {
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
      }
    },
    {
      enabled: !!sessionId,
      refetchInterval: mode === 'qbsd' ? 5000 : false, // Auto-refresh for QBSD
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
      enabled: !!sessionId && session?.status === 'completed',
    }
  );

  const handleRefresh = () => {
    queryClient.invalidateQueries(['session', sessionId]);
    queryClient.invalidateQueries(['data', sessionId]);
  };

  const handleExport = async () => {
    // Implementation would depend on backend export functionality
    console.log('Export functionality to be implemented');
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
  const isCompleted = session?.status === 'completed';
  
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
              label={session?.status || 'Unknown'}
              color={
                session?.status === 'completed' ? 'success' :
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
          <Tab icon={<TableView />} label="Data" disabled={!isCompleted} />
          <Tab icon={<Schema />} label="Schema" disabled={!session?.columns?.length} />
          <Tab icon={<Analytics />} label="Statistics" disabled={!isCompleted} />
          {mode === 'qbsd' && (
            <Tab icon={<CircularProgress size={16} />} label="Monitor" />
          )}
        </Tabs>
      </Box>

      {/* Tab Panels */}
      <Box sx={{ p: 3 }}>
        <TabPanel value={activeTab} index={0}>
          {isCompleted && dataResponse ? (
            <DataTable 
              data={dataResponse}
              sessionId={sessionId}
              sessionType={mode}
            />
          ) : (
            <Alert severity="info">
              {isQBSDRunning ? 'Data will be available when QBSD processing completes' : 'No data available'}
            </Alert>
          )}
        </TabPanel>

        <TabPanel value={activeTab} index={1}>
          {session?.columns?.length ? (
            <SchemaViewer 
              columns={session.columns}
              query={session.schema_query}
              sessionId={sessionId}
              readonly={mode === 'upload'}
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

        {mode === 'qbsd' && (
          <TabPanel value={activeTab} index={3}>
            <QBSDMonitor sessionId={sessionId} />
          </TabPanel>
        )}
      </Box>
    </Box>
  );
};

export default Visualize;