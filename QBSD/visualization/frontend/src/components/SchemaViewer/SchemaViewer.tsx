import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Paper,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Chip,
  IconButton,
  TextField,
  Button,
  Alert,
  Grid,
  Card,
  CardContent,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  CircularProgress,
  Snackbar,
  Checkbox,
  FormControlLabel,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Divider,
  Tooltip,
  LinearProgress,
  Badge,
} from '@mui/material';
import {
  ExpandMore,
  Edit,
  Save,
  Cancel,
  Add,
  Delete,
  Schema as SchemaIcon,
  MoreVert,
  Merge,
  Backup,
  Restore,
  Verified,
  Refresh,
  Warning,
  CheckCircle,
  Error as ErrorIcon,
  Info,
} from '@mui/icons-material';

import { 
  ColumnInfo, 
  ColumnDialogState,
  ReprocessingStatus,
  SchemaValidationResult as SchemaValidationResultType,
  WebSocketMessageExtended
} from '../../types';
import { formatColumnName } from '../../utils/formatting';
import { schemaAPI } from '../../services/api';
import ColumnDialog from '../SchemaEditor/ColumnDialog';
import MergeDialog from '../SchemaEditor/MergeDialog';
import LLMConfigDisplay from '../LLMConfigDisplay';

interface SchemaViewerProps {
  columns: ColumnInfo[];
  query?: string;
  sessionId: string;
  sessionType?: 'load' | 'qbsd';
  readonly?: boolean;
  onColumnsChange?: (columns: ColumnInfo[]) => void;
  websocketManager?: any;
  llmConfig?: any; // LLM configuration for display
}

const SchemaViewer: React.FC<SchemaViewerProps> = ({ 
  columns, 
  query, 
  sessionId, 
  sessionType = 'qbsd',
  readonly = false,
  onColumnsChange,
  websocketManager,
  llmConfig
}) => {
  // State management
  const [localColumns, setLocalColumns] = useState<ColumnInfo[]>(columns || []);
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [dialogState, setDialogState] = useState<ColumnDialogState>({
    open: false,
    mode: 'add'
  });
  const [mergeDialogOpen, setMergeDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [columnToDelete, setColumnToDelete] = useState<string>('');
  const [menuAnchor, setMenuAnchor] = useState<null | HTMLElement>(null);
  const [reprocessingStatus, setReprocessingStatus] = useState<ReprocessingStatus | null>(null);
  const [validationResult, setValidationResult] = useState<SchemaValidationResultType | null>(null);
  const [loading, setLoading] = useState(false);
  const [snackbar, setSnackbar] = useState({ open: false, message: '', severity: 'success' as 'success' | 'error' | 'warning' | 'info' });

  // Update local columns when props change
  useEffect(() => {
    setLocalColumns(columns || []);
  }, [columns]);

  // Set up WebSocket listener for real-time updates
  useEffect(() => {
    if (!websocketManager) return;

    const handleMessage = (message: WebSocketMessageExtended) => {
      if (message.session_id !== sessionId) return;

      switch (message.type) {
        case 'schema_updated':
          if (message.data && 'operation' in message.data) {
            handleSchemaUpdate(message.data);
          }
          break;
        case 'reprocessing_progress':
          if (message.data && 'operation_id' in message.data) {
            handleReprocessingProgress(message.data);
          }
          break;
        case 'reprocessing_completed':
          if (message.data && 'operation_id' in message.data) {
            handleReprocessingCompleted(message.data);
          }
          break;
      }
    };

    websocketManager.addMessageHandler(handleMessage);
    
    return () => {
      websocketManager.removeMessageHandler(handleMessage);
    };
  }, [websocketManager, sessionId]);

  // Load initial validation and reprocessing status
  useEffect(() => {
    if (!readonly && sessionId) {
      loadValidationResult();
      loadReprocessingStatus();
    }
  }, [sessionId, readonly]);

  // WebSocket event handlers
  const handleSchemaUpdate = (data: any) => {
    showSnackbar(`Schema ${data.operation.replace('_', ' ')} completed successfully`, 'success');
    
    // Trigger parent component refresh for both schema and data
    if (onColumnsChange) {
      onColumnsChange(data.columns || []);
    }
    
    // If data was updated (like in column deletion), trigger a broader refresh
    if (data.data_updated || data.refresh_data) {
      // Force a complete refresh of the visualization
      window.dispatchEvent(new CustomEvent('schema-data-updated', { 
        detail: { 
          sessionId, 
          operation: data.operation,
          columns: data.columns 
        }
      }));
    }
  };

  const handleReprocessingProgress = (data: any) => {
    setReprocessingStatus({
      session_id: sessionId,
      status: 'processing',
      progress: data.progress,
      current_step: data.step,
      affected_columns: data.affected_columns,
      processed_documents: data.processed_documents,
      total_documents: data.total_documents,
    });
  };

  const handleReprocessingCompleted = (data: any) => {
    setReprocessingStatus(null);
    showSnackbar(`Reprocessing completed for ${data.affected_columns?.length || 0} columns`, 'success');
  };

  // Utility functions
  const showSnackbar = (message: string, severity: 'success' | 'error' | 'warning' | 'info') => {
    setSnackbar({ open: true, message, severity });
  };

  const loadValidationResult = async () => {
    try {
      const result = await schemaAPI.validateSchema(sessionId);
      setValidationResult(result);
    } catch (error) {
      console.error('Failed to load validation result:', error);
    }
  };

  const loadReprocessingStatus = async () => {
    try {
      const status = await schemaAPI.getReprocessingStatus(sessionId);
      if (status.status === 'processing') {
        setReprocessingStatus(status);
      }
    } catch (error) {
      console.error('Failed to load reprocessing status:', error);
    }
  };

  // Column management handlers
  const handleEditColumn = (column: ColumnInfo) => {
    setDialogState({
      open: true,
      mode: 'edit',
      column
    });
  };

  const handleAddColumn = () => {
    setDialogState({
      open: true,
      mode: 'add'
    });
  };

  const handleDeleteColumn = (columnName: string) => {
    setColumnToDelete(columnName);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = async () => {
    if (!columnToDelete) return;
    
    setLoading(true);
    try {
      await schemaAPI.deleteColumn(sessionId, columnToDelete);
      showSnackbar(`Column "${columnToDelete}" deleted successfully`, 'success');
      setDeleteDialogOpen(false);
      setColumnToDelete('');
      // Trigger parent to reload data
      if (onColumnsChange) {
        const updatedColumns = localColumns.filter(col => col.name !== columnToDelete);
        setLocalColumns(updatedColumns);
        onColumnsChange(updatedColumns);
      }
    } catch (error: any) {
      showSnackbar(error.response?.data?.detail || 'Failed to delete column', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleMergeColumns = () => {
    if (selectedColumns.length < 2) {
      showSnackbar('Select at least 2 columns to merge', 'warning');
      return;
    }
    setMergeDialogOpen(true);
  };

  const handleColumnSelection = (columnName: string, checked: boolean) => {
    if (checked) {
      setSelectedColumns(prev => [...prev, columnName]);
    } else {
      setSelectedColumns(prev => prev.filter(name => name !== columnName));
    }
  };

  const handleSelectAll = () => {
    const selectableColumns = (localColumns || [])
      .filter(col => col && col.name && !col.name.endsWith('_excerpt'))
      .map(col => col.name);
    setSelectedColumns(selectableColumns);
  };

  const handleClearSelection = () => {
    setSelectedColumns([]);
  };

  const handleDialogSuccess = (message: string) => {
    showSnackbar(message, 'success');
    // Clear selection and close dialogs
    setSelectedColumns([]);
    setDialogState({ open: false, mode: 'add' });
    setMergeDialogOpen(false);
    // Trigger parent to reload data
    if (onColumnsChange) {
      // Parent should refetch the data
    }
  };

  const handleDialogError = (message: string) => {
    showSnackbar(message, 'error');
  };

  const handleBackup = async () => {
    setLoading(true);
    try {
      const result = await schemaAPI.backupSchema(sessionId);
      showSnackbar(`Schema backup created: ${result.backup_id}`, 'success');
    } catch (error: any) {
      showSnackbar(error.response?.data?.detail || 'Failed to create backup', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleValidate = async () => {
    setLoading(true);
    try {
      const result = await schemaAPI.validateSchema(sessionId);
      setValidationResult(result);
      showSnackbar('Schema validation completed', 'info');
    } catch (error: any) {
      showSnackbar(error.response?.data?.detail || 'Failed to validate schema', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleReprocess = async () => {
    setLoading(true);
    try {
      await schemaAPI.reprocessDocuments(sessionId, { 
        incremental: true 
      });
      showSnackbar('Document reprocessing started', 'info');
    } catch (error: any) {
      showSnackbar(error.response?.data?.detail || 'Failed to start reprocessing', 'error');
    } finally {
      setLoading(false);
    }
  };

  // Filter out excerpt columns for display
  const displayColumns = (localColumns || []).filter(column => {
    return column && 
           column.name && 
           !column.name.toLowerCase().includes('excerpt') &&
           !column.name.toLowerCase().endsWith('_excerpt');
  });

  const getValidationSeverity = (): 'success' | 'warning' | 'error' => {
    if (!validationResult) return 'success';
    if (validationResult.errors && validationResult.errors.length > 0) return 'error';
    if (validationResult.warnings && validationResult.warnings.length > 0) return 'warning';
    return 'success';
  };

  const getValidationIcon = () => {
    const severity = getValidationSeverity();
    switch (severity) {
      case 'error': return <ErrorIcon />;
      case 'warning': return <Warning />;
      default: return <CheckCircle />;
    }
  };

  return (
    <Box>
      {/* Query and Schema Source Display */}
      {query && (
        <Paper sx={{ p: 3, mb: 3 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <Box sx={{ flex: 1 }}>
              <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                <SchemaIcon sx={{ mr: 1 }} />
                Research Query
              </Typography>
              <Typography variant="body1" color="text.secondary">
                {query}
              </Typography>
            </Box>
            {/* Schema Source Indicator */}
            <Box sx={{ ml: 2, textAlign: 'right' }}>
              <Chip
                icon={sessionType === 'load' ? <Backup /> : <SchemaIcon />}
                label={sessionType === 'load' ? 'Loaded Schema' : 'Generated Schema'}
                color={sessionType === 'load' ? 'secondary' : 'primary'}
                variant="outlined"
                size="small"
              />
            </Box>
          </Box>
        </Paper>
      )}

      {/* Schema Metadata for Load Sessions */}
      {sessionType === 'load' && displayColumns.length > 0 && (
        <Paper sx={{ p: 3, mb: 3, bgcolor: 'action.hover' }}>
          <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
            <Verified sx={{ mr: 1, color: 'success.main' }} />
            Schema Information
          </Typography>
          <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center', mb: llmConfig ? 2 : 0 }}>
            <Chip 
              label={`${displayColumns.length} columns defined`}
              color="primary"
              size="small"
            />
            <Chip 
              label={`${displayColumns.filter(col => col.definition).length} with definitions`}
              color={displayColumns.every(col => col.definition) ? 'success' : 'warning'}
              size="small"
            />
            <Chip 
              label={`${displayColumns.filter(col => col.rationale).length} with rationales`}
              color={displayColumns.every(col => col.rationale) ? 'success' : 'warning'}
              size="small"
            />
          </Box>
          
          {/* LLM Configuration Display */}
          {llmConfig && (
            <Box sx={{ mt: 2, pt: 2, borderTop: '1px solid', borderColor: 'divider' }}>
              <LLMConfigDisplay
                config={llmConfig}
                title="Schema Creation Model"
                variant="inline"
                showDetails={true}
              />
            </Box>
          )}
        </Paper>
      )}

      {/* Reprocessing Progress */}
      {reprocessingStatus && (
        <Paper sx={{ p: 3, mb: 3 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 2 }}>
            <CircularProgress size={20} />
            <Typography variant="h6">
              Reprocessing Documents
            </Typography>
          </Box>
          <Typography variant="body2" color="text.secondary" gutterBottom>
            {reprocessingStatus.current_step}
          </Typography>
          <LinearProgress 
            variant="determinate" 
            value={reprocessingStatus.progress * 100} 
            sx={{ mb: 1 }}
          />
          <Typography variant="caption" color="text.secondary">
            {reprocessingStatus.processed_documents} of {reprocessingStatus.total_documents} documents processed
            {reprocessingStatus.affected_columns && reprocessingStatus.affected_columns.length > 0 && 
              ` (${reprocessingStatus.affected_columns.join(', ')})`
            }
          </Typography>
        </Paper>
      )}

      {/* Schema Overview */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Typography variant="h6">
              Schema Overview ({displayColumns.length} columns)
            </Typography>
            
            {/* Validation Status Badge */}
            {validationResult && (
              <Tooltip title={`${validationResult.errors?.length || 0} errors, ${validationResult.warnings?.length || 0} warnings`}>
                <Badge 
                  badgeContent={(validationResult.errors?.length || 0) + (validationResult.warnings?.length || 0)}
                  color={getValidationSeverity() as 'error' | 'warning' | 'success'}
                  max={99}
                >
                  {getValidationIcon()}
                </Badge>
              </Tooltip>
            )}
          </Box>
          
          {!readonly && (
            <Box sx={{ display: 'flex', gap: 1 }}>
              {/* Selection Controls */}
              {selectedColumns.length > 0 && (
                <Box sx={{ display: 'flex', gap: 1, mr: 2 }}>
                  <Button
                    size="small"
                    onClick={handleClearSelection}
                  >
                    Clear ({selectedColumns.length})
                  </Button>
                  
                  <Button
                    size="small"
                    startIcon={<Merge />}
                    disabled={selectedColumns.length < 2}
                    onClick={handleMergeColumns}
                  >
                    Merge
                  </Button>
                </Box>
              )}
              
              <Button
                size="small"
                onClick={selectedColumns.length === 0 ? handleSelectAll : handleClearSelection}
              >
                {selectedColumns.length === 0 ? 'Select All' : 'Clear All'}
              </Button>
              
              <Button
                startIcon={<Add />}
                variant="outlined"
                onClick={handleAddColumn}
              >
                Add Column
              </Button>
              
              <IconButton
                onClick={(e) => setMenuAnchor(e.currentTarget)}
                disabled={loading}
              >
                <MoreVert />
              </IconButton>
            </Box>
          )}
        </Box>

        {/* Column Grid */}
        <Grid container spacing={2}>
          {displayColumns.map((column, index) => (
            <Grid item xs={12} sm={6} md={4} key={column.name}>
              <Card 
                variant="outlined" 
                sx={{ 
                  height: '100%',
                  position: 'relative',
                  border: selectedColumns.includes(column.name) ? 2 : 1,
                  borderColor: selectedColumns.includes(column.name) ? 'primary.main' : 'divider'
                }}
              >
                <CardContent>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1 }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {!readonly && (
                        <Checkbox
                          size="small"
                          checked={selectedColumns.includes(column.name)}
                          onChange={(e) => handleColumnSelection(column.name, e.target.checked)}
                        />
                      )}
                      <Typography variant="h6" component="div" sx={{ fontSize: '1rem' }}>
                        {formatColumnName(column.name)}
                      </Typography>
                    </Box>
                    
                    {!readonly && (
                      <Box>
                        <IconButton 
                          size="small" 
                          onClick={() => handleEditColumn(column)}
                          title="Edit column"
                        >
                          <Edit fontSize="small" />
                        </IconButton>
                        <IconButton 
                          size="small" 
                          color="error"
                          onClick={() => handleDeleteColumn(column.name)}
                          title="Delete column"
                        >
                          <Delete fontSize="small" />
                        </IconButton>
                      </Box>
                    )}
                  </Box>

                  {column.data_type && (
                    <Chip label={column.data_type} size="small" color="primary" sx={{ mb: 1 }} />
                  )}

                  {column.definition && (
                    <Box sx={{ mb: 2, p: 2, bgcolor: 'grey.50', borderRadius: 1, border: '1px solid', borderColor: 'grey.200' }}>
                      <Typography variant="subtitle2" color="primary" sx={{ fontWeight: 600, mb: 1 }}>
                        Definition
                      </Typography>
                      <Typography variant="body2" color="text.primary" sx={{ lineHeight: 1.5 }}>
                        {column.definition}
                      </Typography>
                    </Box>
                  )}

                  {column.rationale && (
                    <Box sx={{ mb: 2, p: 2, bgcolor: 'info.light', borderRadius: 1, border: '1px solid', borderColor: 'info.main', opacity: 0.9 }}>
                      <Typography variant="subtitle2" color="info.dark" sx={{ fontWeight: 600, mb: 1 }}>
                        Rationale
                      </Typography>
                      <Typography variant="body2" color="text.primary" sx={{ lineHeight: 1.5 }}>
                        {column.rationale}
                      </Typography>
                    </Box>
                  )}

                  {/* Missing metadata warning for loaded schemas */}
                  {sessionType === 'load' && (!column.definition || !column.rationale) && (
                    <Box sx={{ mb: 1, p: 1.5, bgcolor: 'warning.light', borderRadius: 1, border: '1px solid', borderColor: 'warning.main', opacity: 0.8 }}>
                      <Typography variant="caption" color="warning.dark" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                        <Warning fontSize="small" />
                        {!column.definition && !column.rationale 
                          ? 'Missing definition and rationale' 
                          : !column.definition 
                          ? 'Missing definition' 
                          : 'Missing rationale'}
                      </Typography>
                    </Box>
                  )}

                  {(column.non_null_count !== undefined || column.unique_count !== undefined) && (
                    <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                      {column.non_null_count !== undefined && (
                        <Chip 
                          label={`${column.non_null_count} non-null`} 
                          size="small" 
                          variant="outlined" 
                        />
                      )}
                      {column.unique_count !== undefined && (
                        <Chip 
                          label={`${column.unique_count} unique`} 
                          size="small" 
                          variant="outlined" 
                        />
                      )}
                    </Box>
                  )}
                </CardContent>
              </Card>
            </Grid>
          ))}
        </Grid>
      </Paper>

      {/* Validation Results */}
      {validationResult && ((validationResult.errors && validationResult.errors.length > 0) || (validationResult.warnings && validationResult.warnings.length > 0)) && (
        <Paper sx={{ p: 3, mb: 3 }}>
          <Typography variant="h6" gutterBottom>
            Schema Validation
          </Typography>
          
          {validationResult.errors && validationResult.errors.length > 0 && (
            <Alert severity="error" sx={{ mb: 2 }}>
              <Typography variant="subtitle2" gutterBottom>Errors:</Typography>
              {validationResult.errors.map((error, index) => (
                <Typography key={index} variant="body2">• {error}</Typography>
              ))}
            </Alert>
          )}
          
          {validationResult.warnings && validationResult.warnings.length > 0 && (
            <Alert severity="warning" sx={{ mb: 2 }}>
              <Typography variant="subtitle2" gutterBottom>Warnings:</Typography>
              {validationResult.warnings.map((warning, index) => (
                <Typography key={index} variant="body2">• {warning}</Typography>
              ))}
            </Alert>
          )}
          
          {validationResult.suggestions && validationResult.suggestions.length > 0 && (
            <Alert severity="info">
              <Typography variant="subtitle2" gutterBottom>Suggestions:</Typography>
              {validationResult.suggestions.map((suggestion, index) => (
                <Typography key={index} variant="body2">• {suggestion}</Typography>
              ))}
            </Alert>
          )}
        </Paper>
      )}

      {/* Actions Menu */}
      <Menu
        anchorEl={menuAnchor}
        open={Boolean(menuAnchor)}
        onClose={() => setMenuAnchor(null)}
      >
        <MenuItem onClick={handleValidate} disabled={loading}>
          <ListItemIcon>
            <Verified />
          </ListItemIcon>
          <ListItemText>Validate Schema</ListItemText>
        </MenuItem>
        
        <MenuItem onClick={handleBackup} disabled={loading}>
          <ListItemIcon>
            <Backup />
          </ListItemIcon>
          <ListItemText>Create Backup</ListItemText>
        </MenuItem>
        
        <Divider />
        
        <MenuItem onClick={handleReprocess} disabled={loading || Boolean(reprocessingStatus)}>
          <ListItemIcon>
            <Refresh />
          </ListItemIcon>
          <ListItemText>Reprocess All</ListItemText>
        </MenuItem>
      </Menu>

      {/* Dialogs */}
      <ColumnDialog
        open={dialogState.open}
        mode={dialogState.mode}
        sessionId={sessionId}
        column={dialogState.column}
        existingColumns={localColumns}
        onClose={() => setDialogState({ open: false, mode: 'add' })}
        onSuccess={handleDialogSuccess}
        onError={handleDialogError}
      />

      <MergeDialog
        open={mergeDialogOpen}
        sessionId={sessionId}
        columns={localColumns}
        preselectedColumns={selectedColumns}
        onClose={() => setMergeDialogOpen(false)}
        onSuccess={handleDialogSuccess}
        onError={handleDialogError}
      />

      {/* Delete Confirmation Dialog */}
      <Dialog
        open={deleteDialogOpen}
        onClose={() => setDeleteDialogOpen(false)}
      >
        <DialogTitle>Delete Column</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Are you sure you want to delete the column "{columnToDelete}"? 
            This action will remove the column from the schema and delete all associated data. 
            This cannot be undone.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)}>
            Cancel
          </Button>
          <Button 
            onClick={confirmDelete} 
            color="error" 
            variant="contained"
            disabled={loading}
            startIcon={loading ? <CircularProgress size={16} /> : <Delete />}
          >
            {loading ? 'Deleting...' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Snackbar */}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={6000}
        onClose={() => setSnackbar(prev => ({ ...prev, open: false }))}
      >
        <Alert 
          onClose={() => setSnackbar(prev => ({ ...prev, open: false }))}
          severity={snackbar.severity}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
      
      {readonly && (
        <Alert severity="info" sx={{ mt: 2 }}>
          <Typography variant="body2">
            <strong>Read-only mode:</strong> Schema editing features are disabled. 
            To enable editing, ensure you have proper permissions and the session supports schema modifications.
          </Typography>
        </Alert>
      )}
    </Box>
  );
};

export default SchemaViewer;