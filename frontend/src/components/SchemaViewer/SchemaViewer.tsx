import React, { useState, useEffect, useCallback } from 'react';
import {
  Pencil,
  Plus,
  Trash2,
  Database,
  MoreVertical,
  GitMerge,
  Save,
  ShieldCheck,
  RefreshCw,
  AlertTriangle,
  CheckCircle2,
  AlertCircle,
  Info,
  Loader2,
  Copy,
  Check,
} from 'lucide-react';

import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Checkbox } from '@/components/ui/checkbox';
import { Progress } from '@/components/ui/progress';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { useToast } from '@/components/ui/use-toast';
import { cn } from '@/lib/utils';

import {
  ColumnInfo,
  ColumnDialogState,
  ReprocessingStatus,
  SchemaValidationResult as SchemaValidationResultType,
  WebSocketMessageExtended
} from '../../types';
import { formatColumnName } from '../../utils/formatting';
import { copyToClipboard } from '../../utils/clipboard';
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
  llmConfig?: any;
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
  const { toast } = useToast();

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
  const [reprocessDialogOpen, setReprocessDialogOpen] = useState(false);
  const [reprocessingStatus, setReprocessingStatus] = useState<ReprocessingStatus | null>(null);
  const [validationResult, setValidationResult] = useState<SchemaValidationResultType | null>(null);
  const [loading, setLoading] = useState(false);
  const [copiedQuery, setCopiedQuery] = useState(false);

  const handleCopyQuery = async () => {
    if (query) {
      const success = await copyToClipboard(query);
      if (success) {
        setCopiedQuery(true);
        setTimeout(() => setCopiedQuery(false), 2000);
      }
    }
  };

  // WebSocket event handlers (defined before useEffect that uses them)
  const handleSchemaUpdate = useCallback((data: any) => {
    toast({
      title: 'Schema Updated',
      description: `Schema ${data.operation.replace('_', ' ')} completed successfully`,
    });

    if (onColumnsChange) {
      onColumnsChange(data.columns || []);
    }

    if (data.data_updated || data.refresh_data) {
      window.dispatchEvent(new CustomEvent('schema-data-updated', {
        detail: {
          sessionId,
          operation: data.operation,
          columns: data.columns
        }
      }));
    }
  }, [toast, onColumnsChange, sessionId]);

  const handleReprocessingProgress = useCallback((data: any) => {
    setReprocessingStatus({
      session_id: sessionId,
      status: 'processing',
      progress: data.progress,
      current_step: data.step,
      affected_columns: data.affected_columns,
      processed_documents: data.processed_documents,
      total_documents: data.total_documents,
    });
  }, [sessionId]);

  const handleReprocessingCompleted = useCallback((data: any) => {
    setReprocessingStatus(null);
    toast({
      title: 'Reprocessing Complete',
      description: `Completed for ${data.affected_columns?.length || 0} columns`,
    });
  }, [toast]);

  const loadValidationResult = useCallback(async () => {
    try {
      const result = await schemaAPI.validateSchema(sessionId);
      setValidationResult(result);
    } catch (error) {
      console.error('Failed to load validation result:', error);
    }
  }, [sessionId]);

  const loadReprocessingStatus = useCallback(async () => {
    try {
      const status = await schemaAPI.getReprocessingStatus(sessionId);
      if (status.status === 'processing') {
        setReprocessingStatus(status);
      }
    } catch (error) {
      console.error('Failed to load reprocessing status:', error);
    }
  }, [sessionId]);

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
  }, [websocketManager, sessionId, handleSchemaUpdate, handleReprocessingProgress, handleReprocessingCompleted]);

  // Load initial validation and reprocessing status
  useEffect(() => {
    if (!readonly && sessionId) {
      loadValidationResult();
      loadReprocessingStatus();
    }
  }, [sessionId, readonly, loadValidationResult, loadReprocessingStatus]);

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
      toast({
        title: 'Column Deleted',
        description: `Column "${columnToDelete}" deleted successfully`,
      });
      setDeleteDialogOpen(false);
      setColumnToDelete('');
      if (onColumnsChange) {
        const updatedColumns = localColumns.filter(col => col.name !== columnToDelete);
        setLocalColumns(updatedColumns);
        onColumnsChange(updatedColumns);
      }
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.response?.data?.detail || 'Failed to delete column',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleMergeColumns = () => {
    if (selectedColumns.length < 2) {
      toast({
        title: 'Selection Required',
        description: 'Select at least 2 columns to merge',
        variant: 'destructive',
      });
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
    toast({ title: 'Success', description: message });
    setSelectedColumns([]);
    setDialogState({ open: false, mode: 'add' });
    setMergeDialogOpen(false);
  };

  const handleDialogError = (message: string) => {
    toast({ title: 'Error', description: message, variant: 'destructive' });
  };

  const handleBackup = async () => {
    setLoading(true);
    try {
      const result = await schemaAPI.backupSchema(sessionId);
      toast({
        title: 'Backup Created',
        description: `Schema backup created: ${result.backup_id}`,
      });
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.response?.data?.detail || 'Failed to create backup',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleValidate = async () => {
    setLoading(true);
    try {
      const result = await schemaAPI.validateSchema(sessionId);
      setValidationResult(result);
      toast({ title: 'Validation Complete', description: 'Schema validation completed' });
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.response?.data?.detail || 'Failed to validate schema',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleReprocessClick = () => {
    setReprocessDialogOpen(true);
  };

  const confirmReprocess = async () => {
    setReprocessDialogOpen(false);
    setLoading(true);
    try {
      await schemaAPI.reprocessDocuments(sessionId, { incremental: true });
      toast({ title: 'Reprocessing Started', description: 'Document reprocessing started' });
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.response?.data?.detail || 'Failed to start reprocessing',
        variant: 'destructive',
      });
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

  const getValidationSeverity = (): 'success' | 'warning' | 'destructive' => {
    if (!validationResult) return 'success';
    if (validationResult.errors && validationResult.errors.length > 0) return 'destructive';
    if (validationResult.warnings && validationResult.warnings.length > 0) return 'warning';
    return 'success';
  };

  const getValidationIcon = () => {
    const severity = getValidationSeverity();
    switch (severity) {
      case 'destructive': return <AlertCircle className="h-4 w-4" />;
      case 'warning': return <AlertTriangle className="h-4 w-4" />;
      default: return <CheckCircle2 className="h-4 w-4" />;
    }
  };

  return (
    <div className="space-y-6">
      {/* Query and Schema Source Display */}
      {query && (
        <Card>
          <CardContent className="pt-6">
            <div className="flex justify-between items-start">
              <div className="flex-1">
                <h3 className="font-semibold flex items-center gap-2 mb-2">
                  <Database className="h-5 w-5" />
                  Research Query
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6"
                        onClick={handleCopyQuery}
                        aria-label="Copy query to clipboard"
                      >
                        {copiedQuery ? (
                          <Check className="h-3 w-3 text-green-500" />
                        ) : (
                          <Copy className="h-3 w-3" />
                        )}
                      </Button>
                    </TooltipTrigger>
                    <TooltipContent>
                      {copiedQuery ? 'Copied!' : 'Copy query'}
                    </TooltipContent>
                  </Tooltip>
                </h3>
                <p className="text-muted-foreground">{query}</p>
              </div>
              <Badge variant={sessionType === 'load' ? 'secondary' : 'default'}>
                {sessionType === 'load' ? 'Loaded Schema' : 'Generated Schema'}
              </Badge>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Schema Metadata for Load Sessions */}
      {sessionType === 'load' && displayColumns.length > 0 && (
        <Card className="bg-muted/50">
          <CardContent className="pt-6">
            <h3 className="font-semibold flex items-center gap-2 mb-3">
              <ShieldCheck className="h-5 w-5 text-green-500" />
              Schema Information
            </h3>
            <div className="flex flex-wrap gap-2 mb-3">
              <Badge>{displayColumns.length} columns defined</Badge>
              <Badge variant={displayColumns.every(col => col.definition) ? 'success' : 'warning'}>
                {displayColumns.filter(col => col.definition).length} with definitions
              </Badge>
              <Badge variant={displayColumns.every(col => col.rationale) ? 'success' : 'warning'}>
                {displayColumns.filter(col => col.rationale).length} with rationales
              </Badge>
            </div>

            {llmConfig && (
              <div className="pt-3 border-t">
                <LLMConfigDisplay
                  config={llmConfig}
                  title="Schema Creation Model"
                  variant="inline"
                  showDetails={true}
                />
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Reprocessing Progress */}
      {reprocessingStatus && (
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 mb-3">
              <Loader2 className="h-5 w-5 animate-spin" />
              <h3 className="font-semibold">Reprocessing Documents</h3>
            </div>
            <p className="text-sm text-muted-foreground mb-2">
              {reprocessingStatus.current_step}
            </p>
            <Progress value={reprocessingStatus.progress * 100} className="mb-2" />
            <p className="text-xs text-muted-foreground">
              {reprocessingStatus.processed_documents} of {reprocessingStatus.total_documents} documents processed
              {reprocessingStatus.affected_columns && reprocessingStatus.affected_columns.length > 0 &&
                ` (${reprocessingStatus.affected_columns.join(', ')})`
              }
            </p>
          </CardContent>
        </Card>
      )}

      {/* Schema Overview */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex justify-between items-center mb-4">
            <div className="flex items-center gap-3">
              <h3 className="font-semibold">
                Schema Overview ({displayColumns.length} columns)
              </h3>

              {/* Validation Status Badge */}
              {validationResult && (
                <Tooltip>
                  <TooltipTrigger>
                    <Badge variant={getValidationSeverity()} className="gap-1">
                      {getValidationIcon()}
                      {(validationResult.errors?.length || 0) + (validationResult.warnings?.length || 0)}
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent>
                    {validationResult.errors?.length || 0} errors, {validationResult.warnings?.length || 0} warnings
                  </TooltipContent>
                </Tooltip>
              )}
            </div>

            {!readonly && (
              <div className="flex items-center gap-2">
                {/* Selection Controls */}
                {selectedColumns.length > 0 && (
                  <div className="flex gap-2 mr-2">
                    <Button variant="ghost" size="sm" onClick={handleClearSelection}>
                      Clear ({selectedColumns.length})
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      disabled={selectedColumns.length < 2}
                      onClick={handleMergeColumns}
                    >
                      <GitMerge className="h-4 w-4 mr-1" />
                      Merge
                    </Button>
                  </div>
                )}

                <Button
                  variant="ghost"
                  size="sm"
                  onClick={selectedColumns.length === 0 ? handleSelectAll : handleClearSelection}
                >
                  {selectedColumns.length === 0 ? 'Select All' : 'Clear All'}
                </Button>

                <Button variant="outline" size="sm" onClick={handleAddColumn}>
                  <Plus className="h-4 w-4 mr-1" />
                  Add Column
                </Button>

                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button variant="ghost" size="icon" disabled={loading}>
                      <MoreVertical className="h-4 w-4" />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuItem onClick={handleValidate} disabled={loading}>
                      <ShieldCheck className="h-4 w-4 mr-2" />
                      Validate Schema
                    </DropdownMenuItem>
                    <DropdownMenuItem onClick={handleBackup} disabled={loading}>
                      <Save className="h-4 w-4 mr-2" />
                      Create Backup
                    </DropdownMenuItem>
                    <DropdownMenuSeparator />
                    <DropdownMenuItem onClick={handleReprocessClick} disabled={loading || Boolean(reprocessingStatus)}>
                      <RefreshCw className="h-4 w-4 mr-2" />
                      Reprocess All
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </div>
            )}
          </div>

          {/* Column Grid */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {displayColumns.map((column) => (
              <Card
                key={column.name}
                className={cn(
                  "relative",
                  selectedColumns.includes(column.name) && "ring-2 ring-primary"
                )}
              >
                <CardContent className="pt-4">
                  <div className="flex justify-between items-start mb-2">
                    <div className="flex items-center gap-2">
                      {!readonly && (
                        <Checkbox
                          checked={selectedColumns.includes(column.name)}
                          onCheckedChange={(checked) =>
                            handleColumnSelection(column.name, checked as boolean)
                          }
                        />
                      )}
                      <h4 className="font-semibold text-sm">
                        {formatColumnName(column.name)}
                      </h4>
                    </div>

                    {!readonly && (
                      <div className="flex gap-1">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-8 w-8"
                              onClick={() => handleEditColumn(column)}
                              aria-label="Edit column"
                            >
                              <Pencil className="h-4 w-4" />
                            </Button>
                          </TooltipTrigger>
                          <TooltipContent>Edit column definition</TooltipContent>
                        </Tooltip>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-8 w-8 text-destructive hover:text-destructive"
                              onClick={() => handleDeleteColumn(column.name)}
                              aria-label="Delete column"
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </TooltipTrigger>
                          <TooltipContent>Delete column</TooltipContent>
                        </Tooltip>
                      </div>
                    )}
                  </div>

                  {column.data_type && (
                    <Badge className="mb-2">{column.data_type}</Badge>
                  )}

                  {column.definition && (
                    <div className="mb-3 p-3 bg-muted rounded-md">
                      <p className="text-xs font-semibold text-primary mb-1">Definition</p>
                      <p className="text-sm">{column.definition}</p>
                    </div>
                  )}

                  {column.rationale && (
                    <div className="mb-3 p-3 bg-blue-50 dark:bg-blue-950 rounded-md border border-blue-200 dark:border-blue-800">
                      <p className="text-xs font-semibold text-blue-700 dark:text-blue-300 mb-1">Rationale</p>
                      <p className="text-sm">{column.rationale}</p>
                    </div>
                  )}

                  {/* Allowed Values (Closed Set) or Free-form indicator */}
                  {column.allowed_values && column.allowed_values.length > 0 ? (
                    <div className="mb-3 p-3 bg-purple-50 dark:bg-purple-950 rounded-md border border-purple-200 dark:border-purple-800">
                      <p className="text-xs font-semibold text-purple-700 dark:text-purple-300 mb-2">
                        {/* Detect constraint type for better labeling */}
                        {column.allowed_values.length === 1 && column.allowed_values[0].toLowerCase() === 'number'
                          ? 'Numeric Constraint'
                          : column.allowed_values.length === 1 && /^-?\d+(\.\d+)?--?\d+(\.\d+)?$/.test(column.allowed_values[0])
                            ? 'Range Constraint'
                            : 'Allowed Values'}
                      </p>
                      <div className="flex flex-wrap gap-1">
                        {column.allowed_values.length === 1 && column.allowed_values[0].toLowerCase() === 'number' ? (
                          <Badge variant="outline" className="text-xs bg-blue-100 dark:bg-blue-900 border-blue-300 dark:border-blue-700 text-blue-700 dark:text-blue-300">
                            Any Number (int/float)
                          </Badge>
                        ) : column.allowed_values.length === 1 && /^(-?\d+(\.\d+)?)-(-?\d+(\.\d+)?)$/.test(column.allowed_values[0]) ? (
                          <Badge variant="outline" className="text-xs bg-blue-100 dark:bg-blue-900 border-blue-300 dark:border-blue-700 text-blue-700 dark:text-blue-300">
                            Range: {column.allowed_values[0]}
                          </Badge>
                        ) : (
                          column.allowed_values.map((value, idx) => (
                            <Badge key={idx} variant="outline" className="text-xs bg-purple-100 dark:bg-purple-900 border-purple-300 dark:border-purple-700">
                              {value}
                            </Badge>
                          ))
                        )}
                      </div>
                    </div>
                  ) : (
                    <div className="mb-3 p-2 bg-gray-50 dark:bg-gray-900 rounded-md border border-gray-200 dark:border-gray-700">
                      <p className="text-xs text-gray-500 dark:text-gray-400 italic">
                        Free-form (any value accepted)
                      </p>
                    </div>
                  )}

                  {/* Pending Values for Schema Evolution */}
                  {column.pending_values && column.pending_values.length > 0 && !readonly && (
                    <div className="mb-3 p-3 bg-amber-50 dark:bg-amber-950 rounded-md border border-amber-200 dark:border-amber-800">
                      <p className="text-xs font-semibold text-amber-700 dark:text-amber-300 mb-2 flex items-center gap-1">
                        <AlertTriangle className="h-3 w-3" />
                        New Values Detected ({column.pending_values.length})
                      </p>
                      <div className="space-y-1">
                        {column.pending_values.slice(0, 3).map((pv, idx) => (
                          <div key={idx} className="flex items-center justify-between text-xs">
                            <span className="font-medium">{pv.value}</span>
                            <span className="text-amber-600 dark:text-amber-400">
                              {pv.document_count} doc{pv.document_count > 1 ? 's' : ''}
                            </span>
                          </div>
                        ))}
                        {column.pending_values.length > 3 && (
                          <p className="text-xs text-amber-600 dark:text-amber-400 italic">
                            +{column.pending_values.length - 3} more...
                          </p>
                        )}
                      </div>
                    </div>
                  )}

                  {/* Missing metadata warning for loaded schemas */}
                  {sessionType === 'load' && (!column.definition || !column.rationale) && (
                    <div className="mb-2 p-2 bg-yellow-50 dark:bg-yellow-950 rounded-md border border-yellow-200 dark:border-yellow-800">
                      <p className="text-xs text-yellow-700 dark:text-yellow-300 flex items-center gap-1">
                        <AlertTriangle className="h-3 w-3" />
                        {!column.definition && !column.rationale
                          ? 'Missing definition and rationale'
                          : !column.definition
                            ? 'Missing definition'
                            : 'Missing rationale'}
                      </p>
                    </div>
                  )}

                  {(column.non_null_count !== undefined || column.unique_count !== undefined) && (
                    <div className="flex gap-2 mt-2">
                      {column.non_null_count !== undefined && (
                        <Badge variant="outline">{column.non_null_count} non-null</Badge>
                      )}
                      {column.unique_count !== undefined && (
                        <Badge variant="outline">{column.unique_count} unique</Badge>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Validation Results */}
      {validationResult && ((validationResult.errors && validationResult.errors.length > 0) || (validationResult.warnings && validationResult.warnings.length > 0)) && (
        <Card>
          <CardContent className="pt-6 space-y-4">
            <h3 className="font-semibold">Schema Validation</h3>

            {validationResult.errors && validationResult.errors.length > 0 && (
              <Alert variant="destructive">
                <AlertDescription>
                  <p className="font-semibold mb-1">Errors:</p>
                  {validationResult.errors.map((error, index) => (
                    <p key={index} className="text-sm">• {error}</p>
                  ))}
                </AlertDescription>
              </Alert>
            )}

            {validationResult.warnings && validationResult.warnings.length > 0 && (
              <Alert variant="warning">
                <AlertDescription>
                  <p className="font-semibold mb-1">Warnings:</p>
                  {validationResult.warnings.map((warning, index) => (
                    <p key={index} className="text-sm">• {warning}</p>
                  ))}
                </AlertDescription>
              </Alert>
            )}

            {validationResult.suggestions && validationResult.suggestions.length > 0 && (
              <Alert variant="info">
                <AlertDescription>
                  <p className="font-semibold mb-1">Suggestions:</p>
                  {validationResult.suggestions.map((suggestion, index) => (
                    <p key={index} className="text-sm">• {suggestion}</p>
                  ))}
                </AlertDescription>
              </Alert>
            )}
          </CardContent>
        </Card>
      )}

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
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Column</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete the column "{columnToDelete}"?
              This action will remove the column from the schema and delete all associated data.
              This cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={confirmDelete}
              disabled={loading}
            >
              {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              {loading ? 'Deleting...' : 'Delete'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Reprocess Confirmation Dialog */}
      <Dialog open={reprocessDialogOpen} onOpenChange={setReprocessDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <RefreshCw className="h-5 w-5" />
              Reprocess All Documents
            </DialogTitle>
            <DialogDescription>
              This will re-extract values for all columns from all documents.
              This operation may take a significant amount of time depending on the number of documents.
              Existing extracted values will be replaced.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setReprocessDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={confirmReprocess} disabled={loading}>
              {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              {loading ? 'Starting...' : 'Start Reprocessing'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {readonly && (
        <Alert variant="info">
          <Info className="h-4 w-4" />
          <AlertDescription>
            <strong>Read-only mode:</strong> Schema editing features are disabled.
            To enable editing, ensure you have proper permissions and the session supports schema modifications.
          </AlertDescription>
        </Alert>
      )}
    </div>
  );
};

export default SchemaViewer;
