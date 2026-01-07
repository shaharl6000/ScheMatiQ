import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
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
  LayoutGrid,
  LayoutList,
  Search,
  ArrowUpDown,
  X,
  FolderInput,
  FolderPlus,
  Download,
  Upload,
} from 'lucide-react';

import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Checkbox } from '@/components/ui/checkbox';
import { Progress } from '@/components/ui/progress';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
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
  WebSocketMessageExtended,
  SchemaChangeStatus,
  ColumnCluster
} from '../../types';
import { formatColumnName } from '../../utils/formatting';
import { copyToClipboard } from '../../utils/clipboard';
import { schemaAPI } from '../../services/api';
import ColumnDialog from '../SchemaEditor/ColumnDialog';
import MergeDialog from '../SchemaEditor/MergeDialog';
import ReextractionDialog from '../SchemaEditor/ReextractionDialog';
import ContinueDiscoveryDialog from '../SchemaEditor/ContinueDiscoveryDialog';
import ContinueDiscoveryMonitor from '../SchemaEditor/ContinueDiscoveryMonitor';
import LLMConfigDisplay from '../LLMConfigDisplay';
import SchemaColumnDetailPanel from './SchemaColumnDetailPanel';
import { clusterColumns, assignColumnToCluster, createUserCluster } from './clustering';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion';

interface SchemaViewerProps {
  columns: ColumnInfo[];
  query?: string;
  sessionId: string;
  sessionType?: 'load' | 'qbsd';
  readonly?: boolean;
  processingColumns?: Set<string>;
  onColumnsChange?: (columns: ColumnInfo[]) => void;
  onReextractionStarted?: (columns: string[]) => void;
  websocketManager?: any;
  llmConfig?: any;
}

const SchemaViewer: React.FC<SchemaViewerProps> = ({
  columns,
  query,
  sessionId,
  sessionType = 'qbsd',
  readonly = false,
  processingColumns,
  onColumnsChange,
  onReextractionStarted,
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
  const [schemaChanges, setSchemaChanges] = useState<SchemaChangeStatus | null>(null);
  const [reextractionDialogOpen, setReextractionDialogOpen] = useState(false);
  const [continueDiscoveryDialogOpen, setContinueDiscoveryDialogOpen] = useState(false);
  const [continueDiscoveryMonitorOpen, setContinueDiscoveryMonitorOpen] = useState(false);
  const [continueDiscoveryOperationId, setContinueDiscoveryOperationId] = useState<string | null>(null);

  // View mode, search, and sort state - always default to 'detailed'
  const [viewMode, setViewMode] = useState<'compact' | 'detailed'>('detailed');
  const [searchQuery, setSearchQuery] = useState('');
  const [sortBy, setSortBy] = useState<'name' | 'type' | 'completeness' | 'modified'>('name');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('asc');
  const [selectedDetailColumn, setSelectedDetailColumn] = useState<ColumnInfo | null>(null);

  // Clustering state
  const [clusters, setClusters] = useState<ColumnCluster[]>([]);
  const clusteringEnabled = true; // TODO: Add toggle UI for this
  const [editingClusterId, setEditingClusterId] = useState<string | null>(null);
  const [editingClusterName, setEditingClusterName] = useState('');
  const [showNewClusterDialog, setShowNewClusterDialog] = useState(false);
  const [newClusterName, setNewClusterName] = useState('');
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [clustersInitialized, setClustersInitialized] = useState(false);

  // Load clusters from localStorage on mount
  useEffect(() => {
    if (sessionId) {
      const savedClusters = localStorage.getItem(`schema-clusters-${sessionId}`);
      if (savedClusters) {
        try {
          const parsed = JSON.parse(savedClusters);
          if (Array.isArray(parsed) && parsed.length > 0) {
            setClusters(parsed);
          }
        } catch (e) {
          console.error('Failed to parse saved clusters:', e);
        }
      }
      setClustersInitialized(true);
    }
  }, [sessionId]);

  // Save user-modified clusters to localStorage
  useEffect(() => {
    if (sessionId && clustersInitialized && clusters.length > 0) {
      // Only save clusters that have user modifications
      const userModifiedClusters = clusters.filter(c =>
        c.id.startsWith('user_') || // Explicitly user-created or renamed
        c.name !== c.id // Name was changed from auto-generated
      );
      if (userModifiedClusters.length > 0) {
        localStorage.setItem(`schema-clusters-${sessionId}`, JSON.stringify(clusters));
      }
    }
  }, [clusters, sessionId, clustersInitialized]);

  // Export schema with clusters as JSON
  const handleExportSchemaWithClusters = useCallback(() => {
    const schemaExport = {
      version: '1.0',
      exportDate: new Date().toISOString(),
      sessionId,
      query: query || '',
      schema: localColumns.map(col => ({
        name: col.name,
        definition: col.definition || '',
        rationale: col.rationale || '',
        data_type: col.data_type,
        allowed_values: col.allowed_values || [],
        source_document: col.source_document,
        discovery_iteration: col.discovery_iteration
      })),
      clusters: clusters.map(c => ({
        id: c.id,
        name: c.name,
        description: c.description,
        color: c.color,
        collapsed: c.collapsed,
        column_names: c.column_names
      }))
    };

    const blob = new Blob([JSON.stringify(schemaExport, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `schema_with_clusters_${sessionId.slice(0, 8)}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    toast({ title: 'Schema exported', description: 'Schema with clusters saved to JSON file' });
  }, [localColumns, clusters, sessionId, query, toast]);

  // Import clusters from JSON file
  const handleImportClusters = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string;
        const data = JSON.parse(content);

        if (data.clusters && Array.isArray(data.clusters)) {
          // Validate cluster structure
          const validClusters = data.clusters.filter((c: any) =>
            c.id && c.name && Array.isArray(c.column_names)
          ).map((c: any) => ({
            id: c.id.startsWith('user_') ? c.id : `user_imported_${c.id}`,
            name: c.name,
            description: c.description || '',
            color: c.color || '#6B7280',
            collapsed: c.collapsed || false,
            column_names: c.column_names
          }));

          if (validClusters.length > 0) {
            // Merge imported clusters with current column names
            const currentColumnNames = new Set(localColumns.map(c => c.name));
            const filteredClusters = validClusters.map((c: ColumnCluster) => ({
              ...c,
              column_names: c.column_names.filter((name: string) => currentColumnNames.has(name))
            })).filter((c: ColumnCluster) => c.column_names.length > 0);

            if (filteredClusters.length > 0) {
              setClusters(filteredClusters);
              toast({
                title: 'Clusters imported',
                description: `Imported ${filteredClusters.length} cluster(s)`
              });
            } else {
              toast({
                title: 'Import warning',
                description: 'No matching columns found for imported clusters',
                variant: 'destructive'
              });
            }
          } else {
            toast({
              title: 'Import failed',
              description: 'No valid clusters found in file',
              variant: 'destructive'
            });
          }
        } else {
          toast({
            title: 'Import failed',
            description: 'Invalid file format - expected schema export with clusters',
            variant: 'destructive'
          });
        }
      } catch (error) {
        toast({
          title: 'Import failed',
          description: 'Failed to parse JSON file',
          variant: 'destructive'
        });
      }

      // Reset file input
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    };
    reader.readAsText(file);
  }, [localColumns, toast]);

  // Cluster management handlers
  const handleRenameCluster = (clusterId: string, newName: string) => {
    if (!newName.trim()) return;
    setClusters(prev => prev.map(c =>
      c.id === clusterId
        ? { ...c, id: c.id.startsWith('algo_') ? `user_${c.id}` : c.id, name: newName.trim() }
        : c
    ));
    setEditingClusterId(null);
    setEditingClusterName('');
    toast({ title: 'Cluster renamed', description: `Cluster renamed to "${newName.trim()}"` });
  };

  const handleDeleteCluster = (clusterId: string) => {
    const cluster = clusters.find(c => c.id === clusterId);
    if (!cluster) return;

    // Move columns to "Other" or create it
    const otherCluster = clusters.find(c => c.name === 'Other' && c.id !== clusterId);

    setClusters(prev => {
      const remaining = prev.filter(c => c.id !== clusterId);
      if (otherCluster) {
        // Add columns to existing "Other" cluster
        return remaining.map(c =>
          c.id === otherCluster.id
            ? { ...c, column_names: [...c.column_names, ...cluster.column_names] }
            : c
        );
      } else {
        // Create new "Other" cluster
        return [...remaining, {
          id: `user_other_${Date.now()}`,
          name: 'Other',
          color: '#6B7280',
          column_names: cluster.column_names
        }];
      }
    });
    toast({ title: 'Cluster deleted', description: `Columns moved to "Other"` });
  };

  const handleMoveColumn = (columnName: string, targetClusterId: string | 'new') => {
    if (targetClusterId === 'new') {
      setShowNewClusterDialog(true);
      // Store the column to move after dialog closes
      setNewClusterName('');
      (window as any).__pendingMoveColumn = columnName;
      return;
    }

    const updated = assignColumnToCluster(columnName, targetClusterId, clusters);
    setClusters(updated);
    toast({ title: 'Column moved', description: `"${formatColumnName(columnName)}" moved to new cluster` });
  };

  const handleCreateCluster = () => {
    if (!newClusterName.trim()) return;

    const pendingColumn = (window as any).__pendingMoveColumn;
    const newCluster = createUserCluster(
      newClusterName.trim(),
      pendingColumn ? [pendingColumn] : []
    );

    if (pendingColumn) {
      // Remove from old cluster and add new one
      const updated = clusters.map(c => ({
        ...c,
        column_names: c.column_names.filter(name => name !== pendingColumn)
      })).filter(c => c.column_names.length > 0);
      setClusters([...updated, newCluster]);
      delete (window as any).__pendingMoveColumn;
    } else {
      setClusters([...clusters, newCluster]);
    }

    setShowNewClusterDialog(false);
    setNewClusterName('');
    toast({ title: 'Cluster created', description: `New cluster "${newClusterName.trim()}" created` });
  };

  // Helper function to extract error message from API errors (handles Pydantic validation errors)
  const extractErrorMessage = (error: any, fallback: string): string => {
    const detail = error.response?.data?.detail;
    if (typeof detail === 'string') {
      return detail;
    } else if (Array.isArray(detail) && detail.length > 0) {
      // Pydantic validation error format: [{type, loc, msg, input}, ...]
      return detail.map((err: any) => err.msg || String(err)).join('; ');
    } else if (detail && typeof detail === 'object') {
      return detail.msg || JSON.stringify(detail);
    }
    return fallback;
  };

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

  const loadSchemaChangeStatus = useCallback(async () => {
    try {
      const changes = await schemaAPI.getSchemaChangeStatus(sessionId);
      setSchemaChanges(changes);
    } catch (error) {
      console.error('Failed to load schema change status:', error);
    }
  }, [sessionId]);

  // Update local columns when props change
  useEffect(() => {
    setLocalColumns(columns || []);
  }, [columns]);

  // Run clustering when columns change (only after localStorage is checked)
  useEffect(() => {
    if (clusteringEnabled && localColumns.length > 0 && clustersInitialized) {
      const result = clusterColumns(localColumns, clusters, {
        similarityThreshold: 0.5,
        minClusterSize: 1,
        maxClusters: 10,
        respectUserClusters: true
      });
      setClusters(result.clusters);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [localColumns, clusteringEnabled, clustersInitialized]); // Don't include clusters to avoid infinite loop

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

  // Load initial validation, reprocessing, and schema change status
  useEffect(() => {
    if (!readonly && sessionId) {
      loadValidationResult();
      loadReprocessingStatus();
      loadSchemaChangeStatus();
    }
  }, [sessionId, readonly, loadValidationResult, loadReprocessingStatus, loadSchemaChangeStatus]);

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
        description: extractErrorMessage(error, 'Failed to delete column'),
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

  const handleDialogSuccess = (message: string, updatedColumns?: ColumnInfo[], selectedClusterId?: string | null) => {
    toast({ title: 'Success', description: message });
    setSelectedColumns([]);
    setDialogState({ open: false, mode: 'add' });
    setMergeDialogOpen(false);

    // Update columns if provided in the response
    if (updatedColumns && updatedColumns.length > 0) {
      setLocalColumns(updatedColumns);
      if (onColumnsChange) {
        onColumnsChange(updatedColumns);
      }

      // If a cluster was selected for the new column, assign it
      if (selectedClusterId && dialogState.mode === 'add') {
        // Find the newly added column (last one added)
        const newColumnName = updatedColumns.find(
          col => !localColumns.some(lc => lc.name === col.name)
        )?.name;

        if (newColumnName) {
          const updatedClusters = assignColumnToCluster(newColumnName, selectedClusterId, clusters);
          setClusters(updatedClusters);
        }
      }
    }

    // Reload schema change status after any schema operation
    loadSchemaChangeStatus();
  };

  const handleDialogError = (message: string) => {
    toast({ title: 'Error', description: message, variant: 'destructive' });
  };

  const handleReextractionSuccess = (message: string, refreshData?: boolean) => {
    toast({ title: 'Success', description: message });
    setReextractionDialogOpen(false);

    if (refreshData && onColumnsChange) {
      // Trigger parent to invalidate queries (refreshes data table)
      onColumnsChange(localColumns);
    }
    // Reload schema change status - should now show no pending changes
    loadSchemaChangeStatus();
  };

  const handleReextractionError = (message: string) => {
    toast({ title: 'Re-extraction Error', description: message, variant: 'destructive' });
  };

  const handleContinueDiscoverySuccess = (message: string, newColumns: ColumnInfo[]) => {
    toast({ title: 'Success', description: message });
    setContinueDiscoveryDialogOpen(false);

    if (newColumns.length > 0 && onColumnsChange) {
      // Add new columns to local state and notify parent
      const updatedColumns = [...localColumns, ...newColumns];
      setLocalColumns(updatedColumns);
      onColumnsChange(updatedColumns);
    }
    // Reload schema change status
    loadSchemaChangeStatus();
  };

  const handleContinueDiscoveryError = (message: string) => {
    toast({ title: 'Continue Discovery Error', description: message, variant: 'destructive' });
  };

  const handleContinueDiscoveryStarted = (operationId: string) => {
    setContinueDiscoveryOperationId(operationId);
    setContinueDiscoveryMonitorOpen(true);
  };

  const handleMonitorComplete = (newColumns: ColumnInfo[]) => {
    console.log('handleMonitorComplete: received newColumns:', newColumns);
    console.log('handleMonitorComplete: current localColumns count:', localColumns.length);
    setContinueDiscoveryMonitorOpen(false);
    setContinueDiscoveryOperationId(null);
    if (newColumns.length > 0 && onColumnsChange) {
      const updatedColumns = [...localColumns, ...newColumns];
      console.log('handleMonitorComplete: updating to', updatedColumns.length, 'columns');
      setLocalColumns(updatedColumns);
      onColumnsChange(updatedColumns);
      toast({ title: 'Success', description: `Added ${newColumns.length} new columns with extracted values.` });
    } else {
      console.log('handleMonitorComplete: no update - newColumns.length:', newColumns.length, 'onColumnsChange:', !!onColumnsChange);
    }
    loadSchemaChangeStatus();
  };

  const handleMonitorCancel = () => {
    setContinueDiscoveryMonitorOpen(false);
    setContinueDiscoveryOperationId(null);
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
        description: extractErrorMessage(error, 'Failed to create backup'),
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
        description: extractErrorMessage(error, 'Failed to validate schema'),
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
        description: extractErrorMessage(error, 'Failed to start reprocessing'),
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  // Persist view mode to localStorage
  useEffect(() => {
    localStorage.setItem('schemaViewer.viewMode', viewMode);
  }, [viewMode]);

  // Filter out excerpt columns for display
  const displayColumns = (localColumns || []).filter(column => {
    return column &&
      column.name &&
      !column.name.toLowerCase().includes('excerpt') &&
      !column.name.toLowerCase().endsWith('_excerpt');
  });

  // Filter columns by search query
  const filteredColumns = useMemo(() => {
    if (!searchQuery.trim()) return displayColumns;
    const query = searchQuery.toLowerCase();
    return displayColumns.filter(col =>
      col.name.toLowerCase().includes(query) ||
      col.definition?.toLowerCase().includes(query) ||
      col.rationale?.toLowerCase().includes(query)
    );
  }, [displayColumns, searchQuery]);

  // Sort columns
  const sortedColumns = useMemo(() => {
    const cols = [...filteredColumns];
    cols.sort((a, b) => {
      let comparison = 0;
      switch (sortBy) {
        case 'name':
          comparison = a.name.localeCompare(b.name);
          break;
        case 'type':
          comparison = (a.data_type || '').localeCompare(b.data_type || '');
          break;
        case 'completeness':
          const aComplete = (a.non_null_count || 0);
          const bComplete = (b.non_null_count || 0);
          comparison = aComplete - bComplete;
          break;
        case 'modified':
          const aModified = schemaChanges?.changed_columns?.includes(a.name) ? 1 : 0;
          const bModified = schemaChanges?.changed_columns?.includes(b.name) ? 1 : 0;
          comparison = bModified - aModified;
          break;
      }
      return sortOrder === 'asc' ? comparison : -comparison;
    });
    return cols;
  }, [filteredColumns, sortBy, sortOrder, schemaChanges]);

  // Group columns by cluster
  const groupedColumns = useMemo(() => {
    if (!clusteringEnabled || clusters.length === 0) {
      return [{ cluster: null, columns: sortedColumns }];
    }

    const groups: Array<{ cluster: ColumnCluster | null; columns: ColumnInfo[] }> = [];
    const assignedColumns = new Set<string>();

    // Add columns in cluster order
    clusters.forEach(cluster => {
      const clusterColumns = sortedColumns.filter(col =>
        cluster.column_names.includes(col.name)
      );
      if (clusterColumns.length > 0) {
        groups.push({ cluster, columns: clusterColumns });
        clusterColumns.forEach(col => assignedColumns.add(col.name));
      }
    });

    // Add any unclustered columns at the end
    const unclustered = sortedColumns.filter(col => !assignedColumns.has(col.name));
    if (unclustered.length > 0) {
      groups.push({
        cluster: {
          id: 'unclustered',
          name: 'Uncategorized',
          color: '#6B7280',
          column_names: unclustered.map(c => c.name)
        },
        columns: unclustered
      });
    }

    return groups;
  }, [sortedColumns, clusters, clusteringEnabled]);

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

            {/* Toolbar: Search, Sort, View Toggle */}
            <div className="flex items-center gap-2">
              {/* Search */}
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Search columns..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-8 h-8 w-40 text-sm"
                />
                {searchQuery && (
                  <Button
                    variant="ghost"
                    size="icon"
                    className="absolute right-1 top-1/2 -translate-y-1/2 h-6 w-6"
                    onClick={() => setSearchQuery('')}
                  >
                    <X className="h-3 w-3" />
                  </Button>
                )}
              </div>

              {/* Sort */}
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="outline" size="sm" className="h-8 gap-1">
                    <ArrowUpDown className="h-3.5 w-3.5" />
                    Sort
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem onClick={() => setSortBy('name')}>
                    {sortBy === 'name' && <Check className="h-4 w-4 mr-2" />}
                    {sortBy !== 'name' && <span className="w-4 mr-2" />}
                    Name
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => setSortBy('type')}>
                    {sortBy === 'type' && <Check className="h-4 w-4 mr-2" />}
                    {sortBy !== 'type' && <span className="w-4 mr-2" />}
                    Data Type
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => setSortBy('completeness')}>
                    {sortBy === 'completeness' && <Check className="h-4 w-4 mr-2" />}
                    {sortBy !== 'completeness' && <span className="w-4 mr-2" />}
                    Completeness
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => setSortBy('modified')}>
                    {sortBy === 'modified' && <Check className="h-4 w-4 mr-2" />}
                    {sortBy !== 'modified' && <span className="w-4 mr-2" />}
                    Modified First
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={() => setSortOrder(o => o === 'asc' ? 'desc' : 'asc')}>
                    {sortOrder === 'asc' ? '↑ Ascending' : '↓ Descending'}
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>

              {/* View Toggle */}
              <div className="flex items-center border rounded-lg p-0.5 bg-muted/50">
                <Button
                  variant={viewMode === 'compact' ? 'default' : 'ghost'}
                  size="sm"
                  className="h-7 px-2 text-xs"
                  onClick={() => setViewMode('compact')}
                >
                  <LayoutGrid className="h-3.5 w-3.5 mr-1" />
                  Compact
                </Button>
                <Button
                  variant={viewMode === 'detailed' ? 'default' : 'ghost'}
                  size="sm"
                  className="h-7 px-2 text-xs"
                  onClick={() => setViewMode('detailed')}
                >
                  <LayoutList className="h-3.5 w-3.5 mr-1" />
                  Detailed
                </Button>
              </div>
            </div>
          </div>

          {/* Action buttons row */}
          <div className="flex justify-between items-center mb-4">
            <div className="flex items-center gap-2">
              {/* Search results count */}
              {searchQuery && (
                <span className="text-sm text-muted-foreground">
                  Showing {sortedColumns.length} of {displayColumns.length}
                </span>
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

                {/* Re-extract button - shown when schema has real changes (not missing baseline) */}
                {schemaChanges?.has_changes && !schemaChanges?.missing_baseline && (
                  <Button
                    variant="default"
                    size="sm"
                    onClick={() => setReextractionDialogOpen(true)}
                  >
                    <RefreshCw className="h-4 w-4 mr-1" />
                    Re-extract ({(schemaChanges.changed_columns?.length || 0) + (schemaChanges.new_columns?.length || 0)})
                  </Button>
                )}

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
                    {schemaChanges?.missing_baseline && (
                      <DropdownMenuItem
                        onClick={async () => {
                          try {
                            await schemaAPI.captureBaseline(sessionId);
                            toast({ title: 'Success', description: 'Schema baseline captured. Now edit columns to enable re-extraction.' });
                            loadSchemaChangeStatus();
                          } catch (e: any) {
                            toast({ title: 'Error', description: e.response?.data?.detail || 'Failed to capture baseline', variant: 'destructive' });
                          }
                        }}
                        disabled={loading}
                      >
                        <RefreshCw className="h-4 w-4 mr-2" />
                        Capture Schema Baseline
                      </DropdownMenuItem>
                    )}
                    <DropdownMenuSeparator />
                    <DropdownMenuItem onClick={handleReprocessClick} disabled={loading || Boolean(reprocessingStatus)}>
                      <RefreshCw className="h-4 w-4 mr-2" />
                      Reprocess All
                    </DropdownMenuItem>
                    <DropdownMenuSeparator />
                    <DropdownMenuItem onClick={() => setContinueDiscoveryDialogOpen(true)} disabled={loading}>
                      <Plus className="h-4 w-4 mr-2" />
                      Continue Schema Discovery
                    </DropdownMenuItem>
                    <DropdownMenuSeparator />
                    <DropdownMenuItem onClick={handleExportSchemaWithClusters}>
                      <Download className="h-4 w-4 mr-2" />
                      Export Schema with Clusters
                    </DropdownMenuItem>
                    <DropdownMenuItem onClick={() => fileInputRef.current?.click()}>
                      <Upload className="h-4 w-4 mr-2" />
                      Import Clusters
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
                {/* Hidden file input for cluster import */}
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".json"
                  onChange={handleImportClusters}
                  className="hidden"
                />
              </div>
            )}
          </div>

          {/* Column Grid with Sidebar for Detailed View */}
          <div className={cn(
            viewMode === 'detailed' ? "flex gap-4" : ""
          )}>
            {/* Sidebar - Column List (Detailed View Only) */}
            {viewMode === 'detailed' && (
              <div className="w-56 flex-shrink-0">
                <div className="sticky top-4">
                  <h4 className="text-sm font-semibold text-muted-foreground mb-3 uppercase tracking-wide">
                    Columns ({sortedColumns.length})
                  </h4>
                  <ScrollArea className="h-[calc(100vh-300px)]">
                    <div className="space-y-3 pr-2">
                      {groupedColumns.map(({ cluster, columns: groupCols }) => (
                        <div key={cluster?.id || 'all'}>
                          {cluster && clusteringEnabled && (
                            <div className="flex items-center gap-2 mb-1 px-2">
                              <span
                                className="w-2 h-2 rounded-full flex-shrink-0"
                                style={{ backgroundColor: cluster.color }}
                              />
                              <span className="text-xs font-medium text-muted-foreground truncate">
                                {cluster.name}
                              </span>
                              <Badge variant="outline" className="text-[10px] h-4 px-1 ml-auto">
                                {groupCols.length}
                              </Badge>
                            </div>
                          )}
                          <div className="space-y-0.5">
                            {groupCols.map((column) => {
                              const isModified = schemaChanges?.changed_columns?.includes(column.name);
                              const isNew = schemaChanges?.new_columns?.includes(column.name) && !schemaChanges?.missing_baseline;
                              const isSelected = selectedColumns.includes(column.name);
                              return (
                                <button
                                  key={column.name}
                                  onClick={() => {
                                    const element = document.getElementById(`column-${column.name}`);
                                    element?.scrollIntoView({ behavior: 'smooth', block: 'center' });
                                  }}
                                  className={cn(
                                    "w-full text-left px-2 py-1.5 rounded-md text-sm transition-colors",
                                    "hover:bg-muted",
                                    cluster && clusteringEnabled && "pl-4",
                                    isSelected && "bg-primary/10 font-medium",
                                    isModified && "text-amber-600 dark:text-amber-400",
                                    isNew && "text-green-600 dark:text-green-400"
                                  )}
                                >
                                  <span className="truncate block">{formatColumnName(column.name)}</span>
                                </button>
                              );
                            })}
                          </div>
                        </div>
                      ))}
                    </div>
                  </ScrollArea>
                </div>
              </div>
            )}

            {/* Main Grid with Clusters */}
            <div className="flex-1">
              {clusteringEnabled && groupedColumns.length > 1 ? (
                <>
                <Accordion
                  type="multiple"
                  defaultValue={groupedColumns.map(g => g.cluster?.id || 'all')}
                  className="space-y-2"
                >
                  {groupedColumns.map(({ cluster, columns: groupCols }) => (
                    <AccordionItem
                      key={cluster?.id || 'all'}
                      value={cluster?.id || 'all'}
                      className="border rounded-lg px-4"
                    >
                      <AccordionTrigger className="hover:no-underline py-3">
                        <div className="flex items-center gap-3 flex-1">
                          {cluster && (
                            <span
                              className="w-3 h-3 rounded-full flex-shrink-0"
                              style={{ backgroundColor: cluster.color }}
                            />
                          )}
                          {editingClusterId === cluster?.id ? (
                            <Input
                              value={editingClusterName}
                              onChange={(e) => setEditingClusterName(e.target.value)}
                              onKeyDown={(e) => {
                                e.stopPropagation();
                                if (e.key === 'Enter') {
                                  handleRenameCluster(cluster!.id, editingClusterName);
                                } else if (e.key === 'Escape') {
                                  setEditingClusterId(null);
                                  setEditingClusterName('');
                                }
                              }}
                              onClick={(e) => e.stopPropagation()}
                              className="h-7 w-48 text-sm font-semibold"
                              autoFocus
                            />
                          ) : (
                            <span className="font-semibold">{cluster?.name || 'All Columns'}</span>
                          )}
                          <Badge variant="outline" className="ml-2">
                            {groupCols.length}
                          </Badge>
                          {/* Cluster management buttons */}
                          {cluster && !readonly && editingClusterId !== cluster.id && (
                            <div className="flex items-center gap-1 ml-auto mr-2" onClick={(e) => e.stopPropagation()}>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-7 w-7 opacity-60 hover:opacity-100"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setEditingClusterId(cluster.id);
                                  setEditingClusterName(cluster.name);
                                }}
                                title="Rename cluster"
                              >
                                <Pencil className="h-3.5 w-3.5" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-7 w-7 opacity-60 hover:opacity-100 text-destructive hover:text-destructive"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleDeleteCluster(cluster.id);
                                }}
                                title="Delete cluster"
                                disabled={clusters.length <= 1}
                              >
                                <Trash2 className="h-3.5 w-3.5" />
                              </Button>
                            </div>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent>
                        <div className={cn(
                          "grid gap-3 pt-2",
                          viewMode === 'compact'
                            ? "grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4"
                            : "grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4"
                        )}>
                          {groupCols.map((column) => {
                            const isModified = schemaChanges?.changed_columns?.includes(column.name);
                            const isNew = schemaChanges?.new_columns?.includes(column.name) && !schemaChanges?.missing_baseline;
                            const isProcessing = processingColumns?.has(column.name);

                            // Compact view card
                            if (viewMode === 'compact') {
                              return (
                                <Card
                                  key={column.name}
                                  className={cn(
                                    "relative cursor-pointer hover:ring-2 hover:ring-primary/50 transition-all",
                                    selectedColumns.includes(column.name) && "ring-2 ring-primary",
                                    isModified && "border-amber-400 dark:border-amber-600 border-2",
                                    isNew && "border-green-400 dark:border-green-600 border-2",
                                    isProcessing && "border-blue-400 dark:border-blue-600 border-2 animate-pulse"
                                  )}
                                  onClick={() => setSelectedDetailColumn(column)}
                                >
                                  <CardContent className="pt-3 pb-2 px-3">
                                    <div className="flex items-center gap-1.5 mb-1">
                                      {!readonly && (
                                        <Checkbox
                                          className="h-3.5 w-3.5"
                                          checked={selectedColumns.includes(column.name)}
                                          onCheckedChange={(checked) =>
                                            handleColumnSelection(column.name, checked as boolean)
                                          }
                                          onClick={(e) => e.stopPropagation()}
                                        />
                                      )}
                                      <h4 className="font-medium text-xs truncate flex-1">
                                        {formatColumnName(column.name)}
                                      </h4>
                                      {isModified && <span className="w-2 h-2 rounded-full bg-amber-400 flex-shrink-0" title="Modified" />}
                                      {isNew && <span className="w-2 h-2 rounded-full bg-green-400 flex-shrink-0" title="New" />}
                                      {isProcessing && <Loader2 className="h-3 w-3 animate-spin text-blue-500 flex-shrink-0" />}
                                    </div>
                                    <div className="flex items-center gap-1 mb-1">
                                      {column.data_type && (
                                        <Badge variant="outline" className="h-4 text-[10px] px-1">
                                          {column.data_type}
                                        </Badge>
                                      )}
                                      {column.allowed_values && column.allowed_values.length > 0 && (
                                        <Badge variant="outline" className="h-4 text-[10px] px-1 bg-purple-50 dark:bg-purple-950">
                                          {column.allowed_values.length} val
                                        </Badge>
                                      )}
                                    </div>
                                    <p className="text-[10px] text-muted-foreground line-clamp-1">
                                      {column.definition || 'No definition'}
                                    </p>
                                    {/* Move to cluster dropdown */}
                                    {!readonly && clusters.length > 1 && (
                                      <div className="mt-2 pt-2 border-t" onClick={(e) => e.stopPropagation()}>
                                        <DropdownMenu>
                                          <DropdownMenuTrigger asChild>
                                            <Button variant="ghost" size="sm" className="h-6 text-[10px] w-full justify-start px-1">
                                              <FolderInput className="h-3 w-3 mr-1" />
                                              Move to...
                                            </Button>
                                          </DropdownMenuTrigger>
                                          <DropdownMenuContent align="start">
                                            {clusters.filter(c => !c.column_names.includes(column.name)).map(c => (
                                              <DropdownMenuItem
                                                key={c.id}
                                                onClick={() => handleMoveColumn(column.name, c.id)}
                                              >
                                                <span
                                                  className="w-2 h-2 rounded-full mr-2 flex-shrink-0"
                                                  style={{ backgroundColor: c.color }}
                                                />
                                                {c.name}
                                              </DropdownMenuItem>
                                            ))}
                                            <DropdownMenuSeparator />
                                            <DropdownMenuItem onClick={() => handleMoveColumn(column.name, 'new')}>
                                              <FolderPlus className="h-3.5 w-3.5 mr-2" />
                                              New Cluster...
                                            </DropdownMenuItem>
                                          </DropdownMenuContent>
                                        </DropdownMenu>
                                      </div>
                                    )}
                                  </CardContent>
                                </Card>
                              );
                            }

                            // Detailed view card
                            return (
                              <Card
                                key={column.name}
                                id={`column-${column.name}`}
                                className={cn(
                                  "relative",
                                  selectedColumns.includes(column.name) && "ring-2 ring-primary",
                                  isModified && "border-amber-400 dark:border-amber-600 border-2",
                                  isNew && "border-green-400 dark:border-green-600 border-2",
                                  isProcessing && "border-blue-400 dark:border-blue-600 border-2 animate-pulse"
                                )}
                              >
                                <CardContent className="pt-4">
                                  <div className="flex justify-between items-start mb-2">
                                    <div className="flex items-center gap-2 flex-wrap">
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
                                      {isModified && (
                                        <Badge variant="outline" className="text-xs bg-amber-50 dark:bg-amber-950 text-amber-700 dark:text-amber-300 border-amber-300">
                                          Modified
                                        </Badge>
                                      )}
                                      {isNew && (
                                        <Badge variant="outline" className="text-xs bg-green-50 dark:bg-green-950 text-green-700 dark:text-green-300 border-green-300">
                                          New
                                        </Badge>
                                      )}
                                      {isProcessing && (
                                        <Badge variant="outline" className="text-xs bg-blue-50 dark:bg-blue-950 text-blue-700 dark:text-blue-300 border-blue-300">
                                          <Loader2 className="h-3 w-3 animate-spin mr-1" />
                                          Extracting...
                                        </Badge>
                                      )}
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
                                            >
                                              <Pencil className="h-4 w-4" />
                                            </Button>
                                          </TooltipTrigger>
                                          <TooltipContent>Edit column</TooltipContent>
                                        </Tooltip>
                                        {clusters.length > 1 && (
                                          <DropdownMenu>
                                            <Tooltip>
                                              <TooltipTrigger asChild>
                                                <DropdownMenuTrigger asChild>
                                                  <Button variant="ghost" size="icon" className="h-8 w-8">
                                                    <FolderInput className="h-4 w-4" />
                                                  </Button>
                                                </DropdownMenuTrigger>
                                              </TooltipTrigger>
                                              <TooltipContent>Move to cluster</TooltipContent>
                                            </Tooltip>
                                            <DropdownMenuContent align="end">
                                              {clusters.filter(c => !c.column_names.includes(column.name)).map(c => (
                                                <DropdownMenuItem
                                                  key={c.id}
                                                  onClick={() => handleMoveColumn(column.name, c.id)}
                                                >
                                                  <span
                                                    className="w-2 h-2 rounded-full mr-2 flex-shrink-0"
                                                    style={{ backgroundColor: c.color }}
                                                  />
                                                  {c.name}
                                                </DropdownMenuItem>
                                              ))}
                                              <DropdownMenuSeparator />
                                              <DropdownMenuItem onClick={() => handleMoveColumn(column.name, 'new')}>
                                                <FolderPlus className="h-3.5 w-3.5 mr-2" />
                                                New Cluster...
                                              </DropdownMenuItem>
                                            </DropdownMenuContent>
                                          </DropdownMenu>
                                        )}
                                        <Tooltip>
                                          <TooltipTrigger asChild>
                                            <Button
                                              variant="ghost"
                                              size="icon"
                                              className="h-8 w-8 text-destructive"
                                              onClick={() => handleDeleteColumn(column.name)}
                                            >
                                              <Trash2 className="h-4 w-4" />
                                            </Button>
                                          </TooltipTrigger>
                                          <TooltipContent>Delete column</TooltipContent>
                                        </Tooltip>
                                      </div>
                                    )}
                                  </div>
                                  {column.data_type && <Badge className="mb-2">{column.data_type}</Badge>}
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
                                  {column.allowed_values && column.allowed_values.length > 0 ? (
                                    <div className="mb-3 p-3 bg-purple-50 dark:bg-purple-950 rounded-md border border-purple-200 dark:border-purple-800">
                                      <p className="text-xs font-semibold text-purple-700 dark:text-purple-300 mb-2">Allowed Values</p>
                                      <div className="flex flex-wrap gap-1">
                                        {column.allowed_values.map((value, idx) => (
                                          <Badge key={idx} variant="outline" className="text-xs">{value}</Badge>
                                        ))}
                                      </div>
                                    </div>
                                  ) : (
                                    <div className="mb-3 p-2 bg-gray-50 dark:bg-gray-900 rounded-md border border-gray-200 dark:border-gray-700">
                                      <p className="text-xs text-gray-500 dark:text-gray-400 italic">Free-form</p>
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
                            );
                          })}
                        </div>
                      </AccordionContent>
                    </AccordionItem>
                  ))}
                </Accordion>
                {/* Add Cluster button */}
                {!readonly && (
                  <Button
                    variant="outline"
                    size="sm"
                    className="mt-3"
                    onClick={() => {
                      setNewClusterName('');
                      setShowNewClusterDialog(true);
                    }}
                  >
                    <FolderPlus className="h-4 w-4 mr-2" />
                    Add Cluster
                  </Button>
                )}
                </>
              ) : (
                // No clustering - flat grid
                <div className={cn(
                  "grid gap-3",
                  viewMode === 'compact'
                    ? "grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4"
                    : "grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4"
                )}>
                  {sortedColumns.map((column) => {
              const isModified = schemaChanges?.changed_columns?.includes(column.name);
              const isNew = schemaChanges?.new_columns?.includes(column.name) && !schemaChanges?.missing_baseline;
              const isProcessing = processingColumns?.has(column.name);
              const changeDetail = schemaChanges?.column_changes?.[column.name];

              // Compact view card
              if (viewMode === 'compact') {
                return (
                  <Card
                    key={column.name}
                    className={cn(
                      "relative cursor-pointer hover:ring-2 hover:ring-primary/50 transition-all",
                      selectedColumns.includes(column.name) && "ring-2 ring-primary",
                      isModified && "border-amber-400 dark:border-amber-600 border-2",
                      isNew && "border-green-400 dark:border-green-600 border-2",
                      isProcessing && "border-blue-400 dark:border-blue-600 border-2 animate-pulse"
                    )}
                    onClick={() => setSelectedDetailColumn(column)}
                  >
                    <CardContent className="pt-3 pb-2 px-3">
                      {/* Row 1: Checkbox + Name + Status indicators */}
                      <div className="flex items-center gap-1.5 mb-1">
                        {!readonly && (
                          <Checkbox
                            className="h-3.5 w-3.5"
                            checked={selectedColumns.includes(column.name)}
                            onCheckedChange={(checked) =>
                              handleColumnSelection(column.name, checked as boolean)
                            }
                            onClick={(e) => e.stopPropagation()}
                          />
                        )}
                        <h4 className="font-medium text-xs truncate flex-1">
                          {formatColumnName(column.name)}
                        </h4>
                        {isModified && <span className="w-2 h-2 rounded-full bg-amber-400 flex-shrink-0" title="Modified" />}
                        {isNew && <span className="w-2 h-2 rounded-full bg-green-400 flex-shrink-0" title="New" />}
                        {isProcessing && <Loader2 className="h-3 w-3 animate-spin text-blue-500 flex-shrink-0" />}
                      </div>

                      {/* Row 2: Data type + constraint count */}
                      <div className="flex items-center gap-1 mb-1">
                        {column.data_type && (
                          <Badge variant="outline" className="h-4 text-[10px] px-1">
                            {column.data_type}
                          </Badge>
                        )}
                        {column.allowed_values && column.allowed_values.length > 0 && (
                          <Badge variant="outline" className="h-4 text-[10px] px-1 bg-purple-50 dark:bg-purple-950">
                            {column.allowed_values.length} val
                          </Badge>
                        )}
                      </div>

                      {/* Row 3: Truncated definition */}
                      <p className="text-[10px] text-muted-foreground line-clamp-1">
                        {column.definition || 'No definition'}
                      </p>
                    </CardContent>
                  </Card>
                );
              }

              // Detailed view card (original)
              return (
              <Card
                key={column.name}
                id={`column-${column.name}`}
                className={cn(
                  "relative",
                  selectedColumns.includes(column.name) && "ring-2 ring-primary",
                  isModified && "border-amber-400 dark:border-amber-600 border-2",
                  isNew && "border-green-400 dark:border-green-600 border-2",
                  isProcessing && "border-blue-400 dark:border-blue-600 border-2 animate-pulse"
                )}
              >
                <CardContent className="pt-4">
                  <div className="flex justify-between items-start mb-2">
                    <div className="flex items-center gap-2 flex-wrap">
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
                      {isModified && (
                        <Tooltip>
                          <TooltipTrigger>
                            <Badge variant="outline" className="text-xs bg-amber-50 dark:bg-amber-950 text-amber-700 dark:text-amber-300 border-amber-300">
                              Modified
                            </Badge>
                          </TooltipTrigger>
                          <TooltipContent>
                            {changeDetail?.change_type === 'definition' && 'Definition changed'}
                            {changeDetail?.change_type === 'rationale' && 'Rationale changed'}
                            {changeDetail?.change_type === 'allowed_values' && 'Allowed values changed'}
                            {!changeDetail?.change_type && 'Column modified since last extraction'}
                          </TooltipContent>
                        </Tooltip>
                      )}
                      {isNew && (
                        <Tooltip>
                          <TooltipTrigger>
                            <Badge variant="outline" className="text-xs bg-green-50 dark:bg-green-950 text-green-700 dark:text-green-300 border-green-300">
                              New
                            </Badge>
                          </TooltipTrigger>
                          <TooltipContent>New column added since last extraction</TooltipContent>
                        </Tooltip>
                      )}
                      {isProcessing && (
                        <Badge variant="outline" className="text-xs bg-blue-50 dark:bg-blue-950 text-blue-700 dark:text-blue-300 border-blue-300">
                          <Loader2 className="h-3 w-3 animate-spin mr-1" />
                          Extracting...
                        </Badge>
                      )}
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
              );
            })}
                </div>
              )}
            </div>
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
        clusters={clusters}
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

      {/* New Cluster Dialog */}
      <Dialog open={showNewClusterDialog} onOpenChange={setShowNewClusterDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FolderPlus className="h-5 w-5" />
              Create New Cluster
            </DialogTitle>
            <DialogDescription>
              Enter a name for the new cluster. You can move columns to this cluster after creating it.
            </DialogDescription>
          </DialogHeader>
          <div className="py-4">
            <Input
              placeholder="Cluster name"
              value={newClusterName}
              onChange={(e) => setNewClusterName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && newClusterName.trim()) {
                  handleCreateCluster();
                }
              }}
              autoFocus
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => {
              setShowNewClusterDialog(false);
              setNewClusterName('');
              delete (window as any).__pendingMoveColumn;
            }}>
              Cancel
            </Button>
            <Button onClick={handleCreateCluster} disabled={!newClusterName.trim()}>
              Create Cluster
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

      {/* Re-extraction Dialog */}
      <ReextractionDialog
        open={reextractionDialogOpen}
        sessionId={sessionId}
        onClose={() => setReextractionDialogOpen(false)}
        onSuccess={handleReextractionSuccess}
        onError={handleReextractionError}
        onReextractionStarted={onReextractionStarted}
      />

      {/* Continue Discovery Dialog */}
      <ContinueDiscoveryDialog
        open={continueDiscoveryDialogOpen}
        sessionId={sessionId}
        sessionType={sessionType || 'qbsd'}
        currentColumns={
          // Use displayColumns which filters out excerpt columns, then deduplicate
          Array.from(new Map(displayColumns.map(c => [c.name, c])).values())
        }
        query={query || ''}
        onClose={() => setContinueDiscoveryDialogOpen(false)}
        onSuccess={handleContinueDiscoverySuccess}
        onError={handleContinueDiscoveryError}
        onExtractionStarted={onReextractionStarted}
        onDiscoveryStarted={handleContinueDiscoveryStarted}
      />

      {/* Continue Discovery Monitor - Full Page View */}
      {continueDiscoveryMonitorOpen && continueDiscoveryOperationId && (
        <div className="fixed inset-0 z-50 bg-background">
          <ContinueDiscoveryMonitor
            sessionId={sessionId}
            operationId={continueDiscoveryOperationId}
            initialColumns={Array.from(new Set(displayColumns.map(c => c.name)))}
            onComplete={handleMonitorComplete}
            onCancel={handleMonitorCancel}
            onError={handleContinueDiscoveryError}
          />
        </div>
      )}

      {readonly && (
        <Alert variant="info">
          <Info className="h-4 w-4" />
          <AlertDescription>
            <strong>Read-only mode:</strong> Schema editing features are disabled.
            To enable editing, ensure you have proper permissions and the session supports schema modifications.
          </AlertDescription>
        </Alert>
      )}

      {/* Column Detail Panel (for compact view) */}
      <SchemaColumnDetailPanel
        column={selectedDetailColumn}
        isOpen={selectedDetailColumn !== null}
        onClose={() => setSelectedDetailColumn(null)}
        onEdit={(column) => {
          setSelectedDetailColumn(null);
          handleEditColumn(column);
        }}
        onDelete={(columnName) => {
          setSelectedDetailColumn(null);
          handleDeleteColumn(columnName);
        }}
        readonly={readonly}
        schemaChanges={schemaChanges}
        processingColumns={processingColumns}
        sessionType={sessionType}
      />
    </div>
  );
};

export default SchemaViewer;
