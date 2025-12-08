import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Button,
  Box,
  Alert,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  Typography,
  Divider,
  CircularProgress,
  Autocomplete,
  SelectChangeEvent,
  Paper,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
} from '@mui/material';
import {
  Merge,
  Cancel,
  ArrowDownward,
  ViewColumn,
  CallMerge,
} from '@mui/icons-material';

import { 
  ColumnInfo, 
  MergeColumnsRequest,
} from '../../types';
import { schemaAPI } from '../../services/api';

interface MergeDialogProps {
  open: boolean;
  sessionId: string;
  columns: ColumnInfo[];
  preselectedColumns?: string[];
  onClose: () => void;
  onSuccess: (message: string) => void;
  onError: (error: string) => void;
}

const MERGE_STRATEGIES = [
  {
    value: 'concatenate',
    label: 'Concatenate',
    description: 'Combine all values with a separator'
  },
  {
    value: 'first_non_empty',
    label: 'First Non-Empty',
    description: 'Use the first non-empty value found'
  },
  {
    value: 'smart_merge',
    label: 'Smart Merge',
    description: 'Intelligently combine values (experimental)'
  }
];

const MergeDialog: React.FC<MergeDialogProps> = ({
  open,
  sessionId,
  columns,
  preselectedColumns = [],
  onClose,
  onSuccess,
  onError
}) => {
  const [loading, setLoading] = useState(false);
  const [sourceColumns, setSourceColumns] = useState<string[]>([]);
  const [targetColumn, setTargetColumn] = useState('');
  const [mergeStrategy, setMergeStrategy] = useState<'concatenate' | 'smart_merge' | 'first_non_empty'>('concatenate');
  const [separator, setSeparator] = useState(' | ');
  const [definition, setDefinition] = useState('');
  const [rationale, setRationale] = useState('');
  const [errors, setErrors] = useState<Record<string, string>>({});

  // Initialize form when dialog opens
  useEffect(() => {
    if (open) {
      setSourceColumns(preselectedColumns);
      setTargetColumn('');
      setMergeStrategy('concatenate');
      setSeparator(' | ');
      setDefinition('');
      setRationale('');
      setErrors({});
      
      // Auto-generate initial values based on selected columns
      if (preselectedColumns.length > 0) {
        generateMergePreview(preselectedColumns);
      }
    }
  }, [open, preselectedColumns]);

  const generateMergePreview = (selectedColumns: string[]) => {
    if (selectedColumns.length === 0) return;
    
    // Get column objects for selected columns
    const selectedColumnObjs = columns.filter(col => selectedColumns.includes(col.name));
    
    // Generate suggested target name
    const suggestedName = selectedColumns.join('_');
    setTargetColumn(suggestedName);
    
    // Combine definitions and rationales
    const definitions = selectedColumnObjs.map(col => col.definition).filter(Boolean);
    const rationales = selectedColumnObjs.map(col => col.rationale).filter(Boolean);
    
    if (definitions.length > 0) {
      setDefinition(`Combined column containing: ${definitions.join(separator)}`);
    }
    
    if (rationales.length > 0) {
      setRationale(`Merged from multiple columns to consolidate related information: ${rationales.join(separator)}`);
    }
  };

  const validateForm = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (sourceColumns.length < 2) {
      newErrors.sourceColumns = 'Select at least 2 columns to merge';
    }

    if (!targetColumn.trim()) {
      newErrors.targetColumn = 'Target column name is required';
    } else {
      // Check if target column name conflicts (unless it's one of the source columns)
      const isConflict = columns.some(col => 
        col.name === targetColumn && !sourceColumns.includes(col.name)
      );
      if (isConflict) {
        newErrors.targetColumn = 'Target column name conflicts with existing column';
      }
    }

    if (!definition.trim()) {
      newErrors.definition = 'Definition is required';
    }

    if (!rationale.trim()) {
      newErrors.rationale = 'Rationale is required';
    }

    if (mergeStrategy === 'concatenate' && !separator.trim()) {
      newErrors.separator = 'Separator is required for concatenation';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async () => {
    if (!validateForm()) {
      return;
    }

    setLoading(true);
    try {
      const request: MergeColumnsRequest = {
        source_columns: sourceColumns,
        target_column: targetColumn.trim(),
        merge_strategy: mergeStrategy,
        definition: definition.trim(),
        rationale: rationale.trim(),
        separator: mergeStrategy === 'concatenate' ? separator : undefined,
      };
      
      const response = await schemaAPI.mergeColumns(sessionId, request);
      onSuccess(`Merged ${sourceColumns.length} columns into "${targetColumn}" - processing started`);
      onClose();
    } catch (error: any) {
      console.error('Merge operation failed:', error);
      onError(error.response?.data?.detail || 'Failed to merge columns');
    } finally {
      setLoading(false);
    }
  };

  const handleSourceColumnsChange = (event: any, value: string[]) => {
    setSourceColumns(value);
    if (errors.sourceColumns) {
      setErrors(prev => ({ ...prev, sourceColumns: '' }));
    }
    
    // Regenerate preview when selection changes
    if (value.length > 0) {
      generateMergePreview(value);
    } else {
      setTargetColumn('');
      setDefinition('');
      setRationale('');
    }
  };

  const handleStrategyChange = (event: SelectChangeEvent<string>) => {
    const strategy = event.target.value as 'concatenate' | 'smart_merge' | 'first_non_empty';
    setMergeStrategy(strategy);
    
    // Set default separator for concatenate
    if (strategy === 'concatenate' && !separator) {
      setSeparator(' | ');
    }
  };

  const selectedStrategy = MERGE_STRATEGIES.find(s => s.value === mergeStrategy);
  const availableColumns = columns.filter(col => !col.name.endsWith('_excerpt'));

  return (
    <Dialog 
      open={open} 
      onClose={onClose} 
      maxWidth="lg" 
      fullWidth
      disableEscapeKeyDown={loading}
    >
      <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <CallMerge />
        Merge Columns
      </DialogTitle>
      
      <DialogContent dividers>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          {/* Source Columns Selection */}
          <Box>
            <Typography variant="h6" gutterBottom>
              1. Select Columns to Merge
            </Typography>
            <Autocomplete
              multiple
              value={sourceColumns}
              onChange={handleSourceColumnsChange}
              options={availableColumns.map(col => col.name)}
              getOptionLabel={(option) => option}
              renderTags={(value, getTagProps) =>
                value.map((option, index) => (
                  <Chip 
                    variant="outlined" 
                    label={option} 
                    {...getTagProps({ index })} 
                    key={option}
                  />
                ))
              }
              renderInput={(params) => (
                <TextField
                  {...params}
                  variant="outlined"
                  label="Source Columns"
                  placeholder="Select columns to merge..."
                  error={!!errors.sourceColumns}
                  helperText={errors.sourceColumns}
                />
              )}
              disabled={loading}
            />
          </Box>

          {sourceColumns.length > 0 && (
            <>
              <Divider />
              
              {/* Merge Strategy */}
              <Box>
                <Typography variant="h6" gutterBottom>
                  2. Choose Merge Strategy
                </Typography>
                <FormControl fullWidth error={!!errors.mergeStrategy}>
                  <InputLabel>Merge Strategy</InputLabel>
                  <Select
                    value={mergeStrategy}
                    onChange={handleStrategyChange}
                    label="Merge Strategy"
                    disabled={loading}
                  >
                    {MERGE_STRATEGIES.map((strategy) => (
                      <MenuItem key={strategy.value} value={strategy.value}>
                        <Box>
                          <Typography variant="subtitle1">{strategy.label}</Typography>
                          <Typography variant="caption" color="text.secondary">
                            {strategy.description}
                          </Typography>
                        </Box>
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>

                {mergeStrategy === 'concatenate' && (
                  <TextField
                    label="Separator"
                    value={separator}
                    onChange={(e) => setSeparator(e.target.value)}
                    error={!!errors.separator}
                    helperText={errors.separator || 'Characters to separate merged values'}
                    sx={{ mt: 2 }}
                    disabled={loading}
                  />
                )}

                {selectedStrategy && (
                  <Alert severity="info" sx={{ mt: 2 }}>
                    <Typography variant="body2">
                      <strong>{selectedStrategy.label}:</strong> {selectedStrategy.description}
                    </Typography>
                  </Alert>
                )}
              </Box>

              <Divider />

              {/* Target Column Configuration */}
              <Box>
                <Typography variant="h6" gutterBottom>
                  3. Configure Target Column
                </Typography>
                
                <TextField
                  label="Target Column Name"
                  value={targetColumn}
                  onChange={(e) => setTargetColumn(e.target.value)}
                  error={!!errors.targetColumn}
                  helperText={errors.targetColumn || 'Name for the merged column'}
                  fullWidth
                  required
                  disabled={loading}
                  sx={{ mb: 2 }}
                />

                <TextField
                  label="Definition"
                  value={definition}
                  onChange={(e) => setDefinition(e.target.value)}
                  error={!!errors.definition}
                  helperText={errors.definition || 'Describe what the merged column represents'}
                  multiline
                  rows={3}
                  fullWidth
                  required
                  disabled={loading}
                  sx={{ mb: 2 }}
                />

                <TextField
                  label="Rationale"
                  value={rationale}
                  onChange={(e) => setRationale(e.target.value)}
                  error={!!errors.rationale}
                  helperText={errors.rationale || 'Explain why merging these columns is beneficial'}
                  multiline
                  rows={3}
                  fullWidth
                  required
                  disabled={loading}
                />
              </Box>

              <Divider />

              {/* Preview */}
              <Box>
                <Typography variant="h6" gutterBottom>
                  4. Merge Preview
                </Typography>
                <Paper variant="outlined" sx={{ p: 2 }}>
                  <Typography variant="subtitle2" color="primary" gutterBottom>
                    Source Columns:
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
                    {sourceColumns.map((colName) => (
                      <Chip 
                        key={colName} 
                        label={colName} 
                        variant="outlined" 
                        size="small"
                        icon={<ViewColumn />}
                      />
                    ))}
                  </Box>
                  
                  <ArrowDownward sx={{ display: 'block', mx: 'auto', my: 1, color: 'primary.main' }} />
                  
                  <Typography variant="subtitle2" color="primary" gutterBottom>
                    Target Column:
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
                    <Chip 
                      label={targetColumn || 'unnamed'} 
                      color="primary"
                      size="small"
                      icon={<Merge />}
                    />
                  </Box>
                  
                  <Typography variant="caption" color="text.secondary">
                    Strategy: {selectedStrategy?.label} | 
                    {mergeStrategy === 'concatenate' && ` Separator: "${separator}" | `}
                    Source columns will be removed after successful merge
                  </Typography>
                </Paper>
              </Box>
            </>
          )}

          <Alert severity="warning">
            <Typography variant="body2">
              <strong>Important:</strong> Merging columns will trigger document reprocessing to generate new values for the merged column. The source columns will be removed from the schema. This operation cannot be easily undone - consider creating a backup first.
            </Typography>
          </Alert>
        </Box>
      </DialogContent>

      <DialogActions sx={{ p: 2, gap: 1 }}>
        <Button
          onClick={onClose}
          disabled={loading}
          startIcon={<Cancel />}
        >
          Cancel
        </Button>
        
        <Button
          onClick={handleSubmit}
          variant="contained"
          disabled={loading || sourceColumns.length < 2}
          startIcon={loading ? <CircularProgress size={16} /> : <Merge />}
        >
          {loading ? 'Merging...' : `Merge ${sourceColumns.length} Columns`}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default MergeDialog;