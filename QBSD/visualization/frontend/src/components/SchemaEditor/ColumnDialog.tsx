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
} from '@mui/material';
import {
  Save,
  Cancel,
  Add,
  Edit,
} from '@mui/icons-material';

import { 
  ColumnInfo, 
  AddColumnRequest, 
  EditColumnRequest,
  ColumnDialogState 
} from '../../types';
import { schemaAPI } from '../../services/api';

interface ColumnDialogProps {
  open: boolean;
  mode: 'add' | 'edit';
  sessionId: string;
  column?: ColumnInfo;
  existingColumns: ColumnInfo[];
  onClose: () => void;
  onSuccess: (message: string) => void;
  onError: (error: string) => void;
}

const ColumnDialog: React.FC<ColumnDialogProps> = ({
  open,
  mode,
  sessionId,
  column,
  existingColumns,
  onClose,
  onSuccess,
  onError
}) => {
  const [loading, setLoading] = useState(false);
  const [formData, setFormData] = useState({
    name: '',
    definition: '',
    rationale: '',
    new_name: '' // For rename operations
  });
  const [errors, setErrors] = useState<Record<string, string>>({});

  // Initialize form when dialog opens or column changes
  useEffect(() => {
    if (open) {
      if (mode === 'edit' && column) {
        setFormData({
          name: column.name,
          definition: column.definition || '',
          rationale: column.rationale || '',
          new_name: column.name
        });
      } else {
        setFormData({
          name: '',
          definition: '',
          rationale: '',
          new_name: ''
        });
      }
      setErrors({});
    }
  }, [open, mode, column]);

  const validateForm = (): boolean => {
    const newErrors: Record<string, string> = {};

    // Validate name
    const nameToCheck = mode === 'edit' ? formData.new_name : formData.name;
    if (!nameToCheck.trim()) {
      newErrors.name = 'Column name is required';
    } else {
      // Check for duplicates (excluding current column in edit mode)
      const isDuplicate = existingColumns.some(col => 
        col.name === nameToCheck && 
        (mode === 'add' || (mode === 'edit' && col.name !== formData.name))
      );
      if (isDuplicate) {
        newErrors.name = 'Column name already exists';
      }
    }

    // Validate definition
    if (!formData.definition.trim()) {
      newErrors.definition = 'Definition is required';
    }

    // Validate rationale
    if (!formData.rationale.trim()) {
      newErrors.rationale = 'Rationale is required';
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
      if (mode === 'add') {
        const request: AddColumnRequest = {
          name: formData.name.trim(),
          definition: formData.definition.trim(),
          rationale: formData.rationale.trim(),
        };
        
        const response = await schemaAPI.addColumn(sessionId, request);
        onSuccess(`Column "${request.name}" added successfully${response.reprocessing_required ? ' - processing started' : ''}`);
      } else {
        // Edit mode
        const request: EditColumnRequest = {
          name: formData.name,
          definition: formData.definition.trim(),
          rationale: formData.rationale.trim(),
          new_name: formData.new_name.trim() !== formData.name ? formData.new_name.trim() : undefined,
        };
        
        const response = await schemaAPI.editColumn(sessionId, request);
        onSuccess(`Column updated successfully${response.reprocessing_required ? ' - reprocessing started' : ''}`);
      }
      
      onClose();
    } catch (error: any) {
      console.error('Column operation failed:', error);
      onError(error.response?.data?.detail || `Failed to ${mode} column`);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (field: string) => (event: React.ChangeEvent<HTMLInputElement>) => {
    setFormData(prev => ({
      ...prev,
      [field]: event.target.value
    }));
    
    // Clear error when user starts typing
    if (errors[field]) {
      setErrors(prev => ({
        ...prev,
        [field]: ''
      }));
    }
  };

  const dialogTitle = mode === 'add' ? 'Add New Column' : 'Edit Column';
  const submitIcon = mode === 'add' ? <Add /> : <Edit />;
  const submitText = mode === 'add' ? 'Add Column' : 'Save Changes';

  return (
    <Dialog 
      open={open} 
      onClose={onClose} 
      maxWidth="md" 
      fullWidth
      disableEscapeKeyDown={loading}
    >
      <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        {submitIcon}
        {dialogTitle}
      </DialogTitle>
      
      <DialogContent dividers>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          {/* Column Name */}
          <TextField
            label={mode === 'edit' ? 'Current Name' : 'Column Name'}
            value={mode === 'edit' ? formData.name : formData.name}
            onChange={mode === 'edit' ? undefined : handleChange('name')}
            error={!!errors.name}
            helperText={errors.name}
            fullWidth
            required
            disabled={mode === 'edit' || loading}
            variant={mode === 'edit' ? 'filled' : 'outlined'}
          />

          {/* New Name (for edit mode) */}
          {mode === 'edit' && (
            <TextField
              label="New Name (leave unchanged to keep current name)"
              value={formData.new_name}
              onChange={handleChange('new_name')}
              error={!!errors.name}
              helperText={errors.name || 'Leave empty to keep the current name'}
              fullWidth
              disabled={loading}
            />
          )}

          <Divider />

          {/* Definition */}
          <TextField
            label="Definition"
            value={formData.definition}
            onChange={handleChange('definition')}
            error={!!errors.definition}
            helperText={errors.definition || 'Describe what this column represents and what type of information it should contain'}
            multiline
            rows={3}
            fullWidth
            required
            disabled={loading}
          />

          {/* Rationale */}
          <TextField
            label="Rationale"
            value={formData.rationale}
            onChange={handleChange('rationale')}
            error={!!errors.rationale}
            helperText={errors.rationale || 'Explain why this column is important for answering the research query'}
            multiline
            rows={4}
            fullWidth
            required
            disabled={loading}
          />

          {mode === 'add' && (
            <Alert severity="info">
              <Typography variant="body2">
                <strong>Note:</strong> Adding a new column will trigger document processing to extract values for this column from all documents in the dataset. This process may take some time depending on the number of documents.
              </Typography>
            </Alert>
          )}

          {mode === 'edit' && formData.new_name !== formData.name && formData.new_name.trim() && (
            <Alert severity="warning">
              <Typography variant="body2">
                <strong>Note:</strong> Renaming a column will trigger reprocessing to ensure data consistency. This may take some time.
              </Typography>
            </Alert>
          )}
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
          disabled={loading}
          startIcon={loading ? <CircularProgress size={16} /> : submitIcon}
        >
          {loading ? 'Processing...' : submitText}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ColumnDialog;