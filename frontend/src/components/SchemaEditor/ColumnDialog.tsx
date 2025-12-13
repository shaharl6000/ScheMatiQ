import React, { useState, useEffect } from 'react';
import { Plus, Pencil, Loader2 } from 'lucide-react';

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Separator } from '@/components/ui/separator';

import {
  ColumnInfo,
  AddColumnRequest,
  EditColumnRequest,
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
    new_name: ''
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

    const nameToCheck = mode === 'edit' ? formData.new_name : formData.name;
    if (!nameToCheck.trim()) {
      newErrors.name = 'Column name is required';
    } else {
      const isDuplicate = existingColumns.some(col =>
        col.name === nameToCheck &&
        (mode === 'add' || (mode === 'edit' && col.name !== formData.name))
      );
      if (isDuplicate) {
        newErrors.name = 'Column name already exists';
      }
    }

    if (!formData.definition.trim()) {
      newErrors.definition = 'Definition is required';
    }

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

  const handleChange = (field: string, value: string) => {
    setFormData(prev => ({
      ...prev,
      [field]: value
    }));

    if (errors[field]) {
      setErrors(prev => ({
        ...prev,
        [field]: ''
      }));
    }
  };

  const dialogTitle = mode === 'add' ? 'Add New Column' : 'Edit Column';
  const submitIcon = mode === 'add' ? <Plus className="h-4 w-4" /> : <Pencil className="h-4 w-4" />;
  const submitText = mode === 'add' ? 'Add Column' : 'Save Changes';

  return (
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && !loading && onClose()}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            {submitIcon}
            {dialogTitle}
          </DialogTitle>
          <DialogDescription>
            {mode === 'add'
              ? 'Add a new column to the schema'
              : 'Edit column properties'}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          {/* Column Name */}
          <div className="space-y-2">
            <Label htmlFor="name">
              {mode === 'edit' ? 'Current Name' : 'Column Name'} <span className="text-destructive">*</span>
            </Label>
            <Input
              id="name"
              value={mode === 'edit' ? formData.name : formData.name}
              onChange={(e) => mode !== 'edit' && handleChange('name', e.target.value)}
              disabled={mode === 'edit' || loading}
              className={mode === 'edit' ? 'bg-muted' : ''}
            />
            {errors.name && <p className="text-sm text-destructive">{errors.name}</p>}
          </div>

          {/* New Name (for edit mode) */}
          {mode === 'edit' && (
            <div className="space-y-2">
              <Label htmlFor="new_name">New Name</Label>
              <Input
                id="new_name"
                value={formData.new_name}
                onChange={(e) => handleChange('new_name', e.target.value)}
                disabled={loading}
              />
              <p className="text-sm text-muted-foreground">
                Leave unchanged to keep the current name
              </p>
            </div>
          )}

          <Separator />

          {/* Definition */}
          <div className="space-y-2">
            <Label htmlFor="definition">
              Definition <span className="text-destructive">*</span>
            </Label>
            <Textarea
              id="definition"
              value={formData.definition}
              onChange={(e) => handleChange('definition', e.target.value)}
              rows={3}
              disabled={loading}
            />
            {errors.definition && <p className="text-sm text-destructive">{errors.definition}</p>}
            <p className="text-sm text-muted-foreground">
              Describe what this column represents and what type of information it should contain
            </p>
          </div>

          {/* Rationale */}
          <div className="space-y-2">
            <Label htmlFor="rationale">
              Rationale <span className="text-destructive">*</span>
            </Label>
            <Textarea
              id="rationale"
              value={formData.rationale}
              onChange={(e) => handleChange('rationale', e.target.value)}
              rows={4}
              disabled={loading}
            />
            {errors.rationale && <p className="text-sm text-destructive">{errors.rationale}</p>}
            <p className="text-sm text-muted-foreground">
              Explain why this column is important for answering the research query
            </p>
          </div>

          {mode === 'add' && (
            <Alert variant="info">
              <AlertDescription>
                <strong>Note:</strong> Adding a new column will trigger document processing to extract values for this column from all documents in the dataset. This process may take some time depending on the number of documents.
              </AlertDescription>
            </Alert>
          )}

          {mode === 'edit' && formData.new_name !== formData.name && formData.new_name.trim() && (
            <Alert variant="warning">
              <AlertDescription>
                <strong>Note:</strong> Renaming a column will trigger reprocessing to ensure data consistency. This may take some time.
              </AlertDescription>
            </Alert>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={loading}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={loading}>
            {loading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Processing...
              </>
            ) : (
              <>
                {submitIcon}
                <span className="ml-2">{submitText}</span>
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default ColumnDialog;
