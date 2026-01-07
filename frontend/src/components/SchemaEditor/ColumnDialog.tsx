import React, { useState, useEffect } from 'react';
import { Plus, Pencil, Loader2, X, Info } from 'lucide-react';

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
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

import {
  ColumnInfo,
  AddColumnRequest,
  EditColumnRequest,
  ColumnCluster,
} from '../../types';
import { schemaAPI } from '../../services/api';
import { getApiKeyForProvider } from '../../utils/apiKeyStorage';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

interface ColumnDialogProps {
  open: boolean;
  mode: 'add' | 'edit';
  sessionId: string;
  column?: ColumnInfo;
  existingColumns: ColumnInfo[];
  clusters?: ColumnCluster[];
  onClose: () => void;
  onSuccess: (message: string, updatedColumns?: ColumnInfo[], selectedClusterId?: string | null) => void;
  onError: (error: string) => void;
}

const ColumnDialog: React.FC<ColumnDialogProps> = ({
  open,
  mode,
  sessionId,
  column,
  existingColumns,
  clusters = [],
  onClose,
  onSuccess,
  onError
}) => {
  const [loading, setLoading] = useState(false);
  const [formData, setFormData] = useState({
    name: '',
    definition: '',
    rationale: '',
    new_name: '',
    allowed_values: [] as string[],
    auto_expand_threshold: 2
  });
  const [newAllowedValue, setNewAllowedValue] = useState('');
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [selectedClusterId, setSelectedClusterId] = useState<string | null>(null);

  // Initialize form when dialog opens or column changes
  useEffect(() => {
    if (open) {
      if (mode === 'edit' && column) {
        setFormData({
          name: column.name,
          definition: column.definition || '',
          rationale: column.rationale || '',
          new_name: column.name,
          allowed_values: column.allowed_values || [],
          auto_expand_threshold: column.auto_expand_threshold ?? 2
        });
      } else {
        setFormData({
          name: '',
          definition: '',
          rationale: '',
          new_name: '',
          allowed_values: [],
          auto_expand_threshold: 2
        });
      }
      setNewAllowedValue('');
      setErrors({});
      setSelectedClusterId(null);
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
      // Scroll to first error
      const firstErrorField = Object.keys(errors)[0] ||
        (!formData.name.trim() && mode === 'add' ? 'name' :
         !formData.definition.trim() ? 'definition' :
         !formData.rationale.trim() ? 'rationale' : null);
      if (firstErrorField) {
        const element = document.getElementById(firstErrorField);
        element?.scrollIntoView({ behavior: 'smooth', block: 'center' });
        element?.focus();
      }
      return;
    }

    setLoading(true);
    try {
      if (mode === 'add') {
        // Get API key from localStorage for value extraction
        const apiKey = await getApiKeyForProvider('gemini');

        const request: AddColumnRequest = {
          name: formData.name.trim(),
          definition: formData.definition.trim(),
          rationale: formData.rationale.trim(),
          allowed_values: formData.allowed_values.length > 0 ? formData.allowed_values : undefined,
        };

        // Include LLM config if API key is available
        if (apiKey) {
          request.llm_config = {
            provider: 'gemini',
            model: 'gemini-2.5-flash-lite',
            api_key: apiKey,
            max_output_tokens: 2048,
            temperature: 0.1
          };
        }

        const response = await schemaAPI.addColumn(sessionId, request);
        onSuccess(
          `Column "${request.name}" added successfully${response.reprocessing_required ? ' - processing started' : ''}`,
          response.columns,
          selectedClusterId
        );
      } else {
        const request: EditColumnRequest = {
          old_name: formData.name,  // Current name of the column being edited
          definition: formData.definition.trim(),
          rationale: formData.rationale.trim(),
          new_name: formData.new_name.trim() !== formData.name ? formData.new_name.trim() : undefined,
          allowed_values: formData.allowed_values,  // Send even if empty to allow clearing
          reprocess: false,  // Don't reprocess documents for metadata-only edits
        };

        const response = await schemaAPI.editColumn(sessionId, request);
        onSuccess(`Column updated successfully`, response.columns);
      }

      onClose();
    } catch (error: any) {
      console.error('Column operation failed:', error);
      // Handle Pydantic validation errors which return an array of error objects
      const detail = error.response?.data?.detail;
      let errorMessage = `Failed to ${mode} column`;
      if (typeof detail === 'string') {
        errorMessage = detail;
      } else if (Array.isArray(detail) && detail.length > 0) {
        // Pydantic validation error format: [{type, loc, msg, input}, ...]
        errorMessage = detail.map((err: any) => err.msg || String(err)).join('; ');
      } else if (detail && typeof detail === 'object') {
        errorMessage = detail.msg || JSON.stringify(detail);
      }
      onError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (field: string, value: string) => {
    setFormData(prev => ({
      ...prev,
      [field]: field === 'auto_expand_threshold' ? parseInt(value) || 0 : value
    }));

    if (errors[field]) {
      setErrors(prev => ({
        ...prev,
        [field]: ''
      }));
    }
  };

  const handleAddAllowedValue = () => {
    const value = newAllowedValue.trim();
    if (value && !formData.allowed_values.includes(value)) {
      setFormData(prev => ({
        ...prev,
        allowed_values: [...prev.allowed_values, value]
      }));
      setNewAllowedValue('');
    }
  };

  const handleRemoveAllowedValue = (index: number) => {
    setFormData(prev => ({
      ...prev,
      allowed_values: prev.allowed_values.filter((_, i) => i !== index)
    }));
  };

  const handleAllowedValueKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleAddAllowedValue();
    }
  };

  const dialogTitle = mode === 'add' ? 'Add New Column' : 'Edit Column';
  const submitIcon = mode === 'add' ? <Plus className="h-4 w-4" /> : <Pencil className="h-4 w-4" />;
  const submitText = mode === 'add' ? 'Add Column' : 'Save Changes';

  return (
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && !loading && onClose()}>
      <DialogContent className="sm:max-w-lg max-h-[90vh] flex flex-col">
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

        <ScrollArea className="flex-1 overflow-y-auto">
          <div className="space-y-4 py-4 pr-4">
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
              aria-required={mode === 'add'}
              aria-invalid={!!errors.name}
              aria-describedby={errors.name ? 'name-error' : undefined}
            />
            {errors.name && <p id="name-error" className="text-sm text-destructive">{errors.name}</p>}
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
              aria-required="true"
              aria-invalid={!!errors.definition}
              aria-describedby={errors.definition ? 'definition-error' : 'definition-hint'}
            />
            {errors.definition && <p id="definition-error" className="text-sm text-destructive">{errors.definition}</p>}
            <p id="definition-hint" className="text-sm text-muted-foreground">
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
              aria-required="true"
              aria-invalid={!!errors.rationale}
              aria-describedby={errors.rationale ? 'rationale-error' : 'rationale-hint'}
            />
            {errors.rationale && <p id="rationale-error" className="text-sm text-destructive">{errors.rationale}</p>}
            <p id="rationale-hint" className="text-sm text-muted-foreground">
              Explain why this column is important for answering the research query
            </p>
          </div>

          {/* Cluster Selection (only for add mode) */}
          {mode === 'add' && clusters.length > 0 && (
            <div className="space-y-2">
              <Label htmlFor="cluster">Assign to Cluster</Label>
              <Select
                value={selectedClusterId || 'auto'}
                onValueChange={(value) => setSelectedClusterId(value === 'auto' ? null : value)}
              >
                <SelectTrigger id="cluster">
                  <SelectValue placeholder="Auto (algorithm decides)" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="auto">
                    <span className="text-muted-foreground">Auto (algorithm decides)</span>
                  </SelectItem>
                  {clusters.map((c) => (
                    <SelectItem key={c.id} value={c.id}>
                      <div className="flex items-center gap-2">
                        <span
                          className="w-2 h-2 rounded-full flex-shrink-0"
                          style={{ backgroundColor: c.color }}
                        />
                        {c.name}
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-sm text-muted-foreground">
                Choose a cluster for this column, or let the algorithm assign it automatically
              </p>
            </div>
          )}

          <Separator />

          {/* Allowed Values (Closed Set) */}
          <div className="space-y-2">
            <div className="flex items-center gap-1">
              <Label htmlFor="allowed_values">
                Value Constraints (Optional)
              </Label>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Info className="h-4 w-4 text-muted-foreground cursor-help" />
                </TooltipTrigger>
                <TooltipContent side="right" className="max-w-xs">
                  <p>Define constraints for this column: categorical values, numeric types, or ranges. Leave empty for free-form text columns.</p>
                </TooltipContent>
              </Tooltip>
            </div>

            {/* Preset buttons for common constraints */}
            <div className="flex flex-wrap gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setFormData(prev => ({ ...prev, allowed_values: ['yes', 'no'] }))}
                disabled={loading}
                className="text-xs"
              >
                Yes/No
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setFormData(prev => ({ ...prev, allowed_values: ['number'] }))}
                disabled={loading}
                className="text-xs"
              >
                Number
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setFormData(prev => ({ ...prev, allowed_values: ['0-100'] }))}
                disabled={loading}
                className="text-xs"
              >
                0-100 (%)
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setFormData(prev => ({ ...prev, allowed_values: ['0.0-1.0'] }))}
                disabled={loading}
                className="text-xs"
              >
                0-1 (Prob)
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setFormData(prev => ({ ...prev, allowed_values: [] }))}
                disabled={loading}
                className="text-xs text-muted-foreground"
              >
                Clear
              </Button>
            </div>

            {/* Display existing allowed values as badges */}
            {formData.allowed_values.length > 0 && (
              <div className="flex flex-wrap gap-2 p-2 bg-muted/50 rounded-md">
                {formData.allowed_values.map((value, index) => (
                  <Badge key={index} variant="secondary" className="gap-1 pr-1">
                    {value}
                    <button
                      type="button"
                      onClick={() => handleRemoveAllowedValue(index)}
                      className="ml-1 hover:bg-destructive/20 rounded-full p-0.5"
                      disabled={loading}
                      aria-label={`Remove ${value}`}
                    >
                      <X className="h-3 w-3" />
                    </button>
                  </Badge>
                ))}
              </div>
            )}

            {/* Input for adding new values */}
            <div className="flex gap-2">
              <Input
                id="allowed_values"
                placeholder="Add allowed value..."
                value={newAllowedValue}
                onChange={(e) => setNewAllowedValue(e.target.value)}
                onKeyPress={handleAllowedValueKeyPress}
                disabled={loading}
                className="flex-1"
              />
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={handleAddAllowedValue}
                disabled={loading || !newAllowedValue.trim()}
              >
                <Plus className="h-4 w-4" />
              </Button>
            </div>
            <p className="text-sm text-muted-foreground">
              Use presets above or add custom values. For custom ranges, enter "min-max" format (e.g., "1-10").
            </p>

            {/* Auto-expand threshold for schema evolution */}
            {formData.allowed_values.length > 0 && (
              <div className="mt-3 p-3 bg-muted/50 rounded-md">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-1">
                    <Label htmlFor="auto_expand" className="text-sm">
                      Auto-expand threshold
                    </Label>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Info className="h-3 w-3 text-muted-foreground cursor-help" />
                      </TooltipTrigger>
                      <TooltipContent side="right" className="max-w-xs">
                        <p>Automatically add new values to allowed_values when they appear in at least this many documents. Set to 0 to disable auto-expansion.</p>
                      </TooltipContent>
                    </Tooltip>
                  </div>
                  <div className="flex items-center gap-2">
                    <Input
                      id="auto_expand"
                      type="number"
                      min={0}
                      max={10}
                      value={formData.auto_expand_threshold}
                      onChange={(e) => handleChange('auto_expand_threshold', e.target.value)}
                      disabled={loading}
                      className="w-16 h-8 text-center"
                    />
                    <span className="text-xs text-muted-foreground">
                      {formData.auto_expand_threshold === 0 ? 'Disabled' : `${formData.auto_expand_threshold}+ docs`}
                    </span>
                  </div>
                </div>
              </div>
            )}
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
        </ScrollArea>

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
