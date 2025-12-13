import React, { useState, useEffect, useCallback } from 'react';
import { GitMerge, ArrowDown, Columns, Loader2, X } from 'lucide-react';

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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Card } from '@/components/ui/card';

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

  const generateMergePreview = useCallback((selectedColumns: string[]) => {
    if (selectedColumns.length === 0) return;

    const selectedColumnObjs = columns.filter(col => selectedColumns.includes(col.name));

    const suggestedName = selectedColumns.join('_');
    setTargetColumn(suggestedName);

    const definitions = selectedColumnObjs.map(col => col.definition).filter(Boolean);
    const rationales = selectedColumnObjs.map(col => col.rationale).filter(Boolean);

    if (definitions.length > 0) {
      setDefinition(`Combined column containing: ${definitions.join(separator)}`);
    }

    if (rationales.length > 0) {
      setRationale(`Merged from multiple columns to consolidate related information: ${rationales.join(separator)}`);
    }
  }, [columns, separator]);

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

      if (preselectedColumns.length > 0) {
        generateMergePreview(preselectedColumns);
      }
    }
  }, [open, preselectedColumns, generateMergePreview]);

  const validateForm = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (sourceColumns.length < 2) {
      newErrors.sourceColumns = 'Select at least 2 columns to merge';
    }

    if (!targetColumn.trim()) {
      newErrors.targetColumn = 'Target column name is required';
    } else {
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

      await schemaAPI.mergeColumns(sessionId, request);
      onSuccess(`Merged ${sourceColumns.length} columns into "${targetColumn}" - processing started`);
      onClose();
    } catch (error: any) {
      console.error('Merge operation failed:', error);
      onError(error.response?.data?.detail || 'Failed to merge columns');
    } finally {
      setLoading(false);
    }
  };

  const handleRemoveColumn = (colName: string) => {
    const newColumns = sourceColumns.filter(c => c !== colName);
    setSourceColumns(newColumns);
    if (newColumns.length > 0) {
      generateMergePreview(newColumns);
    } else {
      setTargetColumn('');
      setDefinition('');
      setRationale('');
    }
  };

  const handleAddColumn = (colName: string) => {
    if (!sourceColumns.includes(colName)) {
      const newColumns = [...sourceColumns, colName];
      setSourceColumns(newColumns);
      generateMergePreview(newColumns);
    }
  };

  const selectedStrategy = MERGE_STRATEGIES.find(s => s.value === mergeStrategy);
  const availableColumns = columns.filter(col => !col.name.endsWith('_excerpt') && !sourceColumns.includes(col.name));

  return (
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && !loading && onClose()}>
      <DialogContent className="sm:max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <GitMerge className="h-5 w-5" />
            Merge Columns
          </DialogTitle>
          <DialogDescription>
            Combine multiple columns into a single column
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 py-4">
          {/* Source Columns Selection */}
          <div className="space-y-3">
            <h4 className="font-semibold">1. Select Columns to Merge</h4>

            <div className="space-y-2">
              <Label>Selected Columns</Label>
              <div className="flex flex-wrap gap-2 min-h-[40px] p-2 border rounded-md bg-muted/50">
                {sourceColumns.length === 0 ? (
                  <span className="text-muted-foreground text-sm">No columns selected</span>
                ) : (
                  sourceColumns.map((colName) => (
                    <Badge key={colName} variant="secondary" className="gap-1">
                      {colName}
                      <button
                        onClick={() => handleRemoveColumn(colName)}
                        className="ml-1 hover:text-destructive"
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </Badge>
                  ))
                )}
              </div>
              {errors.sourceColumns && (
                <p className="text-sm text-destructive">{errors.sourceColumns}</p>
              )}
            </div>

            <div className="space-y-2">
              <Label>Add Column</Label>
              <Select onValueChange={handleAddColumn}>
                <SelectTrigger>
                  <SelectValue placeholder="Select a column to add..." />
                </SelectTrigger>
                <SelectContent>
                  {availableColumns.map((col) => (
                    <SelectItem key={col.name} value={col.name}>
                      {col.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {sourceColumns.length > 0 && (
            <>
              <Separator />

              {/* Merge Strategy */}
              <div className="space-y-3">
                <h4 className="font-semibold">2. Choose Merge Strategy</h4>
                <div className="space-y-2">
                  <Label>Merge Strategy</Label>
                  <Select
                    value={mergeStrategy}
                    onValueChange={(value: 'concatenate' | 'smart_merge' | 'first_non_empty') => setMergeStrategy(value)}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {MERGE_STRATEGIES.map((strategy) => (
                        <SelectItem key={strategy.value} value={strategy.value}>
                          <div>
                            <div className="font-medium">{strategy.label}</div>
                            <div className="text-xs text-muted-foreground">{strategy.description}</div>
                          </div>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {mergeStrategy === 'concatenate' && (
                  <div className="space-y-2">
                    <Label>Separator</Label>
                    <Input
                      value={separator}
                      onChange={(e) => setSeparator(e.target.value)}
                      placeholder="Characters to separate merged values"
                    />
                    {errors.separator && (
                      <p className="text-sm text-destructive">{errors.separator}</p>
                    )}
                  </div>
                )}

                {selectedStrategy && (
                  <Alert variant="info">
                    <AlertDescription>
                      <strong>{selectedStrategy.label}:</strong> {selectedStrategy.description}
                    </AlertDescription>
                  </Alert>
                )}
              </div>

              <Separator />

              {/* Target Column Configuration */}
              <div className="space-y-3">
                <h4 className="font-semibold">3. Configure Target Column</h4>

                <div className="space-y-2">
                  <Label>Target Column Name <span className="text-destructive">*</span></Label>
                  <Input
                    value={targetColumn}
                    onChange={(e) => setTargetColumn(e.target.value)}
                    placeholder="Name for the merged column"
                  />
                  {errors.targetColumn && (
                    <p className="text-sm text-destructive">{errors.targetColumn}</p>
                  )}
                </div>

                <div className="space-y-2">
                  <Label>Definition <span className="text-destructive">*</span></Label>
                  <Textarea
                    value={definition}
                    onChange={(e) => setDefinition(e.target.value)}
                    rows={3}
                    placeholder="Describe what the merged column represents"
                  />
                  {errors.definition && (
                    <p className="text-sm text-destructive">{errors.definition}</p>
                  )}
                </div>

                <div className="space-y-2">
                  <Label>Rationale <span className="text-destructive">*</span></Label>
                  <Textarea
                    value={rationale}
                    onChange={(e) => setRationale(e.target.value)}
                    rows={3}
                    placeholder="Explain why merging these columns is beneficial"
                  />
                  {errors.rationale && (
                    <p className="text-sm text-destructive">{errors.rationale}</p>
                  )}
                </div>
              </div>

              <Separator />

              {/* Preview */}
              <div className="space-y-3">
                <h4 className="font-semibold">4. Merge Preview</h4>
                <Card className="p-4">
                  <div className="space-y-3">
                    <div>
                      <p className="text-sm font-medium text-primary mb-2">Source Columns:</p>
                      <div className="flex flex-wrap gap-1">
                        {sourceColumns.map((colName) => (
                          <Badge key={colName} variant="outline" className="gap-1">
                            <Columns className="h-3 w-3" />
                            {colName}
                          </Badge>
                        ))}
                      </div>
                    </div>

                    <ArrowDown className="mx-auto text-primary" />

                    <div>
                      <p className="text-sm font-medium text-primary mb-2">Target Column:</p>
                      <Badge className="gap-1">
                        <GitMerge className="h-3 w-3" />
                        {targetColumn || 'unnamed'}
                      </Badge>
                    </div>

                    <p className="text-xs text-muted-foreground">
                      Strategy: {selectedStrategy?.label}
                      {mergeStrategy === 'concatenate' && ` | Separator: "${separator}"`}
                      {' | Source columns will be removed after successful merge'}
                    </p>
                  </div>
                </Card>
              </div>
            </>
          )}

          <Alert variant="warning">
            <AlertDescription>
              <strong>Important:</strong> Merging columns will trigger document reprocessing to generate new values for the merged column. The source columns will be removed from the schema. This operation cannot be easily undone - consider creating a backup first.
            </AlertDescription>
          </Alert>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={loading}>
            Cancel
          </Button>
          <Button
            onClick={handleSubmit}
            disabled={loading || sourceColumns.length < 2}
          >
            {loading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Merging...
              </>
            ) : (
              <>
                <GitMerge className="mr-2 h-4 w-4" />
                Merge {sourceColumns.length} Columns
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default MergeDialog;
