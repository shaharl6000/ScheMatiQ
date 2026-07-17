import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Plus, Trash2, Pencil, Loader2, X, FileJson, List, Info, Upload, Cloud, Check, AlertCircle, Lock, Sparkles, HelpCircle, Copy } from 'lucide-react';
import { useDropzone } from 'react-dropzone';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';
import { Checkbox } from '@/components/ui/checkbox';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

import { InitialSchemaColumn } from '../../types';
import { schematiqAPI, cloudAPI } from '../../services/api';
import {
  DATE_PRESET_BUTTONS,
  formatConstraintBadgeDisplay,
} from '@/utils/columnConstraintLabels';

type SchemaSource = 'none' | 'file' | 'manual';

// Sample schema shown in the "See example" popover next to file-upload options.
// Demonstrates both fully-specified and name-only columns, no `locked` field
// (the default is locked=true when imported).
const EXAMPLE_SCHEMA_JSON = `[
  {
    "name": "organism",
    "definition": "The organism or species from which the protein originates.",
    "rationale": "Provides biological context for the NES analysis."
  },
  {
    "name": "Gene Symbol"
  }
]`;

interface SchemaFile {
  value: string;
  label: string;
  columns_count: number;
  preview: string;
  columns: {
    name: string;
    definition: string;
    rationale: string;
    allowed_values?: string[];
  }[];
}

interface InitialSchemaEditorProps {
  onSchemaChange: (
    schemaPath: string | undefined,
    schemaData: InitialSchemaColumn[] | undefined
  ) => void;
}

interface CloudSchema {
  name: string;
  path: string;
  file_type: string;
  columns_count: number;
  preview: string;
  columns: {
    name: string;
    definition: string;
    rationale: string;
    allowed_values?: string[];
  }[];
}

const ExampleSchemaPopover: React.FC = () => {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(EXAMPLE_SCHEMA_JSON);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard unavailable — silently ignore.
    }
  };

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          className="text-xs text-muted-foreground hover:text-foreground gap-1"
        >
          <HelpCircle className="h-3.5 w-3.5" />
          See example
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-96 space-y-2">
        <p className="text-sm font-medium">Example schema file</p>
        <p className="text-xs text-muted-foreground">
          Upload a JSON array of columns. Only <code className="text-foreground">name</code> is required.{' '}
          <code className="text-foreground">definition</code> and <code className="text-foreground">rationale</code> are optional.
          Any field you do not provide will be filled in by the AI during discovery.
        </p>
        <div className="relative">
          <pre className="text-xs bg-muted rounded p-2 overflow-x-auto max-h-60">
            <code>{EXAMPLE_SCHEMA_JSON}</code>
          </pre>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="absolute top-1 right-1 h-7 w-7"
            onClick={handleCopy}
            aria-label="Copy example"
          >
            {copied ? <Check className="h-3.5 w-3.5 text-green-500" /> : <Copy className="h-3.5 w-3.5" />}
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  );
};

const InitialSchemaEditor: React.FC<InitialSchemaEditorProps> = ({ onSchemaChange }) => {
  const [source, setSource] = useState<SchemaSource>('none');
  const [schemaFiles, setSchemaFiles] = useState<SchemaFile[]>([]);
  const [loadingFiles, setLoadingFiles] = useState(false);
  const [selectedFile, setSelectedFile] = useState<string>('');
  const [columns, setColumns] = useState<InitialSchemaColumn[]>([]);

  // File mode tab state
  const [fileTab, setFileTab] = useState<'upload' | 'cloud'>('cloud');

  // Cloud schemas state
  const [cloudSchemas, setCloudSchemas] = useState<CloudSchema[]>([]);
  const [loadingCloudSchemas, setLoadingCloudSchemas] = useState(false);
  const [selectedCloudSchema, setSelectedCloudSchema] = useState<string>('');

  // File upload state. On successful parse, columns are imported into `columns` and
  // the source switches to 'manual' — these are only used during the brief upload step.
  const [uploadedFile, setUploadedFile] = useState<File | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [uploadSuccess, setUploadSuccess] = useState(false);

  // Column editor dialog state
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingIndex, setEditingIndex] = useState<number | null>(null);
  const [formData, setFormData] = useState({
    name: '',
    definition: '',
    rationale: '',
    allowed_values: [] as string[],
    locked: true
  });
  const [newAllowedValue, setNewAllowedValue] = useState('');

  // Fetch cloud schemas when file mode is selected
  useEffect(() => {
    if (source === 'file') {
      if (cloudSchemas.length === 0) {
        fetchCloudSchemas();
      }
      if (schemaFiles.length === 0) {
        fetchSchemaFiles();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [source]);

  // Keep the latest onSchemaChange in a ref so the notify effect below does not
  // depend on it. The parent (AdvancedSettingsFields) passes a brand-new inline
  // callback on every render; listing it as a dependency made this effect fire
  // on every render and call the parent's updateAdvanced, which resets the New
  // Project dialog's `estimate`/`startConfirmed` state each time. That reset
  // trapped startProject in its "estimate first" branch forever, so the Start
  // button appeared to do nothing whenever pre-defined columns were used.
  const onSchemaChangeRef = useRef(onSchemaChange);
  useEffect(() => {
    onSchemaChangeRef.current = onSchemaChange;
  }, [onSchemaChange]);

  // Notify parent when schema changes. File-uploaded and cloud-selected columns
  // are imported into `columns` (the unified editable list), so the parent only
  // needs to watch `source` and `columns`.
  useEffect(() => {
    if (source === 'none' || columns.length === 0) {
      onSchemaChangeRef.current(undefined, undefined);
    } else {
      onSchemaChangeRef.current(undefined, columns);
    }
  }, [source, columns]);

  // Import a batch of columns into the unified editable list and switch to manual mode.
  // Used by both file-upload and cloud-schema-select paths.
  const importColumns = useCallback((incoming: InitialSchemaColumn[]) => {
    const normalized = incoming.map(col => ({
      name: col.name,
      definition: col.definition ?? '',
      rationale: col.rationale ?? '',
      allowed_values: col.allowed_values,
      // Imported columns default to locked=true unless the source explicitly says false.
      locked: col.locked ?? true,
    }));
    setColumns(normalized);
    setSource('manual');
  }, []);

  const fetchSchemaFiles = async () => {
    setLoadingFiles(true);
    try {
      const files = await schematiqAPI.getSchemaFiles();
      setSchemaFiles(files);
    } catch (error) {
      console.error('Failed to fetch schema files:', error);
    } finally {
      setLoadingFiles(false);
    }
  };

  const fetchCloudSchemas = async () => {
    setLoadingCloudSchemas(true);
    try {
      const schemas = await cloudAPI.getInitialSchemas();
      setCloudSchemas(schemas);
    } catch (error) {
      console.error('Failed to fetch cloud schemas:', error);
    } finally {
      setLoadingCloudSchemas(false);
    }
  };

  // File upload handler. On success, columns are imported into the unified editable
  // list and the source switches to 'manual' so the user can edit them per-row.
  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    const file = acceptedFiles[0];
    if (!file) return;

    setUploadError(null);
    setUploadSuccess(false);
    setUploadedFile(file);

    if (!file.name.endsWith('.json')) {
      setUploadError('File must be a JSON file (.json)');
      return;
    }

    try {
      const text = await file.text();
      const data = JSON.parse(text);

      let parsedColumns: InitialSchemaColumn[] = [];
      if (Array.isArray(data)) {
        parsedColumns = data;
      } else if (data && typeof data === 'object' && 'columns' in data) {
        parsedColumns = data.columns;
      } else {
        setUploadError('Schema must be a JSON array of columns or an object with a "columns" key');
        return;
      }

      for (let i = 0; i < parsedColumns.length; i++) {
        if (!parsedColumns[i].name) {
          setUploadError(`Column ${i + 1} must have a 'name' field`);
          return;
        }
      }

      if (parsedColumns.length === 0) {
        setUploadError('Schema must contain at least one column');
        return;
      }

      importColumns(parsedColumns);
      setUploadSuccess(true);
    } catch (e) {
      if (e instanceof SyntaxError) {
        setUploadError('Invalid JSON file');
      } else {
        setUploadError('Failed to parse schema file');
      }
    }
  }, [importColumns]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/json': ['.json']
    },
    maxFiles: 1
  });

  const handleCloudSchemaSelect = (schemaName: string) => {
    setSelectedCloudSchema(schemaName);
    const schema = cloudSchemas.find(s => s.name === schemaName);
    if (schema) {
      importColumns(schema.columns as InitialSchemaColumn[]);
    }
  };

  const clearUploadedFile = () => {
    setUploadedFile(null);
    setUploadError(null);
    setUploadSuccess(false);
  };

  const handleSourceChange = (value: SchemaSource) => {
    setSource(value);
    if (value !== 'file') {
      setSelectedFile('');
      setSelectedCloudSchema('');
      clearUploadedFile();
    }
    if (value !== 'manual') {
      setColumns([]);
    }
  };

  const handleFileSelect = (filePath: string) => {
    setSelectedFile(filePath);
  };

  const openAddDialog = () => {
    setEditingIndex(null);
    setFormData({ name: '', definition: '', rationale: '', allowed_values: [], locked: true });
    setNewAllowedValue('');
    setDialogOpen(true);
  };

  const openEditDialog = (index: number) => {
    const col = columns[index];
    setEditingIndex(index);
    setFormData({
      name: col.name,
      definition: col.definition ?? '',
      rationale: col.rationale ?? '',
      allowed_values: col.allowed_values || [],
      locked: col.locked ?? true,
    });
    setNewAllowedValue('');
    setDialogOpen(true);
  };

  const handleSaveColumn = () => {
    if (!formData.name.trim()) {
      return;
    }

    const newColumn: InitialSchemaColumn = {
      name: formData.name.trim(),
      definition: formData.definition.trim(),
      rationale: formData.rationale.trim(),
      allowed_values: formData.allowed_values.length > 0 ? formData.allowed_values : undefined,
      locked: formData.locked,
    };

    if (editingIndex !== null) {
      setColumns(prev => prev.map((col, i) => i === editingIndex ? newColumn : col));
    } else {
      setColumns(prev => [...prev, newColumn]);
    }

    setDialogOpen(false);
  };

  const handleDeleteColumn = (index: number) => {
    setColumns(prev => prev.filter((_, i) => i !== index));
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

  return (
    <div className="space-y-4">
      {/* Source Selection */}
      <RadioGroup
        value={source}
        onValueChange={(v: string) => handleSourceChange(v as SchemaSource)}
        className="flex flex-wrap gap-4"
      >
        <div className="flex items-center space-x-2">
          <RadioGroupItem value="none" id="schema-none" />
          <Label htmlFor="schema-none" className="font-normal cursor-pointer">None</Label>
        </div>
        <div className="flex items-center space-x-2">
          <RadioGroupItem value="file" id="schema-file" />
          <Label htmlFor="schema-file" className="font-normal cursor-pointer flex items-center gap-1">
            <FileJson className="h-4 w-4" />
            Load from File
          </Label>
        </div>
        <div className="flex items-center space-x-2">
          <RadioGroupItem value="manual" id="schema-manual" />
          <Label htmlFor="schema-manual" className="font-normal cursor-pointer flex items-center gap-1">
            <List className="h-4 w-4" />
            Manual Entry
          </Label>
        </div>
      </RadioGroup>

      {/* File Selection with Tabs */}
      {source === 'file' && (
        <Tabs value={fileTab} onValueChange={(v) => setFileTab(v as 'upload' | 'cloud')} className="w-full">
          <div className="flex items-center justify-between gap-2">
            <TabsList className="grid grid-cols-2 flex-1">
              <TabsTrigger value="cloud" className="flex items-center gap-2">
                <Cloud className="h-4 w-4" />
                From Cloud
              </TabsTrigger>
              <TabsTrigger value="upload" className="flex items-center gap-2">
                <Upload className="h-4 w-4" />
                Upload File
              </TabsTrigger>
            </TabsList>
            <ExampleSchemaPopover />
          </div>

          {/* Cloud Schema Tab — selection imports immediately into the editable list. */}
          <TabsContent value="cloud" className="space-y-3 mt-4">
            <Select
              value={selectedCloudSchema}
              onValueChange={handleCloudSchemaSelect}
              disabled={loadingCloudSchemas}
            >
              <SelectTrigger>
                {loadingCloudSchemas ? (
                  <div className="flex items-center gap-2">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Loading schemas from cloud...
                  </div>
                ) : (
                  <SelectValue placeholder="Select a schema from cloud storage..." />
                )}
              </SelectTrigger>
              <SelectContent>
                {cloudSchemas.map((schema) => (
                  <SelectItem key={schema.name} value={schema.name}>
                    <div className="flex flex-col">
                      <span className="font-medium">{schema.name}</span>
                      <span className="text-xs text-muted-foreground">
                        {schema.columns_count} columns: {schema.preview}
                      </span>
                    </div>
                  </SelectItem>
                ))}
                {cloudSchemas.length === 0 && !loadingCloudSchemas && (
                  <SelectItem value="__empty__" disabled>
                    No schemas found in cloud storage
                  </SelectItem>
                )}
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">
              Selecting a schema loads its columns into the editable list below — you can edit any field afterwards.
            </p>
          </TabsContent>

          {/* Upload File Tab — successful upload imports columns into the editable list. */}
          <TabsContent value="upload" className="space-y-3 mt-4">
            <div
              {...getRootProps()}
              className={`
                border-2 border-dashed rounded-lg p-6 text-center cursor-pointer
                transition-colors duration-200
                ${isDragActive
                  ? 'border-primary bg-primary/5'
                  : 'border-muted-foreground/25 hover:border-primary/50 hover:bg-muted/50'
                }
              `}
            >
              <input {...getInputProps()} />
              <Upload className="h-8 w-8 mx-auto mb-2 text-muted-foreground" />
              <p className="text-sm font-medium">
                {isDragActive ? 'Drop the file here...' : 'Drag & drop a schema file'}
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                or click to browse (JSON files only)
              </p>
            </div>

            {/* Error message — shown when parsing fails. On success we auto-switch to manual mode. */}
            {uploadError && (
              <div className="flex items-center gap-2 p-3 bg-destructive/10 rounded-lg text-destructive text-sm">
                <AlertCircle className="h-4 w-4" />
                {uploadError}
              </div>
            )}
            {uploadedFile && !uploadError && !uploadSuccess && (
              <p className="text-xs text-muted-foreground">Parsing {uploadedFile.name}...</p>
            )}
          </TabsContent>
        </Tabs>
      )}

      {/* Manual Entry */}
      {source === 'manual' && (
        <div className="space-y-3">
          {/* Column List */}
          {columns.length > 0 && (
            <div className="space-y-2">
              {columns.map((col, idx) => {
                const willAutoComplete = !col.definition?.trim() || !col.rationale?.trim();
                return (
                <Card key={idx}>
                  <CardContent className="py-3 px-4">
                    <div className="flex items-start justify-between gap-2">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                          <span className="font-medium">{col.name}</span>
                          {col.locked && (
                            <Badge variant="outline" className="text-xs gap-1">
                              <Lock className="h-3 w-3" />
                              locked
                            </Badge>
                          )}
                          {willAutoComplete && (
                            <Badge variant="secondary" className="text-xs gap-1">
                              <Sparkles className="h-3 w-3" />
                              AI will fill blanks
                            </Badge>
                          )}
                          {col.allowed_values && col.allowed_values.length > 0 && (
                            <Badge variant="outline" className="text-xs">
                              {col.allowed_values.join(', ')}
                            </Badge>
                          )}
                        </div>
                        <p className="text-sm text-muted-foreground mt-1 line-clamp-2">
                          {col.definition || <span className="italic">Optional — AI will fill this in if left blank</span>}
                        </p>
                      </div>
                      <div className="flex gap-1">
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => openEditDialog(idx)}
                        >
                          <Pencil className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleDeleteColumn(idx)}
                        >
                          <Trash2 className="h-4 w-4 text-destructive" />
                        </Button>
                      </div>
                    </div>
                  </CardContent>
                </Card>
                );
              })}
            </div>
          )}

          {/* Add Column Button */}
          <Button variant="outline" onClick={openAddDialog} className="w-full">
            <Plus className="h-4 w-4 mr-2" />
            Add Column
          </Button>
        </div>
      )}

      {/* Column Editor Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="sm:max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {editingIndex !== null ? <Pencil className="h-4 w-4" /> : <Plus className="h-4 w-4" />}
              {editingIndex !== null ? 'Edit Column' : 'Add Column'}
            </DialogTitle>
            <DialogDescription className="space-y-2 text-left">
              <span className="block">
                Only <span className="font-medium text-foreground">name</span> is required. Definition and rationale are
                optional — the AI will fill them in during discovery if left blank.
              </span>
              <span className="block text-muted-foreground">
                After discovery runs, use <span className="font-medium text-foreground">Re-extract Data</span> on the
                Visualize page whenever you add or change columns so the table is filled from your documents.
              </span>
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-4">
            {/* Column Name */}
            <div className="space-y-2">
              <Label htmlFor="col-name">
                Column Name <span className="text-destructive">*</span>
              </Label>
              <Input
                id="col-name"
                value={formData.name}
                onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
                placeholder="e.g., protein_name"
              />
            </div>

            {/* Definition */}
            <div className="space-y-2">
              <Label htmlFor="col-definition">Definition</Label>
              <Textarea
                id="col-definition"
                value={formData.definition}
                onChange={(e) => setFormData(prev => ({ ...prev, definition: e.target.value }))}
                placeholder="Optional — AI will fill this in if left blank"
                rows={8}
                className="min-h-[168px] resize-y"
              />
            </div>

            {/* Rationale */}
            <div className="space-y-2">
              <Label htmlFor="col-rationale">Rationale</Label>
              <Textarea
                id="col-rationale"
                value={formData.rationale}
                onChange={(e) => setFormData(prev => ({ ...prev, rationale: e.target.value }))}
                placeholder="Optional — AI will fill this in if left blank"
                rows={4}
                className="min-h-[88px] resize-y"
              />
            </div>

            {/* Locked checkbox */}
            <div className="flex items-start gap-2 p-3 bg-muted/40 rounded-md">
              <Checkbox
                id="col-locked"
                checked={formData.locked}
                onCheckedChange={(checked) =>
                  setFormData(prev => ({ ...prev, locked: checked === true }))
                }
                className="mt-0.5"
              />
              <div className="space-y-0.5">
                <Label htmlFor="col-locked" className="cursor-pointer flex items-center gap-1.5 font-medium">
                  <Lock className="h-3.5 w-3.5" />
                  Keep this column
                </Label>
                <p className="text-xs text-muted-foreground">
                  Locked columns are preserved through schema discovery — they will not be dropped or renamed.
                </p>
              </div>
            </div>

            {/* Allowed Values */}
            <div className="space-y-2">
              <div className="flex items-center gap-1">
                <Label htmlFor="col-allowed">Value Constraints (Optional)</Label>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Info className="h-4 w-4 text-muted-foreground cursor-help" />
                  </TooltipTrigger>
                  <TooltipContent side="right" className="max-w-xs">
                    <p>
                      Optional: categories, numbers, ranges, or one date style (use the date buttons for examples).
                      Leave empty for plain text.
                    </p>
                  </TooltipContent>
                </Tooltip>
              </div>

              {/* Preset buttons */}
              <div className="flex flex-wrap gap-2">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => setFormData(prev => ({ ...prev, allowed_values: ['yes', 'no'] }))}
                  className="text-xs"
                >
                  Yes/No
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => setFormData(prev => ({ ...prev, allowed_values: ['number'] }))}
                  className="text-xs"
                >
                  Number
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => setFormData(prev => ({ ...prev, allowed_values: ['0-100'] }))}
                  className="text-xs"
                >
                  0-100 (%)
                </Button>
                {DATE_PRESET_BUTTONS.map(({ token, label, title }) => (
                  <Button
                    key={token}
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => setFormData(prev => ({ ...prev, allowed_values: [token] }))}
                    className="text-xs"
                    title={title}
                  >
                    {label}
                  </Button>
                ))}
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => setFormData(prev => ({ ...prev, allowed_values: [] }))}
                  className="text-xs text-muted-foreground"
                >
                  Clear
                </Button>
              </div>

              {/* Display existing values */}
              {formData.allowed_values.length > 0 && (
                <div className="flex flex-wrap gap-2 p-2 bg-muted/50 rounded-md">
                  {formData.allowed_values.map((value, index) => (
                    <Badge key={index} variant="secondary" className="gap-1 pr-1">
                      {formatConstraintBadgeDisplay(value)}
                      <button
                        type="button"
                        onClick={() => handleRemoveAllowedValue(index)}
                        className="ml-1 hover:bg-destructive/20 rounded-full p-0.5"
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </Badge>
                  ))}
                </div>
              )}

              {/* Add new value */}
              <div className="flex gap-2">
                <Input
                  id="col-allowed"
                  placeholder='Add category or range (e.g. "1-10")...'
                  value={newAllowedValue}
                  onChange={(e) => setNewAllowedValue(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && (e.preventDefault(), handleAddAllowedValue())}
                  className="flex-1"
                />
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={handleAddAllowedValue}
                  disabled={!newAllowedValue.trim()}
                >
                  <Plus className="h-4 w-4" />
                </Button>
              </div>
            </div>
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={handleSaveColumn}
              disabled={!formData.name.trim()}
            >
              {editingIndex !== null ? 'Save Changes' : 'Add Column'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default InitialSchemaEditor;
