import React, { useState, useEffect, useCallback } from 'react';
import { Plus, Trash2, Pencil, Loader2, X, FileJson, List, Info, Upload, Cloud, Check, AlertCircle } from 'lucide-react';
import { useDropzone } from 'react-dropzone';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';
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
import { qbsdAPI, cloudAPI } from '../../services/api';

type SchemaSource = 'none' | 'file' | 'manual';

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

  // File upload state
  const [uploadedFile, setUploadedFile] = useState<File | null>(null);
  const [uploadedSchemaData, setUploadedSchemaData] = useState<InitialSchemaColumn[] | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadSuccess, setUploadSuccess] = useState(false);

  // Column editor dialog state
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingIndex, setEditingIndex] = useState<number | null>(null);
  const [formData, setFormData] = useState({
    name: '',
    definition: '',
    rationale: '',
    allowed_values: [] as string[]
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

  // Notify parent when schema changes
  useEffect(() => {
    if (source === 'none') {
      onSchemaChange(undefined, undefined);
    } else if (source === 'file') {
      if (fileTab === 'cloud' && selectedCloudSchema) {
        // Find the selected cloud schema and pass its columns as data
        const schema = cloudSchemas.find(s => s.name === selectedCloudSchema);
        if (schema) {
          onSchemaChange(undefined, schema.columns as InitialSchemaColumn[]);
        } else {
          onSchemaChange(undefined, undefined);
        }
      } else if (fileTab === 'upload' && uploadedSchemaData) {
        onSchemaChange(undefined, uploadedSchemaData);
      } else {
        onSchemaChange(undefined, undefined);
      }
    } else if (source === 'manual' && columns.length > 0) {
      onSchemaChange(undefined, columns);
    } else {
      onSchemaChange(undefined, undefined);
    }
  }, [source, fileTab, selectedCloudSchema, uploadedSchemaData, columns, cloudSchemas, onSchemaChange]);

  const fetchSchemaFiles = async () => {
    setLoadingFiles(true);
    try {
      const files = await qbsdAPI.getSchemaFiles();
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

  // File upload handler
  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    const file = acceptedFiles[0];
    if (!file) return;

    setUploadError(null);
    setUploadSuccess(false);
    setUploadedFile(file);
    setUploadedSchemaData(null);

    // Validate file extension
    if (!file.name.endsWith('.json')) {
      setUploadError('File must be a JSON file (.json)');
      return;
    }

    // Read and parse the file locally
    try {
      const text = await file.text();
      const data = JSON.parse(text);

      // Validate schema structure
      let columns: InitialSchemaColumn[] = [];
      if (Array.isArray(data)) {
        columns = data;
      } else if (data && typeof data === 'object' && 'columns' in data) {
        columns = data.columns;
      } else {
        setUploadError('Schema must be a JSON array of columns or an object with a "columns" key');
        return;
      }

      // Validate columns have required fields
      for (let i = 0; i < columns.length; i++) {
        const col = columns[i];
        if (!col.name || !col.definition || !col.rationale) {
          setUploadError(`Column ${i + 1} must have 'name', 'definition', and 'rationale' fields`);
          return;
        }
      }

      if (columns.length === 0) {
        setUploadError('Schema must contain at least one column');
        return;
      }

      setUploadedSchemaData(columns);
      setUploadSuccess(true);
    } catch (e) {
      if (e instanceof SyntaxError) {
        setUploadError('Invalid JSON file');
      } else {
        setUploadError('Failed to parse schema file');
      }
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/json': ['.json']
    },
    maxFiles: 1
  });

  const handleCloudSchemaSelect = (schemaName: string) => {
    setSelectedCloudSchema(schemaName);
  };

  const clearUploadedFile = () => {
    setUploadedFile(null);
    setUploadedSchemaData(null);
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
    setFormData({ name: '', definition: '', rationale: '', allowed_values: [] });
    setNewAllowedValue('');
    setDialogOpen(true);
  };

  const openEditDialog = (index: number) => {
    const col = columns[index];
    setEditingIndex(index);
    setFormData({
      name: col.name,
      definition: col.definition,
      rationale: col.rationale,
      allowed_values: col.allowed_values || []
    });
    setNewAllowedValue('');
    setDialogOpen(true);
  };

  const handleSaveColumn = () => {
    if (!formData.name.trim() || !formData.definition.trim() || !formData.rationale.trim()) {
      return;
    }

    const newColumn: InitialSchemaColumn = {
      name: formData.name.trim(),
      definition: formData.definition.trim(),
      rationale: formData.rationale.trim(),
      allowed_values: formData.allowed_values.length > 0 ? formData.allowed_values : undefined
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
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="cloud" className="flex items-center gap-2">
              <Cloud className="h-4 w-4" />
              From Cloud
            </TabsTrigger>
            <TabsTrigger value="upload" className="flex items-center gap-2">
              <Upload className="h-4 w-4" />
              Upload File
            </TabsTrigger>
          </TabsList>

          {/* Cloud Schema Tab */}
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

            {/* Preview selected cloud schema */}
            {selectedCloudSchema && (
              <Card>
                <CardContent className="pt-4">
                  {(() => {
                    const schema = cloudSchemas.find(s => s.name === selectedCloudSchema);
                    if (!schema) return null;
                    return (
                      <>
                        <p className="text-sm font-medium mb-2">
                          Schema Preview ({schema.columns_count} columns)
                        </p>
                        <div className="space-y-2 max-h-60 overflow-y-auto">
                          {schema.columns.map((col, idx) => (
                            <div key={idx} className="text-sm p-2 bg-muted/50 rounded">
                              <div className="flex items-center gap-2">
                                <span className="font-medium">{col.name}</span>
                                {col.allowed_values && col.allowed_values.length > 0 && (
                                  <Badge variant="outline" className="text-xs">
                                    {col.allowed_values.join(', ')}
                                  </Badge>
                                )}
                              </div>
                              <p className="text-muted-foreground text-xs mt-1 line-clamp-1">
                                {col.definition}
                              </p>
                            </div>
                          ))}
                        </div>
                      </>
                    );
                  })()}
                </CardContent>
              </Card>
            )}
          </TabsContent>

          {/* Upload File Tab */}
          <TabsContent value="upload" className="space-y-3 mt-4">
            {!uploadedFile ? (
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
            ) : (
              <div className="space-y-3">
                {/* Uploaded file info */}
                <Card>
                  <CardContent className="py-3 px-4">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        <FileJson className="h-5 w-5 text-primary" />
                        <div>
                          <p className="font-medium text-sm">{uploadedFile.name}</p>
                          <p className="text-xs text-muted-foreground">
                            {uploadedSchemaData
                              ? `${uploadedSchemaData.length} columns`
                              : 'Parsing...'
                            }
                          </p>
                        </div>
                        {uploadSuccess && (
                          <Check className="h-4 w-4 text-green-500" />
                        )}
                      </div>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={clearUploadedFile}
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </div>
                  </CardContent>
                </Card>

                {/* Error message */}
                {uploadError && (
                  <div className="flex items-center gap-2 p-3 bg-destructive/10 rounded-lg text-destructive text-sm">
                    <AlertCircle className="h-4 w-4" />
                    {uploadError}
                  </div>
                )}

                {/* Preview uploaded schema */}
                {uploadedSchemaData && (
                  <Card>
                    <CardContent className="pt-4">
                      <p className="text-sm font-medium mb-2">
                        Schema Preview ({uploadedSchemaData.length} columns)
                      </p>
                      <div className="space-y-2 max-h-60 overflow-y-auto">
                        {uploadedSchemaData.map((col, idx) => (
                          <div key={idx} className="text-sm p-2 bg-muted/50 rounded">
                            <div className="flex items-center gap-2">
                              <span className="font-medium">{col.name}</span>
                              {col.allowed_values && col.allowed_values.length > 0 && (
                                <Badge variant="outline" className="text-xs">
                                  {col.allowed_values.join(', ')}
                                </Badge>
                              )}
                            </div>
                            <p className="text-muted-foreground text-xs mt-1 line-clamp-1">
                              {col.definition}
                            </p>
                          </div>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>
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
              {columns.map((col, idx) => (
                <Card key={idx}>
                  <CardContent className="py-3 px-4">
                    <div className="flex items-start justify-between gap-2">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="font-medium">{col.name}</span>
                          {col.allowed_values && col.allowed_values.length > 0 && (
                            <Badge variant="outline" className="text-xs">
                              {col.allowed_values.join(', ')}
                            </Badge>
                          )}
                        </div>
                        <p className="text-sm text-muted-foreground mt-1 line-clamp-2">
                          {col.definition}
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
              ))}
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
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {editingIndex !== null ? <Pencil className="h-4 w-4" /> : <Plus className="h-4 w-4" />}
              {editingIndex !== null ? 'Edit Column' : 'Add Column'}
            </DialogTitle>
            <DialogDescription>
              Define a column for the initial schema
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
              <Label htmlFor="col-definition">
                Definition <span className="text-destructive">*</span>
              </Label>
              <Textarea
                id="col-definition"
                value={formData.definition}
                onChange={(e) => setFormData(prev => ({ ...prev, definition: e.target.value }))}
                placeholder="What this column represents..."
                rows={2}
              />
            </div>

            {/* Rationale */}
            <div className="space-y-2">
              <Label htmlFor="col-rationale">
                Rationale <span className="text-destructive">*</span>
              </Label>
              <Textarea
                id="col-rationale"
                value={formData.rationale}
                onChange={(e) => setFormData(prev => ({ ...prev, rationale: e.target.value }))}
                placeholder="Why this column is important..."
                rows={2}
              />
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
                    <p>Define allowed values for categorical columns or constraints like "number" or "0-100".</p>
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
                      {value}
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
                  placeholder="Add allowed value..."
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
              disabled={!formData.name.trim() || !formData.definition.trim() || !formData.rationale.trim()}
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
