import React, { useState, useEffect } from 'react';
import { Upload, Trash2, FileText, CheckCircle2, AlertCircle, AlertTriangle, Cloud, Loader2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Checkbox } from '@/components/ui/checkbox';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { cn } from '@/lib/utils';
import { useFileUpload } from '../../hooks/useFileUpload';
import { formatFileSize } from '../../utils/apiHelpers';
import { cloudAPI } from '../../services/api';

interface CloudDataset {
  dataset: string;
  files: {
    name: string;
    path: string;
    size: number;
    content_type?: string;
  }[];
}

interface DocumentUploadProps {
  onFilesChange: (files: File[]) => void;
  uploadedFiles: File[];
  loading: boolean;
  onUpload: () => void;
  canUpload: boolean;
  uploadResult?: {
    status: string;
    message: string;
    uploaded_files: string[];
    warnings: string[];
  } | null;
  // New props for cloud document selection
  sessionId?: string;
  onCloudDocumentsAdd?: (dataset: string, files: string[]) => Promise<void>;
  // Document limit props
  maxDocuments?: number;
  existingDocumentCount?: number;
}

const DocumentUpload: React.FC<DocumentUploadProps> = ({
  onFilesChange,
  uploadedFiles,
  loading,
  onUpload,
  canUpload,
  uploadResult,
  sessionId,
  onCloudDocumentsAdd,
  maxDocuments,
  existingDocumentCount = 0,
}) => {
  // Cloud datasets state
  const [cloudDatasets, setCloudDatasets] = useState<CloudDataset[]>([]);
  const [cloudLoading, setCloudLoading] = useState(false);
  const [selectedDataset, setSelectedDataset] = useState<string>('');
  const [selectedCloudFiles, setSelectedCloudFiles] = useState<string[]>([]);
  const [cloudAddLoading, setCloudAddLoading] = useState(false);
  const [cloudError, setCloudError] = useState<string | null>(null);
  const [cloudSuccess, setCloudSuccess] = useState<string | null>(null);

  const {
    getRootProps,
    getInputProps,
    isDragActive,
    dragError
  } = useFileUpload({
    allowMultiple: true,
    acceptedTypes: {
      'text/plain': ['.txt'],
      'text/markdown': ['.md'],
      'application/pdf': ['.pdf'],
      'application/msword': ['.doc'],
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ['.docx'],
      'application/rtf': ['.rtf'],
    },
    maxSize: 25 * 1024 * 1024, // 25MB per file
    onFilesSelected: onFilesChange,
    externalFiles: uploadedFiles,
  });

  // Fetch cloud datasets on mount
  useEffect(() => {
    const fetchCloudDatasets = async () => {
      setCloudLoading(true);
      try {
        const data = await cloudAPI.getCloudDocuments();
        setCloudDatasets(data);
        // Auto-select first dataset if available
        if (data.length > 0) {
          setSelectedDataset(data[0].dataset);
        }
      } catch (err) {
        // Cloud datasets not available — expected when none configured
      } finally {
        setCloudLoading(false);
      }
    };
    fetchCloudDatasets();
  }, []);

  const removeFile = (index: number) => {
    const newFiles = uploadedFiles.filter((_, i) => i !== index);
    onFilesChange(newFiles);
  };

  const getFileIcon = (filename: string) => {
    const ext = filename.toLowerCase().split('.').pop();
    switch (ext) {
      case 'pdf':
        return <FileText className="h-5 w-5 text-red-500" />;
      case 'doc':
      case 'docx':
        return <FileText className="h-5 w-5 text-blue-500" />;
      default:
        return <FileText className="h-5 w-5 text-muted-foreground" />;
    }
  };

  const totalSize = uploadedFiles.reduce((sum, file) => sum + file.size, 0);
  const isOverSizeLimit = totalSize > 100 * 1024 * 1024; // 100MB total limit

  // Get files for selected dataset
  const currentDatasetFiles = cloudDatasets.find(d => d.dataset === selectedDataset)?.files || [];

  // Toggle file selection
  const toggleCloudFile = (filename: string) => {
    setSelectedCloudFiles(prev =>
      prev.includes(filename)
        ? prev.filter(f => f !== filename)
        : [...prev, filename]
    );
  };

  // Select all files in current dataset
  const selectAllCloudFiles = () => {
    const allFileNames = currentDatasetFiles.map(f => f.name);
    setSelectedCloudFiles(allFileNames);
  };

  // Clear selection
  const clearCloudSelection = () => {
    setSelectedCloudFiles([]);
  };

  // Handle adding cloud documents
  const handleAddCloudDocuments = async () => {
    if (!sessionId || !onCloudDocumentsAdd || selectedCloudFiles.length === 0) return;

    setCloudAddLoading(true);
    setCloudError(null);
    setCloudSuccess(null);

    try {
      await onCloudDocumentsAdd(selectedDataset, selectedCloudFiles);
      setCloudSuccess(`Successfully added ${selectedCloudFiles.length} documents from ${selectedDataset}`);
      setSelectedCloudFiles([]);
    } catch (err: any) {
      setCloudError(err.message || 'Failed to add cloud documents');
    } finally {
      setCloudAddLoading(false);
    }
  };

  const hasCloudDatasets = cloudDatasets.length > 0;

  return (
    <div className="space-y-4">
      <div>
        <h3 className="text-lg font-semibold mb-2">Upload Documents for Processing</h3>
        <p className="text-sm text-muted-foreground">
          Upload text files, PDFs, or documents that will be processed using the extracted schema.
          Each file will be analyzed to extract data according to the discovered column definitions.
        </p>
      </div>


      <Tabs defaultValue="upload" className="w-full">
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="upload">
            <Upload className="h-4 w-4 mr-2" />
            Upload Files
          </TabsTrigger>
          <TabsTrigger value="cloud" disabled={cloudLoading || !hasCloudDatasets}>
            <Cloud className="h-4 w-4 mr-2" />
            Cloud Datasets
            {hasCloudDatasets && (
              <Badge variant="secondary" className="ml-2">
                {cloudDatasets.length}
              </Badge>
            )}
          </TabsTrigger>
        </TabsList>

        {/* Upload Files Tab */}
        <TabsContent value="upload" className="space-y-4 mt-4">
          {/* Drop Zone */}
          <Card
            {...getRootProps()}
            className={cn(
              "p-8 border-2 border-dashed cursor-pointer text-center transition-colors",
              isDragActive
                ? "border-primary bg-primary/5"
                : "border-muted-foreground/25 hover:border-primary hover:bg-muted/50"
            )}
          >
            <input {...getInputProps()} />
            <Upload className={cn(
              "mx-auto h-12 w-12 mb-3",
              isDragActive ? "text-primary" : "text-muted-foreground"
            )} />
            <h4 className="text-lg font-semibold mb-1">
              {isDragActive ? 'Drop files here' : 'Drop documents here or click to browse'}
            </h4>
            <p className="text-sm text-muted-foreground">
              Supported formats: TXT, MD, PDF, DOC, DOCX, RTF
            </p>
            <p className="text-sm text-muted-foreground">
              Max 25MB per file, 100MB total
            </p>
          </Card>

          {/* Error Messages */}
          {dragError && (
            <Alert variant="destructive">
              <AlertDescription>{dragError}</AlertDescription>
            </Alert>
          )}

          {isOverSizeLimit && (
            <Alert variant="destructive">
              <AlertDescription>
                Total file size exceeds 100MB limit. Current size: {formatFileSize(totalSize)}
              </AlertDescription>
            </Alert>
          )}

          {/* File List */}
          {uploadedFiles.length > 0 && (
            <Card>
              <div className="p-4 border-b">
                <h4 className="font-semibold">
                  Uploaded Files ({uploadedFiles.length})
                </h4>
                <p className="text-sm text-muted-foreground">
                  Total size: {formatFileSize(totalSize)}
                </p>
              </div>

              <div className="divide-y">
                {uploadedFiles.map((file, index) => (
                  <div key={index} className="flex items-center justify-between p-3">
                    <div className="flex items-center gap-3">
                      {getFileIcon(file.name)}
                      <div>
                        <p className="font-medium text-sm">{file.name}</p>
                        <p className="text-xs text-muted-foreground">{formatFileSize(file.size)}</p>
                      </div>
                    </div>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => removeFile(index)}
                      disabled={loading}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                ))}
              </div>
            </Card>
          )}

          {/* Document limit warning */}
          {maxDocuments && (() => {
            const remainingSlots = maxDocuments - (existingDocumentCount || 0);
            const threshold = Math.floor(remainingSlots * 0.75);
            if (uploadedFiles.length >= threshold && threshold > 0) {
              return (
                <Alert className="border-amber-500 bg-amber-50 dark:bg-amber-950/20">
                  <AlertTriangle className="h-4 w-4 text-amber-600" />
                  <AlertDescription className="text-amber-700 dark:text-amber-400">
                    {uploadedFiles.length > remainingSlots
                      ? `You've selected ${uploadedFiles.length} documents but only ${remainingSlots} more can be added. A representative sample of ${remainingSlots} will be used.`
                      : `${uploadedFiles.length} of ${remainingSlots} available slots used.`}
                  </AlertDescription>
                </Alert>
              );
            }
            return null;
          })()}

          {/* Upload Result */}
          {uploadResult && (
            <Card className="p-4">
              <div className="flex items-center gap-2 mb-3">
                {uploadResult.status === 'success' ? (
                  <CheckCircle2 className="h-5 w-5 text-green-500" />
                ) : (
                  <AlertCircle className="h-5 w-5 text-destructive" />
                )}
                <span className="font-semibold">{uploadResult.message}</span>
              </div>

              {uploadResult.uploaded_files.length > 0 && (
                <div className="mb-3">
                  <p className="text-sm text-muted-foreground mb-2">
                    Successfully uploaded:
                  </p>
                  <div className="flex flex-wrap gap-1">
                    {uploadResult.uploaded_files.map((filename, index) => (
                      <Badge key={index} variant="secondary">
                        {filename}
                      </Badge>
                    ))}
                  </div>
                </div>
              )}

              {uploadResult.warnings.length > 0 && (
                <Alert variant="warning">
                  <AlertDescription>
                    <p className="font-medium mb-1">Warnings:</p>
                    {uploadResult.warnings.map((warning, index) => (
                      <p key={index} className="text-sm">• {warning}</p>
                    ))}
                  </AlertDescription>
                </Alert>
              )}
            </Card>
          )}

          {/* Upload Button */}
          <Button
            onClick={onUpload}
            disabled={!canUpload || uploadedFiles.length === 0 || loading || isOverSizeLimit}
            className="w-full"
          >
            <Upload className="mr-2 h-4 w-4" />
            {loading ? 'Uploading Documents...' : `Upload ${uploadedFiles.length} Documents`}
          </Button>

          {loading && (
            <div className="space-y-2">
              <Progress value={undefined} className="w-full" />
              <p className="text-sm text-muted-foreground text-center">
                Uploading and validating documents...
              </p>
            </div>
          )}
        </TabsContent>

        {/* Cloud Datasets Tab */}
        <TabsContent value="cloud" className="space-y-4 mt-4">
          {cloudLoading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              <span className="ml-2 text-muted-foreground">Loading cloud datasets...</span>
            </div>
          ) : (
            <>
              {/* Dataset Selector */}
              <div className="space-y-2">
                <label className="text-sm font-medium">Select Dataset</label>
                <Select value={selectedDataset} onValueChange={(value) => {
                  setSelectedDataset(value);
                  setSelectedCloudFiles([]);
                }}>
                  <SelectTrigger>
                    <SelectValue placeholder="Choose a dataset" />
                  </SelectTrigger>
                  <SelectContent>
                    {cloudDatasets.map((dataset) => (
                      <SelectItem key={dataset.dataset} value={dataset.dataset}>
                        {dataset.dataset} ({dataset.files.length} files)
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* File Selection */}
              {selectedDataset && currentDatasetFiles.length > 0 && (
                <Card>
                  <div className="p-4 border-b flex items-center justify-between">
                    <div>
                      <h4 className="font-semibold">
                        Files in {selectedDataset}
                      </h4>
                      <p className="text-sm text-muted-foreground">
                        {selectedCloudFiles.length} of {currentDatasetFiles.length} selected
                      </p>
                    </div>
                    <div className="flex gap-2">
                      <Button variant="outline" size="sm" onClick={selectAllCloudFiles}>
                        Select All
                      </Button>
                      <Button variant="outline" size="sm" onClick={clearCloudSelection}>
                        Clear
                      </Button>
                    </div>
                  </div>

                  <div className="divide-y max-h-[300px] overflow-y-auto">
                    {currentDatasetFiles.map((file) => (
                      <div key={file.name} className="flex items-center gap-3 p-3">
                        <Checkbox
                          checked={selectedCloudFiles.includes(file.name)}
                          onCheckedChange={() => toggleCloudFile(file.name)}
                        />
                        {getFileIcon(file.name)}
                        <div className="flex-1 min-w-0">
                          <p className="font-medium text-sm truncate">{file.name}</p>
                          <p className="text-xs text-muted-foreground">
                            {formatFileSize(file.size)}
                          </p>
                        </div>
                      </div>
                    ))}
                  </div>
                </Card>
              )}

              {/* Cloud Error/Success Messages */}
              {cloudError && (
                <Alert variant="destructive">
                  <AlertCircle className="h-4 w-4" />
                  <AlertDescription>{cloudError}</AlertDescription>
                </Alert>
              )}

              {cloudSuccess && (
                <Alert>
                  <CheckCircle2 className="h-4 w-4 text-green-500" />
                  <AlertDescription>{cloudSuccess}</AlertDescription>
                </Alert>
              )}

              {/* Add Cloud Documents Button */}
              <Button
                onClick={handleAddCloudDocuments}
                disabled={
                  !sessionId ||
                  !onCloudDocumentsAdd ||
                  selectedCloudFiles.length === 0 ||
                  cloudAddLoading
                }
                className="w-full"
              >
                {cloudAddLoading ? (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ) : (
                  <Cloud className="mr-2 h-4 w-4" />
                )}
                {cloudAddLoading
                  ? 'Adding Documents...'
                  : `Add ${selectedCloudFiles.length} Documents from Cloud`
                }
              </Button>

              {!sessionId && (
                <p className="text-sm text-muted-foreground text-center">
                  A session is required to add cloud documents
                </p>
              )}
            </>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
};

export default DocumentUpload;
