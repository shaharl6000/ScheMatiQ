import React from 'react';
import { Upload, Trash2, FileText, CheckCircle2, AlertCircle } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { cn } from '@/lib/utils';
import { useFileUpload } from '../../hooks/useFileUpload';
import { formatFileSize } from '../../utils/apiHelpers';

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
}

const DocumentUpload: React.FC<DocumentUploadProps> = ({
  onFilesChange,
  uploadedFiles,
  loading,
  onUpload,
  canUpload,
  uploadResult,
}) => {
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
    maxSize: 10 * 1024 * 1024, // 10MB per file
    onFilesSelected: onFilesChange,
    externalFiles: uploadedFiles,
  });

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

  return (
    <div className="space-y-4">
      <div>
        <h3 className="text-lg font-semibold mb-2">Upload Documents for Processing</h3>
        <p className="text-sm text-muted-foreground">
          Upload text files, PDFs, or documents that will be processed using the extracted schema.
          Each file will be analyzed to extract data according to the discovered column definitions.
        </p>
      </div>

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
          Max 10MB per file, 100MB total
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
    </div>
  );
};

export default DocumentUpload;
