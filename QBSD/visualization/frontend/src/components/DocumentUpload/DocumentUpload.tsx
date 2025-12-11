import React, { useEffect } from 'react';
import {
  Box,
  Typography,
  Paper,
  Button,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Alert,
  Chip,
  LinearProgress,
} from '@mui/material';
import { 
  CloudUpload, 
  Delete, 
  Description, 
  CheckCircle, 
  Error as ErrorIcon 
} from '@mui/icons-material';
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
    externalFiles: uploadedFiles,  // Sync with parent state to avoid file list accumulation
  });

  const removeFile = (index: number) => {
    const newFiles = uploadedFiles.filter((_, i) => i !== index);
    onFilesChange(newFiles);
  };

  const getFileIcon = (filename: string) => {
    const ext = filename.toLowerCase().split('.').pop();
    switch (ext) {
      case 'pdf':
        return <Description color="error" />;
      case 'doc':
      case 'docx':
        return <Description color="primary" />;
      default:
        return <Description color="action" />;
    }
  };

  const totalSize = uploadedFiles.reduce((sum, file) => sum + file.size, 0);
  const isOverSizeLimit = totalSize > 100 * 1024 * 1024; // 100MB total limit

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Upload Documents for Processing
      </Typography>
      
      <Typography variant="body2" color="text.secondary" paragraph>
        Upload text files, PDFs, or documents that will be processed using the extracted schema.
        Each file will be analyzed to extract data according to the discovered column definitions.
      </Typography>

      {/* Drop Zone */}
      <Paper
        {...getRootProps()}
        sx={{
          p: 3,
          border: '2px dashed',
          borderColor: isDragActive ? 'primary.main' : 'grey.300',
          backgroundColor: isDragActive ? 'action.hover' : 'background.paper',
          cursor: 'pointer',
          mb: 2,
          textAlign: 'center',
          '&:hover': {
            borderColor: 'primary.main',
            backgroundColor: 'action.hover',
          },
        }}
      >
        <input {...getInputProps()} />
        <CloudUpload sx={{ fontSize: 48, color: 'text.secondary', mb: 1 }} />
        <Typography variant="h6" gutterBottom>
          {isDragActive ? 'Drop files here' : 'Drop documents here or click to browse'}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Supported formats: TXT, MD, PDF, DOC, DOCX, RTF
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Max 10MB per file, 100MB total
        </Typography>
      </Paper>

      {/* Error Messages */}
      {dragError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {dragError}
        </Alert>
      )}

      {isOverSizeLimit && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Total file size exceeds 100MB limit. Current size: {formatFileSize(totalSize)}
        </Alert>
      )}

      {/* File List */}
      {uploadedFiles.length > 0 && (
        <Paper sx={{ mb: 2 }}>
          <Box sx={{ p: 2, borderBottom: '1px solid', borderColor: 'divider' }}>
            <Typography variant="subtitle1">
              Uploaded Files ({uploadedFiles.length})
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Total size: {formatFileSize(totalSize)}
            </Typography>
          </Box>
          
          <List>
            {uploadedFiles.map((file, index) => (
              <ListItem key={index} divider>
                <Box sx={{ mr: 2 }}>
                  {getFileIcon(file.name)}
                </Box>
                <ListItemText
                  primary={file.name}
                  secondary={formatFileSize(file.size)}
                />
                <ListItemSecondaryAction>
                  <IconButton
                    edge="end"
                    onClick={() => removeFile(index)}
                    disabled={loading}
                  >
                    <Delete />
                  </IconButton>
                </ListItemSecondaryAction>
              </ListItem>
            ))}
          </List>
        </Paper>
      )}

      {/* Upload Result */}
      {uploadResult && (
        <Paper sx={{ p: 2, mb: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
            {uploadResult.status === 'success' ? (
              <CheckCircle color="success" sx={{ mr: 1 }} />
            ) : (
              <ErrorIcon color="error" sx={{ mr: 1 }} />
            )}
            <Typography variant="subtitle1">
              {uploadResult.message}
            </Typography>
          </Box>
          
          {uploadResult.uploaded_files.length > 0 && (
            <Box sx={{ mb: 1 }}>
              <Typography variant="body2" gutterBottom>
                Successfully uploaded:
              </Typography>
              {uploadResult.uploaded_files.map((filename, index) => (
                <Chip
                  key={index}
                  label={filename}
                  size="small"
                  sx={{ mr: 1, mb: 1 }}
                />
              ))}
            </Box>
          )}
          
          {uploadResult.warnings.length > 0 && (
            <Alert severity="warning" sx={{ mt: 1 }}>
              <Typography variant="body2" gutterBottom>
                Warnings:
              </Typography>
              {uploadResult.warnings.map((warning, index) => (
                <Typography key={index} variant="body2">
                  • {warning}
                </Typography>
              ))}
            </Alert>
          )}
        </Paper>
      )}

      {/* Upload Button */}
      <Button
        variant="contained"
        onClick={onUpload}
        disabled={!canUpload || uploadedFiles.length === 0 || loading || isOverSizeLimit}
        startIcon={<CloudUpload />}
        fullWidth
      >
        {loading ? 'Uploading Documents...' : `Upload ${uploadedFiles.length} Documents`}
      </Button>

      {loading && (
        <Box sx={{ mt: 2 }}>
          <LinearProgress />
          <Typography variant="body2" color="text.secondary" align="center" sx={{ mt: 1 }}>
            Uploading and validating documents...
          </Typography>
        </Box>
      )}
    </Box>
  );
};

export default DocumentUpload;