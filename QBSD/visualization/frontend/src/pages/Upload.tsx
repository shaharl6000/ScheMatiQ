import React, { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  Paper,
  Button,
  Alert,
  CircularProgress,
} from '@mui/material';
import { useDropzone } from 'react-dropzone';
import { CloudUpload } from '@mui/icons-material';

import { uploadAPI } from '../services/api';

const Upload: React.FC = () => {
  const navigate = useNavigate();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // File upload handler - uploads, validates, parses, then navigates to visualization
  const handleFileUpload = async (file: File) => {
    setError(null);
    setLoading(true);

    try {
      // Upload and validate file
      const result = await uploadAPI.uploadFile(file);

      if (!result.validation.is_valid) {
        setError('File validation failed: ' + result.validation.errors.join(', '));
        setLoading(false);
        return;
      }

      // Parse the file
      await uploadAPI.parseFile(result.session_id);

      // Navigate directly to visualization page
      navigate(`/visualize/${result.session_id}?mode=upload`);
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || 'Failed to upload file');
      setLoading(false);
    }
  };

  // Dropzone configuration
  const onDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      handleFileUpload(acceptedFiles[0]);
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/json': ['.json'],
      'application/jsonl': ['.jsonl'],
      'text/plain': ['.jsonl'],
    },
    maxFiles: 1,
    maxSize: 100 * 1024 * 1024, // 100MB
    disabled: loading,
  });

  const dropzoneStyle = {
    border: '2px dashed',
    borderColor: isDragActive ? 'primary.main' : 'grey.300',
    borderRadius: 2,
    p: 6,
    textAlign: 'center',
    cursor: loading ? 'not-allowed' : 'pointer',
    bgcolor: isDragActive ? 'primary.light' : 'background.paper',
    opacity: loading ? 0.7 : 1,
    transition: 'all 0.2s ease',
    '&:hover': {
      borderColor: loading ? 'grey.300' : 'primary.main',
      bgcolor: loading ? 'background.paper' : 'action.hover',
    },
  };

  return (
    <Box sx={{ maxWidth: 800, mx: 'auto', mt: 4, px: 2 }}>
      {/* Header */}
      <Typography variant="h4" gutterBottom>
        Upload Data
      </Typography>

      {/* Explanation */}
      <Typography variant="body1" color="text.secondary" paragraph>
        Upload your data file to visualize your research data. The system will
        automatically extract a schema from your data, allowing you to explore
        and add documents for AI-powered information extraction.
      </Typography>

      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        Supported formats: CSV, JSON, JSONL (up to 100MB)
      </Typography>

      {/* Upload Dropzone */}
      <Paper sx={dropzoneStyle} {...getRootProps()}>
        <input {...getInputProps()} />
        <CloudUpload sx={{ fontSize: 48, color: 'primary.main', mb: 2 }} />

        {loading ? (
          <Box>
            <CircularProgress size={24} sx={{ mb: 1 }} />
            <Typography variant="h6">
              Processing file...
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Please wait while we validate and parse your data
            </Typography>
          </Box>
        ) : isDragActive ? (
          <Typography variant="h6">Drop the file here...</Typography>
        ) : (
          <>
            <Typography variant="h6" gutterBottom>
              Drop your data file here or click to browse
            </Typography>
            <Typography variant="body2" color="text.secondary">
              CSV, JSON, or JSONL files
            </Typography>
          </>
        )}
      </Paper>

      {/* Error Display */}
      {error && (
        <Alert severity="error" sx={{ mt: 3 }}>
          {error}
        </Alert>
      )}

      {/* Back Button */}
      <Box sx={{ mt: 4 }}>
        <Button onClick={() => navigate('/')} variant="outlined" disabled={loading}>
          Back to Home
        </Button>
      </Box>
    </Box>
  );
};

export default Upload;
