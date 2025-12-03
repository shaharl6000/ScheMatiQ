import React, { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  Paper,
  Button,
  Alert,
  CircularProgress,
  Stepper,
  Step,
  StepLabel,
  StepContent,
} from '@mui/material';
import { useDropzone } from 'react-dropzone';
import { CloudUpload, CheckCircle, Error } from '@mui/icons-material';

import { uploadAPI } from '../services/api';
import { FileValidationResult } from '../types';

const steps = [
  'Upload File',
  'Validate Data',
  'Process & Preview',
];

const Upload: React.FC = () => {
  const navigate = useNavigate();
  const [activeStep, setActiveStep] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [uploadedFile, setUploadedFile] = useState<File | null>(null);
  const [validation, setValidation] = useState<FileValidationResult | null>(null);
  const [sessionId, setSessionId] = useState<string | null>(null);

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    if (acceptedFiles.length === 0) return;

    const file = acceptedFiles[0];
    setUploadedFile(file);
    setError(null);
    setLoading(true);

    try {
      const result = await uploadAPI.uploadFile(file);
      setSessionId(result.session_id);
      setValidation(result.validation);
      setActiveStep(1);

      if (result.validation.is_valid) {
        // Auto-proceed to parsing if validation passed
        await handleParseFile(result.session_id);
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to upload file');
    } finally {
      setLoading(false);
    }
  }, []);

  const handleParseFile = async (sessionId: string) => {
    setLoading(true);
    setError(null);

    try {
      await uploadAPI.parseFile(sessionId);
      setActiveStep(2);
      
      // Navigate to visualization
      setTimeout(() => {
        navigate(`/visualize/${sessionId}`);
      }, 1000);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to parse file');
    } finally {
      setLoading(false);
    }
  };

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
  });

  const dropzoneStyle = {
    border: '2px dashed',
    borderColor: isDragActive ? 'primary.main' : 'grey.300',
    borderRadius: 2,
    p: 6,
    textAlign: 'center',
    cursor: 'pointer',
    bgcolor: isDragActive ? 'primary.light' : 'background.paper',
    transition: 'all 0.2s ease',
    '&:hover': {
      borderColor: 'primary.main',
      bgcolor: 'primary.light',
    },
  };

  return (
    <Box sx={{ maxWidth: 800, mx: 'auto', mt: 4 }}>
      <Typography variant="h4" gutterBottom>
        Upload Data
      </Typography>
      <Typography variant="body1" color="text.secondary" paragraph>
        Upload your CSV or JSON file to start visualizing your data.
      </Typography>

      <Stepper activeStep={activeStep} orientation="vertical" sx={{ mt: 4 }}>
        <Step>
          <StepLabel>Upload File</StepLabel>
          <StepContent>
            <Paper sx={dropzoneStyle} {...getRootProps()}>
              <input {...getInputProps()} />
              <CloudUpload sx={{ fontSize: 48, color: 'primary.main', mb: 2 }} />
              {isDragActive ? (
                <Typography variant="h6">Drop the file here...</Typography>
              ) : (
                <>
                  <Typography variant="h6" gutterBottom>
                    Drag & drop your file here, or click to select
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Supports CSV, JSON, and JSONL files up to 100MB
                  </Typography>
                </>
              )}
            </Paper>
            
            {uploadedFile && (
              <Alert severity="info" sx={{ mt: 2 }}>
                Selected: {uploadedFile.name} ({(uploadedFile.size / 1024 / 1024).toFixed(2)} MB)
              </Alert>
            )}
          </StepContent>
        </Step>

        <Step>
          <StepLabel 
            error={validation && !validation.is_valid}
            icon={loading ? <CircularProgress size={24} /> : undefined}
          >
            Validate Data
          </StepLabel>
          <StepContent>
            {validation && (
              <Box>
                {validation.is_valid ? (
                  <Alert severity="success" icon={<CheckCircle />} sx={{ mb: 2 }}>
                    File validation successful!
                  </Alert>
                ) : (
                  <Alert severity="error" icon={<Error />} sx={{ mb: 2 }}>
                    Validation failed
                  </Alert>
                )}

                {validation.warnings.length > 0 && (
                  <Alert severity="warning" sx={{ mb: 2 }}>
                    <Typography variant="body2" component="div">
                      Warnings:
                      <ul>
                        {validation.warnings.map((warning, index) => (
                          <li key={index}>{warning}</li>
                        ))}
                      </ul>
                    </Typography>
                  </Alert>
                )}

                {validation.errors.length > 0 && (
                  <Alert severity="error" sx={{ mb: 2 }}>
                    <Typography variant="body2" component="div">
                      Errors:
                      <ul>
                        {validation.errors.map((error, index) => (
                          <li key={index}>{error}</li>
                        ))}
                      </ul>
                    </Typography>
                  </Alert>
                )}

                {validation.is_valid && (
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                    Detected format: <strong>{validation.detected_format?.toUpperCase()}</strong>
                    {validation.estimated_rows && (
                      <> • Estimated rows: <strong>{validation.estimated_rows.toLocaleString()}</strong></>
                    )}
                    {validation.estimated_columns && (
                      <> • Estimated columns: <strong>{validation.estimated_columns}</strong></>
                    )}
                  </Typography>
                )}

                {validation.is_valid && sessionId && (
                  <Button
                    variant="contained"
                    onClick={() => handleParseFile(sessionId)}
                    disabled={loading}
                  >
                    {loading ? 'Processing...' : 'Process File'}
                  </Button>
                )}
              </Box>
            )}
          </StepContent>
        </Step>

        <Step>
          <StepLabel 
            icon={loading ? <CircularProgress size={24} /> : undefined}
          >
            Process & Preview
          </StepLabel>
          <StepContent>
            <Alert severity="info">
              Processing your data and preparing visualization...
            </Alert>
            
            {loading && (
              <Box sx={{ display: 'flex', alignItems: 'center', mt: 2 }}>
                <CircularProgress size={20} sx={{ mr: 2 }} />
                <Typography variant="body2">
                  Parsing file and extracting schema...
                </Typography>
              </Box>
            )}
          </StepContent>
        </Step>
      </Stepper>

      {error && (
        <Alert severity="error" sx={{ mt: 3 }}>
          {error}
        </Alert>
      )}

      <Box sx={{ mt: 4 }}>
        <Button onClick={() => navigate('/')} variant="outlined">
          Back to Home
        </Button>
      </Box>
    </Box>
  );
};

export default Upload;