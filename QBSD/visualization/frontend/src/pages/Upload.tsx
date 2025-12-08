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
  Grid,
  Card,
  CardContent,
  TextField,
} from '@mui/material';
import { useDropzone } from 'react-dropzone';
import { CloudUpload, CheckCircle, Error, Schema, TableView } from '@mui/icons-material';

import { uploadAPI } from '../services/api';
import { 
  FileValidationResult, 
  DualFileUploadResult, 
  SchemaExtractionResult,
  DocumentUploadResult,
  ProcessingStatus
} from '../types';
import CompatibilityDisplay from '../components/CompatibilityDisplay/CompatibilityDisplay';
import DocumentUpload from '../components/DocumentUpload/DocumentUpload';
import UploadProcessingMonitor from '../components/UploadProcessingMonitor/UploadProcessingMonitor';
import LLMSelector from '../components/LLMSelector';

const singleFileSteps = [
  'Upload File',
  'Validate Data',
  'Process & Preview',
];

const enhancedUploadSteps = [
  'Upload & Process Data',
  'Extract Schema', 
  'Upload Documents',
  'Process with AI',
  'Review Results',
];

const dualFileSteps = [
  'Upload Schema & Data Files',
  'Validate Files',
  'Check Compatibility',
  'Process & Preview',
];

const Upload: React.FC = () => {
  const navigate = useNavigate();
  
  // Mode selection
  const [mode, setMode] = useState<'single' | 'enhanced' | 'dual'>('single');
  
  // Common state
  const [activeStep, setActiveStep] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sessionId, setSessionId] = useState<string | null>(null);
  
  // Single file mode state
  const [uploadedFile, setUploadedFile] = useState<File | null>(null);
  const [validation, setValidation] = useState<FileValidationResult | null>(null);
  
  // Enhanced upload workflow state
  const [enhancedQuery, setEnhancedQuery] = useState('');
  const [extractedSchema, setExtractedSchema] = useState<SchemaExtractionResult | null>(null);
  const [uploadedDocuments, setUploadedDocuments] = useState<File[]>([]);
  const [documentUploadResult, setDocumentUploadResult] = useState<DocumentUploadResult | null>(null);
  const [processingStatus, setProcessingStatus] = useState<ProcessingStatus | null>(null);
  
  // Dual file mode state
  const [schemaFile, setSchemaFile] = useState<File | null>(null);
  const [dataFile, setDataFile] = useState<File | null>(null);
  const [dualResult, setDualResult] = useState<DualFileUploadResult | null>(null);
  
  // LLM selection state
  const [showLLMSelector, setShowLLMSelector] = useState(false);
  const [selectedLLMConfig, setSelectedLLMConfig] = useState<any>(null);

  // Reset state when mode changes
  const handleModeChange = (newMode: 'single' | 'enhanced' | 'dual') => {
    setMode(newMode);
    setActiveStep(0);
    setError(null);
    setSessionId(null);
    
    // Reset all mode-specific state
    setUploadedFile(null);
    setValidation(null);
    
    setEnhancedQuery('');
    setExtractedSchema(null);
    setUploadedDocuments([]);
    setDocumentUploadResult(null);
    setProcessingStatus(null);
    
    setSchemaFile(null);
    setDataFile(null);
    setDualResult(null);
  };

  // Enhanced upload workflow handlers
  const handleEnhancedFileUpload = async () => {
    if (!uploadedFile) {
      setError('Please select a file');
      return;
    }

    setError(null);
    setLoading(true);

    try {
      // Step 1: Upload and parse file
      const uploadResult = await uploadAPI.uploadFile(uploadedFile);
      setSessionId(uploadResult.session_id);
      setValidation(uploadResult.validation);
      
      if (!uploadResult.validation.is_valid) {
        setError('File validation failed: ' + uploadResult.validation.errors.join(', '));
        return;
      }

      // Parse the file
      await uploadAPI.parseFile(uploadResult.session_id);
      setActiveStep(1); // Move to Extract Schema step
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to upload and process file');
    } finally {
      setLoading(false);
    }
  };

  const handleSchemaExtraction = async () => {
    if (!sessionId) {
      setError('No session available for schema extraction');
      return;
    }

    setError(null);
    setLoading(true);

    try {
      const result = await uploadAPI.extractSchema(sessionId, enhancedQuery);
      setExtractedSchema(result);
      
      // Auto-navigate to Data tab after schema extraction
      setTimeout(() => {
        navigate(`/visualize/${sessionId}?mode=upload`);
      }, 1500);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to extract schema');
    } finally {
      setLoading(false);
    }
  };

  const handleDocumentUpload = async () => {
    if (!sessionId || uploadedDocuments.length === 0) {
      setError('Please upload at least one document');
      return;
    }

    setError(null);
    setLoading(true);

    try {
      const result = await uploadAPI.addDocuments(sessionId, uploadedDocuments);
      setDocumentUploadResult(result);
      setActiveStep(3); // Move to Process step
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to upload documents');
    } finally {
      setLoading(false);
    }
  };

  const handleDocumentProcessing = async () => {
    if (!sessionId) {
      setError('No session available for processing');
      return;
    }

    // Show LLM selector first
    setShowLLMSelector(true);
  };

  const handleLLMSelection = async (llmConfig: any) => {
    setSelectedLLMConfig(llmConfig);
    setShowLLMSelector(false);
    setError(null);
    setLoading(true);

    try {
      await uploadAPI.processDocuments(sessionId!, llmConfig);
      setActiveStep(4); // Move to Review Results step
      
      // Start polling for progress
      startProcessingStatusPolling();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to start document processing');
      setLoading(false);
    }
  };

  const startProcessingStatusPolling = () => {
    const pollStatus = async () => {
      if (!sessionId) return;
      
      try {
        const status = await uploadAPI.getProcessingStatus(sessionId);
        setProcessingStatus(status);
        
        if (status.status === 'completed') {
          setLoading(false);
          // Don't auto-navigate, let user choose when to view results
        } else if (status.status === 'error') {
          setLoading(false);
          setError('Document processing failed');
        } else if (status.status === 'processing_documents') {
          // Continue polling
          setTimeout(pollStatus, 3000);
        }
      } catch (err: any) {
        console.error('Error polling processing status:', err);
        setTimeout(pollStatus, 5000); // Retry with longer delay
      }
    };

    pollStatus();
  };

  // Single file upload handler (existing)
  const handleSingleFileUpload = async () => {
    if (!uploadedFile) {
      setError('Please select a file');
      return;
    }

    setError(null);
    setLoading(true);

    try {
      const result = await uploadAPI.uploadFile(uploadedFile);
      setSessionId(result.session_id);
      setValidation(result.validation);
      setActiveStep(1);

      if (result.validation.is_valid) {
        setActiveStep(2);
        await uploadAPI.parseFile(result.session_id);
        
        setTimeout(() => {
          navigate(`/visualize/${result.session_id}`);
        }, 1000);
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to upload file');
    } finally {
      setLoading(false);
    }
  };

  // Dual file upload handlers
  const handleDualFileUpload = async () => {
    if (!schemaFile || !dataFile) {
      setError('Both schema and data files are required');
      return;
    }

    setError(null);
    setLoading(true);

    try {
      const result = await uploadAPI.uploadDualFiles(schemaFile, dataFile);
      setSessionId(result.session_id);
      setDualResult(result);
      setActiveStep(1);

      // Auto-proceed if both files are valid and compatible
      if (result.schema_validation.is_valid && 
          result.data_validation.is_valid && 
          result.compatibility.is_compatible) {
        setActiveStep(2);
        setTimeout(() => {
          setActiveStep(3);
          handleProcessDualFiles(result.session_id);
        }, 1000);
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to upload files');
    } finally {
      setLoading(false);
    }
  };

  const handleProcessDualFiles = async (sessionId: string) => {
    setLoading(true);
    setError(null);

    try {
      await uploadAPI.processDualFiles(sessionId);
      
      // Navigate to visualization
      setTimeout(() => {
        navigate(`/visualize/${sessionId}`);
      }, 1000);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to process files');
    } finally {
      setLoading(false);
    }
  };

  // Single file upload (legacy)
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

  // Schema file dropzone
  const { getRootProps: getSchemaRootProps, getInputProps: getSchemaInputProps, isDragActive: isSchemaActive } = useDropzone({
    onDrop: useCallback((acceptedFiles: File[]) => {
      if (acceptedFiles.length > 0) {
        setSchemaFile(acceptedFiles[0]);
      }
    }, []),
    accept: {
      'application/json': ['.json'],
    },
    maxFiles: 1,
    maxSize: 10 * 1024 * 1024, // 10MB for schema
  });

  // Data file dropzone
  const { getRootProps: getDataRootProps, getInputProps: getDataInputProps, isDragActive: isDataActive } = useDropzone({
    onDrop: useCallback((acceptedFiles: File[]) => {
      if (acceptedFiles.length > 0) {
        setDataFile(acceptedFiles[0]);
      }
    }, []),
    accept: {
      'text/csv': ['.csv'],
      'application/json': ['.json'],
      'application/jsonl': ['.jsonl'],
      'text/plain': ['.jsonl'],
    },
    maxFiles: 1,
    maxSize: 100 * 1024 * 1024, // 100MB
  });

  // Single file dropzone (legacy)
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

  const currentSteps = mode === 'enhanced' ? enhancedUploadSteps : 
                     mode === 'dual' ? dualFileSteps : singleFileSteps;

  return (
    <Box sx={{ maxWidth: 1000, mx: 'auto', mt: 4 }}>
      {/* Header */}
      <Typography variant="h4" gutterBottom>
        Upload Data
      </Typography>
      <Typography variant="body1" color="text.secondary" paragraph>
        Upload your data files to start visualizing. Choose between single file upload or dual upload with schema validation.
      </Typography>

      {/* Mode Selection */}
      <Paper sx={{ p: 3, mb: 4 }}>
        <Typography variant="h6" gutterBottom>
          Upload Mode Selection
        </Typography>
        
        <Grid container spacing={2}>
          <Grid item xs={12} sm={4}>
            <Card 
              variant={mode === 'single' ? 'outlined' : 'elevation'}
              sx={{ 
                cursor: 'pointer', 
                border: mode === 'single' ? '2px solid' : 'none',
                borderColor: 'primary.main',
                backgroundColor: mode === 'single' ? 'primary.50' : 'background.paper'
              }}
              onClick={() => handleModeChange('single')}
            >
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Single File Upload
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Upload a CSV or JSON file for basic visualization
                </Typography>
              </CardContent>
            </Card>
          </Grid>
          
          <Grid item xs={12} sm={4}>
            <Card 
              variant={mode === 'enhanced' ? 'outlined' : 'elevation'}
              sx={{ 
                cursor: 'pointer', 
                border: mode === 'enhanced' ? '2px solid' : 'none',
                borderColor: 'primary.main',
                backgroundColor: mode === 'enhanced' ? 'primary.50' : 'background.paper'
              }}
              onClick={() => handleModeChange('enhanced')}
            >
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                  <Typography variant="h6">
                    Enhanced Upload
                  </Typography>
                  <Schema color="primary" />
                </Box>
                <Typography variant="body2" color="text.secondary">
                  Extract schema from data, then upload documents for AI processing
                </Typography>
              </CardContent>
            </Card>
          </Grid>
          
          <Grid item xs={12} sm={4}>
            <Card 
              variant={mode === 'dual' ? 'outlined' : 'elevation'}
              sx={{ 
                cursor: 'pointer', 
                border: mode === 'dual' ? '2px solid' : 'none',
                borderColor: 'primary.main',
                backgroundColor: mode === 'dual' ? 'primary.50' : 'background.paper'
              }}
              onClick={() => handleModeChange('dual')}
            >
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Schema + Data Upload
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Upload separate schema and data files with validation
                </Typography>
              </CardContent>
            </Card>
          </Grid>
        </Grid>
      </Paper>

      <Stepper activeStep={activeStep} orientation="vertical" sx={{ mt: 4 }}>
        {/* Step 1: Upload Files */}
        <Step>
          <StepLabel>{currentSteps[0]}</StepLabel>
          <StepContent>
            {mode === 'enhanced' ? (
              <>
                {/* Enhanced Upload: Single File Upload */}
                <Paper sx={dropzoneStyle} {...getRootProps()}>
                  <input {...getInputProps()} />
                  <CloudUpload sx={{ fontSize: 48, color: 'primary.main', mb: 2 }} />
                  {isDragActive ? (
                    <Typography variant="h6">Drop the file here...</Typography>
                  ) : (
                    <>
                      <Typography variant="h6" gutterBottom>
                        Upload your data file
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        CSV, JSON, or JSONL files up to 100MB
                      </Typography>
                    </>
                  )}
                </Paper>
                
                {uploadedFile && (
                  <Alert severity="info" sx={{ mt: 2 }}>
                    Selected: {uploadedFile.name} ({(uploadedFile.size / 1024 / 1024).toFixed(2)} MB)
                  </Alert>
                )}
                
                <Box sx={{ display: 'flex', justifyContent: 'center', mt: 3 }}>
                  <Button
                    variant="contained"
                    size="large"
                    onClick={handleEnhancedFileUpload}
                    disabled={!uploadedFile || loading}
                    startIcon={loading ? <CircularProgress size={20} /> : <CloudUpload />}
                  >
                    {loading ? 'Processing...' : 'Upload & Parse File'}
                  </Button>
                </Box>
              </>
            ) : mode === 'dual' ? (
              <Grid container spacing={3}>
                {/* Schema File Upload */}
                <Grid item xs={12} md={6}>
                  <Card>
                    <CardContent>
                      <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Schema color="primary" />
                        Schema File (.json)
                      </Typography>
                      <Paper 
                        sx={{ 
                          ...dropzoneStyle, 
                          borderColor: isSchemaActive ? 'primary.main' : 'grey.300',
                          bgcolor: isSchemaActive ? 'primary.light' : 'background.paper'
                        }} 
                        {...getSchemaRootProps()}
                      >
                        <input {...getSchemaInputProps()} />
                        <Schema sx={{ fontSize: 32, color: 'primary.main', mb: 1 }} />
                        {isSchemaActive ? (
                          <Typography variant="body1">Drop schema file here...</Typography>
                        ) : (
                          <>
                            <Typography variant="body1" gutterBottom>
                              QBSD Schema JSON
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              JSON file with schema definitions
                            </Typography>
                          </>
                        )}
                      </Paper>
                      {schemaFile && (
                        <Alert severity="success" sx={{ mt: 2 }}>
                          <Typography variant="body2">
                            <strong>{schemaFile.name}</strong><br/>
                            {(schemaFile.size / 1024).toFixed(1)} KB
                          </Typography>
                        </Alert>
                      )}
                    </CardContent>
                  </Card>
                </Grid>

                {/* Data File Upload */}
                <Grid item xs={12} md={6}>
                  <Card>
                    <CardContent>
                      <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <TableView color="secondary" />
                        Data File (.csv, .json, .jsonl)
                      </Typography>
                      <Paper 
                        sx={{ 
                          ...dropzoneStyle, 
                          borderColor: isDataActive ? 'secondary.main' : 'grey.300',
                          bgcolor: isDataActive ? 'secondary.light' : 'background.paper'
                        }} 
                        {...getDataRootProps()}
                      >
                        <input {...getDataInputProps()} />
                        <TableView sx={{ fontSize: 32, color: 'secondary.main', mb: 1 }} />
                        {isDataActive ? (
                          <Typography variant="body1">Drop data file here...</Typography>
                        ) : (
                          <>
                            <Typography variant="body1" gutterBottom>
                              Data File
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              CSV, JSON, or JSONL up to 100MB
                            </Typography>
                          </>
                        )}
                      </Paper>
                      {dataFile && (
                        <Alert severity="success" sx={{ mt: 2 }}>
                          <Typography variant="body2">
                            <strong>{dataFile.name}</strong><br/>
                            {(dataFile.size / 1024 / 1024).toFixed(2)} MB
                          </Typography>
                        </Alert>
                      )}
                    </CardContent>
                  </Card>
                </Grid>

                {/* Upload Button */}
                <Grid item xs={12}>
                  <Box sx={{ display: 'flex', justifyContent: 'center', mt: 2 }}>
                    <Button
                      variant="contained"
                      size="large"
                      onClick={handleDualFileUpload}
                      disabled={!schemaFile || !dataFile || loading}
                      startIcon={loading ? <CircularProgress size={20} /> : <CloudUpload />}
                    >
                      {loading ? 'Uploading...' : 'Upload Both Files'}
                    </Button>
                  </Box>
                </Grid>
              </Grid>
            ) : (
              <>
                {/* Single File Upload */}
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
              </>
            )}
          </StepContent>
        </Step>

        {/* Step 2: Extract Schema / Validation */}
        <Step>
          <StepLabel 
            error={(mode === 'dual' && dualResult) ? (!dualResult.schema_validation.is_valid || !dualResult.data_validation.is_valid) : (validation ? !validation.is_valid : false)}
            icon={loading ? <CircularProgress size={24} /> : undefined}
          >
            {currentSteps[1]}
          </StepLabel>
          <StepContent>
            {mode === 'enhanced' ? (
              <Box>
                <Typography variant="h6" gutterBottom>
                  Extract Schema from Data
                </Typography>
                <Typography variant="body2" color="text.secondary" paragraph>
                  Provide an optional research query to guide schema extraction, or leave blank for automatic discovery.
                </Typography>
                
                <TextField
                  fullWidth
                  label="Research Query (Optional)"
                  placeholder="e.g., What are the key properties of nuclear export signals?"
                  value={enhancedQuery}
                  onChange={(e) => setEnhancedQuery(e.target.value)}
                  multiline
                  rows={3}
                  sx={{ mb: 3 }}
                />
                
                {extractedSchema && (
                  <Alert severity="success" sx={{ mb: 3 }}>
                    <Typography variant="body1" gutterBottom>
                      ✅ Schema extracted successfully!
                    </Typography>
                    <Typography variant="body2">
                      Found {extractedSchema.total_columns} columns ready for document processing.
                    </Typography>
                  </Alert>
                )}
                
                <Box sx={{ display: 'flex', justifyContent: 'center', mt: 3 }}>
                  <Button
                    variant="contained"
                    size="large"
                    onClick={handleSchemaExtraction}
                    disabled={!sessionId || loading || !!extractedSchema}
                    startIcon={loading ? <CircularProgress size={20} /> : <Schema />}
                  >
                    {loading ? 'Extracting...' : 'Extract Schema'}
                  </Button>
                </Box>
              </Box>
            ) : mode === 'dual' && dualResult ? (
              <Box>
                {/* Schema Validation */}
                <Typography variant="h6" gutterBottom>Schema Validation</Typography>
                {dualResult.schema_validation.is_valid ? (
                  <Alert severity="success" icon={<CheckCircle />} sx={{ mb: 2 }}>
                    Schema file validation successful! Found {dualResult.schema_validation.detected_columns.length} columns.
                  </Alert>
                ) : (
                  <Alert severity="error" icon={<Error />} sx={{ mb: 2 }}>
                    Schema validation failed
                    <ul style={{ margin: '8px 0 0 20px' }}>
                      {dualResult.schema_validation.errors.map((error, index) => (
                        <li key={index}>{error}</li>
                      ))}
                    </ul>
                  </Alert>
                )}

                {/* Data Validation */}
                <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>Data Validation</Typography>
                {dualResult.data_validation.is_valid ? (
                  <Alert severity="success" icon={<CheckCircle />} sx={{ mb: 2 }}>
                    Data file validation successful!
                  </Alert>
                ) : (
                  <Alert severity="error" icon={<Error />} sx={{ mb: 2 }}>
                    Data validation failed
                    <ul style={{ margin: '8px 0 0 20px' }}>
                      {dualResult.data_validation.errors.map((error, index) => (
                        <li key={index}>{error}</li>
                      ))}
                    </ul>
                  </Alert>
                )}
              </Box>
            ) : validation && mode === 'single' ? (
              <Box>
                {validation.is_valid ? (
                  <Alert severity="success" icon={<CheckCircle />} sx={{ mb: 2 }}>
                    File validation successful!
                  </Alert>
                ) : (
                  <Alert severity="error" icon={<Error />} sx={{ mb: 2 }}>
                    Validation failed
                    <ul style={{ margin: '8px 0 0 20px' }}>
                      {validation.errors.map((error, index) => (
                        <li key={index}>{error}</li>
                      ))}
                    </ul>
                  </Alert>
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
            ) : null}
          </StepContent>
        </Step>

        {/* Step 3: Upload Documents (enhanced mode) or Compatibility (dual mode) */}
        {mode === 'enhanced' ? (
          <Step>
            <StepLabel>{currentSteps[2]}</StepLabel>
            <StepContent>
              <DocumentUpload
                onFilesChange={setUploadedDocuments}
                uploadedFiles={uploadedDocuments}
                loading={loading}
                onUpload={handleDocumentUpload}
                canUpload={extractedSchema !== null}
                uploadResult={documentUploadResult}
              />
            </StepContent>
          </Step>
        ) : mode === 'dual' ? (
          <Step>
            <StepLabel
              error={dualResult ? !dualResult.compatibility.is_compatible : false}
            >
              {currentSteps[2]}
            </StepLabel>
            <StepContent>
              {dualResult && (
                <CompatibilityDisplay
                  schemaValidation={dualResult.schema_validation}
                  dataValidation={dualResult.data_validation}
                  compatibility={dualResult.compatibility}
                />
              )}

              {dualResult?.compatibility.is_compatible && sessionId && (
                <Box sx={{ mt: 3, display: 'flex', justifyContent: 'center' }}>
                  <Button
                    variant="contained"
                    size="large"
                    onClick={() => handleProcessDualFiles(sessionId)}
                    disabled={loading}
                    startIcon={loading ? <CircularProgress size={20} /> : <CheckCircle />}
                  >
                    {loading ? 'Processing...' : 'Proceed to Visualization'}
                  </Button>
                </Box>
              )}
            </StepContent>
          </Step>
        ) : null}
        
        {/* Step 4: Process with AI (enhanced mode only) */}
        {mode === 'enhanced' && (
          <Step>
            <StepLabel>{currentSteps[3]}</StepLabel>
            <StepContent>
              <Box>
                <Typography variant="h6" gutterBottom>
                  Process Documents with AI
                </Typography>
                <Typography variant="body2" color="text.secondary" paragraph>
                  The system will analyze your uploaded documents using the extracted schema to generate additional table rows.
                </Typography>
                
                {documentUploadResult && (
                  <Alert severity="success" sx={{ mb: 3 }}>
                    <Typography variant="body1" gutterBottom>
                      ✅ Documents uploaded successfully!
                    </Typography>
                    <Typography variant="body2">
                      Ready to process {documentUploadResult.uploaded_files.length} documents.
                    </Typography>
                  </Alert>
                )}
                
                <Box sx={{ display: 'flex', justifyContent: 'center', mt: 3 }}>
                  <Button
                    variant="contained"
                    size="large"
                    onClick={handleDocumentProcessing}
                    disabled={!documentUploadResult || loading}
                    startIcon={loading ? <CircularProgress size={20} /> : <TableView />}
                  >
                    {loading ? 'Starting Processing...' : 'Process Documents'}
                  </Button>
                </Box>
              </Box>
            </StepContent>
          </Step>
        )}
        
        {/* Step 5: Review Results (enhanced mode only) */}
        {mode === 'enhanced' && (
          <Step>
            <StepLabel>{currentSteps[4]}</StepLabel>
            <StepContent>
              <UploadProcessingMonitor
                sessionId={sessionId}
                status={processingStatus}
                loading={loading}
                onNavigateToResults={() => navigate(`/visualize/${sessionId}?mode=upload`)}
                llmConfig={selectedLLMConfig}
              />
            </StepContent>
          </Step>
        )}

        {/* Final Step: Process & Preview (single and dual modes only) */}
        {mode !== 'enhanced' && (
          <Step>
            <StepLabel 
              icon={loading ? <CircularProgress size={24} /> : undefined}
            >
              {currentSteps[currentSteps.length - 1]}
            </StepLabel>
            <StepContent>
              <Alert severity="info">
                Processing your data and preparing visualization...
              </Alert>
              
              {loading && (
                <Box sx={{ display: 'flex', alignItems: 'center', mt: 2 }}>
                  <CircularProgress size={20} sx={{ mr: 2 }} />
                  <Typography variant="body2">
                    {mode === 'dual' ? 'Processing schema and data files...' : 'Parsing file and extracting schema...'}
                  </Typography>
                </Box>
              )}
            </StepContent>
          </Step>
        )}
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

      {/* LLM Selection Dialog */}
      <LLMSelector
        open={showLLMSelector}
        onClose={() => setShowLLMSelector(false)}
        onConfirm={handleLLMSelection}
        title="Select AI Model for Document Processing"
        description="Choose the AI model that will extract information from your uploaded documents using the schema."
        preservedConfig={extractedSchema?.extracted_metadata?.llm_config?.value_extraction_backend || null}
        loading={loading}
      />
    </Box>
  );
};

export default Upload;