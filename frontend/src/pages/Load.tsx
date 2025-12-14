import { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useDropzone } from 'react-dropzone';
import { Upload, ArrowLeft, Loader2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Card } from '@/components/ui/card';
import { cn } from '@/lib/utils';
import { loadAPI } from '../services/api';

const Load = () => {
  const navigate = useNavigate();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // File load handler - uploads, validates, parses, then navigates to visualization
  const handleFileLoad = async (file: File) => {
    setError(null);
    setLoading(true);

    try {
      // Upload and validate file
      const result = await loadAPI.uploadFile(file);

      if (!result.validation.is_valid) {
        setError('File validation failed: ' + result.validation.errors.join(', '));
        setLoading(false);
        return;
      }

      // Parse the file
      await loadAPI.parseFile(result.session_id);

      // Navigate directly to visualization page
      navigate(`/visualize/${result.session_id}?mode=load`);
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || 'Failed to load file');
      setLoading(false);
    }
  };

  // Dropzone configuration
  const onDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      handleFileLoad(acceptedFiles[0]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
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

  return (
    <div className="max-w-3xl mx-auto">
      {/* Header */}
      <h1 className="text-3xl font-bold tracking-tight mb-2">
        Load Existing QBSD
      </h1>

      {/* Explanation */}
      <p className="text-muted-foreground mb-2">
        Load an existing QBSD data file to visualize and edit your research data.
        The system will extract the schema from your data, allowing you to explore
        and add documents for AI-powered information extraction.
      </p>

      <p className="text-sm text-muted-foreground mb-6">
        Supported formats: CSV, JSON, JSONL (up to 100MB)
      </p>

      {/* Load Dropzone */}
      <Card
        {...getRootProps()}
        className={cn(
          "border-2 border-dashed p-12 text-center cursor-pointer transition-all",
          isDragActive
            ? "border-primary bg-primary/5"
            : "border-muted-foreground/25 hover:border-primary hover:bg-muted/50",
          loading && "opacity-50 cursor-not-allowed"
        )}
      >
        <input {...getInputProps()} />
        <Upload className={cn(
          "mx-auto h-12 w-12 mb-4",
          isDragActive ? "text-primary" : "text-muted-foreground"
        )} />

        {loading ? (
          <div className="space-y-2">
            <Loader2 className="mx-auto h-6 w-6 animate-spin text-primary" />
            <h3 className="text-lg font-semibold">
              Processing file...
            </h3>
            <p className="text-sm text-muted-foreground">
              Please wait while we validate and parse your data
            </p>
          </div>
        ) : isDragActive ? (
          <h3 className="text-lg font-semibold text-primary">
            Drop the file here...
          </h3>
        ) : (
          <>
            <h3 className="text-lg font-semibold mb-1">
              Drop your data file here or click to browse
            </h3>
            <p className="text-sm text-muted-foreground">
              CSV, JSON, or JSONL files
            </p>
          </>
        )}
      </Card>

      {/* Error Display */}
      {error && (
        <Alert variant="destructive" className="mt-6">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {/* Back Button */}
      <div className="mt-6">
        <Button
          variant="outline"
          onClick={() => navigate('/')}
          disabled={loading}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to Home
        </Button>
      </div>
    </div>
  );
};

export default Load;
