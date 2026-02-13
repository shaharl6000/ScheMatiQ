import { useState, useCallback, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useDropzone } from 'react-dropzone';
import { Upload, ArrowLeft, Loader2, FileSpreadsheet, ChevronDown, Database } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Card } from '@/components/ui/card';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { cn } from '@/lib/utils';
import { loadAPI, cloudAPI } from '../services/api';

interface Template {
  name: string;
  path: string;
  file_type: string;
  description?: string;
  row_count?: number;
  column_count?: number;
}

const Load = () => {
  const navigate = useNavigate();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [templates, setTemplates] = useState<Template[]>([]);
  const [loadingTemplates, setLoadingTemplates] = useState(true);

  // Fetch available templates on mount
  useEffect(() => {
    const fetchTemplates = async () => {
      try {
        const data = await cloudAPI.getTemplates();
        // Ensure data is an array before setting
        setTemplates(Array.isArray(data) ? data : []);
      } catch (err) {
        // Templates not available — expected when none configured
        setTemplates([]);
      } finally {
        setLoadingTemplates(false);
      }
    };
    fetchTemplates();
  }, []);

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

  // Template load handler
  const handleTemplateLoad = async (templateName: string) => {
    setError(null);
    setLoading(true);

    try {
      const result = await cloudAPI.loadTemplate(templateName);
      navigate(`/visualize/${result.session_id}?mode=load`);
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || 'Failed to load template');
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
      <p className="text-muted-foreground mb-6">
        Load a data file to visualize, edit, and extend with AI extraction.
      </p>

      {/* Template Selection */}
      <Card className="p-4 mb-4 bg-muted/30">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Database className="h-5 w-5 text-primary" />
            <div>
              <h3 className="font-medium">Load from Examples</h3>
              <p className="text-sm text-muted-foreground">
                Choose from pre-loaded example tables
              </p>
            </div>
          </div>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" disabled={loading || loadingTemplates}>
                <FileSpreadsheet className="mr-2 h-4 w-4" />
                Select Example
                <ChevronDown className="ml-2 h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-64 max-h-80 overflow-y-auto">
              {loadingTemplates ? (
                <DropdownMenuItem disabled className="flex items-center gap-2 py-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span>Loading examples...</span>
                </DropdownMenuItem>
              ) : templates.length === 0 ? (
                <DropdownMenuItem disabled className="py-2 whitespace-normal">
                  Example datasets are being prepared. Upload your own file to get started.
                </DropdownMenuItem>
              ) : (
                templates.map((template) => (
                  <DropdownMenuItem
                    key={template.name}
                    onClick={() => handleTemplateLoad(template.name)}
                    className="flex flex-col items-start py-2"
                  >
                    <span className="font-medium">{template.name}</span>
                    <span className="text-xs text-muted-foreground">
                      {template.file_type.toUpperCase()}
                      {template.row_count !== undefined && ` • ${template.row_count} rows`}
                      {template.column_count !== undefined && ` • ${template.column_count} cols`}
                    </span>
                  </DropdownMenuItem>
                ))
              )}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </Card>

      {/* Divider */}
      <div className="relative my-6">
        <div className="absolute inset-0 flex items-center">
          <span className="w-full border-t" />
        </div>
        <div className="relative flex justify-center text-xs uppercase">
          <span className="bg-background px-2 text-muted-foreground">
            Or upload your own file
          </span>
        </div>
      </div>

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
              CSV, JSON, JSONL, or saved project files (.qbsd.json)
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
