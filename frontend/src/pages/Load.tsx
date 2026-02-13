import { useState, useCallback, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useDropzone } from 'react-dropzone';
import { Upload, ArrowLeft, Loader2, ChevronDown, Database } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
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
    <div className="max-w-4xl mx-auto">
      {/* Top nav bar — matches QBSDConfig */}
      <div className="flex items-center justify-between mb-8">
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="icon" onClick={() => navigate('/')} aria-label="Back to Home">
            <ArrowLeft className="h-5 w-5" />
          </Button>
        </div>
      </div>

      <div className="max-w-3xl mx-auto">
        {/* Header */}
        <h1 className="text-3xl font-bold tracking-tight mb-1">
          Load Data
        </h1>
        <p className="text-muted-foreground mb-8">
          Upload a file or try an example to get started.
        </p>

        {/* Load from Examples */}
        <div className="flex items-center justify-between rounded-lg border border-border/50 bg-muted/30 px-4 py-3 mb-6">
          <div className="flex items-center gap-3">
            <Database className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm text-muted-foreground">
              Try a pre-loaded example
            </span>
          </div>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm" disabled={loading || loadingTemplates}>
                {loadingTemplates ? (
                  <>
                    <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" />
                    Loading...
                  </>
                ) : (
                  <>
                    Select Example
                    <ChevronDown className="ml-2 h-3.5 w-3.5" />
                  </>
                )}
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-64 max-h-80 overflow-y-auto">
              {templates.length === 0 ? (
                <DropdownMenuItem disabled className="py-2 whitespace-normal text-sm">
                  No examples available yet
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

        {/* Dropzone */}
        <div
          {...getRootProps()}
          className={cn(
            "border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-all",
            isDragActive
              ? "border-primary bg-primary/5"
              : "border-muted-foreground/25 hover:border-primary hover:bg-muted/50",
            loading && "opacity-50 cursor-not-allowed"
          )}
        >
          <input {...getInputProps()} />
          <Upload className={cn(
            "mx-auto h-8 w-8 mb-2",
            isDragActive ? "text-primary" : "text-muted-foreground"
          )} />

          {loading ? (
            <div className="space-y-2">
              <Loader2 className="mx-auto h-6 w-6 animate-spin text-primary" />
              <p className="text-sm text-muted-foreground">
                Processing file...
              </p>
            </div>
          ) : isDragActive ? (
            <p className="text-sm text-primary">
              Drop the file here...
            </p>
          ) : (
            <>
              <p className="text-sm text-muted-foreground mb-1">
                Drop your data file here or click to browse
              </p>
              <p className="text-xs text-muted-foreground">
                CSV, JSON, JSONL, or .qbsd.json
              </p>
            </>
          )}
        </div>

        {/* Error Display */}
        {error && (
          <Alert variant="destructive" className="mt-6">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
      </div>
    </div>
  );
};

export default Load;
