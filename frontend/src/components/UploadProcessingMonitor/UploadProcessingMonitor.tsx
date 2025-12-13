import React from 'react';
import {
  CheckCircle2,
  Clock,
  Play,
  AlertCircle,
  Table2,
  Database,
  Eye,
} from 'lucide-react';

import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Progress } from '@/components/ui/progress';

import { ProcessingStatus } from '../../types';
import LLMConfigDisplay from '../LLMConfigDisplay';

interface UploadProcessingMonitorProps {
  sessionId?: string | null;
  status: ProcessingStatus | null;
  loading: boolean;
  error?: string | null;
  onNavigateToResults?: () => void;
  llmConfig?: any;
}

const UploadProcessingMonitor: React.FC<UploadProcessingMonitorProps> = ({
  sessionId,
  status,
  loading,
  error,
  onNavigateToResults,
  llmConfig,
}) => {
  const getStatusVariant = (statusValue?: string): "default" | "secondary" | "destructive" | "outline" | "success" | "warning" | "info" => {
    switch (statusValue) {
      case 'completed': return 'success';
      case 'processing_documents': return 'warning';
      case 'error': return 'destructive';
      default: return 'default';
    }
  };

  const getStatusIcon = (statusValue?: string) => {
    switch (statusValue) {
      case 'completed': return <CheckCircle2 className="h-4 w-4 text-green-500" />;
      case 'processing_documents': return <Play className="h-4 w-4 text-yellow-500" />;
      case 'error': return <AlertCircle className="h-4 w-4 text-destructive" />;
      default: return <Clock className="h-4 w-4 text-muted-foreground" />;
    }
  };

  const formatStatus = (statusValue?: string) => {
    switch (statusValue) {
      case 'processing_documents': return 'Processing Documents';
      case 'completed': return 'Processing Complete';
      case 'error': return 'Processing Failed';
      default: return 'Preparing...';
    }
  };

  if (loading && !status) {
    return (
      <div>
        <h3 className="font-semibold mb-4">Starting Document Processing</h3>
        <Progress className="mb-2" />
        <p className="text-sm text-muted-foreground">
          Initializing AI processing pipeline...
        </p>
      </div>
    );
  }

  return (
    <div>
      <h3 className="font-semibold mb-4">Document Processing Status</h3>

      {error && (
        <Alert variant="destructive" className="mb-4">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {status && (
        <>
          {/* Status Overview */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
            <Card>
              <CardContent className="pt-6">
                <div className="flex justify-between items-center mb-4">
                  <h4 className="font-semibold">Processing Status</h4>
                  <Badge variant={getStatusVariant(status.status)} className="gap-1">
                    {getStatusIcon(status.status)}
                    {formatStatus(status.status)}
                  </Badge>
                </div>

                <p className="text-sm text-muted-foreground mb-2">
                  Documents: {status.processed_documents}/{status.total_documents}
                </p>

                <Progress value={status.progress * 100} className="mb-2" />

                <p className="text-sm">
                  Progress: {(status.progress * 100).toFixed(1)}%
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="pt-6">
                <h4 className="font-semibold flex items-center gap-2 mb-4">
                  <Table2 className="h-5 w-5" />
                  Data Statistics
                </h4>

                <div className="space-y-3">
                  <div className="flex justify-between">
                    <span className="text-sm">Original Rows</span>
                    <span className="text-sm font-medium">{status.original_row_count?.toLocaleString() || '0'}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-sm">Additional Rows Added</span>
                    <span className="text-sm font-medium">{status.additional_rows_added?.toLocaleString() || '0'}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-sm">Total Documents</span>
                    <span className="text-sm font-medium">{status.total_documents?.toLocaleString() || '0'}</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Processing Details */}
          {status.status === 'processing_documents' && (
            <Card className="mb-4">
              <CardContent className="pt-6">
                <h4 className="font-semibold flex items-center gap-2 mb-3">
                  <Database className="h-5 w-5" />
                  AI Processing in Progress
                </h4>

                <p className="text-sm text-muted-foreground mb-4">
                  The system is analyzing your uploaded documents using the extracted schema.
                  Each document is being processed to extract structured data according to the
                  discovered column definitions.
                </p>

                <div className="flex flex-wrap gap-2 mb-4">
                  <Badge>
                    {status.processed_documents} of {status.total_documents} documents processed
                  </Badge>
                  <Badge variant="secondary">
                    {status.additional_rows_added} new rows extracted
                  </Badge>
                </div>

                {status.processed_documents > 0 && (
                  <p className="text-sm text-green-600">
                    ✓ Processing is active and making progress
                  </p>
                )}

                {llmConfig && (
                  <div className="mt-4 pt-4 border-t">
                    <LLMConfigDisplay
                      config={llmConfig}
                      title="AI Model Used for Processing"
                      variant="inline"
                      showDetails={true}
                    />
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {/* Processing Stats */}
          {status.processing_stats && Object.keys(status.processing_stats).length > 0 && (
            <Card className="mb-4">
              <CardContent className="pt-6">
                <h4 className="font-semibold mb-4">Processing Statistics</h4>

                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-4">
                  {Object.entries(status.processing_stats).map(([key, value]) => (
                    <div key={key} className="text-center p-4 border rounded-md">
                      <p className="text-2xl font-semibold text-primary">
                        {typeof value === 'number' ? value.toLocaleString() : String(value)}
                      </p>
                      <p className="text-sm text-muted-foreground">
                        {key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                      </p>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Completion Message */}
          {status.status === 'completed' && (
            <Alert variant="success" className="mb-4">
              <CheckCircle2 className="h-4 w-4" />
              <AlertDescription>
                <p className="font-semibold mb-1">
                  Document processing completed successfully!
                </p>
                <p className="text-sm mb-3">
                  Added {status.additional_rows_added} new rows to your dataset from {status.total_documents} documents.
                </p>
                {onNavigateToResults && (
                  <Button onClick={onNavigateToResults} size="lg">
                    <Eye className="h-4 w-4 mr-2" />
                    View Results
                  </Button>
                )}
              </AlertDescription>
            </Alert>
          )}

          {/* Technical Details */}
          <Card className="bg-muted/50">
            <CardContent className="pt-4">
              <p className="text-sm text-muted-foreground">
                <strong>Session ID:</strong> {status.session_id}<br/>
                <strong>Last Updated:</strong> {new Date(status.last_modified).toLocaleString()}
              </p>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
};

export default UploadProcessingMonitor;
