import React, { useState, useCallback } from 'react';
import { AlertTriangle, CheckCircle2, Upload, Loader2, FileText, ChevronDown, ChevronRight, RefreshCw } from 'lucide-react';

import { Alert, AlertDescription } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { cn } from '@/lib/utils';

import { DocumentAvailabilityResponse } from '../../types';
import { loadAPI } from '../../services/api';

interface MissingDocumentsSectionProps {
  sessionId: string;
  availability: DocumentAvailabilityResponse | null;
  loading: boolean;
  onRefresh: () => void;
}

const MissingDocumentsSection: React.FC<MissingDocumentsSectionProps> = ({
  sessionId,
  availability,
  loading,
  onRefresh,
}) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const [uploadedFiles, setUploadedFiles] = useState<File[]>([]);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);

  const handleFileChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    setUploadedFiles(files);
    setUploadError(null);
  }, []);

  const handleUploadAndRecheck = useCallback(async () => {
    if (uploadedFiles.length === 0) return;

    setIsUploading(true);
    setUploadError(null);

    try {
      await loadAPI.addDocuments(sessionId, uploadedFiles);
      setUploadedFiles([]);
      // Re-run the pre-check to update the availability
      onRefresh();
    } catch (error: any) {
      setUploadError(error.response?.data?.detail || 'Failed to upload documents');
    } finally {
      setIsUploading(false);
    }
  }, [sessionId, uploadedFiles, onRefresh]);

  const clearUploadedFiles = useCallback(() => {
    setUploadedFiles([]);
    setUploadError(null);
  }, []);

  // Loading state
  if (loading) {
    return (
      <Alert>
        <Loader2 className="h-4 w-4 animate-spin" />
        <AlertDescription>Checking document availability...</AlertDescription>
      </Alert>
    );
  }

  // No availability data yet
  if (!availability) {
    return null;
  }

  const { missing_documents, local_documents, cloud_documents, can_proceed, rows_with_missing_docs, total_rows } = availability;
  const hasMissingDocs = missing_documents.length > 0;
  const totalAvailable = local_documents.length + cloud_documents.length;

  // All documents available - show success
  if (!hasMissingDocs) {
    return (
      <Alert className="border-green-200 bg-green-50 dark:bg-green-950/20">
        <CheckCircle2 className="h-4 w-4 text-green-600" />
        <AlertDescription className="text-green-800 dark:text-green-200">
          <span className="font-medium">All {totalAvailable} source documents are available.</span>
          {local_documents.length > 0 && cloud_documents.length > 0 && (
            <span className="text-green-600 dark:text-green-400">
              {' '}({local_documents.length} local, {cloud_documents.length} from cloud)
            </span>
          )}
        </AlertDescription>
      </Alert>
    );
  }

  // Some documents missing - show warning with upload option
  return (
    <div className="space-y-3">
      <Alert variant="destructive" className="border-amber-200 bg-amber-50 dark:bg-amber-950/20 [&>svg]:text-amber-600">
        <AlertTriangle className="h-4 w-4" />
        <AlertDescription className="text-amber-800 dark:text-amber-200">
          <div className="space-y-1">
            <p className="font-medium">
              {missing_documents.length} document{missing_documents.length !== 1 ? 's' : ''} missing
              {rows_with_missing_docs > 0 && (
                <span className="text-amber-600 dark:text-amber-400">
                  {' '}({rows_with_missing_docs} of {total_rows} rows will be skipped)
                </span>
              )}
            </p>
            {can_proceed && totalAvailable > 0 && (
              <p className="text-sm text-amber-700 dark:text-amber-300">
                {totalAvailable} document{totalAvailable !== 1 ? 's are' : ' is'} available and will be processed.
              </p>
            )}
          </div>
        </AlertDescription>
      </Alert>

      <Collapsible open={isExpanded} onOpenChange={setIsExpanded}>
        <CollapsibleTrigger asChild>
          <Button variant="ghost" size="sm" className="w-full justify-between p-2 h-auto">
            <div className="flex items-center gap-2 text-sm">
              {isExpanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
              <span>Missing documents ({missing_documents.length})</span>
            </div>
            <span className="text-xs text-muted-foreground">
              Click to {isExpanded ? 'hide' : 'view and upload'}
            </span>
          </Button>
        </CollapsibleTrigger>

        <CollapsibleContent className="space-y-3 pt-2">
          {/* Missing documents list */}
          <Card className="p-3">
            <div className="max-h-32 overflow-y-auto space-y-1">
              {missing_documents.slice(0, 20).map((doc) => (
                <div key={doc.name} className="flex items-center gap-2 text-sm">
                  <FileText className="h-3 w-3 text-muted-foreground" />
                  <span className="truncate flex-1">{doc.name}</span>
                  {doc.affected_rows.length > 0 && (
                    <Badge variant="outline" className="text-xs">
                      {doc.affected_rows.length} row{doc.affected_rows.length !== 1 ? 's' : ''}
                    </Badge>
                  )}
                </div>
              ))}
              {missing_documents.length > 20 && (
                <p className="text-xs text-muted-foreground pt-1">
                  +{missing_documents.length - 20} more documents...
                </p>
              )}
            </div>
          </Card>

          {/* Upload section */}
          <Card className="p-3 space-y-3">
            <div className="flex items-center gap-2">
              <Upload className="h-4 w-4 text-muted-foreground" />
              <span className="text-sm font-medium">Upload missing documents</span>
            </div>

            <input
              type="file"
              multiple
              accept=".txt,.md,.pdf"
              onChange={handleFileChange}
              className="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-primary file:text-primary-foreground hover:file:bg-primary/90 cursor-pointer"
            />

            {uploadedFiles.length > 0 && (
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">
                  {uploadedFiles.length} file{uploadedFiles.length !== 1 ? 's' : ''} selected
                </span>
                <div className="flex gap-2">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={clearUploadedFiles}
                    disabled={isUploading}
                  >
                    Clear
                  </Button>
                  <Button
                    size="sm"
                    onClick={handleUploadAndRecheck}
                    disabled={isUploading}
                    className="gap-1"
                  >
                    {isUploading ? (
                      <>
                        <Loader2 className="h-3 w-3 animate-spin" />
                        Uploading...
                      </>
                    ) : (
                      <>
                        <RefreshCw className="h-3 w-3" />
                        Upload & Re-check
                      </>
                    )}
                  </Button>
                </div>
              </div>
            )}

            {uploadError && (
              <Alert variant="destructive" className="py-2">
                <AlertDescription className="text-xs">{uploadError}</AlertDescription>
              </Alert>
            )}
          </Card>
        </CollapsibleContent>
      </Collapsible>
    </div>
  );
};

export default MissingDocumentsSection;
