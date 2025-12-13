import React, { useState, useEffect } from 'react';
import {
  ChevronDown,
  ChevronUp,
  RefreshCw,
  CheckCircle2,
  AlertCircle,
  Info,
  Loader2,
} from 'lucide-react';

import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Progress } from '@/components/ui/progress';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible';
import { cn } from '@/lib/utils';

import { ReprocessingStatus } from '../../types';
import { schemaAPI } from '../../services/api';

interface ReprocessingMonitorProps {
  sessionId: string;
  status?: ReprocessingStatus | null;
  onStatusUpdate?: (status: ReprocessingStatus | null) => void;
}

const ReprocessingMonitor: React.FC<ReprocessingMonitorProps> = ({
  sessionId,
  status,
  onStatusUpdate
}) => {
  const [expanded, setExpanded] = useState(true);
  const [localStatus, setLocalStatus] = useState<ReprocessingStatus | null>(status || null);
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    setLocalStatus(status || null);
  }, [status]);

  // Auto-refresh status when processing
  useEffect(() => {
    if (!localStatus || localStatus.status !== 'processing') return;

    const interval = setInterval(async () => {
      try {
        const updatedStatus = await schemaAPI.getReprocessingStatus(sessionId);
        setLocalStatus(updatedStatus.status === 'processing' ? updatedStatus : null);
        if (onStatusUpdate) {
          onStatusUpdate(updatedStatus.status === 'processing' ? updatedStatus : null);
        }
      } catch (error) {
        console.error('Failed to refresh reprocessing status:', error);
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [localStatus?.status, sessionId, onStatusUpdate]);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      const updatedStatus = await schemaAPI.getReprocessingStatus(sessionId);
      setLocalStatus(updatedStatus.status === 'processing' ? updatedStatus : null);
      if (onStatusUpdate) {
        onStatusUpdate(updatedStatus.status === 'processing' ? updatedStatus : null);
      }
    } catch (error) {
      console.error('Failed to refresh status:', error);
    } finally {
      setRefreshing(false);
    }
  };

  if (!localStatus || localStatus.status === 'idle') {
    return null;
  }

  const getStatusIcon = () => {
    switch (localStatus.status) {
      case 'processing':
        return <Loader2 className="h-4 w-4 animate-spin" />;
      case 'completed':
        return <CheckCircle2 className="h-4 w-4 text-green-500" />;
      case 'failed':
        return <AlertCircle className="h-4 w-4 text-destructive" />;
      default:
        return <Info className="h-4 w-4" />;
    }
  };

  const getStatusVariant = (): "default" | "secondary" | "destructive" | "outline" | "success" | "warning" | "info" => {
    switch (localStatus.status) {
      case 'processing':
        return 'default';
      case 'completed':
        return 'success';
      case 'failed':
        return 'destructive';
      default:
        return 'info';
    }
  };

  const formatEstimatedTime = (estimated?: string): string => {
    if (!estimated) return 'Unknown';

    try {
      const estimatedDate = new Date(estimated);
      const now = new Date();
      const diffMs = estimatedDate.getTime() - now.getTime();

      if (diffMs <= 0) return 'Soon';

      const diffMinutes = Math.ceil(diffMs / (1000 * 60));
      if (diffMinutes < 60) return `~${diffMinutes}m`;

      const diffHours = Math.ceil(diffMinutes / 60);
      return `~${diffHours}h`;
    } catch {
      return 'Unknown';
    }
  };

  return (
    <Card className={cn(
      "mb-4",
      localStatus.status === 'processing' && "border-2 border-primary"
    )}>
      <Collapsible open={expanded} onOpenChange={setExpanded}>
        <CardContent className="pt-4">
          <div className="flex justify-between items-start">
            <div className="flex items-center gap-2 flex-1">
              {getStatusIcon()}
              <h4 className="font-semibold">Document Reprocessing</h4>
              <Badge variant={getStatusVariant()}>
                {localStatus.status.charAt(0).toUpperCase() + localStatus.status.slice(1)}
              </Badge>
            </div>

            <div className="flex items-center gap-1">
              <Button
                variant="ghost"
                size="icon"
                className="h-8 w-8"
                onClick={handleRefresh}
                disabled={refreshing}
              >
                {refreshing ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
              </Button>

              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="icon" className="h-8 w-8">
                  {expanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                </Button>
              </CollapsibleTrigger>
            </div>
          </div>

          <CollapsibleContent>
            <div className="mt-4 space-y-4">
              {/* Progress Bar */}
              {localStatus.status === 'processing' && (
                <div className="space-y-2">
                  <p className="text-sm text-muted-foreground">
                    {localStatus.current_step}
                  </p>

                  <Progress value={localStatus.progress * 100} className="h-2" />

                  <div className="flex justify-between text-xs text-muted-foreground">
                    <span>{Math.round(localStatus.progress * 100)}% complete</span>
                    <span>ETA: {formatEstimatedTime(localStatus.estimated_completion)}</span>
                  </div>
                </div>
              )}

              {/* Document Progress */}
              <div>
                <p className="text-sm mb-2">
                  <strong>Progress:</strong> {localStatus.processed_documents} of {localStatus.total_documents} documents
                </p>

                {localStatus.total_documents > 0 && (
                  <Progress
                    value={(localStatus.processed_documents / localStatus.total_documents) * 100}
                    className="h-1"
                  />
                )}
              </div>

              {/* Affected Columns */}
              {localStatus.affected_columns.length > 0 && (
                <div>
                  <p className="text-sm mb-2"><strong>Affected Columns:</strong></p>
                  <div className="flex flex-wrap gap-1">
                    {localStatus.affected_columns.map((column, index) => (
                      <Badge key={index} variant="outline">
                        {column}
                      </Badge>
                    ))}
                  </div>
                </div>
              )}

              {/* Status Messages */}
              {localStatus.status === 'completed' && (
                <Alert variant="success">
                  <CheckCircle2 className="h-4 w-4" />
                  <AlertDescription>
                    Reprocessing completed successfully! All documents have been processed with the updated schema.
                  </AlertDescription>
                </Alert>
              )}

              {localStatus.status === 'failed' && (
                <Alert variant="destructive">
                  <AlertCircle className="h-4 w-4" />
                  <AlertDescription>
                    Reprocessing failed. Please check the system logs for more details or try again.
                  </AlertDescription>
                </Alert>
              )}

              {localStatus.status === 'processing' && (
                <Alert variant="info">
                  <Info className="h-4 w-4" />
                  <AlertDescription>
                    Reprocessing is in progress. You can continue using other parts of the application while this completes in the background.
                  </AlertDescription>
                </Alert>
              )}
            </div>
          </CollapsibleContent>
        </CardContent>
      </Collapsible>
    </Card>
  );
};

export default ReprocessingMonitor;
