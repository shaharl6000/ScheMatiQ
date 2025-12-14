import React, { useState, useEffect, useCallback } from 'react';
import {
  AlertTriangle,
  Check,
  X,
  CheckCircle2,
  FileText,
  Loader2,
  RefreshCw,
  ChevronDown,
  ChevronRight,
  Settings,
  Info,
} from 'lucide-react';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Separator } from '@/components/ui/separator';
import { ScrollArea } from '@/components/ui/scroll-area';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible';
import { useToast } from '@/components/ui/use-toast';
import { cn } from '@/lib/utils';

import { schemaAPI } from '../../services/api';
import { formatColumnName } from '../../utils/formatting';

interface PendingValueDetail {
  value: string;
  document_count: number;
  first_seen: string;
  documents: string[];
}

interface ColumnSuggestion {
  column_name: string;
  pending_values?: PendingValueDetail[];
  current_allowed_values?: string[];
  auto_expand_threshold?: number;
  suggested_values?: string[];
  value_details?: Record<string, PendingValueDetail>;
  auto_approved?: boolean;
}

interface SchemaSuggestionsReviewProps {
  sessionId: string;
  onSuggestionsChange?: () => void;
}

const SchemaSuggestionsReview: React.FC<SchemaSuggestionsReviewProps> = ({
  sessionId,
  onSuggestionsChange,
}) => {
  const { toast } = useToast();
  const [suggestions, setSuggestions] = useState<ColumnSuggestion[]>([]);
  const [totalPending, setTotalPending] = useState(0);
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [expandedColumns, setExpandedColumns] = useState<Set<string>>(new Set());

  const loadSuggestions = useCallback(async () => {
    try {
      setLoading(true);
      const result = await schemaAPI.getSuggestions(sessionId);
      setSuggestions(result.suggestions);
      setTotalPending(result.total_pending);
    } catch (error) {
      console.error('Failed to load suggestions:', error);
      toast({
        title: 'Error',
        description: 'Failed to load schema suggestions',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  }, [sessionId, toast]);

  useEffect(() => {
    loadSuggestions();
  }, [loadSuggestions]);

  const toggleColumnExpanded = (columnName: string) => {
    setExpandedColumns(prev => {
      const next = new Set(prev);
      if (next.has(columnName)) {
        next.delete(columnName);
      } else {
        next.add(columnName);
      }
      return next;
    });
  };

  const handleApprove = async (columnName: string, value: string) => {
    const actionKey = `approve-${columnName}-${value}`;
    setActionLoading(actionKey);
    try {
      await schemaAPI.approveSuggestion(sessionId, columnName, value);
      toast({
        title: 'Value Approved',
        description: `"${value}" added to allowed values for ${formatColumnName(columnName)}`,
      });
      await loadSuggestions();
      onSuggestionsChange?.();
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.response?.data?.detail || 'Failed to approve value',
        variant: 'destructive',
      });
    } finally {
      setActionLoading(null);
    }
  };

  const handleReject = async (columnName: string, value: string) => {
    const actionKey = `reject-${columnName}-${value}`;
    setActionLoading(actionKey);
    try {
      await schemaAPI.rejectSuggestion(sessionId, columnName, value);
      toast({
        title: 'Value Rejected',
        description: `"${value}" removed from suggestions`,
      });
      await loadSuggestions();
      onSuggestionsChange?.();
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.response?.data?.detail || 'Failed to reject value',
        variant: 'destructive',
      });
    } finally {
      setActionLoading(null);
    }
  };

  const handleBulkApprove = async (columnName?: string) => {
    const actionKey = columnName ? `bulk-${columnName}` : 'bulk-all';
    setActionLoading(actionKey);
    try {
      const result = await schemaAPI.bulkApproveSuggestions(sessionId, columnName);
      toast({
        title: 'Bulk Approval Complete',
        description: `Approved ${result.approved_count} values`,
      });
      await loadSuggestions();
      onSuggestionsChange?.();
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.response?.data?.detail || 'Failed to bulk approve',
        variant: 'destructive',
      });
    } finally {
      setActionLoading(null);
    }
  };

  const getPendingValues = (suggestion: ColumnSuggestion): PendingValueDetail[] => {
    if (suggestion.pending_values) {
      return suggestion.pending_values;
    }
    if (suggestion.suggested_values && suggestion.value_details) {
      return suggestion.suggested_values.map(val =>
        suggestion.value_details![val] || {
          value: val,
          document_count: 1,
          first_seen: new Date().toISOString(),
          documents: []
        }
      );
    }
    return [];
  };

  if (loading) {
    return (
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center justify-center py-8">
            <Loader2 className="h-6 w-6 animate-spin mr-2" />
            <span>Loading schema suggestions...</span>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (totalPending === 0) {
    return (
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center justify-center py-8 text-muted-foreground">
            <CheckCircle2 className="h-5 w-5 mr-2 text-green-500" />
            <span>No pending schema suggestions</span>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-lg">
            <AlertTriangle className="h-5 w-5 text-amber-500" />
            Schema Suggestions Review
            <Badge variant="secondary">{totalPending} pending</Badge>
          </CardTitle>
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={loadSuggestions}
              disabled={loading}
            >
              <RefreshCw className={cn("h-4 w-4 mr-1", loading && "animate-spin")} />
              Refresh
            </Button>
            <Button
              variant="default"
              size="sm"
              onClick={() => handleBulkApprove()}
              disabled={actionLoading === 'bulk-all'}
            >
              {actionLoading === 'bulk-all' ? (
                <Loader2 className="h-4 w-4 mr-1 animate-spin" />
              ) : (
                <Check className="h-4 w-4 mr-1" />
              )}
              Approve All
            </Button>
          </div>
        </div>
      </CardHeader>

      <CardContent>
        <Alert className="mb-4">
          <Info className="h-4 w-4" />
          <AlertDescription>
            These values were detected in documents but don't match existing allowed values.
            Review and approve to add them to the schema, or reject to dismiss.
          </AlertDescription>
        </Alert>

        <ScrollArea className="max-h-[500px]">
          <div className="space-y-3">
            {suggestions.map((suggestion) => {
              const pendingValues = getPendingValues(suggestion);
              const isExpanded = expandedColumns.has(suggestion.column_name);

              return (
                <Collapsible
                  key={suggestion.column_name}
                  open={isExpanded}
                  onOpenChange={() => toggleColumnExpanded(suggestion.column_name)}
                >
                  <div className="border rounded-lg">
                    <CollapsibleTrigger className="w-full">
                      <div className="flex items-center justify-between p-3 hover:bg-muted/50 rounded-t-lg">
                        <div className="flex items-center gap-2">
                          {isExpanded ? (
                            <ChevronDown className="h-4 w-4" />
                          ) : (
                            <ChevronRight className="h-4 w-4" />
                          )}
                          <span className="font-medium">
                            {formatColumnName(suggestion.column_name)}
                          </span>
                          <Badge variant="outline" className="text-xs">
                            {pendingValues.length} new value{pendingValues.length !== 1 ? 's' : ''}
                          </Badge>
                        </div>
                        <div className="flex items-center gap-2">
                          {suggestion.auto_expand_threshold && (
                            <Tooltip>
                              <TooltipTrigger>
                                <Badge variant="secondary" className="text-xs gap-1">
                                  <Settings className="h-3 w-3" />
                                  Auto: {suggestion.auto_expand_threshold}+
                                </Badge>
                              </TooltipTrigger>
                              <TooltipContent>
                                Auto-approve values appearing in {suggestion.auto_expand_threshold}+ documents
                              </TooltipContent>
                            </Tooltip>
                          )}
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleBulkApprove(suggestion.column_name);
                            }}
                            disabled={actionLoading === `bulk-${suggestion.column_name}`}
                            className="text-xs"
                          >
                            {actionLoading === `bulk-${suggestion.column_name}` ? (
                              <Loader2 className="h-3 w-3 animate-spin" />
                            ) : (
                              <>Approve All</>
                            )}
                          </Button>
                        </div>
                      </div>
                    </CollapsibleTrigger>

                    <CollapsibleContent>
                      <Separator />
                      <div className="p-3 space-y-2">
                        {/* Current allowed values */}
                        {suggestion.current_allowed_values && suggestion.current_allowed_values.length > 0 && (
                          <div className="mb-3">
                            <p className="text-xs text-muted-foreground mb-1">Current allowed values:</p>
                            <div className="flex flex-wrap gap-1">
                              {suggestion.current_allowed_values.map((val, idx) => (
                                <Badge key={idx} variant="secondary" className="text-xs">
                                  {val}
                                </Badge>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Pending values */}
                        <div className="space-y-2">
                          {pendingValues.map((pv) => {
                            const approveKey = `approve-${suggestion.column_name}-${pv.value}`;
                            const rejectKey = `reject-${suggestion.column_name}-${pv.value}`;
                            const isApproving = actionLoading === approveKey;
                            const isRejecting = actionLoading === rejectKey;

                            return (
                              <div
                                key={pv.value}
                                className="flex items-center justify-between p-2 bg-amber-50 dark:bg-amber-950/30 rounded-md border border-amber-200 dark:border-amber-800"
                              >
                                <div className="flex-1">
                                  <div className="flex items-center gap-2">
                                    <span className="font-medium text-sm">{pv.value}</span>
                                    <Badge variant="outline" className="text-xs">
                                      {pv.document_count} doc{pv.document_count !== 1 ? 's' : ''}
                                    </Badge>
                                  </div>
                                  {pv.documents.length > 0 && (
                                    <div className="mt-1">
                                      <Tooltip>
                                        <TooltipTrigger asChild>
                                          <button className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground">
                                            <FileText className="h-3 w-3" />
                                            View documents
                                          </button>
                                        </TooltipTrigger>
                                        <TooltipContent side="bottom" className="max-w-xs">
                                          <p className="font-semibold mb-1">Found in:</p>
                                          <ul className="text-xs space-y-0.5">
                                            {pv.documents.slice(0, 5).map((doc, idx) => (
                                              <li key={idx} className="truncate">{doc}</li>
                                            ))}
                                            {pv.documents.length > 5 && (
                                              <li className="text-muted-foreground">
                                                +{pv.documents.length - 5} more...
                                              </li>
                                            )}
                                          </ul>
                                        </TooltipContent>
                                      </Tooltip>
                                    </div>
                                  )}
                                </div>
                                <div className="flex gap-1">
                                  <Tooltip>
                                    <TooltipTrigger asChild>
                                      <Button
                                        variant="ghost"
                                        size="icon"
                                        className="h-8 w-8 text-green-600 hover:text-green-700 hover:bg-green-100"
                                        onClick={() => handleApprove(suggestion.column_name, pv.value)}
                                        disabled={isApproving || isRejecting}
                                      >
                                        {isApproving ? (
                                          <Loader2 className="h-4 w-4 animate-spin" />
                                        ) : (
                                          <Check className="h-4 w-4" />
                                        )}
                                      </Button>
                                    </TooltipTrigger>
                                    <TooltipContent>Approve and add to allowed values</TooltipContent>
                                  </Tooltip>
                                  <Tooltip>
                                    <TooltipTrigger asChild>
                                      <Button
                                        variant="ghost"
                                        size="icon"
                                        className="h-8 w-8 text-red-600 hover:text-red-700 hover:bg-red-100"
                                        onClick={() => handleReject(suggestion.column_name, pv.value)}
                                        disabled={isApproving || isRejecting}
                                      >
                                        {isRejecting ? (
                                          <Loader2 className="h-4 w-4 animate-spin" />
                                        ) : (
                                          <X className="h-4 w-4" />
                                        )}
                                      </Button>
                                    </TooltipTrigger>
                                    <TooltipContent>Reject suggestion</TooltipContent>
                                  </Tooltip>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    </CollapsibleContent>
                  </div>
                </Collapsible>
              );
            })}
          </div>
        </ScrollArea>
      </CardContent>
    </Card>
  );
};

export default SchemaSuggestionsReview;
