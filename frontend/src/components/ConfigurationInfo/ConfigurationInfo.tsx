import React from 'react';
import { Brain, Clock, Info, Plus, Edit, Trash2, CheckCircle, XCircle } from 'lucide-react';

import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import LLMConfigDisplay from '../LLMConfigDisplay';
import { CreationMetadata, ModificationAction } from '../../types';

interface ConfigurationInfoProps {
  session?: any;
  compact?: boolean;
}

const ConfigurationInfo: React.FC<ConfigurationInfoProps> = ({
  session,
  compact = false,
}) => {
  if (!session?.metadata) {
    return null;
  }

  const { metadata } = session;
  const creationMetadata: CreationMetadata | undefined = session.creation_metadata;
  const modificationHistory: ModificationAction[] = session.modification_history || [];
  const llmConfig = metadata?.extracted_schema?.llm_configuration;
  const schemaBackend = llmConfig?.schema_creation_backend;
  const extractionBackend = llmConfig?.value_extraction_backend;

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'Unknown';
    try {
      return new Date(dateString).toLocaleDateString();
    } catch {
      return 'Unknown';
    }
  };

  const formatDateTime = (dateString?: string) => {
    if (!dateString) return 'Unknown';
    try {
      return new Date(dateString).toLocaleString();
    } catch {
      return 'Unknown';
    }
  };

  const getActionIcon = (actionType: string) => {
    switch (actionType) {
      case 'column_added':
        return <Plus className="h-4 w-4 text-green-600" />;
      case 'column_edited':
        return <Edit className="h-4 w-4 text-blue-600" />;
      case 'column_deleted':
        return <Trash2 className="h-4 w-4 text-red-600" />;
      default:
        return <Info className="h-4 w-4" />;
    }
  };

  const formatActionDetails = (action: ModificationAction) => {
    switch (action.action_type) {
      case 'column_added':
        return `Added column "${action.column_name}"`;
      case 'column_edited':
        const changes = [];
        if (action.details.definition_changed) changes.push('definition');
        if (action.details.rationale_changed) changes.push('rationale');
        if (action.details.allowed_values_changed) changes.push('allowed values');
        if (action.details.new_name) changes.push(`renamed from "${action.details.original_name}"`);
        return `Edited "${action.column_name}"${changes.length > 0 ? `: ${changes.join(', ')}` : ''}`;
      case 'column_deleted':
        return `Deleted column "${action.column_name}"`;
      default:
        return action.action_type;
    }
  };

  if (compact) {
    return (
      <div className="flex items-center gap-2 flex-wrap">
        {schemaBackend && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge variant="outline" className="gap-1 cursor-help">
                <Brain className="h-3 w-3" />
                Schema: {schemaBackend.model?.replace('gemini-', '') || 'Unknown'}
              </Badge>
            </TooltipTrigger>
            <TooltipContent>
              Schema created with {schemaBackend.provider} {schemaBackend.model}
            </TooltipContent>
          </Tooltip>
        )}

        {extractionBackend && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge variant="secondary" className="gap-1 cursor-help">
                <Brain className="h-3 w-3" />
                Extraction: {extractionBackend.model?.replace('gemini-', '') || 'Unknown'}
              </Badge>
            </TooltipTrigger>
            <TooltipContent>
              Value extraction with {extractionBackend.provider} {extractionBackend.model}
            </TooltipContent>
          </Tooltip>
        )}

        {metadata.created && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge variant="outline" className="gap-1 cursor-help">
                <Clock className="h-3 w-3" />
                {formatDate(metadata.created)}
              </Badge>
            </TooltipTrigger>
            <TooltipContent>
              Created: {formatDate(metadata.created)}
            </TooltipContent>
          </Tooltip>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Creation Information Card */}
      <Card>
        <CardContent className="pt-6 space-y-4">
          <h3 className="font-semibold flex items-center gap-2">
            <Info className="h-5 w-5 text-primary" />
            Creation Information
          </h3>

          {/* Creation Metadata */}
          {creationMetadata ? (
            <div className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="p-3 bg-muted rounded-md">
                  <p className="text-xs text-muted-foreground">Created</p>
                  <p className="font-medium">{formatDateTime(creationMetadata.created_at)}</p>
                </div>
                <div className="p-3 bg-muted rounded-md">
                  <p className="text-xs text-muted-foreground">LLM Model</p>
                  <p className="font-medium">{creationMetadata.llm_model || 'Unknown'}</p>
                </div>
                <div className="p-3 bg-muted rounded-md">
                  <p className="text-xs text-muted-foreground">Iterations</p>
                  <p className="font-medium">{creationMetadata.iterations_count || 0}</p>
                </div>
                <div className="p-3 bg-muted rounded-md">
                  <p className="text-xs text-muted-foreground">Final Schema Size</p>
                  <p className="font-medium">{creationMetadata.final_schema_size || 0} columns</p>
                </div>
              </div>

              {/* Creation Query */}
              {creationMetadata.creation_query && (
                <div>
                  <p className="text-sm font-medium mb-2">Creation Query</p>
                  <p className="text-sm text-muted-foreground p-3 bg-muted rounded-md italic">
                    "{creationMetadata.creation_query}"
                  </p>
                </div>
              )}

              {/* Convergence Status */}
              <div className="flex items-center gap-2">
                {creationMetadata.convergence_achieved ? (
                  <CheckCircle className="h-4 w-4 text-green-500" />
                ) : (
                  <XCircle className="h-4 w-4 text-yellow-500" />
                )}
                <span className="text-sm text-muted-foreground">
                  {creationMetadata.convergence_achieved
                    ? 'Schema converged successfully'
                    : 'Schema creation stopped before convergence'}
                </span>
              </div>
            </div>
          ) : (
            <div className="space-y-4">
              {/* Fallback to basic session info */}
              <div className="flex flex-wrap gap-2">
                <Badge>
                  Type: {session.type === 'load' ? 'Load Existing' : 'ScheMatiQ Pipeline'}
                </Badge>
                {metadata.created && (
                  <Badge variant="outline">
                    Created: {formatDate(metadata.created)}
                  </Badge>
                )}
                {metadata.last_modified && (
                  <Badge variant="outline">
                    Modified: {formatDate(metadata.last_modified)}
                  </Badge>
                )}
              </div>

              {/* Query Information */}
              {session.schema_query && (
                <div>
                  <p className="text-sm font-medium mb-2">Research Query</p>
                  <p className="text-sm text-muted-foreground p-3 bg-muted rounded-md italic">
                    "{session.schema_query}"
                  </p>
                </div>
              )}
            </div>
          )}

          {/* AI Model Configuration */}
          {(schemaBackend || extractionBackend) && (
            <>
              <Separator />
              <p className="text-sm font-medium mb-2">AI Model Configuration</p>

              <div className="space-y-4">
                {schemaBackend && (
                  <LLMConfigDisplay
                    config={schemaBackend}
                    title="Schema Creation Model"
                    variant="inline"
                    showDetails={true}
                  />
                )}

                {extractionBackend && (
                  <LLMConfigDisplay
                    config={extractionBackend}
                    title="Value Extraction Model"
                    variant="inline"
                    showDetails={true}
                  />
                )}
              </div>
            </>
          )}

          {/* Data Source */}
          {metadata.source && (
            <div>
              <p className="text-sm font-medium mb-2">Data Source</p>
              <p className="text-sm text-muted-foreground">{metadata.source}</p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Modification History Card */}
      <Card>
        <CardContent className="pt-6 space-y-4">
          <h3 className="font-semibold flex items-center gap-2">
            <Edit className="h-5 w-5 text-primary" />
            Modification History
          </h3>

          {modificationHistory.length > 0 ? (
            <div className="space-y-3">
              {modificationHistory.map((modification, index) => (
                <div
                  key={index}
                  className="flex items-start gap-3 p-3 rounded-md bg-muted/50 border border-border"
                >
                  <div className="flex items-center justify-center w-8 h-8 rounded-full bg-background border border-border">
                    {getActionIcon(modification.action_type)}
                  </div>

                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium text-foreground">
                      {formatActionDetails(modification)}
                    </div>
                    <div className="text-xs text-muted-foreground mt-1">
                      {formatDateTime(modification.timestamp)}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-8 text-muted-foreground">
              <Edit className="h-8 w-8 mx-auto mb-2 opacity-50" />
              <p>No modifications have been made to this schema.</p>
              <p className="text-sm mt-1">
                Changes will appear here when you edit, add, or delete columns.
              </p>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default ConfigurationInfo;
