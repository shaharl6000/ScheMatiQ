import React from 'react';
import { Brain, Clock, Info } from 'lucide-react';

import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import LLMConfigDisplay from '../LLMConfigDisplay';

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
    <Card className="mb-4">
      <CardContent className="pt-6 space-y-4">
        <h3 className="font-semibold flex items-center gap-2">
          <Info className="h-5 w-5 text-primary" />
          Session Configuration
        </h3>

        {/* Basic Session Info */}
        <div>
          <p className="text-sm font-medium mb-2">Session Information</p>
          <div className="flex flex-wrap gap-2">
            <Badge>
              Type: {session.type === 'load' ? 'Load Existing' : 'QBSD Pipeline'}
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
  );
};

export default ConfigurationInfo;
