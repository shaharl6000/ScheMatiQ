import React, { useState, useEffect, useCallback } from 'react';
import { AlertTriangle, Plus, X, Loader2, FileText, Clock, RefreshCw, Sparkles } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible';
import { useToast } from '@/components/ui/use-toast';

import { ObservationUnitInfo } from '../../types';
import { observationUnitAPI } from '../../services/api';

// Validation limits for observation unit fields
const VALIDATION_LIMITS = {
  NAME_MAX_LENGTH: 100,
  DEFINITION_MIN_LENGTH: 10,
  DEFINITION_MAX_LENGTH: 500,
  MAX_EXAMPLE_NAMES: 20,
  EXAMPLE_NAME_MAX_LENGTH: 100,
} as const;

interface ObservationUnitEditModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sessionId: string;
  observationUnit: ObservationUnitInfo;
  onUpdate: (updated: ObservationUnitInfo) => void;
  onReextractionRequest?: () => void;
  onRegenerateSchema?: () => void;
}

const ObservationUnitEditModal: React.FC<ObservationUnitEditModalProps> = ({
  open,
  onOpenChange,
  sessionId,
  observationUnit,
  onUpdate,
  onReextractionRequest,
  onRegenerateSchema,
}) => {
  const { toast } = useToast();
  type LoadingAction = 'save' | 'reextract' | 'rediscover' | null;
  const [loadingAction, setLoadingAction] = useState<LoadingAction>(null);
  const [name, setName] = useState('');
  const [definition, setDefinition] = useState('');
  const [exampleNames, setExampleNames] = useState<string[]>([]);
  const [newExample, setNewExample] = useState('');
  const [metadataOpen, setMetadataOpen] = useState(false);

  // Validation states
  const [errors, setErrors] = useState<{ name?: string; definition?: string }>({});

  // Initialize form when modal opens or observationUnit changes
  useEffect(() => {
    if (open && observationUnit) {
      setName(observationUnit.name || '');
      setDefinition(observationUnit.definition || '');
      setExampleNames(observationUnit.example_names || []);
      setNewExample('');
      setErrors({});
    }
  }, [open, observationUnit]);

  const validateForm = useCallback((): boolean => {
    const newErrors: { name?: string; definition?: string } = {};

    if (!name.trim()) {
      newErrors.name = 'Name is required';
    } else if (name.length > VALIDATION_LIMITS.NAME_MAX_LENGTH) {
      newErrors.name = `Name must be ${VALIDATION_LIMITS.NAME_MAX_LENGTH} characters or less`;
    }

    if (!definition.trim()) {
      newErrors.definition = 'Definition is required';
    } else if (definition.length < VALIDATION_LIMITS.DEFINITION_MIN_LENGTH) {
      newErrors.definition = `Definition must be at least ${VALIDATION_LIMITS.DEFINITION_MIN_LENGTH} characters`;
    } else if (definition.length > VALIDATION_LIMITS.DEFINITION_MAX_LENGTH) {
      newErrors.definition = `Definition must be ${VALIDATION_LIMITS.DEFINITION_MAX_LENGTH} characters or less`;
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  }, [name, definition]);

  const handleAddExample = useCallback(() => {
    const trimmed = newExample.trim();
    if (!trimmed) return;

    if (trimmed.length > VALIDATION_LIMITS.EXAMPLE_NAME_MAX_LENGTH) {
      toast({
        title: 'Invalid example',
        description: `Example name must be ${VALIDATION_LIMITS.EXAMPLE_NAME_MAX_LENGTH} characters or less`,
        variant: 'destructive',
      });
      return;
    }

    if (exampleNames.length >= VALIDATION_LIMITS.MAX_EXAMPLE_NAMES) {
      toast({
        title: 'Limit reached',
        description: `Maximum ${VALIDATION_LIMITS.MAX_EXAMPLE_NAMES} example names allowed`,
        variant: 'destructive',
      });
      return;
    }

    if (exampleNames.includes(trimmed)) {
      toast({
        title: 'Duplicate',
        description: 'This example already exists',
        variant: 'destructive',
      });
      return;
    }

    setExampleNames([...exampleNames, trimmed]);
    setNewExample('');
  }, [newExample, exampleNames, toast]);

  const handleRemoveExample = useCallback((index: number) => {
    setExampleNames(prev => prev.filter((_, i) => i !== index));
  }, []);

  const handleSave = async () => {
    if (!validateForm()) return;

    setLoadingAction('save');
    try {
      const response = await observationUnitAPI.updateDefinition(sessionId, {
        name: name.trim(),
        definition: definition.trim(),
        example_names: exampleNames.length > 0 ? exampleNames : undefined,
      });

      // Call onUpdate with the updated observation unit
      onUpdate({
        name: response.observation_unit.name,
        definition: response.observation_unit.definition,
        example_names: response.observation_unit.example_names,
        source_document: response.observation_unit.source_document,
        discovery_iteration: response.observation_unit.discovery_iteration,
      });

      toast({
        title: 'Success',
        description: response.message,
      });


      onOpenChange(false);
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.response?.data?.detail || 'Failed to update observation unit definition',
        variant: 'destructive',
      });
    } finally {
      setLoadingAction(null);
    }
  };

  const handleSaveAndReextract = async () => {
    if (!validateForm()) return;

    setLoadingAction('reextract');
    try {
      const response = await observationUnitAPI.updateDefinition(sessionId, {
        name: name.trim(),
        definition: definition.trim(),
        example_names: exampleNames.length > 0 ? exampleNames : undefined,
      });

      onUpdate({
        name: response.observation_unit.name,
        definition: response.observation_unit.definition,
        example_names: response.observation_unit.example_names,
        source_document: response.observation_unit.source_document,
        discovery_iteration: response.observation_unit.discovery_iteration,
      });

      toast({
        title: 'Success',
        description: 'Definition updated. Opening re-extraction dialog...',
      });

      onOpenChange(false);

      if (onReextractionRequest) {
        onReextractionRequest();
      }
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.response?.data?.detail || 'Failed to update observation unit definition',
        variant: 'destructive',
      });
    } finally {
      setLoadingAction(null);
    }
  };

  const handleSaveAndRegenerate = async () => {
    if (!validateForm()) return;

    setLoadingAction('rediscover');
    try {
      const response = await observationUnitAPI.updateDefinition(sessionId, {
        name: name.trim(),
        definition: definition.trim(),
        example_names: exampleNames.length > 0 ? exampleNames : undefined,
      });

      onUpdate({
        name: response.observation_unit.name,
        definition: response.observation_unit.definition,
        example_names: response.observation_unit.example_names,
        source_document: response.observation_unit.source_document,
        discovery_iteration: response.observation_unit.discovery_iteration,
      });

      toast({
        title: 'Success',
        description: 'Definition updated. Rediscovering schema...',
      });

      onOpenChange(false);

      // Trigger schema rediscovery
      if (onRegenerateSchema) {
        onRegenerateSchema();
      }
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.response?.data?.detail || 'Failed to update observation unit definition',
        variant: 'destructive',
      });
    } finally {
      setLoadingAction(null);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey && e.target === document.activeElement) {
      const target = e.target as HTMLElement;
      if (target.tagName !== 'TEXTAREA') {
        e.preventDefault();
        if (target.id === 'new-example-input') {
          handleAddExample();
        }
      }
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[550px]" onKeyDown={handleKeyDown}>
        <DialogHeader>
          <DialogTitle>Edit Observation Unit</DialogTitle>
          <DialogDescription>
            Define what constitutes a single row in your extracted table.
          </DialogDescription>
        </DialogHeader>

        {/* Warning Banner */}
        <Alert variant="default" className="bg-amber-50 border-amber-200">
          <AlertTriangle className="h-4 w-4 text-amber-600" />
          <AlertDescription className="text-amber-800 text-sm">
            Changing this definition may affect data consistency. Existing rows were
            extracted with the previous definition.
          </AlertDescription>
        </Alert>

        <div className="space-y-4 py-4">
          {/* Name Field */}
          <div className="space-y-2">
            <Label htmlFor="name">
              Name <span className="text-red-500">*</span>
            </Label>
            <Input
              id="name"
              value={name}
              onChange={(e) => {
                setName(e.target.value);
                if (errors.name) setErrors({ ...errors, name: undefined });
              }}
              placeholder="e.g., Protein, Model-Benchmark Evaluation"
              maxLength={VALIDATION_LIMITS.NAME_MAX_LENGTH}
              className={errors.name ? 'border-red-500' : ''}
            />
            <div className="flex justify-between">
              {errors.name ? (
                <span className="text-xs text-red-500">{errors.name}</span>
              ) : (
                <span className="text-xs text-muted-foreground">Short label for rows</span>
              )}
              <span className="text-xs text-muted-foreground">{name.length}/{VALIDATION_LIMITS.NAME_MAX_LENGTH}</span>
            </div>
          </div>

          {/* Definition Field */}
          <div className="space-y-2">
            <Label htmlFor="definition">
              Definition <span className="text-red-500">*</span>
            </Label>
            <Textarea
              id="definition"
              value={definition}
              onChange={(e) => {
                setDefinition(e.target.value);
                if (errors.definition) setErrors({ ...errors, definition: undefined });
              }}
              placeholder="e.g., Each row represents a unique protein mentioned in the documents..."
              rows={3}
              maxLength={VALIDATION_LIMITS.DEFINITION_MAX_LENGTH}
              className={errors.definition ? 'border-red-500' : ''}
            />
            <div className="flex justify-between">
              {errors.definition ? (
                <span className="text-xs text-red-500">{errors.definition}</span>
              ) : (
                <span className="text-xs text-muted-foreground">
                  Describe what constitutes one row ({VALIDATION_LIMITS.DEFINITION_MIN_LENGTH}-{VALIDATION_LIMITS.DEFINITION_MAX_LENGTH} chars)
                </span>
              )}
              <span className="text-xs text-muted-foreground">{definition.length}/{VALIDATION_LIMITS.DEFINITION_MAX_LENGTH}</span>
            </div>
          </div>

          {/* Example Names */}
          <div className="space-y-2">
            <Label>Example Names (optional)</Label>
            <div className="flex gap-2">
              <Input
                id="new-example-input"
                value={newExample}
                onChange={(e) => setNewExample(e.target.value)}
                placeholder="Add an example..."
                maxLength={VALIDATION_LIMITS.EXAMPLE_NAME_MAX_LENGTH}
                disabled={exampleNames.length >= VALIDATION_LIMITS.MAX_EXAMPLE_NAMES}
              />
              <Button
                type="button"
                variant="outline"
                size="icon"
                onClick={handleAddExample}
                disabled={!newExample.trim() || exampleNames.length >= VALIDATION_LIMITS.MAX_EXAMPLE_NAMES}
              >
                <Plus className="h-4 w-4" />
              </Button>
            </div>
            {exampleNames.length > 0 && (
              <div className="flex flex-wrap gap-1 mt-2">
                {exampleNames.map((example, index) => (
                  <Badge key={index} variant="secondary" className="pr-1">
                    {example}
                    <button
                      type="button"
                      onClick={() => handleRemoveExample(index)}
                      className="ml-1 hover:text-red-500"
                    >
                      <X className="h-3 w-3" />
                    </button>
                  </Badge>
                ))}
              </div>
            )}
            <span className="text-xs text-muted-foreground">
              {exampleNames.length}/{VALIDATION_LIMITS.MAX_EXAMPLE_NAMES} examples
            </span>
          </div>

          {/* Read-only Metadata */}
          {(observationUnit.source_document || observationUnit.discovery_iteration !== undefined) && (
            <Collapsible open={metadataOpen} onOpenChange={setMetadataOpen}>
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm" className="w-full justify-start text-muted-foreground">
                  {metadataOpen ? 'Hide' : 'Show'} discovery metadata
                </Button>
              </CollapsibleTrigger>
              <CollapsibleContent className="space-y-2 pt-2">
                {observationUnit.source_document && (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <FileText className="h-4 w-4" />
                    <span>Source: {observationUnit.source_document}</span>
                  </div>
                )}
                {observationUnit.discovery_iteration !== undefined && (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Clock className="h-4 w-4" />
                    <span>Discovered in iteration {observationUnit.discovery_iteration}</span>
                  </div>
                )}
              </CollapsibleContent>
            </Collapsible>
          )}
        </div>

        <DialogFooter className="sm:justify-between">
          <Button variant="ghost" onClick={() => onOpenChange(false)} disabled={!!loadingAction}>
            Cancel
          </Button>
          <div className="flex gap-2">
            {onReextractionRequest && (
              <Button
                variant="secondary"
                onClick={handleSaveAndReextract}
                disabled={!!loadingAction}
                size="sm"
              >
                {loadingAction === 'reextract' ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <RefreshCw className="h-3 w-3 mr-1" />}
                Re-extract Table
              </Button>
            )}
            {onRegenerateSchema && (
              <Button
                variant="secondary"
                onClick={handleSaveAndRegenerate}
                disabled={!!loadingAction}
                size="sm"
              >
                {loadingAction === 'rediscover' ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Sparkles className="h-3 w-3 mr-1" />}
                Rediscover Schema
              </Button>
            )}
            <Button onClick={handleSave} disabled={!!loadingAction} className="min-w-[70px]">
              {loadingAction === 'save' ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Save'}
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default ObservationUnitEditModal;
