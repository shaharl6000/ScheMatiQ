import { useState } from 'react';
import { ChevronDown } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Checkbox } from '@/components/ui/checkbox';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { InfoTooltip } from '@/components/InfoTooltip/InfoTooltip';
import { ModelSelector } from '@/components/ModelSelector';
import InitialSchemaEditor from '@/components/InitialSchemaEditor/InitialSchemaEditor';
import {
  DEFAULT_DOCUMENT_RANDOMIZATION_SEED,
  DEFAULT_DOCUMENTS_BATCH_SIZE,
  DEFAULT_MAX_KEYS_SCHEMA,
  DEFAULT_SCHEMA_MODEL,
  DEFAULT_RELEASE_EXTRACTION_MODEL,
  LLM_PROVIDER_NAMES,
  getDefaultModelForProvider,
  type LLMProviderKey,
} from '@/constants';
import type { InitialSchemaColumn } from '@/types';

/**
 * Normalized model for the advanced extraction settings, shared by the classic
 * config screen and the Workspace new-project dialog. Each screen adapts this
 * model to/from its own config representation; this component only renders the
 * fields and reports changes through onChange.
 */
export interface AdvancedSettingsValue {
  skipValueExtraction: boolean;
  maxKeysSchema: number;
  batchStrategy: 'smart' | 'fixed';
  documentsBatchSize: number;
  // Developer-mode schema params
  seed: number;
  convergenceThreshold: number | null;
  // Observation unit
  observationUnitMode: 'auto' | 'name_only' | 'full';
  observationUnitName: string;
  observationUnitDefinition: string;
  reviewObservationUnit: boolean;
  // LLM configuration (shown when allowLlmConfig)
  schemaProvider: string;
  schemaModel: string;
  schemaTemperature: number;
  valueProvider: string;
  valueModel: string;
  valueTemperature: number;
  // Retriever (developer mode)
  retrieverModelName: string;
  retrieverPassageChars: number;
  retrieverOverlap: number;
  retrieverK: number;
  retrieverDynamicK: number;
  // Developer mode: bypass the document limit on upload
  bypassLimit: boolean;
  // Pre-defined columns
  initialSchemaPath?: string;
  initialSchemaData?: InitialSchemaColumn[] | null;
}

/** Backend retriever defaults (mirror backend RetrieverConfig pydantic model). */
export const RETRIEVER_DEFAULTS = {
  model_name: 'all-MiniLM-L6-v2',
  passage_chars: 512,
  overlap: 64,
  k: 15,
  dynamic_k_threshold: 0.65,
} as const;

export const DEFAULT_ADVANCED_SETTINGS: AdvancedSettingsValue = {
  skipValueExtraction: false,
  maxKeysSchema: DEFAULT_MAX_KEYS_SCHEMA,
  batchStrategy: 'smart',
  documentsBatchSize: DEFAULT_DOCUMENTS_BATCH_SIZE,
  seed: DEFAULT_DOCUMENT_RANDOMIZATION_SEED,
  convergenceThreshold: null,
  observationUnitMode: 'auto',
  observationUnitName: '',
  observationUnitDefinition: '',
  reviewObservationUnit: false,
  schemaProvider: 'gemini',
  schemaModel: DEFAULT_SCHEMA_MODEL,
  schemaTemperature: 0,
  valueProvider: 'gemini',
  valueModel: DEFAULT_RELEASE_EXTRACTION_MODEL,
  valueTemperature: 0,
  retrieverModelName: RETRIEVER_DEFAULTS.model_name,
  retrieverPassageChars: RETRIEVER_DEFAULTS.passage_chars,
  retrieverOverlap: RETRIEVER_DEFAULTS.overlap,
  retrieverK: RETRIEVER_DEFAULTS.k,
  retrieverDynamicK: RETRIEVER_DEFAULTS.dynamic_k_threshold,
  bypassLimit: false,
  initialSchemaPath: undefined,
  initialSchemaData: null,
};

/** True when any retriever field differs from the backend defaults. */
export function retrieverIsCustomized(value: AdvancedSettingsValue): boolean {
  return (
    value.retrieverModelName !== RETRIEVER_DEFAULTS.model_name ||
    value.retrieverPassageChars !== RETRIEVER_DEFAULTS.passage_chars ||
    value.retrieverOverlap !== RETRIEVER_DEFAULTS.overlap ||
    value.retrieverK !== RETRIEVER_DEFAULTS.k ||
    value.retrieverDynamicK !== RETRIEVER_DEFAULTS.dynamic_k_threshold
  );
}

/** Builds the initial_observation_unit payload from the model, if any. */
export function observationUnitFromValue(
  value: AdvancedSettingsValue,
): { name: string; definition?: string } | undefined {
  const name = value.observationUnitName.trim();
  if (value.observationUnitMode === 'name_only' && name) {
    return { name };
  }
  if (value.observationUnitMode === 'full' && name) {
    return { name, definition: value.observationUnitDefinition.trim() || undefined };
  }
  return undefined;
}

interface AdvancedSettingsFieldsProps {
  value: AdvancedSettingsValue;
  onChange: (patch: Partial<AdvancedSettingsValue>) => void;
  developerMode: boolean;
  allowLlmConfig: boolean;
  providers: LLMProviderKey[];
  maxDocuments?: number;
  className?: string;
}

export function AdvancedSettingsFields({
  value,
  onChange,
  developerMode,
  allowLlmConfig,
  providers,
  maxDocuments,
  className,
}: AdvancedSettingsFieldsProps) {
  const [editingSchemaLlm, setEditingSchemaLlm] = useState(false);
  const [editingValueLlm, setEditingValueLlm] = useState(false);

  return (
    <div className={className ?? 'space-y-4'}>
      {/* Schema-only mode */}
      <div className="flex items-center gap-2">
        <Checkbox
          id="adv-skip-value"
          checked={value.skipValueExtraction}
          onCheckedChange={(checked) => onChange({ skipValueExtraction: checked === true })}
        />
        <Label htmlFor="adv-skip-value" className="text-sm cursor-pointer inline-flex items-center gap-1.5">
          Discover columns only (skip data extraction)
          <InfoTooltip text="Discover only the table schema without extracting data values. Faster and lower cost." />
        </Label>
      </div>

      {/* Schema parameters */}
      <div className="space-y-3">
        <Label className="text-sm font-medium">Schema Parameters</Label>
        <div className="grid grid-cols-3 gap-3">
          <div className="space-y-1">
            <Label htmlFor="adv-max-keys" className="text-xs text-muted-foreground inline-flex items-center gap-1">
              Max Columns
              <InfoTooltip text="Maximum number of columns in your table." />
            </Label>
            <Input
              id="adv-max-keys"
              type="number"
              min={1}
              max={500}
              value={value.maxKeysSchema}
              onChange={(e) => onChange({ maxKeysSchema: parseInt(e.target.value, 10) || DEFAULT_MAX_KEYS_SCHEMA })}
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="adv-batch-strategy" className="text-xs text-muted-foreground inline-flex items-center gap-1">
              Batching
              <InfoTooltip text="How documents are grouped for schema discovery. Smart packs as many documents as fit the model's context window automatically. Fixed lets you set an exact number per batch." />
            </Label>
            <Select
              value={value.batchStrategy}
              onValueChange={(strategy) => onChange({ batchStrategy: strategy as 'smart' | 'fixed' })}
            >
              <SelectTrigger id="adv-batch-strategy"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="smart">Smart (automatic)</SelectItem>
                <SelectItem value="fixed">Fixed size</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {value.batchStrategy === 'fixed' && (
            <div className="space-y-1">
              <Label htmlFor="adv-batch-size" className="text-xs text-muted-foreground inline-flex items-center gap-1">
                Docs per batch
                <InfoTooltip text="How many documents are analyzed together in each schema refinement step." />
              </Label>
              <Input
                id="adv-batch-size"
                type="number"
                min={1}
                max={20}
                value={value.documentsBatchSize}
                onChange={(e) => onChange({ documentsBatchSize: parseInt(e.target.value, 10) || DEFAULT_DOCUMENTS_BATCH_SIZE })}
              />
            </div>
          )}
        </div>

        {value.batchStrategy === 'fixed' ? (
          <p className="text-xs text-muted-foreground">
            Analyzing more documents together gives the model more context and usually
            produces a richer schema. Each model has a limited context window, so higher
            values raise the chance of exceeding it; batches that do are split automatically.
          </p>
        ) : (
          <p className="text-xs text-muted-foreground">
            Documents are grouped automatically to fit as many as possible within the
            model's context window, giving it broad context for a richer schema without
            exceeding the limit.
          </p>
        )}
        {developerMode && (
          <div className="grid grid-cols-2 gap-3">
              <div className="space-y-1">
                <Label htmlFor="adv-seed" className="text-xs text-muted-foreground">Seed</Label>
                <Input
                  id="adv-seed"
                  type="number"
                  value={value.seed}
                  onChange={(e) => onChange({ seed: parseInt(e.target.value, 10) || 0 })}
                />
              </div>
              <div className="space-y-1">
                <Label htmlFor="adv-convergence" className="text-xs text-muted-foreground inline-flex items-center gap-1">
                  Convergence Threshold
                  <InfoTooltip text="Number of consecutive batches without schema change needed to stop discovery." />
                </Label>
                <Input
                  id="adv-convergence"
                  type="number"
                  min={1}
                  max={20}
                  placeholder="default"
                  value={value.convergenceThreshold ?? ''}
                  onChange={(e) => {
                    const raw = e.target.value.trim();
                    onChange({ convergenceThreshold: raw === '' ? null : (parseInt(raw, 10) || null) });
                  }}
                />
              </div>
          </div>
        )}
      </div>

      <hr />

      {/* Pre-define columns */}
      <Collapsible>
        <CollapsibleTrigger className="group flex items-center gap-2 text-sm font-medium hover:text-foreground transition-colors">
          <ChevronDown className="h-4 w-4 transition-transform duration-200 group-data-[state=open]:rotate-180" />
          <span>Pre-define columns</span>
          <span className="text-xs font-normal text-muted-foreground">(optional)</span>
        </CollapsibleTrigger>
        <CollapsibleContent className="mt-3">
          <p className="text-xs text-muted-foreground mb-3">
            Add columns you want to appear in the final table. Only the column name is
            required; definition and rationale are optional and will be filled by the
            AI during discovery if left blank.
          </p>
          <InitialSchemaEditor
            onSchemaChange={(path, data) => onChange({ initialSchemaPath: path, initialSchemaData: data })}
          />
        </CollapsibleContent>
      </Collapsible>

      <hr />

      {/* Observation unit */}
      <div className="space-y-3">
        <Label className="text-sm font-medium inline-flex items-center gap-1.5">
          Observation Unit
          <InfoTooltip text="What each row in your table represents (e.g., 'a research paper' or 'a patient'). Usually auto-detected, but you can customize it if needed." />
        </Label>
        <RadioGroup
          value={value.observationUnitMode === 'auto' ? 'auto' : 'specify'}
          onValueChange={(mode) => {
            if (mode === 'auto') {
              onChange({ observationUnitMode: 'auto', observationUnitName: '', observationUnitDefinition: '' });
            } else {
              onChange({ observationUnitMode: 'name_only', reviewObservationUnit: false });
            }
          }}
          className="space-y-3"
        >
          <div className="flex items-start space-x-3">
            <RadioGroupItem value="auto" id="adv-obs-auto" className="mt-1" />
            <div className="space-y-1">
              <Label htmlFor="adv-obs-auto" className="font-medium cursor-pointer">Auto-detect (recommended)</Label>
              <p className="text-sm text-muted-foreground">
                The system will automatically determine the observation unit from your query and documents.
              </p>
              {value.observationUnitMode === 'auto' && (
                <div className="flex items-center gap-2 mt-1.5">
                  <Checkbox
                    id="adv-review-obs"
                    checked={value.reviewObservationUnit}
                    onCheckedChange={(checked) => onChange({ reviewObservationUnit: checked === true })}
                  />
                  <Label htmlFor="adv-review-obs" className="text-sm cursor-pointer inline-flex items-center gap-1.5">
                    Review before schema generation
                    <InfoTooltip text="Pause after the observation unit is discovered so you can review and edit it before schema generation begins." />
                  </Label>
                </div>
              )}
            </div>
          </div>
          <div className="flex items-start space-x-3">
            <RadioGroupItem value="specify" id="adv-obs-specify" className="mt-1" />
            <div className="space-y-1 flex-1">
              <Label htmlFor="adv-obs-specify" className="font-medium cursor-pointer">I'll specify</Label>
              <p className="text-sm text-muted-foreground">
                Provide a unit name; optionally add a definition for full control.
              </p>
              {value.observationUnitMode !== 'auto' && (
                <div className="space-y-2 mt-2">
                  <Input
                    placeholder="e.g., Research Paper, Model-Benchmark Evaluation"
                    value={value.observationUnitName}
                    onChange={(e) => onChange({ observationUnitName: e.target.value })}
                  />
                  <Collapsible>
                    <CollapsibleTrigger className="flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors">
                      <ChevronDown className="h-3.5 w-3.5" />
                      <span>Add definition</span>
                    </CollapsibleTrigger>
                    <CollapsibleContent className="mt-2">
                      <Input
                        placeholder="e.g., Each row represents a single research paper"
                        value={value.observationUnitDefinition}
                        onChange={(e) => onChange({
                          observationUnitDefinition: e.target.value,
                          observationUnitMode: e.target.value.trim() ? 'full' : 'name_only',
                        })}
                      />
                    </CollapsibleContent>
                  </Collapsible>
                </div>
              )}
            </div>
          </div>
        </RadioGroup>
      </div>

      {/* LLM configuration */}
      {allowLlmConfig && (
        <>
          <hr />
          {/* Schema creation LLM */}
          <div className="space-y-3">
            {editingSchemaLlm ? (
              <>
                <div className="flex items-center justify-between">
                  <Label className="text-sm font-medium">Schema Creation LLM</Label>
                  <Button variant="ghost" size="sm" onClick={() => setEditingSchemaLlm(false)}>Done</Button>
                </div>
                <div className="space-y-2">
                  <div className="space-y-1">
                    <Label className="text-xs">Provider</Label>
                    <Select
                      value={value.schemaProvider}
                      onValueChange={(provider) => onChange({ schemaProvider: provider, schemaModel: getDefaultModelForProvider(provider as LLMProviderKey) })}
                    >
                      <SelectTrigger><SelectValue /></SelectTrigger>
                      <SelectContent>
                        {providers.map((provider) => (
                          <SelectItem key={provider} value={provider}>{LLM_PROVIDER_NAMES[provider]}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">Model</Label>
                    <ModelSelector
                      provider={value.schemaProvider as LLMProviderKey}
                      value={value.schemaModel}
                      onChange={(modelId) => onChange({ schemaModel: modelId })}
                      showDetails
                    />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">Temperature</Label>
                    <Input
                      type="number"
                      min={0}
                      max={2}
                      step={0.1}
                      value={value.schemaTemperature}
                      onChange={(e) => onChange({ schemaTemperature: parseFloat(e.target.value) || 0 })}
                    />
                  </div>
                </div>
              </>
            ) : (
              <div className="flex items-center justify-between">
                <div className="text-sm">
                  <span className="font-medium">Schema LLM:</span>{' '}
                  <span className="text-muted-foreground">
                    {LLM_PROVIDER_NAMES[value.schemaProvider as LLMProviderKey]} / {value.schemaModel}
                    {value.schemaTemperature !== 0 && ` (temp: ${value.schemaTemperature})`}
                  </span>
                </div>
                <Button variant="ghost" size="sm" onClick={() => setEditingSchemaLlm(true)}>Edit</Button>
              </div>
            )}
          </div>

          <hr />

          {/* Value extraction LLM */}
          <div className="space-y-3">
            {editingValueLlm ? (
              <>
                <div className="flex items-center justify-between">
                  <Label className="text-sm font-medium">Value Extraction LLM</Label>
                  <Button variant="ghost" size="sm" onClick={() => setEditingValueLlm(false)}>Done</Button>
                </div>
                <div className="space-y-2">
                  <div className="space-y-1">
                    <Label className="text-xs">Provider</Label>
                    <Select
                      value={value.valueProvider}
                      onValueChange={(provider) => onChange({ valueProvider: provider, valueModel: getDefaultModelForProvider(provider as LLMProviderKey) })}
                    >
                      <SelectTrigger><SelectValue /></SelectTrigger>
                      <SelectContent>
                        {providers.map((provider) => (
                          <SelectItem key={provider} value={provider}>{LLM_PROVIDER_NAMES[provider]}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">Model</Label>
                    <ModelSelector
                      provider={value.valueProvider as LLMProviderKey}
                      value={value.valueModel}
                      onChange={(modelId) => onChange({ valueModel: modelId })}
                      showDetails
                    />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">Temperature</Label>
                    <Input
                      type="number"
                      min={0}
                      max={2}
                      step={0.1}
                      value={value.valueTemperature}
                      onChange={(e) => onChange({ valueTemperature: parseFloat(e.target.value) || 0 })}
                    />
                  </div>
                </div>
              </>
            ) : (
              <div className="flex items-center justify-between">
                <div className="text-sm">
                  <span className="font-medium">Value LLM:</span>{' '}
                  <span className="text-muted-foreground">
                    {LLM_PROVIDER_NAMES[value.valueProvider as LLMProviderKey]} / {value.valueModel}
                    {value.valueTemperature !== 0 && ` (temp: ${value.valueTemperature})`}
                  </span>
                </div>
                <Button variant="ghost" size="sm" onClick={() => setEditingValueLlm(true)}>Edit</Button>
              </div>
            )}
          </div>
        </>
      )}

      {/* Retriever (developer mode) */}
      {developerMode && (
        <>
          <hr />
          <div className="space-y-2">
            <Label className="text-sm font-medium">Retriever</Label>
            <div className="space-y-2">
              <div className="space-y-1">
                <Label className="text-xs">Model Name</Label>
                <Input
                  value={value.retrieverModelName}
                  onChange={(e) => onChange({ retrieverModelName: e.target.value })}
                />
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-xs">Passage Chars</Label>
                  <Input
                    type="number"
                    min={128}
                    max={2048}
                    value={value.retrieverPassageChars}
                    onChange={(e) => onChange({ retrieverPassageChars: parseInt(e.target.value, 10) || RETRIEVER_DEFAULTS.passage_chars })}
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Overlap</Label>
                  <Input
                    type="number"
                    min={0}
                    max={256}
                    value={value.retrieverOverlap}
                    onChange={(e) => onChange({ retrieverOverlap: parseInt(e.target.value, 10) || 0 })}
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">K</Label>
                  <Input
                    type="number"
                    min={1}
                    max={50}
                    value={value.retrieverK}
                    onChange={(e) => onChange({ retrieverK: parseInt(e.target.value, 10) || 1 })}
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Dynamic K</Label>
                  <Input
                    type="number"
                    min={0}
                    max={1}
                    step={0.05}
                    value={value.retrieverDynamicK}
                    onChange={(e) => onChange({ retrieverDynamicK: parseFloat(e.target.value) || 0 })}
                  />
                </div>
              </div>
            </div>
          </div>
        </>
      )}

      {/* Bypass document limit (developer mode) */}
      {developerMode && (
        <>
          <hr />
          <div className="flex items-center justify-between p-3 border rounded-lg bg-amber-50 border-amber-200 dark:bg-amber-950/20 dark:border-amber-800">
            <div>
              <Label className="text-sm font-medium">Bypass Document Limit</Label>
              <p className="text-xs text-muted-foreground">
                Disable the {maxDocuments ? `${maxDocuments}-document ` : 'document '}limit for testing.
              </p>
            </div>
            <Switch checked={value.bypassLimit} onCheckedChange={(checked) => onChange({ bypassLimit: checked })} />
          </div>
        </>
      )}
    </div>
  );
}

export default AdvancedSettingsFields;
