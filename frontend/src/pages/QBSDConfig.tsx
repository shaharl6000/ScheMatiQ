import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Sparkles, Settings, ArrowLeft, Loader2, ChevronDown } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuLabel,
} from '@/components/ui/dropdown-menu';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';

import { qbsdAPI } from '../services/api';
import { QBSDConfig, LLMConfig, RetrieverConfig } from '../types';

const DOCUMENT_DIRECTORIES = [
  { value: '../research/data/file', label: 'file' },
  { value: '../research/data/abstracts', label: 'abstracts' },
  { value: '../research/data/full_text', label: 'full_text' },
  { value: '../research/data/manually', label: 'manually' },
  { value: '../research/data/NES_DATA', label: 'NES_DATA' },
  { value: '../research/data/files', label: 'files' },
  { value: '../research/data/arxivDIGESTables', label: 'arxivDIGESTables' },
  { value: '../research/data/schema_6_filtered', label: 'schema_6_filtered' },
];

const QBSDConfigPage = () => {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [config, setConfig] = useState<QBSDConfig>({
    query: 'Given a protein sequence, can it be determined whether or not it contains a nuclear export signal (NES)? If it does, how strong is the NES, and what is the confidence in that assessment?',
    docs_path: ['../research/data/file'],
    max_keys_schema: 100,
    documents_batch_size: 1,
    schema_creation_backend: {
      provider: 'gemini',
      model: 'gemini-2.5-flash',
      max_tokens: 8192,
      temperature: 0.2,
      max_context_tokens: 1000000,
    },
    value_extraction_backend: {
      provider: 'gemini',
      model: 'gemini-2.5-flash-lite',
      max_tokens: 8192,
      temperature: 0.2,
      max_context_tokens: 1000000,
    },
    output_path: 'outputs/visualization_output.json',
    document_randomization_seed: 42,
  });

  const handleConfigChange = (field: string, value: any) => {
    setConfig(prev => ({
      ...prev,
      [field]: value,
    }));
  };

  const handleSchemaBackendChange = (field: keyof LLMConfig, value: any) => {
    setConfig(prev => ({
      ...prev,
      schema_creation_backend: {
        ...prev.schema_creation_backend,
        [field]: value,
      },
    }));
  };

  const handleValueBackendChange = (field: keyof LLMConfig, value: any) => {
    setConfig(prev => ({
      ...prev,
      value_extraction_backend: {
        ...prev.value_extraction_backend,
        [field]: value,
      },
    }));
  };

  const handleRetrieverChange = (field: keyof RetrieverConfig, value: any) => {
    setConfig(prev => ({
      ...prev,
      retriever: {
        ...prev.retriever!,
        [field]: value,
      },
    }));
  };

  const handleSubmit = async () => {
    setLoading(true);
    setError(null);

    try {
      const result = await qbsdAPI.configure(config);
      navigate(`/visualize/${result.session_id}?mode=qbsd`);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to configure QBSD');
    } finally {
      setLoading(false);
    }
  };

  const selectedPaths = Array.isArray(config.docs_path) ? config.docs_path : [config.docs_path];
  const isFormValid = config.query.trim() !== '' && selectedPaths.length > 0;

  return (
    <div className="max-w-4xl mx-auto">
      <h1 className="text-3xl font-bold tracking-tight mb-2">
        Configure QBSD
      </h1>
      <p className="text-muted-foreground mb-6">
        Set up your Query-Based Schema Discovery parameters to run AI-powered data extraction.
      </p>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-primary" />
            Basic Configuration
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Research Query */}
          <div className="space-y-2">
            <Label htmlFor="query">
              Research Query <span className="text-destructive">*</span>
            </Label>
            <Textarea
              id="query"
              rows={3}
              value={config.query}
              onChange={(e) => handleConfigChange('query', e.target.value)}
              placeholder="e.g., Given a protein sequence, can it be determined whether or not it contains a nuclear export signal (NES)?"
              className="resize-none"
            />
            <p className="text-sm text-muted-foreground">
              The research question that will guide schema discovery
            </p>
          </div>

          {/* Document Paths and Max Keys */}
          <div className="grid md:grid-cols-3 gap-4">
            <div className="md:col-span-2 space-y-2">
              <Label htmlFor="docs_path">
                Document Paths <span className="text-destructive">*</span>
              </Label>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="outline" className="w-full justify-between">
                    {selectedPaths.length === 0
                      ? 'Select directories...'
                      : `${selectedPaths.length} selected`}
                    <ChevronDown className="ml-2 h-4 w-4" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent className="w-full min-w-[300px]">
                  <DropdownMenuLabel>Select Directories</DropdownMenuLabel>
                  {DOCUMENT_DIRECTORIES.map((dir) => (
                    <DropdownMenuCheckboxItem
                      key={dir.value}
                      checked={selectedPaths.includes(dir.value)}
                      onCheckedChange={(checked) => {
                        const newPaths = checked
                          ? [...selectedPaths, dir.value]
                          : selectedPaths.filter(p => p !== dir.value);
                        handleConfigChange('docs_path', newPaths);
                      }}
                    >
                      {dir.label}
                    </DropdownMenuCheckboxItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>
              <p className="text-sm text-muted-foreground">
                Select one or more document directories
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="max_keys">Max Schema Keys</Label>
              <Input
                id="max_keys"
                type="number"
                value={config.max_keys_schema}
                onChange={(e) => handleConfigChange('max_keys_schema', parseInt(e.target.value))}
                min={1}
                max={500}
              />
              <p className="text-sm text-muted-foreground">Maximum columns</p>
            </div>
          </div>

          {/* Batch Size and Seed */}
          <div className="grid md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="batch_size">Document Batch Size</Label>
              <Input
                id="batch_size"
                type="number"
                value={config.documents_batch_size}
                onChange={(e) => handleConfigChange('documents_batch_size', parseInt(e.target.value))}
                min={1}
                max={20}
              />
              <p className="text-sm text-muted-foreground">Documents per iteration</p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="seed">Randomization Seed</Label>
              <Input
                id="seed"
                type="number"
                value={config.document_randomization_seed}
                onChange={(e) => handleConfigChange('document_randomization_seed', parseInt(e.target.value))}
              />
              <p className="text-sm text-muted-foreground">For reproducible ordering</p>
            </div>
          </div>

          {/* LLM Configuration Accordions */}
          <Accordion type="multiple" className="mt-6">
            {/* Schema Creation LLM */}
            <AccordionItem value="schema-llm">
              <AccordionTrigger className="hover:no-underline">
                <div className="flex items-center gap-2">
                  <Settings className="h-4 w-4" />
                  <span className="font-semibold">Schema Creation LLM</span>
                  <Badge className="ml-2">{config.schema_creation_backend.provider.toUpperCase()}</Badge>
                  <Badge variant="outline">{config.schema_creation_backend.model}</Badge>
                </div>
              </AccordionTrigger>
              <AccordionContent>
                <p className="text-sm text-muted-foreground mb-4">
                  LLM used for discovering schema structure and column definitions
                </p>
                <div className="grid md:grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <Label>Provider</Label>
                    <Select
                      value={config.schema_creation_backend.provider}
                      onValueChange={(value) => handleSchemaBackendChange('provider', value)}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="gemini">Google Gemini</SelectItem>
                        <SelectItem value="openai">OpenAI</SelectItem>
                        <SelectItem value="together">Together AI</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="md:col-span-2 space-y-2">
                    <Label>Model</Label>
                    <Input
                      value={config.schema_creation_backend.model}
                      onChange={(e) => handleSchemaBackendChange('model', e.target.value)}
                      placeholder="e.g., gemini-2.5-flash, gpt-4"
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Max Tokens</Label>
                    <Input
                      type="number"
                      value={config.schema_creation_backend.max_tokens}
                      onChange={(e) => handleSchemaBackendChange('max_tokens', parseInt(e.target.value))}
                      min={512}
                      max={32768}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Temperature</Label>
                    <Input
                      type="number"
                      value={config.schema_creation_backend.temperature}
                      onChange={(e) => handleSchemaBackendChange('temperature', parseFloat(e.target.value))}
                      min={0}
                      max={2}
                      step={0.1}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Max Context Tokens</Label>
                    <Input
                      type="number"
                      value={config.schema_creation_backend.max_context_tokens || ''}
                      onChange={(e) => handleSchemaBackendChange('max_context_tokens', e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="Optional"
                    />
                  </div>
                </div>
              </AccordionContent>
            </AccordionItem>

            {/* Value Extraction LLM */}
            <AccordionItem value="value-llm">
              <AccordionTrigger className="hover:no-underline">
                <div className="flex items-center gap-2">
                  <Settings className="h-4 w-4" />
                  <span className="font-semibold">Value Extraction LLM</span>
                  <Badge variant="secondary" className="ml-2">{config.value_extraction_backend.provider.toUpperCase()}</Badge>
                  <Badge variant="outline">{config.value_extraction_backend.model}</Badge>
                </div>
              </AccordionTrigger>
              <AccordionContent>
                <p className="text-sm text-muted-foreground mb-4">
                  LLM used for extracting actual data values from documents
                </p>
                <div className="grid md:grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <Label>Provider</Label>
                    <Select
                      value={config.value_extraction_backend.provider}
                      onValueChange={(value) => handleValueBackendChange('provider', value)}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="gemini">Google Gemini</SelectItem>
                        <SelectItem value="openai">OpenAI</SelectItem>
                        <SelectItem value="together">Together AI</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="md:col-span-2 space-y-2">
                    <Label>Model</Label>
                    <Input
                      value={config.value_extraction_backend.model}
                      onChange={(e) => handleValueBackendChange('model', e.target.value)}
                      placeholder="e.g., gemini-2.5-flash-lite, gpt-4"
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Max Tokens</Label>
                    <Input
                      type="number"
                      value={config.value_extraction_backend.max_tokens}
                      onChange={(e) => handleValueBackendChange('max_tokens', parseInt(e.target.value))}
                      min={512}
                      max={32768}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Temperature</Label>
                    <Input
                      type="number"
                      value={config.value_extraction_backend.temperature}
                      onChange={(e) => handleValueBackendChange('temperature', parseFloat(e.target.value))}
                      min={0}
                      max={2}
                      step={0.1}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Max Context Tokens</Label>
                    <Input
                      type="number"
                      value={config.value_extraction_backend.max_context_tokens || ''}
                      onChange={(e) => handleValueBackendChange('max_context_tokens', e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="Optional"
                    />
                  </div>
                </div>
              </AccordionContent>
            </AccordionItem>

            {/* Retriever Settings */}
            <AccordionItem value="retriever">
              <AccordionTrigger className="hover:no-underline">
                <div className="flex items-center gap-2">
                  <span className="font-semibold">Retriever Settings</span>
                  <Badge variant="secondary" className="ml-2">{config.retriever?.model_name || 'Default'}</Badge>
                </div>
              </AccordionTrigger>
              <AccordionContent>
                <div className="grid md:grid-cols-3 gap-4">
                  <div className="md:col-span-2 space-y-2">
                    <Label>Model Name</Label>
                    <Input
                      value={config.retriever?.model_name || ''}
                      onChange={(e) => handleRetrieverChange('model_name', e.target.value)}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Passage Characters</Label>
                    <Input
                      type="number"
                      value={config.retriever?.passage_chars || 512}
                      onChange={(e) => handleRetrieverChange('passage_chars', parseInt(e.target.value))}
                      min={128}
                      max={2048}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Overlap</Label>
                    <Input
                      type="number"
                      value={config.retriever?.overlap || 64}
                      onChange={(e) => handleRetrieverChange('overlap', parseInt(e.target.value))}
                      min={0}
                      max={256}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Retrieval K</Label>
                    <Input
                      type="number"
                      value={config.retriever?.k || 15}
                      onChange={(e) => handleRetrieverChange('k', parseInt(e.target.value))}
                      min={1}
                      max={50}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Dynamic K Threshold</Label>
                    <Input
                      type="number"
                      value={config.retriever?.dynamic_k_threshold || 0.65}
                      onChange={(e) => handleRetrieverChange('dynamic_k_threshold', parseFloat(e.target.value))}
                      min={0}
                      max={1}
                      step={0.05}
                    />
                  </div>
                </div>
              </AccordionContent>
            </AccordionItem>
          </Accordion>

          {/* Actions */}
          <div className="flex justify-between items-center pt-4 border-t">
            <Button variant="outline" onClick={() => navigate('/')}>
              <ArrowLeft className="mr-2 h-4 w-4" />
              Back to Home
            </Button>

            <Button
              size="lg"
              onClick={handleSubmit}
              disabled={!isFormValid || loading}
            >
              {loading ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Sparkles className="mr-2 h-4 w-4" />
              )}
              {loading ? 'Starting QBSD...' : 'Start QBSD'}
            </Button>
          </div>
        </CardContent>
      </Card>

      {error && (
        <Alert variant="destructive" className="mt-6">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
    </div>
  );
};

export default QBSDConfigPage;
