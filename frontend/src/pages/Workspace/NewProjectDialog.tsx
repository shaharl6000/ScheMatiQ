// Modal dialog for creating a new ScheMatiQ project (folder/cloud docs, cost estimate, start).
// Parent: Workspace (index.tsx).

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  ChevronDown,
  Cloud,
  FolderOpen,
  HelpCircle,
  Loader2,
  Mail,
  Play,
  Sparkles,
} from 'lucide-react';

import {
  AdvancedSettingsFields,
  type AdvancedSettingsValue,
} from '@/components/AdvancedSettings/AdvancedSettingsFields';
import { CloudDatasetPicker, type CloudDataset } from '@/components/CloudDatasetPicker/CloudDatasetPicker';
import { ConsentDialog, getSavedConsent } from '@/components/ConsentDialog/ConsentDialog';
import { CostBreakdown } from '@/components/CostBreakdown/CostBreakdown';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { useToast } from '@/components/ui/use-toast';
import {
  getAvailableProviders,
  type LLMProviderKey,
} from '@/constants';
import { cloudAPI, configAPI, loadAPI, schematiqAPI } from '@/services/api';
import type { CostEstimate } from '@/types';
import { getConfiguredProviders } from '@/utils/apiKeyStorage';

import { DEFAULT_PROVIDER, SHOW_API_KEY_FIELD, WORKSPACE_DEFAULT_ADVANCED } from './constants';
import { buildConfig, formatCost, formatFileSize } from './helpers';
import type { DocumentSourceInput, NewProjectDialogProps } from './types';

export function NewProjectDialog({ open, onOpenChange, onCreated }: NewProjectDialogProps) {
  const navigate = useNavigate();
  const { toast } = useToast();
  const folderInputRef = useRef<HTMLInputElement | null>(null);
  const [query, setQuery] = useState('');
  const [showHelp, setShowHelp] = useState(false);
  const [apiKey, setApiKey] = useState('');
  const [files, setFiles] = useState<File[]>([]);
  const [documentSource, setDocumentSource] = useState<'upload' | 'cloud'>('upload');
  const [datasets, setDatasets] = useState<CloudDataset[]>([]);
  const [datasetsLoading, setDatasetsLoading] = useState(true);
  const [selectedDatasets, setSelectedDatasets] = useState<string[]>([]);
  const [serverHasKeys, setServerHasKeys] = useState(false);
  const [estimate, setEstimate] = useState<CostEstimate | null>(null);
  const [estimating, setEstimating] = useState(false);
  const [creating, setCreating] = useState(false);
  const [startConfirmed, setStartConfirmed] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [advanced, setAdvanced] = useState<AdvancedSettingsValue>(WORKSPACE_DEFAULT_ADVANCED);
  const [developerMode, setDeveloperMode] = useState(false);
  const [allowLlmConfig, setAllowLlmConfig] = useState(false);
  const [dataCollectionEnabled, setDataCollectionEnabled] = useState(false);
  const [consentOpen, setConsentOpen] = useState(false);
  const [maxDocuments, setMaxDocuments] = useState<number | undefined>(undefined);
  const [providers, setProviders] = useState<LLMProviderKey[]>([DEFAULT_PROVIDER as LLMProviderKey]);

  const updateAdvanced = useCallback((patch: Partial<AdvancedSettingsValue>) => {
    setAdvanced((prev) => ({ ...prev, ...patch }));
    setEstimate(null);
    setStartConfirmed(false);
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const config = await configAPI.getConfig().catch(() => null);
      const configured = await getConfiguredProviders().catch(() => []);
      if (cancelled) return;
      const available = getAvailableProviders(configured) as LLMProviderKey[];
      setServerHasKeys(Boolean(config?.server_has_api_keys));
      setDeveloperMode(Boolean(config?.developer_mode));
      setAllowLlmConfig(Boolean(config?.allow_llm_config));
      setDataCollectionEnabled(Boolean(config?.data_collection_enabled));
      setMaxDocuments(typeof config?.max_documents === 'number' ? config.max_documents : undefined);
      setProviders(available.length > 0 ? available : [DEFAULT_PROVIDER as LLMProviderKey]);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await cloudAPI.getDatasets();
        if (!cancelled) setDatasets(Array.isArray(data) ? data : []);
      } catch {
        if (!cancelled) setDatasets([]);
      } finally {
        if (!cancelled) setDatasetsLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (folderInputRef.current) {
      folderInputRef.current.setAttribute('webkitdirectory', '');
      folderInputRef.current.setAttribute('directory', '');
    }
  }, []);

  const selectedBytes = useMemo(
    () => files.reduce((sum, file) => sum + file.size, 0),
    [files]
  );

  const hasDocuments = documentSource === 'cloud' ? selectedDatasets.length > 0 : files.length > 0;
  const canEstimate = query.trim().length > 0 && hasDocuments && (serverHasKeys || apiKey.trim().length > 0);

  const estimateProject = useCallback(async () => {
    setError(null);
    setEstimating(true);
    try {
      const docs: DocumentSourceInput = documentSource === 'cloud'
        ? { mode: 'cloud', datasets: selectedDatasets }
        : { mode: 'upload' };
      const config = buildConfig(query.trim(), apiKey.trim(), advanced, docs);
      const result = await schematiqAPI.estimateCostPreview(
        config,
        documentSource === 'cloud'
          ? []
          : files.map((file) => ({ name: file.webkitRelativePath || file.name, size: file.size })),
      );
      setEstimate(result);
      setStartConfirmed(false);
      return result;
    } catch (err: any) {
      const detail = err?.response?.data?.detail || err?.message || 'Failed to estimate this project';
      setError(detail);
      throw err;
    } finally {
      setEstimating(false);
    }
  }, [apiKey, files, query, advanced, documentSource, selectedDatasets]);

  const runCreate = useCallback(async (optOut: boolean) => {
    setCreating(true);
    setError(null);
    try {
      const docs: DocumentSourceInput = documentSource === 'cloud'
        ? { mode: 'cloud', datasets: selectedDatasets }
        : { mode: 'upload' };
      const config = buildConfig(query.trim(), apiKey.trim(), advanced, docs, optOut);
      const result = await schematiqAPI.configure(config);
      if (documentSource === 'upload') {
        await loadAPI.addDocuments(result.session_id, files, advanced.bypassLimit);
      }
      await schematiqAPI.run(result.session_id);
      toast({
        title: 'Project started',
        description: 'The workspace will update as schema and data arrive.',
      });
      onCreated(result.session_id);
      onOpenChange(false);
      navigate(`/workspace/${result.session_id}`, { replace: true });
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to start project');
    } finally {
      setCreating(false);
    }
  }, [apiKey, files, navigate, onCreated, onOpenChange, query, toast, advanced, documentSource, selectedDatasets]);

  const startProject = useCallback(async () => {
    if (!query.trim() || !hasDocuments) {
      setError(
        documentSource === 'cloud'
          ? 'Select at least one cloud dataset and enter a research question first.'
          : 'Choose a folder of documents and enter a research question first.',
      );
      return;
    }
    if (!serverHasKeys && !apiKey.trim()) {
      setError('Add an API key or configure server-side API keys before starting.');
      return;
    }
    if (!estimate && !startConfirmed) {
      await estimateProject();
      setStartConfirmed(true);
      return;
    }

    // Consent gate: skip when data collection is off or in developer mode;
    // otherwise honor saved consent, or prompt for it.
    if (!dataCollectionEnabled || developerMode) {
      await runCreate(false);
      return;
    }
    const { consentGiven, savedOptOut } = getSavedConsent();
    if (consentGiven) {
      await runCreate(savedOptOut);
      return;
    }
    setConsentOpen(true);
  }, [apiKey, dataCollectionEnabled, developerMode, documentSource, estimate, estimateProject, hasDocuments, query, runCreate, serverHasKeys, startConfirmed]);

  return (
    <>
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <div className="flex items-start justify-between gap-2">
            <DialogTitle>New Project</DialogTitle>
            <button
              type="button"
              onClick={() => setShowHelp((v) => !v)}
              className="shrink-0 inline-flex items-center gap-1.5 rounded-md px-2 py-1 text-xs font-medium text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
              aria-expanded={showHelp}
              aria-controls="workspace-how-it-works"
              title="How ScheMatiQ works"
            >
              <HelpCircle className="h-4 w-4" />
              How it works
            </button>
          </div>
          <DialogDescription>
            Pick a local folder or a cloud dataset, describe the research question, estimate cost, then start extraction.
          </DialogDescription>
        </DialogHeader>

        {showHelp && (
          <div
            id="workspace-how-it-works"
            className="rounded-md border bg-muted/40 p-4 text-sm text-muted-foreground space-y-3"
          >
            <p className="text-foreground">
              ScheMatiQ reads a set of documents and turns them into a structured, editable table,
              guided by a research question you write in plain language.
            </p>
            <ol className="list-decimal space-y-1.5 pl-5">
              <li>
                <span className="font-medium text-foreground">Describe your research question</span> — what
                you want to learn or compare across the documents.
              </li>
              <li>
                <span className="font-medium text-foreground">Add documents</span> — choose a local folder
                or a cloud dataset to analyze.
              </li>
              <li>
                <span className="font-medium text-foreground">Estimate &amp; Start</span> — preview the cost,
                then run extraction. ScheMatiQ discovers the columns (a schema) from your question and fills
                in the table.
              </li>
            </ol>
            <p>
              Once it finishes you can edit cells, chat with the table to refine it, and re-extract. New
              here? The video and paper linked at the bottom of this dialog walk through a full example.
            </p>
          </div>
        )}

        <div className="grid gap-4">
          <div className="grid gap-2">
            <Label htmlFor="workspace-query">Research question</Label>
            <Textarea
              id="workspace-query"
              value={query}
              onChange={(event) => {
                setQuery(event.target.value);
                setEstimate(null);
                setStartConfirmed(false);
              }}
              placeholder="What database should ScheMatiQ build from these documents?"
              rows={4}
            />
          </div>

          <div className="grid gap-2">
            <Label>Documents</Label>
            <div className="flex gap-2">
              <Button
                type="button"
                size="sm"
                variant={documentSource === 'upload' ? 'default' : 'outline'}
                onClick={() => {
                  setDocumentSource('upload');
                  setEstimate(null);
                  setStartConfirmed(false);
                  folderInputRef.current?.click();
                }}
              >
                <FolderOpen className="h-4 w-4" />
                Choose Folder
              </Button>
              <Button
                type="button"
                size="sm"
                variant={documentSource === 'cloud' ? 'default' : 'outline'}
                onClick={() => { setDocumentSource('cloud'); setEstimate(null); setStartConfirmed(false); }}
              >
                <Cloud className="h-4 w-4" />
                Cloud dataset
              </Button>
            </div>

            {documentSource === 'upload' ? (
              <span className="text-sm text-muted-foreground">
                {files.length > 0
                  ? `${files.length} files, ${formatFileSize(selectedBytes)}`
                  : 'No folder selected'}
              </span>
            ) : (
              <CloudDatasetPicker
                datasets={datasets}
                loading={datasetsLoading}
                selected={selectedDatasets}
                onChange={(names) => { setSelectedDatasets(names); setEstimate(null); setStartConfirmed(false); }}
                maxDocuments={maxDocuments}
                bypassLimit={advanced.bypassLimit}
              />
            )}

            <input
              ref={folderInputRef}
              type="file"
              multiple
              className="hidden"
              onChange={(event) => {
                setFiles(Array.from(event.target.files || []));
                setEstimate(null);
                setStartConfirmed(false);
              }}
            />
          </div>

          {SHOW_API_KEY_FIELD && (
          <div className="grid gap-2">
            <Label htmlFor="workspace-api-key">API key</Label>
            <Input
              id="workspace-api-key"
              value={apiKey}
              type="password"
              onChange={(event) => {
                setApiKey(event.target.value);
                setEstimate(null);
                setStartConfirmed(false);
              }}
              placeholder={serverHasKeys ? 'Optional: server keys are configured' : 'Required unless server keys are configured'}
            />
          </div>
          )}

          <Collapsible>
            <CollapsibleTrigger className="group flex items-center gap-2 text-sm font-medium hover:text-foreground transition-colors">
              <ChevronDown className="h-4 w-4 transition-transform duration-200 group-data-[state=open]:rotate-180" />
              <span>Advanced settings</span>
              <span className="text-xs font-normal text-muted-foreground">(optional)</span>
            </CollapsibleTrigger>
            <CollapsibleContent className="mt-3">
              <AdvancedSettingsFields
                value={advanced}
                onChange={updateAdvanced}
                developerMode={developerMode}
                allowLlmConfig={allowLlmConfig}
                providers={providers}
                maxDocuments={maxDocuments}
              />
            </CollapsibleContent>
          </Collapsible>

          {estimate && (
            <div className="rounded-md border bg-muted/30 p-3 text-sm">
              <div className="font-medium">Estimated cost</div>
              <div className="mt-1 text-muted-foreground">{formatCost(estimate)}</div>
              <CostBreakdown
                estimate={estimate}
                skipValueExtraction={advanced.skipValueExtraction}
                className="mt-2"
              />
            </div>
          )}

          {error && (
            <div className="rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">
              {error}
            </div>
          )}
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={estimateProject} disabled={!canEstimate || estimating || creating}>
            {estimating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
            Estimate
          </Button>
          <Button type="button" onClick={startProject} disabled={!canEstimate || estimating || creating}>
            {creating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
            {estimate || startConfirmed ? 'Start Project' : 'Estimate & Start'}
          </Button>
        </DialogFooter>

        {/* Project & lab resources — mirrors the classic start page */}
        <div className="mt-2 border-t pt-4 space-y-3">
          <div className="flex items-center justify-center gap-2 flex-wrap font-['Google_Sans',sans-serif]">
            <a
              href="https://youtube.com/watch?v=VILym_Ch0hg&feature=youtu.be"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-4 py-1 rounded-full text-white text-xs transition-all duration-300 shadow-sm hover:shadow-lg hover:-translate-y-0.5"
              style={{ background: 'linear-gradient(135deg, #ff4444 0%, #cc0000 100%)' }}
            >
              <span className="flex items-center justify-center w-4 h-4">
                <i className="fa-brands fa-youtube text-sm"></i>
              </span>
              <span>Demo Video</span>
            </a>
            <a
              href="https://arxiv.org/pdf/2604.09237"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-4 py-1 rounded-full bg-[#363636] hover:bg-[#2b2b2b] text-white text-xs transition-all duration-300 shadow-sm hover:shadow-lg hover:-translate-y-0.5"
            >
              <span className="flex items-center justify-center w-4 h-4">
                <i className="ai ai-arxiv text-base"></i>
              </span>
              <span>arXiv</span>
            </a>
            <a
              href="https://github.com/shaharl6000/ScheMatiQ"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-4 py-1 rounded-full bg-[#363636] hover:bg-[#2b2b2b] text-white text-xs transition-all duration-300 shadow-sm hover:shadow-lg hover:-translate-y-0.5"
            >
              <span className="flex items-center justify-center w-4 h-4">
                <i className="fab fa-github text-base"></i>
              </span>
              <span>Code</span>
            </a>
            <a
              href="https://x.com/EliyaHabba/status/2043690798257250662"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-4 py-1 rounded-full bg-[#363636] hover:bg-[#2b2b2b] text-white text-xs transition-all duration-300 shadow-sm hover:shadow-lg hover:-translate-y-0.5"
            >
              <span className="flex items-center justify-center w-4 h-4">
                <i className="fa-brands fa-x-twitter text-xs"></i>
              </span>
              <span>Twitter</span>
            </a>
          </div>

          <div className="flex flex-col sm:flex-row items-center justify-center gap-2 sm:gap-3">
            <img src="/huji_icon.png" alt="HUJI NLP Lab" className="h-8 w-auto dark:invert" />
            <div className="flex flex-col items-center sm:items-start gap-0.5 text-xs text-muted-foreground">
              <span className="font-medium text-foreground">The Hebrew University of Jerusalem</span>
              <span className="flex items-center gap-3 flex-wrap">
                <a href="mailto:shahar.levy2@mail.huji.ac.il" className="inline-flex items-center gap-1 hover:text-primary hover:underline transition-colors">
                  <Mail className="h-3 w-3" />
                  shahar.levy2@mail.huji.ac.il
                </a>
                <a href="mailto:eliya.habba@mail.huji.ac.il" className="inline-flex items-center gap-1 hover:text-primary hover:underline transition-colors">
                  <Mail className="h-3 w-3" />
                  eliya.habba@mail.huji.ac.il
                </a>
              </span>
            </div>
          </div>
        </div>

      </DialogContent>
    </Dialog>
    <ConsentDialog open={consentOpen} onOpenChange={setConsentOpen} onConfirm={runCreate} />
    </>
  );
}
