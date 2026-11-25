// Full-canvas Start Hub: Minimal Living-Sentence Interface.
// A single responsive sentence owns the entire landing screen:
//
//   Structure [ your documents ] around [ any research question ]
//
// Works in either order (question first or documents first):
//   - Type directly inline inside [ any research question ] (wraps naturally if long).
//   - Drop files anywhere on the screen or click [ your documents ] to pick
//     files, folders, or cloud datasets.
//   - Unfilled slots shimmer gently (~9.5s cycle) and rock softly after ~9s idle
//     (every ~20s), stopping immediately on hover, focus, or drag.
//   - Once both slots are filled, a single "Build Table ->" CTA appears below.
//   - Returning-user Import, Recent projects, and Sample tables live cleanly
//     behind the muted Open icon in the top bar.
// Parent: Workspace (index.tsx).

import { type ClipboardEvent, type KeyboardEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  ArrowRight,
  Check,
  ChevronDown,
  Cloud,
  FileText,
  FolderOpen,
  Loader2,
  Pencil,
  Sparkles,
  Upload,
  X,
} from 'lucide-react';

import {
  AdvancedSettingsFields,
  type AdvancedSettingsValue,
} from '@/components/AdvancedSettings/AdvancedSettingsFields';
import { CloudDatasetPicker, type CloudDataset } from '@/components/CloudDatasetPicker/CloudDatasetPicker';
import { ConsentDialog, getSavedConsent } from '@/components/ConsentDialog/ConsentDialog';
import { CostBreakdown } from '@/components/CostBreakdown/CostBreakdown';
import { Button } from '@/components/ui/button';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { useToast } from '@/components/ui/use-toast';
import { getAvailableProviders, type LLMProviderKey } from '@/constants';
import { cloudAPI, configAPI, loadAPI, schematiqAPI } from '@/services/api';
import type { CostEstimate } from '@/types';
import { getConfiguredProviders } from '@/utils/apiKeyStorage';

import { DEFAULT_PROVIDER, SHOW_API_KEY_FIELD, WORKSPACE_DEFAULT_ADVANCED } from './constants';
import { buildConfig, formatCost, formatFileSize } from './helpers';
import type { DocumentSourceInput } from './types';

const ACCEPTED_EXTS = ['.txt', '.md', '.pdf', '.doc', '.docx', '.rtf', '.json'];

function insertPlainTextAtCaret(text: string): void {
  const selection = window.getSelection();
  if (!selection || selection.rangeCount === 0) return;
  const range = selection.getRangeAt(0);
  range.deleteContents();
  const node = document.createTextNode(text);
  range.insertNode(node);
  range.setStartAfter(node);
  range.collapse(true);
  selection.removeAllRanges();
  selection.addRange(range);
}

function focusEndOfElement(el: HTMLElement | null): void {
  if (!el) return;
  el.focus();
  const selection = window.getSelection();
  if (!selection) return;
  const range = document.createRange();
  range.selectNodeContents(el);
  range.collapse(false);
  selection.removeAllRanges();
  selection.addRange(range);
}

export function StartHub({
  onCreated,
}: {
  onCreated: (sessionId: string) => void;
}) {
  const navigate = useNavigate();
  const { toast } = useToast();
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const folderInputRef = useRef<HTMLInputElement | null>(null);
  const questionSpanRef = useRef<HTMLSpanElement | null>(null);
  const apiKeyInputRef = useRef<HTMLInputElement | null>(null);
  const prevHasLocalFilesRef = useRef(false);

  const [query, setQuery] = useState('');
  const [questionFocused, setQuestionFocused] = useState(false);
  const [docsPopoverOpen, setDocsPopoverOpen] = useState(false);
  const [hoveredSlot, setHoveredSlot] = useState<'question' | 'documents' | null>(null);
  const [nudging, setNudging] = useState(false);

  const [apiKey, setApiKey] = useState('');
  const [files, setFiles] = useState<File[]>([]);
  const [windowDragDepth, setWindowDragDepth] = useState(0);
  const [docSlotDragOver, setDocSlotDragOver] = useState(false);
  const [documentSource, setDocumentSource] = useState<'upload' | 'cloud'>('upload');
  const [datasets, setDatasets] = useState<CloudDataset[]>([]);
  const [datasetsLoading, setDatasetsLoading] = useState(true);
  const [selectedDatasets, setSelectedDatasets] = useState<string[]>([]);
  const [serverHasKeys, setServerHasKeys] = useState(false);
  const [estimate, setEstimate] = useState<CostEstimate | null>(null);
  const [estimating, setEstimating] = useState(false);
  const [estimateError, setEstimateError] = useState<string | null>(null);
  // Bumped to re-run a failed estimate without the user having to edit inputs.
  const [estimateRetry, setEstimateRetry] = useState(0);
  const [costDetailsOpen, setCostDetailsOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [apiKeyHint, setApiKeyHint] = useState(false);
  const [advanced, setAdvanced] = useState<AdvancedSettingsValue>(WORKSPACE_DEFAULT_ADVANCED);
  const [developerMode, setDeveloperMode] = useState(false);
  const [allowLlmConfig, setAllowLlmConfig] = useState(false);
  const [dataCollectionEnabled, setDataCollectionEnabled] = useState(false);
  const [consentOpen, setConsentOpen] = useState(false);
  const [maxDocuments, setMaxDocuments] = useState<number | undefined>(undefined);
  const [providers, setProviders] = useState<LLMProviderKey[]>([DEFAULT_PROVIDER as LLMProviderKey]);

  useEffect(() => {
    if (folderInputRef.current) {
      folderInputRef.current.setAttribute('webkitdirectory', '');
      folderInputRef.current.setAttribute('directory', '');
    }
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
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    let cancelled = false;
    cloudAPI.getDatasets()
      .then((data) => { if (!cancelled) setDatasets(Array.isArray(data) ? data : []); })
      .catch(() => { if (!cancelled) setDatasets([]); })
      .finally(() => { if (!cancelled) setDatasetsLoading(false); });
    return () => { cancelled = true; };
  }, []);

  const hasQuery = query.trim().length > 0;
  const hasLocalFiles = files.length > 0;
  const hasDocuments = documentSource === 'cloud' ? selectedDatasets.length > 0 : hasLocalFiles;
  const hasApiKey = serverHasKeys || apiKey.trim().length > 0;
  const bothSlotsFilled = hasQuery && hasDocuments;
  const canStart = bothSlotsFilled && hasApiKey;
  const selectedBytes = useMemo(() => files.reduce((sum, f) => sum + f.size, 0), [files]);
  const windowDragging = windowDragDepth > 0;

  useEffect(() => {
    if (apiKeyHint && hasApiKey) setApiKeyHint(false);
  }, [apiKeyHint, hasApiKey]);

  // Type-anywhere-to-start: if the user simply starts typing on the landing
  // screen without clicking first, route keystrokes directly into [ your question ].
  useEffect(() => {
    const handleGlobalKeyDown = (e: globalThis.KeyboardEvent) => {
      if (e.defaultPrevented || e.metaKey || e.ctrlKey || e.altKey) return;
      if (e.key.length !== 1) return;
      if (docsPopoverOpen || consentOpen || creating) return;

      const target = e.target as HTMLElement | null;
      if (
        target &&
        (target.tagName === 'INPUT' ||
          target.tagName === 'TEXTAREA' ||
          target.tagName === 'SELECT' ||
          target.isContentEditable ||
          // A focused button or link owns Space for activation. Letters still
          // route into the question, so type-anywhere keeps working after a click.
          (e.key === ' ' &&
            target.closest('button, a[href], [role="button"], [role="menuitem"], [role="tab"]')))
      ) {
        return;
      }
      // Don't hijack typing when a modal dialog or open popover (e.g. Command Palette,
      // UX Lab, Open/Recent popover) is mounted.
      if (
        document.querySelector('[role="dialog"]') ||
        document.querySelector('[data-radix-popper-content-wrapper]')
      ) {
        return;
      }

      const el = questionSpanRef.current;
      if (!el) return;
      const currentText = el.textContent || '';
      if (!currentText.trim() && e.key === ' ') return;

      e.preventDefault();
      el.textContent = currentText + e.key;
      focusEndOfElement(el);
      setQuery(el.textContent || '');
      setError(null);
    };

    window.addEventListener('keydown', handleGlobalKeyDown);
    return () => window.removeEventListener('keydown', handleGlobalKeyDown);
  }, [consentOpen, creating, docsPopoverOpen]);

  // Docs-first flow: the instant local files land and no question is entered yet,
  // place the cursor right inside [ your question ] so the user can type immediately.
  useEffect(() => {
    const justAddedFirstLocalFiles =
      documentSource === 'upload' && hasLocalFiles && !prevHasLocalFilesRef.current;
    prevHasLocalFilesRef.current = documentSource === 'upload' && hasLocalFiles;
    if (justAddedFirstLocalFiles) {
      setDocsPopoverOpen(false);
      if (!hasQuery) {
        window.requestAnimationFrame(() => focusEndOfElement(questionSpanRef.current));
      }
    }
  }, [documentSource, hasLocalFiles, hasQuery]);

  // Rare, polite idle nudge: wait ~9s of stillness before a single soft rock,
  // then rest ~20s between nudges. Stops immediately on hover, focus, or drag.
  const canIdleNudge =
    !bothSlotsFilled &&
    !questionFocused &&
    !docsPopoverOpen &&
    !hoveredSlot &&
    !windowDragging &&
    !creating;

  useEffect(() => {
    if (!canIdleNudge) {
      setNudging(false);
      return undefined;
    }

    let stopTimer: number | undefined;
    let repeatTimer: number | undefined;

    const triggerNudge = () => {
      setNudging(true);
      stopTimer = window.setTimeout(() => setNudging(false), 850);
    };

    const initialTimer = window.setTimeout(() => {
      triggerNudge();
      repeatTimer = window.setInterval(triggerNudge, 20000);
    }, 9000);

    return () => {
      window.clearTimeout(initialTimer);
      if (stopTimer !== undefined) window.clearTimeout(stopTimer);
      if (repeatTimer !== undefined) window.clearInterval(repeatTimer);
      setNudging(false);
    };
  }, [canIdleNudge, hasQuery, hasDocuments]);

  // Live debounced automatic cost estimation once both slots + API key are ready.
  // `estimating` is raised immediately (not when the debounce fires) so the
  // Build button stays disabled while the shown estimate is stale: a run must
  // never start without the user having seen the cost of these exact inputs.
  useEffect(() => {
    if (!canStart) {
      setEstimate(null);
      setEstimating(false);
      setEstimateError(null);
      return undefined;
    }
    let cancelled = false;
    setEstimating(true);
    setEstimateError(null);
    const timer = window.setTimeout(async () => {
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
        if (!cancelled) setEstimate(result);
      } catch (err: any) {
        if (!cancelled) {
          setEstimate(null);
          setEstimateError(err?.response?.data?.detail || err?.message || 'Failed to estimate this project');
        }
      } finally {
        if (!cancelled) setEstimating(false);
      }
    }, 650);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [advanced, apiKey, canStart, documentSource, estimateRetry, files, query, selectedDatasets]);

  const mergeIncomingFiles = useCallback((incoming: File[]) => {
    // Hidden files (.DS_Store and the like) are dropped silently; anything else
    // that is filtered out is reported so files never vanish without a trace.
    const visible = incoming.filter((f) => !f.name.startsWith('.'));
    const valid = visible.filter((f) => {
      const lower = f.name.toLowerCase();
      return ACCEPTED_EXTS.some((ext) => lower.endsWith(ext));
    });
    const skipped = visible.length - valid.length;
    if (skipped > 0) {
      toast({
        title: `Skipped ${skipped} unsupported file${skipped === 1 ? '' : 's'}`,
        description: `Supported: ${ACCEPTED_EXTS.map((ext) => ext.slice(1).toUpperCase()).join(', ')}.`,
      });
    }
    if (valid.length === 0) return;
    // Key on the relative path inside a picked folder, not the bare name, so two
    // same-named files in different subfolders are both kept.
    const fileKey = (f: File) => `${f.webkitRelativePath || f.name}:${f.size}`;
    setFiles((prev) => {
      const seen = new Set(prev.map(fileKey));
      const next = [...prev];
      for (const f of valid) {
        const key = fileKey(f);
        if (!seen.has(key)) {
          seen.add(key);
          next.push(f);
        }
      }
      return next;
    });
    setDocumentSource('upload');
    setError(null);
  }, [toast]);

  const removeFileAt = (idx: number) => {
    setFiles((prev) => prev.filter((_, i) => i !== idx));
  };

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
        title: 'Building your table',
        description: 'Schema and extracted rows will stream into the workspace.',
      });
      onCreated(result.session_id);
      navigate(`/workspace/${result.session_id}`, { replace: true });
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to start project');
    } finally {
      setCreating(false);
    }
  }, [advanced, apiKey, documentSource, files, navigate, onCreated, query, selectedDatasets, toast]);

  const handleBuildTable = useCallback(async () => {
    if (creating || !bothSlotsFilled) return;
    if (!hasApiKey) {
      // The key field is hidden when SHOW_API_KEY_FIELD is off, so the hint and
      // focus alone would make this click a silent no-op. Always say why.
      setError('Add an API key or configure server-side API keys before starting.');
      if (SHOW_API_KEY_FIELD) {
        setApiKeyHint(true);
        apiKeyInputRef.current?.focus();
      }
      return;
    }
    // A failed estimate makes this click a retry instead of a dead end.
    if (estimateError) {
      setEstimateRetry((n) => n + 1);
      return;
    }
    // Never start a paid run without a current estimate on screen.
    if (estimating || !estimate) return;
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
  }, [bothSlotsFilled, creating, dataCollectionEnabled, developerMode, estimate, estimateError, estimating, hasApiKey, runCreate]);

  const handleQuestionPaste = (e: ClipboardEvent<HTMLSpanElement>) => {
    e.preventDefault();
    const plain = e.clipboardData
      .getData('text/plain')
      .replace(/\r\n?/g, ' ')
      .replace(/\n+/g, ' ');
    insertPlainTextAtCaret(plain);
    const nextText = questionSpanRef.current?.textContent || '';
    setQuery(nextText);
    setError(null);
  };

  const handleQuestionKeyDown = (e: KeyboardEvent<HTMLSpanElement>) => {
    if (e.key === 'Escape') {
      e.preventDefault();
      questionSpanRef.current?.blur();
      return;
    }
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      questionSpanRef.current?.blur();
      if (hasQuery && hasDocuments) {
        void handleBuildTable();
      }
    }
  };

  const showCloudOption = datasetsLoading || datasets.length > 0;
  const docsPillLabel = windowDragging || docSlotDragOver
    ? (hasDocuments ? 'drop to add more' : 'drop files here')
    : !hasDocuments
      ? 'your documents'
      : documentSource === 'cloud'
        ? `${selectedDatasets.length} dataset${selectedDatasets.length === 1 ? '' : 's'}`
        : `${files.length} document${files.length === 1 ? '' : 's'}`;

  const docsSummaryShort = documentSource === 'cloud'
    ? `${selectedDatasets.length} dataset${selectedDatasets.length === 1 ? '' : 's'}`
    : `${files.length} doc${files.length === 1 ? '' : 's'}`;

  const questionShimmer = !hasQuery && !questionFocused;
  const docsShimmer = !hasDocuments && !docsPopoverOpen && !windowDragging;
  const questionNudge = nudging && !hasQuery;
  const docsNudge = nudging && !hasDocuments;

  const questionState = questionFocused ? 'editing' : hasQuery ? 'filled' : 'empty';
  const docsState = windowDragging || docSlotDragOver
    ? 'drag'
    : docsPopoverOpen
      ? 'editing'
      : hasDocuments
        ? 'filled'
        : 'empty';

  return (
    <div
      className="workspace-start-hub"
      onDragEnter={(e) => {
        if (Array.from(e.dataTransfer.types || []).includes('Files')) {
          e.preventDefault();
          setWindowDragDepth((d) => d + 1);
        }
      }}
      onDragOver={(e) => {
        if (Array.from(e.dataTransfer.types || []).includes('Files')) {
          e.preventDefault();
        }
      }}
      onDragLeave={() => {
        setWindowDragDepth((d) => Math.max(0, d - 1));
      }}
      onDrop={(e) => {
        e.preventDefault();
        setWindowDragDepth(0);
        setDocSlotDragOver(false);
        if (e.dataTransfer.files?.length) {
          mergeIncomingFiles(Array.from(e.dataTransfer.files));
        }
      }}
    >
      {/* Hidden shared file & folder pickers */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        accept={ACCEPTED_EXTS.join(',')}
        className="hidden"
        onChange={(e) => {
          mergeIncomingFiles(Array.from(e.target.files || []));
          e.target.value = '';
        }}
      />
      <input
        ref={folderInputRef}
        type="file"
        multiple
        className="hidden"
        onChange={(e) => {
          mergeIncomingFiles(Array.from(e.target.files || []));
          e.target.value = '';
        }}
      />

      <div className="workspace-start-hub-inner">
        {/* =================================================================
            THE LIVING SENTENCE (single-line on desktop, balanced wrap on small)
           ================================================================= */}
        <h1 className="starthub-sentence">
          {/* Keep "Structure [ your documents ]" grouped so narrow screens wrap cleanly */}
          <span className="starthub-docs-clause">
            <span>Structure </span>

            {/* Interactive Slot 1: Documents (clickable popover + live drop target) */}
            <Popover
              open={docsPopoverOpen}
              onOpenChange={(open) => {
                setDocsPopoverOpen(open);
                if (!open && hasDocuments && !hasQuery) {
                  window.requestAnimationFrame(() => focusEndOfElement(questionSpanRef.current));
                }
              }}
            >
              <PopoverTrigger asChild>
                <button
                  type="button"
                  onMouseEnter={() => setHoveredSlot('documents')}
                  onMouseLeave={() => setHoveredSlot((s) => (s === 'documents' ? null : s))}
                  onDragOver={(e) => {
                    e.preventDefault();
                    setDocSlotDragOver(true);
                  }}
                  onDragLeave={() => setDocSlotDragOver(false)}
                  onDrop={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    setDocSlotDragOver(false);
                    setWindowDragDepth(0);
                    if (e.dataTransfer.files?.length) {
                      mergeIncomingFiles(Array.from(e.dataTransfer.files));
                    }
                  }}
                  data-state={docsState}
                  data-shimmer={docsShimmer ? 'true' : undefined}
                  data-nudging={docsNudge ? 'true' : undefined}
                  title={
                    hasDocuments
                      ? 'Click to manage documents, or drag & drop more files anywhere'
                      : 'Click to browse files or folders, or drag & drop documents here'
                  }
                  className="starthub-slot-pill cursor-pointer"
                >
                  {hasDocuments && !windowDragging && !docSlotDragOver ? (
                    <Check className="starthub-slot-icon text-emerald-600 stroke-[2.5]" />
                  ) : (
                    <Upload className="starthub-slot-icon opacity-70" />
                  )}
                  <span>{docsPillLabel}</span>
                </button>
              </PopoverTrigger>

              <PopoverContent align="center" sideOffset={10} className="w-[340px] sm:w-[380px] p-3.5 space-y-3">
                {showCloudOption && (
                  <div className="inline-flex w-full rounded-md border bg-muted/40 p-0.5 text-xs">
                    <button
                      type="button"
                      onClick={() => setDocumentSource('upload')}
                      className={`flex-1 rounded py-1 font-medium transition-colors ${
                        documentSource === 'upload'
                          ? 'bg-background text-foreground shadow-sm'
                          : 'text-muted-foreground hover:text-foreground'
                      }`}
                    >
                      Files / Folder
                    </button>
                    <button
                      type="button"
                      onClick={() => setDocumentSource('cloud')}
                      className={`flex-1 inline-flex items-center justify-center gap-1 rounded py-1 font-medium transition-colors ${
                        documentSource === 'cloud'
                          ? 'bg-background text-foreground shadow-sm'
                          : 'text-muted-foreground hover:text-foreground'
                      }`}
                    >
                      <Cloud className="h-3.5 w-3.5" />
                      Cloud datasets
                      {datasets.length > 0 && (
                        <span className="text-[10px] text-muted-foreground">({datasets.length})</span>
                      )}
                    </button>
                  </div>
                )}

                {documentSource === 'upload' ? (
                  <div className="space-y-3">
                    <div className="grid grid-cols-2 gap-2">
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        className="h-9 text-xs gap-1.5"
                        onClick={() => fileInputRef.current?.click()}
                      >
                        <FileText className="h-3.5 w-3.5" />
                        {hasLocalFiles ? 'Add files' : 'Select files'}
                      </Button>
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        className="h-9 text-xs gap-1.5"
                        onClick={() => folderInputRef.current?.click()}
                      >
                        <FolderOpen className="h-3.5 w-3.5" />
                        {hasLocalFiles ? 'Add folder' : 'Select folder'}
                      </Button>
                    </div>

                    <p className="text-[11px] text-center text-muted-foreground">
                      Or drag &amp; drop PDF, DOCX, TXT, MD, or JSON anywhere on this page
                    </p>

                    {files.length > 0 && (
                      <div className="pt-2.5 border-t border-border/70 space-y-2">
                        <div className="flex items-center justify-between text-xs">
                          <span className="font-semibold text-foreground">
                            {files.length} file{files.length === 1 ? '' : 's'}{' '}
                            <span className="font-normal text-muted-foreground">
                              ({formatFileSize(selectedBytes)})
                            </span>
                          </span>
                          <button
                            type="button"
                            onClick={() => setFiles([])}
                            className="text-[11px] text-muted-foreground hover:text-destructive"
                          >
                            Clear all
                          </button>
                        </div>
                        <div className="max-h-40 overflow-y-auto space-y-1 pr-0.5">
                          {files.map((file, idx) => (
                            <div
                              key={`${file.name}-${file.size}-${idx}`}
                              className="flex items-center gap-2 rounded-md bg-muted/40 px-2 py-1 text-xs"
                            >
                              <FileText className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                              <span className="flex-1 truncate text-foreground" title={file.webkitRelativePath || file.name}>
                                {file.name}
                              </span>
                              <button
                                type="button"
                                onClick={() => removeFileAt(idx)}
                                aria-label={`Remove ${file.name}`}
                                className="rounded p-0.5 text-muted-foreground hover:text-destructive"
                              >
                                <X className="h-3 w-3" />
                              </button>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="space-y-2.5">
                    <CloudDatasetPicker
                      datasets={datasets}
                      loading={datasetsLoading}
                      selected={selectedDatasets}
                      onChange={setSelectedDatasets}
                      maxDocuments={maxDocuments}
                      bypassLimit={advanced.bypassLimit}
                    />
                    {selectedDatasets.length > 0 && (
                      <div className="flex justify-end">
                        <Button
                          type="button"
                          size="sm"
                          className="h-7 text-xs"
                          onClick={() => {
                            setDocsPopoverOpen(false);
                            if (!hasQuery) {
                              window.requestAnimationFrame(() => focusEndOfElement(questionSpanRef.current));
                            }
                          }}
                        >
                          Done
                        </Button>
                      </div>
                    )}
                  </div>
                )}
              </PopoverContent>
            </Popover>
          </span>

          <span> around </span>

          {/* Interactive Slot 2: Direct inline-editable question */}
          <span
            onClick={(e) => {
              setDocsPopoverOpen(false);
              if (e.target !== questionSpanRef.current) {
                focusEndOfElement(questionSpanRef.current);
              }
            }}
            onMouseEnter={() => setHoveredSlot('question')}
            onMouseLeave={() => setHoveredSlot((s) => (s === 'question' ? null : s))}
            data-state={questionState}
            data-shimmer={questionShimmer ? 'true' : undefined}
            data-shimmer-delay={!hasDocuments ? '1' : undefined}
            data-nudging={questionNudge ? 'true' : undefined}
            data-nudge-delay={!hasDocuments ? '1' : undefined}
            className="starthub-slot-pill cursor-text"
          >
            {hasQuery && !questionFocused ? (
              <Check className="starthub-slot-icon text-emerald-600 stroke-[2.5]" />
            ) : (
              <Pencil className="starthub-slot-icon opacity-70" />
            )}
            <span
              ref={questionSpanRef}
              role="textbox"
              aria-label="Your research question"
              aria-multiline="true"
              contentEditable
              suppressContentEditableWarning
              spellCheck
              data-empty={hasQuery ? undefined : 'true'}
              data-placeholder="any research question"
              className="starthub-inline-input"
              onFocus={() => {
                setDocsPopoverOpen(false);
                setQuestionFocused(true);
              }}
              onBlur={() => setQuestionFocused(false)}
              onInput={(e) => {
                const text = e.currentTarget.textContent || '';
                if (!text.trim() && e.currentTarget.innerHTML !== '') {
                  e.currentTarget.textContent = '';
                }
                setQuery(text);
                setError(null);
              }}
              onPaste={handleQuestionPaste}
              onKeyDown={handleQuestionKeyDown}
              onDrop={(e) => {
                if (e.dataTransfer.files?.length) {
                  e.preventDefault();
                }
              }}
            />
          </span>
        </h1>

        {/* =================================================================
            READY ACTION ZONE (appears smoothly once BOTH slots are filled)
           ================================================================= */}
        {bothSlotsFilled && (
          <div className="starthub-step-enter mt-8 max-w-[540px] mx-auto space-y-4">
            {SHOW_API_KEY_FIELD && (
              <div className="space-y-1.5 text-left">
                <div className="flex items-center justify-between">
                  <Label htmlFor="starthub-api-key" className="text-xs font-semibold">API key</Label>
                  {apiKeyHint && (
                    <span className="text-xs font-medium text-amber-600">API key required</span>
                  )}
                </div>
                <Input
                  ref={apiKeyInputRef}
                  id="starthub-api-key"
                  value={apiKey}
                  type="password"
                  onChange={(e) => setApiKey(e.target.value)}
                  placeholder={serverHasKeys ? 'Optional: server keys configured' : 'Required unless server keys are configured'}
                  className={apiKeyHint ? 'ring-2 ring-amber-400/75 border-amber-400' : ''}
                />
              </div>
            )}

            {(error || estimateError) && (
              <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-3 text-xs text-destructive">
                {error || `Could not estimate cost: ${estimateError}. Click Build Table to retry.`}
              </div>
            )}

            <div className="flex flex-col items-center gap-3">
              <Button
                type="button"
                size="lg"
                onClick={() => void handleBuildTable()}
                // Without an API key the click stays enabled so it can explain why
                // nothing starts; with one, it waits for a current estimate (or retries a failed one).
                disabled={creating || (hasApiKey && !estimateError && (estimating || !estimate))}
                className="h-12 px-8 rounded-xl text-sm font-semibold gap-2 shadow-md shadow-primary/20"
              >
                {creating ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Building Table…
                  </>
                ) : (
                  <>
                    <span>
                      Build Table · {docsSummaryShort}
                      {estimate && !estimating ? ` · ~${formatCost(estimate)}` : ''}
                    </span>
                    <ArrowRight className="h-4 w-4" />
                  </>
                )}
              </Button>

              <div className="w-full flex items-center justify-center gap-4 flex-wrap text-xs text-muted-foreground">
                <Collapsible className="w-full">
                  <div className="flex items-center justify-center gap-4 flex-wrap">
                    <CollapsibleTrigger className="group inline-flex items-center gap-1 hover:text-foreground transition-colors">
                      <ChevronDown className="h-3.5 w-3.5 transition-transform group-data-[state=open]:rotate-180" />
                      <span>Advanced settings</span>
                    </CollapsibleTrigger>

                    {(estimating || estimate) && (
                      <>
                        <span className="text-muted-foreground/35">·</span>
                        <button
                          type="button"
                          onClick={() => setCostDetailsOpen((v) => !v)}
                          className="inline-flex items-center gap-1.5 hover:text-foreground transition-colors"
                        >
                          {estimating ? (
                            <>
                              <Loader2 className="h-3 w-3 animate-spin" />
                              <span>Estimating cost…</span>
                            </>
                          ) : (
                            <>
                              <Sparkles className="h-3 w-3 text-primary" />
                              <span className="tabular-nums font-medium text-foreground">
                                Est. {formatCost(estimate)}
                              </span>
                              <span className="underline underline-offset-2">
                                {costDetailsOpen ? 'hide' : 'details'}
                              </span>
                            </>
                          )}
                        </button>
                      </>
                    )}
                  </div>

                  <CollapsibleContent className="mt-4 rounded-xl border bg-card p-4 text-left shadow-sm">
                    <AdvancedSettingsFields
                      value={advanced}
                      onChange={(patch) => setAdvanced((prev) => ({ ...prev, ...patch }))}
                      developerMode={developerMode}
                      allowLlmConfig={allowLlmConfig}
                      providers={providers}
                      maxDocuments={maxDocuments}
                    />
                  </CollapsibleContent>
                </Collapsible>

                {costDetailsOpen && estimate && !estimating && (
                  <div className="w-full rounded-xl border bg-card px-4 py-3 text-left shadow-sm">
                    <CostBreakdown
                      estimate={estimate}
                      skipValueExtraction={advanced.skipValueExtraction}
                    />
                  </div>
                )}
              </div>
            </div>
          </div>
        )}
      </div>

      <ConsentDialog
        open={consentOpen}
        onOpenChange={setConsentOpen}
        onConfirm={(optOut) => {
          setConsentOpen(false);
          void runCreate(optOut);
        }}
      />
    </div>
  );
}
