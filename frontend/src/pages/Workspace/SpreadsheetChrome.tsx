// Top menu bar and formatting toolbar for the workspace spreadsheet chrome.
// Parent: Workspace (index.tsx).

import {
  AlignCenter,
  AlignLeft,
  AlignRight,
  ArrowLeftRight,
  Bold,
  ChevronDown,
  Download,
  Italic,
  LifeBuoy,
  Printer,
  Quote,
  RotateCw,
  Save,
  Search,
  Sparkles,
  Strikethrough,
  Underline,
} from 'lucide-react';

import { ScheMatiQLogo } from '@/components/shared/ScheMatiQLogo';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';

import { CellLegend } from './CellLegend';
import { TABLE_FONT_OPTIONS, TABLE_FONT_SIZE_OPTIONS, WORKSPACE_MENUS } from './constants';
import { OpenProjectPopover } from './OpenProjectPopover';
import type {
  TableDisplayOptions,
  TableTextAlign,
  WorkspaceMenuAction,
  WorkspaceMenuItem,
} from './types';

// Record over TableTextAlign, so TypeScript rejects a new alignment that has
// no label or icon. 'start' follows each cell's text direction, so it gets its
// own icon instead of looking like a fixed left alignment.
const TABLE_ALIGN_META: Record<TableTextAlign, { label: string; Icon: typeof AlignLeft }> = {
  start: { label: 'Auto', Icon: ArrowLeftRight },
  left: { label: 'Left', Icon: AlignLeft },
  center: { label: 'Center', Icon: AlignCenter },
  right: { label: 'Right', Icon: AlignRight },
};
const TABLE_ALIGN_ORDER: TableTextAlign[] = ['start', 'left', 'center', 'right'];

export function SpreadsheetChrome({
  projectTitle,
  sessionStatus,
  canUseProjectActions,
  displayOptions,
  onNewProject,
  onImportProject,
  onOpenSession,
  onOpenClassic,
  onProjectDetails,
  onRefresh,
  onPrint,
  onExport,
  onSaveProject,
  onSaveProjectWithDocs,
  onHome,
  onSearch,
  onUndo,
  onRedo,
  onEstimateCost,
  onOpenChat,
  onSplitView,
  onToggleCompactRows,
  onKeyboardShortcuts,
  onRunPendingEdits,
  onAddDocuments,
  onApplyFormat,
  onReportIssue,
  onCite,
  rerunDisabled,
  minimalChrome,
}: {
  projectTitle: string;
  sessionStatus: string;
  canUseProjectActions: boolean;
  displayOptions: TableDisplayOptions;
  onNewProject: () => void;
  onImportProject: () => void;
  onOpenSession?: (sessionId: string, mode: 'load' | 'schematiq') => void;
  onOpenClassic: () => void;
  onProjectDetails: () => void;
  onRefresh: () => void;
  onPrint: () => void;
  onExport: () => void;
  onSaveProject: () => void;
  onSaveProjectWithDocs: () => void;
  onHome: () => void;
  onSearch: () => void;
  onUndo: () => void;
  onRedo: () => void;
  onEstimateCost: () => void;
  onOpenChat: () => void;
  onSplitView: () => void;
  onToggleCompactRows: () => void;
  onKeyboardShortcuts: () => void;
  onRunPendingEdits: () => void;
  onAddDocuments: () => void;
  onApplyFormat: (patch: Partial<TableDisplayOptions>) => void;
  onReportIssue: () => void;
  onCite: () => void;
  rerunDisabled: boolean;
  minimalChrome?: boolean;
}) {
  // Exhaustive by construction: Record<WorkspaceMenuAction, ...> makes TypeScript
  // reject a new menu action that has no handler, which is what previously let
  // ~19 menu entries render as clickable no-ops.
  const menuActions: Record<WorkspaceMenuAction, () => void> = {
    newProject: onNewProject,
    importProject: onImportProject,
    openClassic: onOpenClassic,
    exportCsv: onExport,
    saveProject: onSaveProject,
    saveProjectWithDocs: onSaveProjectWithDocs,
    undo: onUndo,
    redo: onRedo,
    find: onSearch,
    openChat: onOpenChat,
    splitView: onSplitView,
    toggleCompactRows: onToggleCompactRows,
    keyboardShortcuts: onKeyboardShortcuts,
    projectDetails: onProjectDetails,
    reextract: onRunPendingEdits,
    addDocuments: onAddDocuments,
    estimateCost: onEstimateCost,
    refreshProject: onRefresh,
    reportIssue: onReportIssue,
    cite: onCite,
  };

  // Fallback guards a malformed align value restored from localStorage.
  const activeAlign = TABLE_ALIGN_META[displayOptions.align] ?? TABLE_ALIGN_META.start;

  const isDisabled = (item: WorkspaceMenuItem) => {
    // Re-extract has its own gating (no session, nothing pending, or already
    // starting), which already covers the no-project case.
    if (item.action === 'reextract') return rerunDisabled;
    return Boolean(item.requiresProject) && !canUseProjectActions;
  };

  return (
    <div
      className="workspace-chrome"
      data-minimal={minimalChrome ? 'true' : undefined}
      role="toolbar"
      aria-label={minimalChrome ? 'ScheMatiQ navigation bar' : 'Spreadsheet menu and formatting toolbar'}
    >
      <div className="workspace-chrome-titlebar">
        <button
          type="button"
          className="workspace-file-mark"
          onClick={onHome}
          title="ScheMatiQ home"
          aria-label="ScheMatiQ home"
        >
          <ScheMatiQLogo className="workspace-file-mark-logo" />
          <span className="workspace-file-mark-name">ScheMatiQ</span>
        </button>
        {!minimalChrome && (
          <>
            <div className="workspace-file-title">
              <div className="workspace-file-name">{projectTitle}</div>
              <div className="workspace-file-status">{sessionStatus}</div>
            </div>
            <div className="workspace-menu-row">
              {WORKSPACE_MENUS.map((menu) => (
                <DropdownMenu key={menu.label}>
                  <DropdownMenuTrigger asChild>
                    <button className="workspace-menu-button" type="button">
                      {menu.label}
                    </button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="start" className="workspace-menu-content w-56">
                    {menu.items.map((item) => (
                      <DropdownMenuItem
                        key={item.action}
                        disabled={isDisabled(item)}
                        onClick={menuActions[item.action]}
                      >
                        {item.label}
                      </DropdownMenuItem>
                    ))}
                  </DropdownMenuContent>
                </DropdownMenu>
              ))}
            </div>
          </>
        )}
        <div className="workspace-chrome-links">
          <OpenProjectPopover
            onImportProject={onImportProject}
            onOpenSession={onOpenSession}
          />
          <button
            type="button"
            className="workspace-chrome-link cursor-pointer"
            onClick={onReportIssue}
            title="Report an issue"
            aria-label="Report an issue"
          >
            <LifeBuoy size={15} aria-hidden="true" />
          </button>
          <a
            href="https://youtube.com/watch?v=VILym_Ch0hg&feature=youtu.be"
            target="_blank"
            rel="noopener noreferrer"
            className="workspace-chrome-link"
            title="Demonstration video"
            aria-label="Demonstration video"
          >
            <i className="fa-brands fa-youtube"></i>
          </a>
          <a
            href="https://arxiv.org/pdf/2604.09237"
            target="_blank"
            rel="noopener noreferrer"
            className="workspace-chrome-link"
            title="arXiv paper"
            aria-label="arXiv paper"
          >
            <i className="ai ai-arxiv"></i>
          </a>
          <button
            type="button"
            className="workspace-chrome-link cursor-pointer"
            onClick={onCite}
            title="Cite ScheMatiQ"
            aria-label="Cite ScheMatiQ"
          >
            <Quote size={15} aria-hidden="true" />
          </button>
          <a
            href="https://github.com/shaharl6000/ScheMatiQ"
            target="_blank"
            rel="noopener noreferrer"
            className="workspace-chrome-link"
            title="Code on GitHub"
            aria-label="Code on GitHub"
          >
            <i className="fab fa-github"></i>
          </a>
          <a
            href="https://x.com/EliyaHabba/status/2043690798257250662"
            target="_blank"
            rel="noopener noreferrer"
            className="workspace-chrome-link"
            title="Twitter / X"
            aria-label="Twitter / X"
          >
            <i className="fa-brands fa-x-twitter"></i>
          </a>
        </div>
      </div>

      {!minimalChrome && (
      <div className="workspace-toolbar-row">
        <button className="workspace-toolbar-icon" type="button" onClick={onPrint} title="Print">
          <Printer className="h-3.5 w-3.5" />
        </button>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="workspace-toolbar-icon" type="button" disabled={!canUseProjectActions} title="Export">
              <Download className="h-3.5 w-3.5" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="workspace-menu-content w-72">
            <DropdownMenuItem onClick={onExport} disabled={!canUseProjectActions}>
              <Download className="h-4 w-4 mr-2 shrink-0" />
              <div>
                <div>Download Table (.csv)</div>
                <div className="text-xs text-muted-foreground">Clean data for Excel, no metadata</div>
              </div>
            </DropdownMenuItem>
            <DropdownMenuItem onClick={onSaveProject} disabled={!canUseProjectActions}>
              <Save className="h-4 w-4 mr-2 shrink-0" />
              <div>
                <div>Save Project (.schematiq.json)</div>
                <div className="text-xs text-muted-foreground">Full project with schema and history, for reloading</div>
              </div>
            </DropdownMenuItem>
            <DropdownMenuItem onClick={onSaveProjectWithDocs} disabled={!canUseProjectActions}>
              <Save className="h-4 w-4 mr-2 shrink-0" />
              <div>
                <div>Save Project with Documents (.zip)</div>
                <div className="text-xs text-muted-foreground">Bundle including the original source files, so previews survive re-import</div>
              </div>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>

        <span className="workspace-toolbar-separator" />

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="workspace-toolbar-select workspace-toolbar-font" type="button">
              {displayOptions.fontFamily}
              <ChevronDown className="h-3.5 w-3.5" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="workspace-menu-content w-40">
            {TABLE_FONT_OPTIONS.map((font) => (
              <DropdownMenuItem key={font} onClick={() => onApplyFormat({ fontFamily: font })}>
                {font}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="workspace-toolbar-select workspace-toolbar-size" type="button">
              {displayOptions.fontSize}
              <ChevronDown className="h-3.5 w-3.5" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="workspace-menu-content w-28">
            {TABLE_FONT_SIZE_OPTIONS.map((size) => (
              <DropdownMenuItem key={size} onClick={() => onApplyFormat({ fontSize: size })}>
                {size}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>

        <span className="workspace-toolbar-separator" />

        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.bold} onClick={() => onApplyFormat({ bold: !displayOptions.bold })} title="Bold">
          <Bold className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.italic} onClick={() => onApplyFormat({ italic: !displayOptions.italic })} title="Italic">
          <Italic className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.underline} onClick={() => onApplyFormat({ underline: !displayOptions.underline })} title="Underline">
          <Underline className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" data-active={displayOptions.strikethrough} onClick={() => onApplyFormat({ strikethrough: !displayOptions.strikethrough })} title="Strikethrough">
          <Strikethrough className="h-3.5 w-3.5" />
        </button>

        <span className="workspace-toolbar-separator" />

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="workspace-toolbar-icon" type="button" title={`Align: ${activeAlign.label}`}>
              <activeAlign.Icon className="h-3.5 w-3.5" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="workspace-menu-content w-32">
            {TABLE_ALIGN_ORDER.map((value) => (
              <DropdownMenuItem key={value} onClick={() => onApplyFormat({ align: value })}>
                {TABLE_ALIGN_META[value].label}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
        <button className="workspace-toolbar-icon" type="button" onClick={onSearch} title="Find in workspace">
          <Search className="h-3.5 w-3.5" />
        </button>
        <CellLegend />

        <span className="workspace-toolbar-spacer" />

        <button className="workspace-toolbar-action" type="button" onClick={onEstimateCost} disabled={!canUseProjectActions}>
          <Sparkles className="h-3.5 w-3.5" />
          Estimate
        </button>
        <button
          className="workspace-toolbar-action"
          type="button"
          onClick={onRunPendingEdits}
          disabled={rerunDisabled}
          title="Re-extract values from source documents after schema or observation-unit edits"
        >
          <RotateCw className="h-3.5 w-3.5" />
          Re-extract
        </button>
      </div>
      )}
    </div>
  );
}
