// Top menu bar and formatting toolbar for the workspace spreadsheet chrome.
// Parent: Workspace (index.tsx).

import {
  AlignLeft,
  Bold,
  ChevronDown,
  Download,
  Italic,
  Printer,
  RotateCw,
  Save,
  Search,
  Sigma,
  Sparkles,
  Strikethrough,
  Type,
  Underline,
} from 'lucide-react';

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';

import { TABLE_FONT_OPTIONS, TABLE_FONT_SIZE_OPTIONS, WORKSPACE_MENUS } from './constants';
import type { TableDisplayOptions, TableTextAlign } from './types';

export function SpreadsheetChrome({
  projectTitle,
  sessionStatus,
  canUseProjectActions,
  displayOptions,
  onNewProject,
  onImportProject,
  onOpenClassic,
  onProjectDetails,
  onRefresh,
  onPrint,
  onExport,
  onSaveProject,
  onSaveProjectWithDocs,
  onHome,
  onSearch,
  onEstimateCost,
  onShowSheet,
  onShowChat,
  onSplitView,
  onRunPendingEdits,
  onAddDocuments,
  onApplyFormat,
  rerunDisabled,
}: {
  projectTitle: string;
  sessionStatus: string;
  canUseProjectActions: boolean;
  displayOptions: TableDisplayOptions;
  onNewProject: () => void;
  onImportProject: () => void;
  onOpenClassic: () => void;
  onProjectDetails: () => void;
  onRefresh: () => void;
  onPrint: () => void;
  onExport: () => void;
  onSaveProject: () => void;
  onSaveProjectWithDocs: () => void;
  onHome: () => void;
  onSearch: () => void;
  onEstimateCost: () => void;
  onShowSheet: () => void;
  onShowChat: () => void;
  onSplitView: () => void;
  onRunPendingEdits: () => void;
  onAddDocuments: () => void;
  onApplyFormat: (patch: Partial<TableDisplayOptions>) => void;
  rerunDisabled: boolean;
}) {
  const runMenuItem = (label: string) => {
    if (label === 'New project') onNewProject();
    if (label === 'Import project') onImportProject();
    if (label === 'Open classic visualizer') onOpenClassic();
    if (label === 'Download table (.csv)') onExport();
    if (label === 'Save project (.schematiq.json)') onSaveProject();
    if (label === 'Save project with documents (.zip)') onSaveProjectWithDocs();
    if (label === 'Project details') onProjectDetails();
    if (label === 'Refresh project') onRefresh();
    if (label === 'Estimate cost') onEstimateCost();
    if (label === 'Show sheet full screen') onShowSheet();
    if (label === 'Show chat full screen') onShowChat();
    if (label === 'Split view') onSplitView();
    if (label === 'Re-extract table') onRunPendingEdits();
    if (label === 'Add documents') onAddDocuments();
  };

  const isDisabled = (label: string) => {
    if (label === 'New project' || label === 'Import project') return false;
    if (label === 'Re-extract table') return rerunDisabled;
    return !canUseProjectActions && [
      'Open classic visualizer',
      'Download table (.csv)',
      'Save project (.schematiq.json)',
      'Save project with documents (.zip)',
      'Project details',
      'Refresh project',
      'Estimate cost',
      'Show sheet full screen',
      'Show chat full screen',
      'Split view',
      'Re-extract table',
      'Add documents',
    ].includes(label);
  };

  return (
    <div className="workspace-chrome" role="toolbar" aria-label="Spreadsheet menu and formatting toolbar">
      <div className="workspace-chrome-titlebar">
        <button
          type="button"
          className="workspace-file-mark"
          onClick={onHome}
          title="ScheMatiQ home"
          aria-label="ScheMatiQ home"
        >
          <img src="/icon.png" alt="" className="workspace-file-mark-logo" />
          <span className="workspace-file-mark-name">ScheMatiQ</span>
        </button>
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
                    key={item}
                    disabled={isDisabled(item)}
                    onClick={() => runMenuItem(item)}
                  >
                    {item}
                  </DropdownMenuItem>
                ))}
              </DropdownMenuContent>
            </DropdownMenu>
          ))}
        </div>
        <div className="workspace-chrome-links">
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

        <button className="workspace-toolbar-icon" type="button" title="Text color">
          <Type className="h-3.5 w-3.5" />
        </button>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="workspace-toolbar-icon" type="button" title="Align">
              <AlignLeft className="h-3.5 w-3.5" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="workspace-menu-content w-32">
            {(['left', 'center', 'right'] as TableTextAlign[]).map((align) => (
              <DropdownMenuItem key={align} onClick={() => onApplyFormat({ align })}>
                {align[0].toUpperCase() + align.slice(1)}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
        <button className="workspace-toolbar-icon" type="button" title="Functions">
          <Sigma className="h-3.5 w-3.5" />
        </button>
        <button className="workspace-toolbar-icon" type="button" onClick={onSearch} title="Search">
          <Search className="h-3.5 w-3.5" />
        </button>

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
    </div>
  );
}
