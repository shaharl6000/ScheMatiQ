import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
  MoreVertical,
  Plus,
  BookmarkPlus,
  BarChart3,
  Columns3,
  Save,
  FolderOpen,
  Trash2,
  Eye,
  EyeOff,
  Filter,
  Merge,
  Search,
  FileText,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/checkbox';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';
import { ColumnVisibilityState, FilterPreset, FilterRule, SortColumn, generatePresetId } from './types/filters';
import { formatColumnName } from '../../utils/formatting';
import { UnitSummary, DocumentSummary } from '../../types/unit';

interface TableOptionsMenuProps {
  // Add Filter
  onAddFilter: () => void;
  // Add Row
  onAddRow: () => void;
  readonly: boolean;
  // Presets
  sessionId: string;
  currentFilters: FilterRule[];
  currentSort: SortColumn[];
  onLoadPreset: (filters: FilterRule[], sort: SortColumn[]) => void;
  // Fullness
  fullnessThreshold: number;
  onFullnessChange: (value: number) => void;
  visibleColumnsCount: number;
  totalColumnsCount: number;
  hiddenByFullnessCount: number;
  // Column visibility
  columns: string[];
  visibility: ColumnVisibilityState;
  onToggleColumn: (columnName: string) => void;
  onShowAll: () => void;
  onHideAll: () => void;
  // Unit-specific (optional, only rendered in By Unit view)
  unitList?: UnitSummary[];
  selectedUnits?: string[];
  onUnitChange?: (units: string[]) => void;
  unitDataLoading?: boolean;
  onMergeUnits?: () => void;
  mergeDisabled?: boolean;
  onToggleSuggestions?: () => void;
  showSuggestions?: boolean;
  suggestionsCount?: number;
  // Document filter (optional, for standard/by-document view)
  documentList?: DocumentSummary[];
  selectedDocuments?: string[];
  onDocumentChange?: (documents: string[]) => void;
  documentDataLoading?: boolean;
}

/** Reusable multi-select filter submenu content */
function MultiSelectFilterContent({
  items,
  selected,
  onChange,
  searchPlaceholder,
  itemLabel,
}: {
  items: { name: string; rowCount: number }[];
  selected: string[];
  onChange: (items: string[]) => void;
  searchPlaceholder: string;
  itemLabel: string;
}) {
  const [search, setSearch] = useState('');

  const filteredItems = useMemo(() => {
    if (!search.trim()) return items;
    const term = search.toLowerCase();
    return items.filter(item => item.name.toLowerCase().includes(term));
  }, [items, search]);

  const selectedSet = useMemo(() => new Set(selected), [selected]);
  const allSelected = selected.length === 0 || selected.length === items.length; // empty or full = all
  const noneSelected = selected.length > 0 && items.length > 0 &&
    !items.some(i => selectedSet.has(i.name)); // has values but none match items

  const handleToggle = (name: string) => {
    if (selectedSet.has(name)) {
      const next = selected.filter(n => n !== name);
      // If removing the last item, treat as "all" (no filter)
      onChange(next.length === 0 ? [] : next);
    } else {
      const next = [...selected, name];
      // If all items are now selected, switch to "all" (no filter)
      if (next.length === items.length) {
        onChange([]);
      } else {
        onChange(next);
      }
    }
  };

  return (
    <div
      className="w-80 max-w-[90vw] flex flex-col"
      style={{ maxHeight: 'min(500px, 70vh)' }}
      onClick={(e) => e.stopPropagation()}
      onPointerDown={(e) => e.stopPropagation()}
      onKeyDown={(e) => e.stopPropagation()}
    >
      {/* Sticky search */}
      <div className="p-2 border-b shrink-0">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
          <Input
            placeholder={searchPlaceholder}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 pl-8 text-sm"
            autoFocus
          />
        </div>
      </div>

      {/* Quick actions */}
      <div className="flex gap-1 p-2 border-b shrink-0">
        <Button
          variant={allSelected && !noneSelected ? 'default' : 'outline'}
          size="sm"
          className="flex-1 h-7 text-xs"
          onClick={() => onChange([])}
        >
          All
        </Button>
        <Button
          variant={noneSelected ? 'default' : 'outline'}
          size="sm"
          className="flex-1 h-7 text-xs"
          onClick={() => onChange(['__none__'])}
        >
          None
        </Button>
      </div>

      {/* Item list — flex-1 + overflow-auto so it scrolls within the constrained container */}
      <div className="flex-1 min-h-0 overflow-y-auto">
        <div className="p-1">
          {filteredItems.map((item) => {
            const isChecked = (selected.length === 0) || selectedSet.has(item.name);
            return (
              <div
                key={item.name}
                className="flex items-center gap-2 py-1.5 px-2 rounded hover:bg-muted/50 cursor-pointer"
                onClick={() => {
                  if (selected.length === 0) {
                    // Switch from "all" to "all except this one"
                    onChange(items.filter(i => i.name !== item.name).map(i => i.name));
                  } else if (noneSelected) {
                    // From "none" state, select just this one
                    onChange([item.name]);
                  } else {
                    handleToggle(item.name);
                  }
                }}
              >
                <Checkbox
                  checked={isChecked}
                  className="shrink-0"
                />
                <Tooltip>
                  <TooltipTrigger asChild>
                    <span className="flex-1 text-sm truncate min-w-0">{item.name}</span>
                  </TooltipTrigger>
                  <TooltipContent side="right" className="max-w-[400px]">
                    <p className="break-all">{item.name}</p>
                  </TooltipContent>
                </Tooltip>
                <Badge variant="outline" className="h-5 px-1.5 text-xs shrink-0">
                  {item.rowCount}
                </Badge>
              </div>
            );
          })}
          {filteredItems.length === 0 && (
            <div className="py-4 text-center text-sm text-muted-foreground">
              No {itemLabel}s match &ldquo;{search}&rdquo;
            </div>
          )}
        </div>
      </div>

      {/* Footer */}
      <div className="p-2 border-t text-xs text-center text-muted-foreground shrink-0">
        {noneSelected
          ? `No ${itemLabel}s selected (showing all)`
          : allSelected
            ? `All ${items.length} ${itemLabel}s`
            : `${selected.length} of ${items.length} ${itemLabel}s selected`}
      </div>
    </div>
  );
}

/** Format badge text for multi-select filter trigger */
function filterBadgeText(selected: string[], itemLabel: string): string | null {
  if (selected.length === 0) return null;
  // __none__ sentinel means "none selected" — no badge needed
  if (selected.length === 1 && selected[0] === '__none__') return null;
  if (selected.length === 1) return selected[0];
  return `${selected.length} ${itemLabel}s`;
}

const FULLNESS_PRESETS = [0, 25, 50, 75, 100];
const PRESET_STORAGE_KEY_PREFIX = 'dataTable_presets_';

const TableOptionsMenu: React.FC<TableOptionsMenuProps> = ({
  onAddFilter,
  onAddRow,
  readonly,
  sessionId,
  currentFilters,
  currentSort,
  onLoadPreset,
  fullnessThreshold,
  onFullnessChange,
  visibleColumnsCount,
  totalColumnsCount,
  hiddenByFullnessCount,
  columns,
  visibility,
  onToggleColumn,
  onShowAll,
  onHideAll,
  unitList,
  selectedUnits,
  onUnitChange,
  unitDataLoading,
  onMergeUnits,
  mergeDisabled,
  onToggleSuggestions,
  showSuggestions,
  suggestionsCount,
  documentList,
  selectedDocuments,
  onDocumentChange,
  documentDataLoading,
}) => {
  // Preset state
  const [presets, setPresets] = useState<FilterPreset[]>([]);
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [presetToDelete, setPresetToDelete] = useState<string | null>(null);
  const [newPresetName, setNewPresetName] = useState('');
  const [newPresetDescription, setNewPresetDescription] = useState('');

  const storageKey = `${PRESET_STORAGE_KEY_PREFIX}${sessionId}`;

  // Load presets from localStorage
  useEffect(() => {
    try {
      const stored = localStorage.getItem(storageKey);
      if (stored) {
        setPresets(JSON.parse(stored));
      }
    } catch {
      // Ignore localStorage errors
    }
  }, [storageKey]);

  const savePresets = useCallback((newPresets: FilterPreset[]) => {
    setPresets(newPresets);
    try {
      localStorage.setItem(storageKey, JSON.stringify(newPresets));
    } catch {
      // Ignore localStorage errors
    }
  }, [storageKey]);

  const handleSavePreset = () => {
    if (!newPresetName.trim()) return;
    const newPreset: FilterPreset = {
      id: generatePresetId(),
      name: newPresetName.trim(),
      description: newPresetDescription.trim() || undefined,
      filters: currentFilters,
      sort: currentSort,
      createdAt: new Date().toISOString(),
    };
    savePresets([...presets, newPreset]);
    setSaveDialogOpen(false);
    setNewPresetName('');
    setNewPresetDescription('');
  };

  const handleDeletePreset = (presetId: string) => {
    setPresetToDelete(presetId);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = () => {
    if (presetToDelete) {
      savePresets(presets.filter(p => p.id !== presetToDelete));
    }
    setDeleteDialogOpen(false);
    setPresetToDelete(null);
  };

  const formatPresetSummary = (preset: FilterPreset): string => {
    const parts: string[] = [];
    if (preset.filters.length > 0) {
      parts.push(`${preset.filters.length} filter${preset.filters.length > 1 ? 's' : ''}`);
    }
    if (preset.sort.length > 0) {
      parts.push(`${preset.sort.length} sort${preset.sort.length > 1 ? 's' : ''}`);
    }
    return parts.length > 0 ? parts.join(', ') : 'Empty preset';
  };

  const canSavePreset = currentFilters.length > 0 || currentSort.length > 0;

  // Active state indicator
  const visibleCount = columns.filter(col => visibility[col] !== false).length;
  const hiddenCount = columns.length - visibleCount;

  // Badge text for unit filter
  const unitBadgeText = filterBadgeText(selectedUnits || [], 'unit');
  // Badge text for document filter
  const docBadgeText = filterBadgeText(selectedDocuments || [], 'doc');

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="icon" className="relative h-9 w-9">
            <MoreVertical className="h-4 w-4" />
            <span className="sr-only">Table options</span>
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-56">
          {/* Add Filter */}
          <DropdownMenuItem onClick={onAddFilter}>
            <Filter className="h-4 w-4 mr-2" />
            Add Filter
          </DropdownMenuItem>

          {/* Add Row */}
          {!readonly && (
            <DropdownMenuItem onClick={onAddRow}>
              <Plus className="h-4 w-4 mr-2" />
              Add Row
            </DropdownMenuItem>
          )}

          <DropdownMenuSeparator />

          {/* Presets submenu */}
          <DropdownMenuSub>
            <DropdownMenuSubTrigger>
              <BookmarkPlus className="h-4 w-4 mr-2" />
              Presets
              {presets.length > 0 && (
                <span className="ml-auto text-xs text-muted-foreground">
                  {presets.length}
                </span>
              )}
            </DropdownMenuSubTrigger>
            <DropdownMenuSubContent className="w-64 max-h-80 overflow-y-auto">
              <DropdownMenuItem
                onClick={() => setSaveDialogOpen(true)}
                disabled={!canSavePreset}
              >
                <Save className="h-4 w-4 mr-2" />
                Save current as preset
              </DropdownMenuItem>

              {presets.length > 0 && (
                <>
                  <DropdownMenuSeparator />
                  {presets.map((preset) => (
                    <DropdownMenuItem
                      key={preset.id}
                      className="flex items-center justify-between group"
                      onClick={() => onLoadPreset(preset.filters, preset.sort)}
                    >
                      <div className="flex items-center gap-2 flex-1 min-w-0">
                        <FolderOpen className="h-4 w-4 shrink-0" />
                        <div className="min-w-0">
                          <div className="font-medium truncate">{preset.name}</div>
                          <div className="text-xs text-muted-foreground">
                            {formatPresetSummary(preset)}
                          </div>
                        </div>
                      </div>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6 opacity-0 group-hover:opacity-100 shrink-0"
                        onClick={(e) => {
                          e.stopPropagation();
                          handleDeletePreset(preset.id);
                        }}
                      >
                        <Trash2 className="h-3 w-3 text-destructive" />
                      </Button>
                    </DropdownMenuItem>
                  ))}
                </>
              )}

              {presets.length === 0 && (
                <>
                  <DropdownMenuSeparator />
                  <div className="px-2 py-4 text-sm text-center text-muted-foreground">
                    No saved presets yet.
                    <br />
                    Add filters and save them for quick access.
                  </div>
                </>
              )}
            </DropdownMenuSubContent>
          </DropdownMenuSub>

          {/* Unit-specific items (only in By Unit view) */}
          {unitList && (
            <>
              <DropdownMenuSeparator />

              {/* Filter by Unit submenu - multi-select */}
              <DropdownMenuSub>
                <DropdownMenuSubTrigger disabled={unitDataLoading}>
                  <Filter className="h-4 w-4 mr-2" />
                  Filter by Unit
                  {unitBadgeText && (
                    <Badge variant="secondary" className="ml-auto h-5 px-1.5 text-xs max-w-[100px] truncate">
                      {unitBadgeText}
                    </Badge>
                  )}
                </DropdownMenuSubTrigger>
                <DropdownMenuSubContent className="p-0" style={{ maxHeight: 'min(500px, 70vh)' }}>
                  <MultiSelectFilterContent
                    items={unitList}
                    selected={selectedUnits || []}
                    onChange={(units) => onUnitChange?.(units)}
                    searchPlaceholder="Search units..."
                    itemLabel="unit"
                  />
                </DropdownMenuSubContent>
              </DropdownMenuSub>

              {/* Merge Units */}
              <DropdownMenuItem onClick={onMergeUnits} disabled={mergeDisabled}>
                <Merge className="h-4 w-4 mr-2" />
                Merge Units
              </DropdownMenuItem>

              {/* Suggestions toggle */}
              <DropdownMenuCheckboxItem
                checked={showSuggestions}
                onCheckedChange={() => onToggleSuggestions?.()}
              >
                Merge Suggestions
                {(suggestionsCount ?? 0) > 0 && (
                  <Badge variant="destructive" className="ml-auto h-5 px-1.5 text-xs">
                    {suggestionsCount}
                  </Badge>
                )}
              </DropdownMenuCheckboxItem>
            </>
          )}

          {/* Document filter (only in standard/by-document view when documents exist) */}
          {documentList && documentList.length > 0 && onDocumentChange && (
            <>
              {!unitList && <DropdownMenuSeparator />}
              <DropdownMenuSub>
                <DropdownMenuSubTrigger disabled={documentDataLoading}>
                  <FileText className="h-4 w-4 mr-2" />
                  Filter by Document
                  {docBadgeText && (
                    <Badge variant="secondary" className="ml-auto h-5 px-1.5 text-xs max-w-[100px] truncate">
                      {docBadgeText}
                    </Badge>
                  )}
                </DropdownMenuSubTrigger>
                <DropdownMenuSubContent className="p-0" style={{ maxHeight: 'min(500px, 70vh)' }}>
                  <MultiSelectFilterContent
                    items={documentList}
                    selected={selectedDocuments || []}
                    onChange={onDocumentChange}
                    searchPlaceholder="Search documents..."
                    itemLabel="document"
                  />
                </DropdownMenuSubContent>
              </DropdownMenuSub>
            </>
          )}

          <DropdownMenuSeparator />

          {/* Column Fullness submenu */}
          <DropdownMenuSub>
            <DropdownMenuSubTrigger>
              <BarChart3 className="h-4 w-4 mr-2" />
              Completeness
              {fullnessThreshold > 0 && (
                <Badge variant="secondary" className="ml-auto h-5 px-1.5 text-xs">
                  {fullnessThreshold}%
                </Badge>
              )}
            </DropdownMenuSubTrigger>
            <DropdownMenuSubContent className="w-72">
              <div className="p-3 space-y-2" onSelect={(e: any) => e.preventDefault()}>
                <Label htmlFor="fullness-slider-menu" className="text-sm font-medium">
                  Minimum Completeness
                </Label>
                <p className="text-xs text-muted-foreground">
                  Hide columns with fewer than {fullnessThreshold}% non-empty values
                </p>
              </div>

              <DropdownMenuSeparator />

              {/* Slider - prevent menu close on interaction */}
              <div
                className="p-3 space-y-3"
                onPointerDown={(e) => e.stopPropagation()}
                onClick={(e) => e.stopPropagation()}
                onKeyDown={(e) => e.stopPropagation()}
              >
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">{fullnessThreshold}%</span>
                </div>
                <input
                  id="fullness-slider-menu"
                  type="range"
                  min="0"
                  max="100"
                  step="5"
                  value={fullnessThreshold}
                  onChange={(e) => onFullnessChange(parseInt(e.target.value, 10))}
                  className="w-full h-2 bg-muted rounded-lg appearance-none cursor-pointer accent-primary"
                />
                <div className="flex items-center justify-between text-xs text-muted-foreground">
                  <span>0%</span>
                  <span>50%</span>
                  <span>100%</span>
                </div>
              </div>

              <DropdownMenuSeparator />

              {/* Preset buttons */}
              <div
                className="p-3 space-y-2"
                onClick={(e) => e.stopPropagation()}
              >
                <Label className="text-xs text-muted-foreground">Quick presets</Label>
                <div className="flex gap-1">
                  {FULLNESS_PRESETS.map((preset) => (
                    <Button
                      key={preset}
                      variant={fullnessThreshold === preset ? 'default' : 'outline'}
                      size="sm"
                      className={cn(
                        'flex-1 text-xs h-7',
                        fullnessThreshold === preset && 'pointer-events-none'
                      )}
                      onClick={() => onFullnessChange(preset)}
                    >
                      {preset === 0 ? 'All' : `${preset}%`}
                    </Button>
                  ))}
                </div>
              </div>

              <DropdownMenuSeparator />

              <div className="p-3 space-y-1">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Columns meeting threshold:</span>
                  <span className="font-medium">
                    {visibleColumnsCount} / {totalColumnsCount}
                  </span>
                </div>
                {hiddenByFullnessCount > 0 && (
                  <p className="text-xs text-orange-600 dark:text-orange-400">
                    {hiddenByFullnessCount} column{hiddenByFullnessCount !== 1 ? 's' : ''} hidden (below {fullnessThreshold}% completeness)
                  </p>
                )}
              </div>
            </DropdownMenuSubContent>
          </DropdownMenuSub>

          {/* Show/Hide Columns submenu */}
          <DropdownMenuSub>
            <DropdownMenuSubTrigger>
              <Columns3 className="h-4 w-4 mr-2" />
              Show/Hide Columns
              {hiddenCount > 0 && (
                <Badge variant="secondary" className="ml-auto h-5 px-1.5 text-xs">
                  {visibleCount}/{columns.length}
                </Badge>
              )}
            </DropdownMenuSubTrigger>
            <DropdownMenuSubContent className="w-56">
              {/* Quick actions */}
              <div
                className="flex gap-1 p-2"
                onClick={(e) => e.stopPropagation()}
              >
                <Button
                  variant="outline"
                  size="sm"
                  className="flex-1 gap-1"
                  onClick={onShowAll}
                  disabled={hiddenCount === 0}
                >
                  <Eye className="h-3 w-3" />
                  Show All
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  className="flex-1 gap-1"
                  onClick={onHideAll}
                  disabled={visibleCount === 0}
                >
                  <EyeOff className="h-3 w-3" />
                  Hide All
                </Button>
              </div>

              <DropdownMenuSeparator />

              {/* Column list */}
              <ScrollArea className="h-[300px]">
                <div className="p-2 space-y-1">
                  {columns.map((column) => {
                    const isVisible = visibility[column] !== false;
                    const displayName = formatColumnName(column);

                    return (
                      <div
                        key={column}
                        className="flex items-center space-x-2 py-1 px-1 rounded hover:bg-muted/50 cursor-pointer"
                        onClick={(e) => {
                          e.stopPropagation();
                          onToggleColumn(column);
                        }}
                      >
                        <Checkbox
                          id={`menu-col-vis-${column}`}
                          checked={isVisible}
                          onCheckedChange={() => onToggleColumn(column)}
                        />
                        <Label
                          htmlFor={`menu-col-vis-${column}`}
                          className="flex-1 text-sm cursor-pointer truncate"
                        >
                          {displayName}
                        </Label>
                        {column.startsWith('_') && (
                          <Badge variant="outline" className="text-xs h-5">
                            Meta
                          </Badge>
                        )}
                      </div>
                    );
                  })}
                </div>
              </ScrollArea>

              <DropdownMenuSeparator />

              <div className="p-2 text-xs text-center text-muted-foreground">
                {visibleCount} of {columns.length} columns visible
              </div>
            </DropdownMenuSubContent>
          </DropdownMenuSub>
        </DropdownMenuContent>
      </DropdownMenu>

      {/* Save Preset Dialog */}
      <Dialog open={saveDialogOpen} onOpenChange={setSaveDialogOpen}>
        <DialogContent className="sm:max-w-[400px]">
          <DialogHeader>
            <DialogTitle>Save Filter Preset</DialogTitle>
            <DialogDescription>
              Save your current filters and sort settings for quick access later.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="preset-name-menu">Name</Label>
              <Input
                id="preset-name-menu"
                value={newPresetName}
                onChange={(e) => setNewPresetName(e.target.value)}
                placeholder="My Filter Preset"
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="preset-description-menu">Description (optional)</Label>
              <Input
                id="preset-description-menu"
                value={newPresetDescription}
                onChange={(e) => setNewPresetDescription(e.target.value)}
                placeholder="Filter for active items..."
              />
            </div>
            <div className="text-sm text-muted-foreground">
              This preset will include:
              <ul className="list-disc list-inside mt-1">
                {currentFilters.length > 0 && (
                  <li>{currentFilters.length} filter{currentFilters.length > 1 ? 's' : ''}</li>
                )}
                {currentSort.length > 0 && (
                  <li>{currentSort.length} sort column{currentSort.length > 1 ? 's' : ''}</li>
                )}
              </ul>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setSaveDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleSavePreset} disabled={!newPresetName.trim()}>
              <Save className="h-4 w-4 mr-2" />
              Save Preset
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Preset?</AlertDialogTitle>
            <AlertDialogDescription>
              This action cannot be undone. The preset will be permanently deleted.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
};

export default TableOptionsMenu;
