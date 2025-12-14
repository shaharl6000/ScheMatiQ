import React, { useState, useEffect, useCallback } from 'react';
import { Save, FolderOpen, Trash2, BookmarkPlus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
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
import { FilterPreset, FilterRule, SortColumn, generatePresetId } from './types/filters';

interface FilterPresetsProps {
  sessionId: string;
  currentFilters: FilterRule[];
  currentSort: SortColumn[];
  onLoadPreset: (filters: FilterRule[], sort: SortColumn[]) => void;
}

const STORAGE_KEY_PREFIX = 'dataTable_presets_';

const FilterPresets: React.FC<FilterPresetsProps> = ({
  sessionId,
  currentFilters,
  currentSort,
  onLoadPreset,
}) => {
  const [presets, setPresets] = useState<FilterPreset[]>([]);
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [presetToDelete, setPresetToDelete] = useState<string | null>(null);
  const [newPresetName, setNewPresetName] = useState('');
  const [newPresetDescription, setNewPresetDescription] = useState('');

  const storageKey = `${STORAGE_KEY_PREFIX}${sessionId}`;

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

  // Save presets to localStorage
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

  const handleLoadPreset = (preset: FilterPreset) => {
    onLoadPreset(preset.filters, preset.sort);
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

  const canSave = currentFilters.length > 0 || currentSort.length > 0;

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="sm" className="gap-1">
            <BookmarkPlus className="h-4 w-4" />
            Presets
            {presets.length > 0 && (
              <span className="ml-1 text-xs text-muted-foreground">
                ({presets.length})
              </span>
            )}
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start" className="w-64">
          {/* Save current as preset */}
          <DropdownMenuItem
            onClick={() => setSaveDialogOpen(true)}
            disabled={!canSave}
          >
            <Save className="h-4 w-4 mr-2" />
            Save current as preset
          </DropdownMenuItem>

          {presets.length > 0 && (
            <>
              <DropdownMenuSeparator />

              {/* List of saved presets */}
              {presets.map((preset) => (
                <DropdownMenuItem
                  key={preset.id}
                  className="flex items-center justify-between group"
                  onClick={() => handleLoadPreset(preset)}
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
              <Label htmlFor="preset-name">Name</Label>
              <Input
                id="preset-name"
                value={newPresetName}
                onChange={(e) => setNewPresetName(e.target.value)}
                placeholder="My Filter Preset"
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="preset-description">Description (optional)</Label>
              <Input
                id="preset-description"
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

export default FilterPresets;
