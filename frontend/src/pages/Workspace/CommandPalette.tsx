// Linear-style Cmd/Ctrl+K Command Palette for quick navigation, column jumps,
// document jumps, and workspace actions.
// Parent: Workspace (index.tsx).

import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { CornerDownLeft, Search } from 'lucide-react';

import { Dialog, DialogContent, DialogTitle } from '@/components/ui/dialog';

export interface CommandPaletteItem {
  id: string;
  label: string;
  subtitle?: string;
  group: 'Actions' | 'Sheets' | 'Columns' | 'Documents';
  keywords?: string;
  shortcut?: string;
  icon?: ReactNode;
  disabled?: boolean;
  onSelect: () => void;
}

const GROUP_ORDER: CommandPaletteItem['group'][] = ['Actions', 'Sheets', 'Columns', 'Documents'];

export function CommandPalette({
  open,
  onOpenChange,
  items,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  items: CommandPaletteItem[];
}) {
  const [query, setQuery] = useState('');
  const [activeIdx, setActiveIdx] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const listRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) {
      setQuery('');
      setActiveIdx(0);
      return;
    }
    const frame = window.requestAnimationFrame(() => inputRef.current?.focus());
    return () => window.cancelAnimationFrame(frame);
  }, [open]);

  const filtered = useMemo(() => {
    const enabled = items.filter((item) => !item.disabled);
    const q = query.trim().toLowerCase();
    if (!q) return enabled;
    const tokens = q.split(/\s+/).filter(Boolean);
    return enabled.filter((item) => {
      const haystack = `${item.label} ${item.subtitle ?? ''} ${item.group} ${item.keywords ?? ''}`.toLowerCase();
      return tokens.every((tok) => haystack.includes(tok));
    });
  }, [items, query]);

  const grouped = useMemo(() => {
    const out: Array<{ group: CommandPaletteItem['group']; items: Array<{ item: CommandPaletteItem; flatIndex: number }> }> = [];
    let flatIndex = 0;
    for (const group of GROUP_ORDER) {
      const matching = filtered.filter((item) => item.group === group);
      if (matching.length === 0) continue;
      const baseIndex = flatIndex;
      flatIndex += matching.length;
      out.push({
        group,
        items: matching.map((item, offset) => ({ item, flatIndex: baseIndex + offset })),
      });
    }
    return out;
  }, [filtered]);

  const flatOrder = useMemo(
    () => grouped.flatMap((g) => g.items.map((x) => x.item)),
    [grouped],
  );

  useEffect(() => {
    setActiveIdx(0);
  }, [query]);

  useEffect(() => {
    const activeEl = listRef.current?.querySelector<HTMLElement>('[data-active="true"]');
    activeEl?.scrollIntoView({ block: 'nearest' });
  }, [activeIdx]);

  const runSelected = (item?: CommandPaletteItem) => {
    if (!item) return;
    onOpenChange(false);
    // Defer slightly so Dialog unmounts and focus restores cleanly before action runs.
    window.requestAnimationFrame(() => item.onSelect());
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="workspace-cmd-dialog max-w-xl gap-0 overflow-hidden p-0">
        <DialogTitle className="sr-only">Command Palette</DialogTitle>
        <div className="flex items-center gap-2.5 border-b px-3.5 py-2.5">
          <Search className="h-4 w-4 shrink-0 text-muted-foreground" />
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'ArrowDown') {
                e.preventDefault();
                setActiveIdx((i) => (flatOrder.length ? (i + 1) % flatOrder.length : 0));
              } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                setActiveIdx((i) => (flatOrder.length ? (i - 1 + flatOrder.length) % flatOrder.length : 0));
              } else if (e.key === 'Enter') {
                e.preventDefault();
                runSelected(flatOrder[activeIdx]);
              }
            }}
            placeholder="Type a command, column, sheet, or document…"
            className="flex-1 bg-transparent text-sm text-foreground outline-none placeholder:text-muted-foreground/70"
          />
          <kbd className="rounded border bg-muted px-1.5 py-0.5 font-mono text-[10px] text-muted-foreground">
            ESC
          </kbd>
        </div>

        <div ref={listRef} className="max-h-[360px] overflow-y-auto py-1.5">
          {grouped.length === 0 ? (
            <div className="px-4 py-8 text-center text-sm text-muted-foreground">
              No matching commands or items.
            </div>
          ) : (
            grouped.map(({ group, items: groupItems }) => (
              <div key={group} className="mb-1 last:mb-0">
                <div className="px-3.5 py-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                  {group}
                </div>
                {groupItems.map(({ item, flatIndex }) => {
                  const isActive = flatIndex === activeIdx;
                  return (
                    <button
                      key={item.id}
                      type="button"
                      data-active={isActive ? 'true' : undefined}
                      onMouseEnter={() => setActiveIdx(flatIndex)}
                      onClick={() => runSelected(item)}
                      className={`flex w-full items-center gap-2.5 px-3.5 py-2 text-left text-sm transition-colors ${
                        isActive ? 'bg-accent text-accent-foreground' : 'text-foreground hover:bg-accent/50'
                      }`}
                    >
                      <span className="flex h-4 w-4 shrink-0 items-center justify-center text-muted-foreground">
                        {item.icon}
                      </span>
                      <span className="min-w-0 flex-1">
                        <span className="block truncate font-medium">{item.label}</span>
                        {item.subtitle && (
                          <span className="block truncate text-xs text-muted-foreground">
                            {item.subtitle}
                          </span>
                        )}
                      </span>
                      {item.shortcut ? (
                        <kbd className="shrink-0 rounded border bg-muted/70 px-1.5 py-0.5 font-mono text-[10px] text-muted-foreground">
                          {item.shortcut}
                        </kbd>
                      ) : isActive ? (
                        <CornerDownLeft className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                      ) : null}
                    </button>
                  );
                })}
              </div>
            ))
          )}
        </div>

        <div className="flex items-center justify-between border-t bg-muted/30 px-3.5 py-1.5 text-[11px] text-muted-foreground">
          <span>↑↓ navigate · ↵ select · esc close</span>
          <span>{flatOrder.length} item{flatOrder.length === 1 ? '' : 's'}</span>
        </div>
      </DialogContent>
    </Dialog>
  );
}
