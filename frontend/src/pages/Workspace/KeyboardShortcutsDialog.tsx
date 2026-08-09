// Read-only reference for the shortcuts that work inside the sheet.
// Parent: Workspace (index.tsx).
//
// Every entry below is a shortcut the grid actually handles: the formatting
// three are registered by SpreadsheetSurface against Handsontable's
// ShortcutManager 'grid' context, the rest are Handsontable built-ins enabled by
// the copyPaste/undo/columnSorting settings on the table, plus the
// column-header Delete handled by beforeKeyDown. Nothing aspirational is
// listed -- an inaccurate reference is worse than none.

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';

type Shortcut = { keys: string[]; label: string };

const GROUPS: { title: string; items: Shortcut[] }[] = [
  {
    title: 'Formatting',
    items: [
      { keys: ['Mod', 'B'], label: 'Bold the selected cells' },
      { keys: ['Mod', 'I'], label: 'Italicise the selected cells' },
      { keys: ['Mod', 'U'], label: 'Underline the selected cells' },
    ],
  },
  {
    title: 'Editing',
    items: [
      { keys: ['Mod', 'Z'], label: 'Undo' },
      { keys: ['Mod', 'Y'], label: 'Redo' },
      { keys: ['Mod', 'C'], label: 'Copy' },
      { keys: ['Mod', 'X'], label: 'Cut' },
      { keys: ['Mod', 'V'], label: 'Paste' },
      { keys: ['Enter'], label: 'Edit the selected cell' },
      { keys: ['Esc'], label: 'Cancel the current edit' },
      { keys: ['Delete'], label: 'Clear the selected cells' },
      { keys: ['Delete'], label: 'Delete the columns, when whole columns are selected by their headers' },
    ],
  },
  {
    title: 'Selection',
    items: [
      { keys: ['Mod', 'A'], label: 'Select the whole sheet' },
      { keys: ['↑', '↓', '←', '→'], label: 'Move between cells' },
      { keys: ['Tab'], label: 'Move to the next cell' },
      { keys: ['Shift', 'Tab'], label: 'Move to the previous cell' },
    ],
  },
];

function Keys({ keys, isMac }: { keys: string[]; isMac: boolean }) {
  return (
    <span className="flex shrink-0 items-center gap-1">
      {keys.map((key, index) => (
        <span key={`${key}-${index}`} className="flex items-center gap-1">
          {index > 0 && <span className="text-muted-foreground">+</span>}
          <kbd className="rounded border bg-muted px-1.5 py-0.5 font-mono text-[11px] leading-none">
            {key === 'Mod' ? (isMac ? '⌘' : 'Ctrl') : key}
          </kbd>
        </span>
      ))}
    </span>
  );
}

export function KeyboardShortcutsDialog({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const isMac =
    typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform || '');

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[80vh] overflow-y-auto sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Keyboard shortcuts</DialogTitle>
          <DialogDescription>
            These work while the sheet has focus. Click a cell first.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-5">
          {GROUPS.map((group) => (
            <div key={group.title}>
              <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                {group.title}
              </h3>
              <ul className="space-y-1.5">
                {group.items.map((item, index) => (
                  <li
                    key={`${group.title}-${index}`}
                    className="flex items-start justify-between gap-4 text-sm"
                  >
                    <span className="text-muted-foreground">{item.label}</span>
                    <Keys keys={item.keys} isMac={isMac} />
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  );
}
