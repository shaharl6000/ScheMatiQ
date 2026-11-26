// Floating in-sheet Find bar (Cmd/Ctrl+F) replacing window.prompt().
// Supports live incremental search, match count, Prev/Next navigation, and Esc to clear.
// Parent: Workspace (index.tsx).

import { useEffect, useRef } from 'react';
import { ChevronDown, ChevronUp, Search, X } from 'lucide-react';

export interface SearchMatch {
  row: number;
  col: number;
}

export function InlineSearchBar({
  open,
  query,
  matches,
  activeIndex,
  onQueryChange,
  onNext,
  onPrev,
  onClose,
}: {
  open: boolean;
  query: string;
  matches: SearchMatch[];
  activeIndex: number;
  onQueryChange: (value: string) => void;
  onNext: () => void;
  onPrev: () => void;
  onClose: () => void;
}) {
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!open) return;
    // Focus and select existing query on open (including repeated Cmd+F presses).
    const frame = window.requestAnimationFrame(() => {
      inputRef.current?.focus();
      inputRef.current?.select();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [open]);

  if (!open) return null;

  const hasQuery = query.trim().length > 0;
  const countLabel = !hasQuery
    ? ''
    : matches.length === 0
      ? 'No matches'
      : `${Math.min(activeIndex + 1, matches.length)} / ${matches.length}`;

  return (
    <div
      className="workspace-inline-search"
      role="search"
      aria-label="Find in sheet"
    >
      <Search className="h-3.5 w-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
      <input
        ref={inputRef}
        type="text"
        dir="auto"
        value={query}
        onChange={(e) => onQueryChange(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Escape') {
            e.preventDefault();
            e.stopPropagation();
            onClose();
          } else if (e.key === 'Enter') {
            e.preventDefault();
            if (e.shiftKey) onPrev();
            else onNext();
          }
        }}
        placeholder="Find in sheet…"
        className="workspace-inline-search-input"
        aria-label="Find in sheet"
      />
      {countLabel && (
        <span
          className={`workspace-inline-search-count${matches.length === 0 ? ' is-empty' : ''}`}
          aria-live="polite"
        >
          {countLabel}
        </span>
      )}
      <div className="workspace-inline-search-nav">
        <button
          type="button"
          className="workspace-inline-search-btn"
          onClick={onPrev}
          disabled={matches.length === 0}
          title="Previous match (Shift+Enter)"
          aria-label="Previous match"
        >
          <ChevronUp className="h-3.5 w-3.5" />
        </button>
        <button
          type="button"
          className="workspace-inline-search-btn"
          onClick={onNext}
          disabled={matches.length === 0}
          title="Next match (Enter)"
          aria-label="Next match"
        >
          <ChevronDown className="h-3.5 w-3.5" />
        </button>
        <button
          type="button"
          className="workspace-inline-search-btn"
          onClick={onClose}
          title="Close search (Esc)"
          aria-label="Close search"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
    </div>
  );
}
