import { useCallback, useEffect, useState } from 'react';

// Owns chat-panel width and the pointer-driven divider drag interaction.
// Parent: Workspace (index.tsx).
const DEFAULT_CHAT_WIDTH = 380;

// Below this the chat panel is effectively collapsed; above (near the viewport
// width) the sheet is collapsed. We persist a custom divider width across
// sessions, but never *restore* a fully collapsed side: reopening a workspace
// always lands in split view so the user can't get stuck with one pane hidden.
const MIN_SPLIT_WIDTH = 56;

// Must match the `max-width: 900px` breakpoint in Workspace.css, where the body
// stops being a sheet|chat split and stacks the two panes vertically. Below it
// the pixel chat width is meaningless, so the collapse thresholds derived from
// it have to be switched off.
const STACKED_QUERY = '(max-width: 900px)';

export function useWorkspaceLayout() {
  const [chatWidth, setChatWidth] = useState(() => {
    const saved = Number(localStorage.getItem('workspace.chatWidth'));
    if (!Number.isFinite(saved)) return DEFAULT_CHAT_WIDTH;
    const sheetHiddenThreshold = window.innerWidth - 80;
    if (saved < MIN_SPLIT_WIDTH || saved >= sheetHiddenThreshold) {
      return DEFAULT_CHAT_WIDTH;
    }
    return saved;
  });
  const [isDraggingDivider, setIsDraggingDivider] = useState(false);
  // The collapse thresholds below compare chatWidth against the viewport, so
  // they need the viewport as reactive state. Reading window.innerWidth during
  // render left them stale until an unrelated state change forced a re-render.
  const [viewportWidth, setViewportWidth] = useState(() => window.innerWidth);
  const [isStacked, setIsStacked] = useState(
    () => window.matchMedia(STACKED_QUERY).matches,
  );
  // Explicit close/reopen via a button, independent of chatWidth so it also
  // works while stacked (where pixel width is meaningless). Not persisted --
  // same "never restore a workspace with a pane stuck hidden" rule as above.
  const [isChatCollapsed, setIsChatCollapsed] = useState(false);

  useEffect(() => {
    const media = window.matchMedia(STACKED_QUERY);
    const sync = () => {
      setViewportWidth(window.innerWidth);
      setIsStacked(media.matches);
    };
    sync();
    window.addEventListener('resize', sync);
    media.addEventListener('change', sync);
    return () => {
      window.removeEventListener('resize', sync);
      media.removeEventListener('change', sync);
    };
  }, []);

  const startDividerDrag = useCallback((event: React.PointerEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDraggingDivider(true);
    // A manual close leaves the pane invisible regardless of chatWidth, so a
    // drag on the (still-rendered) divider would otherwise resize a pane the
    // user can't see. Dragging always means "I want this pane back."
    setIsChatCollapsed(false);

    const handleMove = (moveEvent: PointerEvent) => {
      const nextWidth = window.innerWidth - moveEvent.clientX;
      const maxWidth = Math.max(24, window.innerWidth - 24);
      const clamped = Math.min(maxWidth, Math.max(0, nextWidth));
      const snapped = clamped < 56 ? 0 : clamped > window.innerWidth - 80 ? window.innerWidth : clamped;
      setChatWidth(snapped);
      localStorage.setItem('workspace.chatWidth', String(snapped));
    };

    const handleUp = () => {
      setIsDraggingDivider(false);
      window.removeEventListener('pointermove', handleMove);
      window.removeEventListener('pointerup', handleUp);
    };

    window.addEventListener('pointermove', handleMove);
    window.addEventListener('pointerup', handleUp);
  }, []);

  return {
    chatWidth,
    setChatWidth,
    isDraggingDivider,
    startDividerDrag,
    viewportWidth,
    isStacked,
    isChatCollapsed,
    setIsChatCollapsed,
  };
}
