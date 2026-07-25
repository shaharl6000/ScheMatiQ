import { useCallback, useState } from 'react';

// Owns chat-panel width and the pointer-driven divider drag interaction.
// Parent: Workspace (index.tsx).
const DEFAULT_CHAT_WIDTH = 380;

// Below this the chat panel is effectively collapsed; above (near the viewport
// width) the sheet is collapsed. We persist a custom divider width across
// sessions, but never *restore* a fully collapsed side: reopening a workspace
// always lands in split view so the user can't get stuck with one pane hidden.
const MIN_SPLIT_WIDTH = 56;

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

  const startDividerDrag = useCallback((event: React.PointerEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDraggingDivider(true);

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
  };
}
