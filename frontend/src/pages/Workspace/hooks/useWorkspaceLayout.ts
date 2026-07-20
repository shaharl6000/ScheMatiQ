import { useCallback, useState } from 'react';

// Owns chat-panel width and the pointer-driven divider drag interaction.
// Parent: Workspace (index.tsx).
export function useWorkspaceLayout() {
  const [chatWidth, setChatWidth] = useState(() => {
    const saved = Number(localStorage.getItem('workspace.chatWidth'));
    return Number.isFinite(saved) ? saved : 380;
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
