import { useState, useCallback, useEffect, useRef } from 'react';

export const MIN_COLUMN_WIDTH = 10;
export const DEFAULT_COLUMN_WIDTH = 80;

interface UseColumnResizeOptions {
  sessionId: string;
}

interface UseColumnResizeReturn {
  columnWidths: Record<string, number>;
  getColumnWidth: (column: string) => number | undefined;
  setColumnWidth: (column: string, width: number) => void;
  resetColumnWidths: () => void;
  handleResizeStart: (e: React.MouseEvent, column: string, currentWidth: number) => void;
  isResizing: boolean;
}

const STORAGE_KEY_PREFIX = 'dataTable_colWidths_';

export function useColumnResize({ sessionId }: UseColumnResizeOptions): UseColumnResizeReturn {
  const storageKey = `${STORAGE_KEY_PREFIX}${sessionId}`;

  const [columnWidths, setColumnWidths] = useState<Record<string, number>>(() => {
    try {
      const stored = localStorage.getItem(storageKey);
      if (stored) {
        return JSON.parse(stored);
      }
    } catch {
      // Ignore localStorage errors
    }
    return {};
  });

  const [isResizing, setIsResizing] = useState(false);
  const resizingRef = useRef<{
    column: string;
    startX: number;
    startWidth: number;
  } | null>(null);
  const cleanupRef = useRef<(() => void) | null>(null);

  // Cleanup listeners on unmount to prevent memory leaks
  useEffect(() => {
    return () => {
      if (cleanupRef.current) {
        cleanupRef.current();
      }
    };
  }, []);

  // Persist to localStorage when state changes
  useEffect(() => {
    try {
      if (Object.keys(columnWidths).length > 0) {
        localStorage.setItem(storageKey, JSON.stringify(columnWidths));
      } else {
        localStorage.removeItem(storageKey);
      }
    } catch {
      // Ignore localStorage errors
    }
  }, [columnWidths, storageKey]);

  const getColumnWidth = useCallback((column: string): number | undefined => {
    return columnWidths[column];
  }, [columnWidths]);

  const setColumnWidth = useCallback((column: string, width: number) => {
    const clampedWidth = Math.max(MIN_COLUMN_WIDTH, width);
    setColumnWidths(prev => ({ ...prev, [column]: clampedWidth }));
  }, []);

  const resetColumnWidths = useCallback(() => {
    setColumnWidths({});
  }, []);

  const handleResizeStart = useCallback((e: React.MouseEvent, column: string, currentWidth: number) => {
    e.preventDefault();
    e.stopPropagation();

    resizingRef.current = {
      column,
      startX: e.clientX,
      startWidth: currentWidth,
    };
    setIsResizing(true);

    // Add global styles during drag
    document.body.style.userSelect = 'none';
    document.body.style.cursor = 'col-resize';

    const handleMouseMove = (moveEvent: MouseEvent) => {
      if (!resizingRef.current) return;
      const { column: col, startX, startWidth } = resizingRef.current;
      const newWidth = Math.max(MIN_COLUMN_WIDTH, startWidth + (moveEvent.clientX - startX));
      setColumnWidths(prev => ({ ...prev, [col]: newWidth }));
    };

    const handleMouseUp = () => {
      resizingRef.current = null;
      setIsResizing(false);
      cleanupRef.current = null;

      // Remove global styles
      document.body.style.userSelect = '';
      document.body.style.cursor = '';

      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };

    // Store cleanup for unmount safety
    cleanupRef.current = handleMouseUp;

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
  }, []);

  return {
    columnWidths,
    getColumnWidth,
    setColumnWidth,
    resetColumnWidths,
    handleResizeStart,
    isResizing,
  };
}
