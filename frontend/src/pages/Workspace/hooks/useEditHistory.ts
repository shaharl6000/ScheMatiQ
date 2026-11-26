import { useCallback, useMemo, useRef } from 'react';

/**
 * A single reversible action, following the Command pattern: it carries whatever
 * it needs to reverse and to re-apply itself. Both may be async because
 * value-edit commands round-trip to the server; format commands run
 * synchronously against client state.
 */
export interface EditCommand {
  undo: () => void | Promise<void>;
  redo: () => void | Promise<void>;
}

export interface EditHistory {
  /** Record a command after its forward action has been applied. Clears redo. */
  push: (command: EditCommand) => void;
  /** Reverse the most recent command (LIFO). Returns false if nothing to undo. */
  undo: () => boolean;
  /** Re-apply the most recently undone command. Returns false if nothing to redo. */
  redo: () => boolean;
  /** Drop both stacks (e.g. when the underlying data is replaced). */
  clear: () => void;
}

const DEFAULT_LIMIT = 100;

/**
 * Minimal, headless undo/redo stack. Both stacks live in refs so recording,
 * undoing, or redoing never triggers a re-render; the feature is driven purely
 * from the Ctrl/Cmd+Z and Ctrl/Cmd+Y (or Ctrl/Cmd+Shift+Z) shortcuts, so no
 * reactive "can undo" state is needed.
 *
 * Deliberately generic: it knows nothing about formats, cells, or the grid.
 * Each command owns its own inverse and forward action, so heterogeneous
 * actions (client-only format changes and server-backed value edits) share one
 * ordered history.
 */
export function useEditHistory(
  limit: number = DEFAULT_LIMIT,
  onDepthChange?: (undoDepth: number, redoDepth: number) => void,
): EditHistory {
  const undoRef = useRef<EditCommand[]>([]);
  const redoRef = useRef<EditCommand[]>([]);
  const onDepthChangeRef = useRef(onDepthChange);
  onDepthChangeRef.current = onDepthChange;

  const notify = useCallback(() => {
    onDepthChangeRef.current?.(undoRef.current.length, redoRef.current.length);
  }, []);

  const push = useCallback((command: EditCommand) => {
    undoRef.current.push(command);
    if (undoRef.current.length > limit) {
      undoRef.current.shift();
    }
    // A fresh action invalidates the redo branch, matching every editor's
    // behaviour (you cannot redo once you have diverged).
    redoRef.current = [];
    notify();
  }, [limit, notify]);

  const undo = useCallback(() => {
    const command = undoRef.current.pop();
    if (!command) return false;
    redoRef.current.push(command);
    notify();
    void command.undo();
    return true;
  }, [notify]);

  const redo = useCallback(() => {
    const command = redoRef.current.pop();
    if (!command) return false;
    undoRef.current.push(command);
    notify();
    void command.redo();
    return true;
  }, [notify]);

  const clear = useCallback(() => {
    undoRef.current = [];
    redoRef.current = [];
    notify();
  }, [notify]);

  return useMemo(() => ({ push, undo, redo, clear }), [push, undo, redo, clear]);
}
