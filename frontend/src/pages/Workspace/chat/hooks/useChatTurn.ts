// Owns the lifecycle of a single chat "turn" -- the window between the user
// sending something and the agent's reply landing. Centralising it here keeps
// the busy flag and the cancel wiring in one place (instead of duplicated in
// every ChatPanel handler) and gives future turn-level features -- retry,
// streaming, per-turn state -- a single seam to hang off.
import { useCallback, useRef, useState } from 'react';

export interface ChatTurnController {
  // True while a turn is in flight. Drives the composer's disabled state and
  // the Send/Stop button swap.
  busy: boolean;
  // Runs one turn. `work` receives an AbortSignal to thread into its request so
  // Stop can abort the wait. Resolves to the work's value, or null when the
  // user stopped the turn -- callers treat null as "stopped" and skip their
  // success handling rather than surfacing it as an error.
  runTurn: <T>(work: (signal: AbortSignal) => Promise<T>) => Promise<T | null>;
  // Aborts the in-flight turn, if any. A no-op when idle.
  stop: () => void;
}

export function useChatTurn(): ChatTurnController {
  const [busy, setBusy] = useState(false);
  // Only one turn runs at a time (the composer is disabled while busy), so a
  // single controller ref is enough to route Stop to the active request.
  const controllerRef = useRef<AbortController | null>(null);

  const stop = useCallback(() => {
    controllerRef.current?.abort();
  }, []);

  const runTurn = useCallback(
    async <T>(work: (signal: AbortSignal) => Promise<T>): Promise<T | null> => {
      const controller = new AbortController();
      controllerRef.current = controller;
      setBusy(true);
      try {
        return await work(controller.signal);
      } catch (error) {
        // A user-initiated Stop surfaces as a rejected request; swallow it and
        // report "stopped" via null so callers don't render it as a failure.
        // Any other rejection is a real error and propagates to the caller.
        if (controller.signal.aborted) return null;
        throw error;
      } finally {
        // Guard against a fast re-send having already installed a newer
        // controller: only the turn that owns the current ref clears state.
        if (controllerRef.current === controller) {
          controllerRef.current = null;
          setBusy(false);
        }
      }
    },
    [],
  );

  return { busy, runTurn, stop };
}
