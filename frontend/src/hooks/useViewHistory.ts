import { useState, useEffect, useCallback, useRef } from 'react';
import type { ViewMode } from '../types/unit';
import type { NavigationBlocker } from './useNavigationGuard';

export interface ViewHistoryEntry {
  tab: string;
  viewMode: ViewMode;
}

export interface ViewHistoryResult {
  /** Record the target view for a new history entry (call with the NEW tab/viewMode). */
  pushViewState: (entry: ViewHistoryEntry) => void;
  /** Go back one view state. Returns true if a view was restored, false if at base. */
  goBack: () => boolean;
  /** NavigationBlocker for the processing-exit dialog. */
  blocker: NavigationBlocker;
  /** Guard for programmatic navigation (header link, etc.). */
  requestNavigation: (fn: () => void) => void;
}

/**
 * Manages view-state history (tab + viewMode) for the Visualize page by storing
 * each entry in `history.state`. Both browser back AND forward correctly restore
 * the view because the state lives in the browser's own history stack.
 *
 * Also handles exit-blocking when `shouldBlockExit` is true (e.g. processing).
 */
export function useViewHistory(
  currentTab: string,
  currentViewMode: ViewMode,
  shouldBlockExit: boolean,
  onRestore: (entry: ViewHistoryEntry) => void,
): ViewHistoryResult {
  const depthRef = useRef(0);
  const isRestoringRef = useRef(false);
  const ignoreNextPopRef = useRef(false);
  const skipCleanupRef = useRef(false);

  // --- Exit-blocking state ---
  const [blocked, setBlocked] = useState(false);
  const pendingNavRef = useRef<(() => void) | null>(null);

  // Mark position 0 as our base on mount so we can distinguish
  // "back to initial view" from "navigating away from the page" in popstate.
  useEffect(() => {
    const existing = window.history.state || {};
    window.history.replaceState(
      { ...existing, viewHistoryBase: true, tab: currentTab, viewMode: currentViewMode },
      '',
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // mount only

  const proceed = useCallback(() => {
    setBlocked(false);
    const nav = pendingNavRef.current;
    pendingNavRef.current = null;
    if (nav) {
      // Programmatic case — skip cleanup (component is about to unmount)
      skipCleanupRef.current = true;
      nav();
    } else {
      // popstate case — go back past our re-push
      ignoreNextPopRef.current = true;
      window.history.go(-1);
    }
  }, []);

  const reset = useCallback(() => {
    setBlocked(false);
    pendingNavRef.current = null;
  }, []);

  const blocker: NavigationBlocker = {
    state: blocked ? 'blocked' : 'unblocked',
    proceed,
    reset,
    requestNavigation: () => {}, // replaced below
  };

  // --- Push a view state into browser history ---
  const pushViewState = useCallback((entry: ViewHistoryEntry) => {
    if (isRestoringRef.current) return; // don't push while restoring
    // Dedupe against current position
    const s = window.history.state;
    if (s && s.tab === entry.tab && s.viewMode === entry.viewMode) return;

    depthRef.current++;
    window.history.pushState(
      { viewHistory: true, tab: entry.tab, viewMode: entry.viewMode, depth: depthRef.current },
      '',
    );
  }, []);

  // --- Go back one view state (for in-app arrow) ---
  const goBack = useCallback((): boolean => {
    if (depthRef.current > 0) {
      depthRef.current--; // optimistic — popstate handler will set the exact value
      window.history.back(); // popstate handler will restore from history.state
      return true;
    }
    return false; // no view history, caller should leave page
  }, []);

  // --- Programmatic navigation guard (for header link, etc.) ---
  const requestNavigation = useCallback((fn: () => void) => {
    if (shouldBlockExit) {
      pendingNavRef.current = fn;
      setBlocked(true);
    } else {
      fn();
    }
  }, [shouldBlockExit]);

  // Assign requestNavigation to blocker
  blocker.requestNavigation = requestNavigation;

  // --- Popstate handler ---
  useEffect(() => {
    const handler = (event: PopStateEvent) => {
      if (ignoreNextPopRef.current) {
        ignoreNextPopRef.current = false;
        return;
      }

      const state = event.state;

      // Back or forward into a view history entry — restore it
      if (state?.viewHistory && state.tab != null && state.viewMode != null) {
        depthRef.current = state.depth ?? 0;
        isRestoringRef.current = true;
        onRestore({ tab: state.tab, viewMode: state.viewMode });
        setTimeout(() => { isRestoringRef.current = false; }, 0);
        return;
      }

      // Back to the base entry (initial view on this page)
      if (state?.viewHistoryBase) {
        depthRef.current = 0;
        isRestoringRef.current = true;
        onRestore({ tab: state.tab, viewMode: state.viewMode });
        setTimeout(() => { isRestoringRef.current = false; }, 0);
        return;
      }

      // No viewHistory state — this is navigation leaving the page
      if (shouldBlockExit) {
        window.history.pushState(null, '', window.location.href);
        pendingNavRef.current = null;
        setBlocked(true);
        return;
      }

      // Otherwise let the browser navigate away naturally
    };

    window.addEventListener('popstate', handler);
    return () => {
      window.removeEventListener('popstate', handler);
    };
  }, [shouldBlockExit, onRestore]);

  // --- beforeunload (tab close / refresh) ---
  useEffect(() => {
    if (!shouldBlockExit) return;
    const handler = (e: BeforeUnloadEvent) => { e.preventDefault(); };
    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, [shouldBlockExit]);

  // --- Cleanup: remove leftover history entries on unmount ---
  useEffect(() => {
    return () => {
      if (skipCleanupRef.current) {
        skipCleanupRef.current = false;
        return;
      }
      const depth = depthRef.current;
      if (depth > 0) {
        depthRef.current = 0;
        window.history.go(-depth);
      }
    };
  }, []);

  return { pushViewState, goBack, blocker, requestNavigation };
}
