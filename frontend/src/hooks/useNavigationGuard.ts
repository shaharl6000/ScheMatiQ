import { useState, useEffect, useCallback, useRef } from 'react';

export interface NavigationBlocker {
  state: 'unblocked' | 'blocked';
  proceed: () => void;
  reset: () => void;
  requestNavigation: (fn: () => void) => void;
}

/**
 * Guards against accidental navigation (browser back, tab close, and programmatic).
 * Works with BrowserRouter (no data router required).
 *
 * When shouldBlock is true:
 *   - popstate (browser back/forward) is intercepted via a sentinel history entry
 *   - beforeunload fires the native "Leave site?" prompt on tab close/refresh
 *   - requestNavigation(fn) stores the fn and shows the blocker dialog
 *
 * proceed() — executes the pending navigation (programmatic or browser back)
 * reset()   — cancels and stays on the page
 */
export function useNavigationGuard(shouldBlock: boolean): NavigationBlocker {
  const [blocked, setBlocked] = useState(false);
  const pendingNavRef = useRef<(() => void) | null>(null);
  const ignoreNextPop = useRef(false);
  // When proceed() handles a programmatic nav, skip cleanup's history.go(-1)
  // because the component will unmount and the sentinel is no longer our concern.
  const skipCleanupRef = useRef(false);

  const proceed = useCallback(() => {
    setBlocked(false);
    const nav = pendingNavRef.current;
    pendingNavRef.current = null;
    if (nav) {
      // Programmatic case — skip sentinel cleanup (component is about to unmount)
      skipCleanupRef.current = true;
      nav();
    } else {
      // Popstate case — go back past our re-push AND sentinel entry
      ignoreNextPop.current = true;
      window.history.go(-2);
    }
  }, []);

  const reset = useCallback(() => {
    setBlocked(false);
    pendingNavRef.current = null;
  }, []);

  // For programmatic navigation (in-app buttons, header link)
  const requestNavigation = useCallback((fn: () => void) => {
    if (shouldBlock) {
      pendingNavRef.current = fn;
      setBlocked(true);
    } else {
      fn();
    }
  }, [shouldBlock]);

  // Intercept popstate (browser back/forward)
  useEffect(() => {
    if (!shouldBlock) return;

    const handler = () => {
      if (ignoreNextPop.current) {
        ignoreNextPop.current = false;
        return;
      }
      // Push the current URL back to undo the navigation
      window.history.pushState(null, '', window.location.href);
      pendingNavRef.current = null; // popstate, not programmatic
      setBlocked(true);
    };

    // Push an extra history entry so the first back press triggers popstate
    // on our entry rather than leaving the page.
    window.history.pushState(null, '', window.location.href);

    window.addEventListener('popstate', handler);
    return () => {
      window.removeEventListener('popstate', handler);
      if (skipCleanupRef.current) {
        // proceed() already handled navigation; don't undo it with go(-1).
        skipCleanupRef.current = false;
        return;
      }
      // Clean up the extra entry we pushed (go back silently).
      ignoreNextPop.current = true;
      window.history.go(-1);
    };
  }, [shouldBlock]);

  // Block tab close / refresh
  useEffect(() => {
    if (!shouldBlock) return;
    const handler = (e: BeforeUnloadEvent) => {
      e.preventDefault();
    };
    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, [shouldBlock]);

  return { state: blocked ? 'blocked' : 'unblocked', proceed, reset, requestNavigation };
}
