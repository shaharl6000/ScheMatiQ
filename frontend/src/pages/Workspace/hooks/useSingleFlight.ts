// Wraps an async action so at most one call runs at a time. Calls that arrive
// while one is running collapse into a single trailing call that starts after
// it, with their arguments combined by `merge`. Every caller gets a promise
// that settles once a run started at or after its request has finished, so no
// request is dropped and none can be overtaken by an older result.
// The returned function is stable; it always invokes the latest `action`.
// Errors from `action` are swallowed here (the action reports its own).
// Parent: Workspace (index.tsx), for the row-data refresh.

import { useCallback, useEffect, useRef } from 'react';

type Queued<T> = { promise: Promise<void>; arg: T | undefined };

export function useSingleFlight<T>(
  action: (arg?: T) => Promise<void>,
  merge: (queued: T | undefined, incoming: T | undefined) => T,
): (arg?: T) => Promise<void> {
  const actionRef = useRef(action);
  const mergeRef = useRef(merge);
  useEffect(() => {
    actionRef.current = action;
    mergeRef.current = merge;
  }, [action, merge]);

  const inFlightRef = useRef<Promise<void> | null>(null);
  const queuedRef = useRef<Queued<T> | null>(null);

  return useCallback((arg?: T): Promise<void> => {
    const start = (startArg: T | undefined): Promise<void> => {
      const run: Promise<void> = actionRef.current(startArg)
        .catch(() => undefined)
        .finally(() => {
          if (inFlightRef.current === run) inFlightRef.current = null;
        });
      inFlightRef.current = run;
      return run;
    };

    const inFlight = inFlightRef.current;
    if (!inFlight) return start(arg);

    const queued = queuedRef.current;
    if (queued) {
      queued.arg = mergeRef.current(queued.arg, arg);
      return queued.promise;
    }

    const next: Queued<T> = { promise: Promise.resolve(), arg };
    next.promise = inFlight.then(() => {
      queuedRef.current = null;
      return start(next.arg);
    });
    queuedRef.current = next;
    return next.promise;
  }, []);
}
