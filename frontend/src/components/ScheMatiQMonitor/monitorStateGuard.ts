export type ProcessingState =
  | 'idle'
  | 'starting'
  | 'schema'
  | 'observation_unit_review'
  | 'extraction'
  | 'completed'
  | 'stopped'
  | 'error';

export type ReviewEntryMode = 'initial' | 'mid_run' | 'post_run' | null;

export interface PolledMonitorContext {
  currentState: ProcessingState;
  polledStatus?: string;
  schemaCompleted?: boolean;
  columnsDiscovered?: number;
  schemaProgressComplete: boolean;
  extractionProgressComplete: boolean;
  stopRequested: boolean;
  activeReextractionId: string | null;
  reviewEntryMode: ReviewEntryMode;
}

export interface PolledMonitorTransition {
  nextState: ProcessingState;
  isExternalRediscoveryReset: boolean;
  clearStopRequested?: boolean;
}

/**
 * Determine the next monitor `ProcessingState` from a polled `/schematiq/status`
 * response while enforcing state-machine invariants:
 * - Never let a stale `'processing'` poll regress a stop-requested state.
 * - Never let a polled `'completed'` / `'stopped'` / `'processing'` status kick
 *   the user out of a manually opened mid-run or post-run Observation Unit editor.
 * - Never let `/schematiq/status`'s `'completed'` or `'stopped'` status overwrite
 *   an active re-extraction run (`activeReextractionId !== null`), even if Stop
 *   was requested on the re-extraction.
 * - Clear `stopRequested` once the backend reaches a terminal polled status
 *   (`'stopped'`, `'completed'`, or `'error'`) so later external runs aren't blocked
 *   when WebSocket events were missed.
 */
export function resolvePolledMonitorState(
  ctx: PolledMonitorContext,
): PolledMonitorTransition {
  const {
    currentState,
    polledStatus,
    schemaCompleted,
    columnsDiscovered,
    schemaProgressComplete,
    extractionProgressComplete,
    stopRequested,
    activeReextractionId,
    reviewEntryMode,
  } = ctx;

  if (!polledStatus) {
    return { nextState: currentState, isExternalRediscoveryReset: false };
  }

  const inManualReviewEditor =
    currentState === 'observation_unit_review' &&
    (reviewEntryMode === 'mid_run' || reviewEntryMode === 'post_run');

  const monitorThinksRunFinished =
    currentState === 'completed' ||
    currentState === 'stopped' ||
    schemaProgressComplete ||
    extractionProgressComplete;

  if (
    polledStatus === 'processing' &&
    schemaCompleted === false &&
    (columnsDiscovered ?? 0) === 0 &&
    !stopRequested &&
    !inManualReviewEditor &&
    monitorThinksRunFinished
  ) {
    return { nextState: 'schema', isExternalRediscoveryReset: true };
  }

  if (polledStatus === 'error') {
    return {
      nextState: 'error',
      isExternalRediscoveryReset: false,
      ...(stopRequested ? { clearStopRequested: true } : {}),
    };
  }

  // While the user is actively editing the observation unit in mid_run or
  // post_run mode, do not let background status polls close the review panel.
  if (inManualReviewEditor) {
    return { nextState: currentState, isExternalRediscoveryReset: false };
  }

  // While a re-extraction operation is active, `/schematiq/status` still reports
  // the prior pipeline's 'completed' or 'stopped' status; keep 'extraction'
  // (must be checked BEFORE `stopRequested` so stopping a re-extraction does
  // not prematurely flip to 'completed' from the prior pipeline status).
  if (
    activeReextractionId &&
    (polledStatus === 'completed' || polledStatus === 'stopped')
  ) {
    return { nextState: currentState, isExternalRediscoveryReset: false };
  }

  // While a stop request is in flight, ignore non-terminal polled states so the
  // UI does not flip back to 'starting' / 'extraction' before the backend settles.
  if (stopRequested) {
    if (polledStatus === 'stopped' || polledStatus === 'completed') {
      return {
        nextState: polledStatus,
        isExternalRediscoveryReset: false,
        clearStopRequested: true,
      };
    }
    return { nextState: currentState, isExternalRediscoveryReset: false };
  }

  if (polledStatus === 'observation_unit_review') {
    return {
      nextState: 'observation_unit_review',
      isExternalRediscoveryReset: false,
    };
  }

  if (polledStatus === 'completed') {
    return { nextState: 'completed', isExternalRediscoveryReset: false };
  }

  if (polledStatus === 'stopped') {
    return { nextState: 'stopped', isExternalRediscoveryReset: false };
  }

  if (polledStatus === 'processing') {
    if (schemaCompleted && !schemaProgressComplete) {
      return { nextState: 'extraction', isExternalRediscoveryReset: false };
    }
    if (currentState === 'idle') {
      return { nextState: 'starting', isExternalRediscoveryReset: false };
    }
  }

  return { nextState: currentState, isExternalRediscoveryReset: false };
}
