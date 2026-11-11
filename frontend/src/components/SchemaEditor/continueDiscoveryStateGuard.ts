export type ContinueDiscoveryPhase =
  | 'idle'
  | 'discovering'
  | 'awaiting_confirmation'
  | 'extracting';

export interface ContinueDiscoveryActivityState {
  phase: ContinueDiscoveryPhase;
  operationId: string | null;
  dialogDismissedDuringDiscovery: boolean;
}

export type ContinueDiscoveryLifecycleEvent =
  | { type: 'dialog_opened' }
  | { type: 'dialog_closed' }
  | { type: 'discovery_started'; operationId?: string | null; fromDialog?: boolean }
  | {
      type: 'discovery_completed';
      newColumnCount: number;
      operationId?: string | null;
      fromDialog?: boolean;
    }
  | { type: 'discovery_stopped' }
  | { type: 'discovery_failed' }
  | {
      type: 'columns_confirmed';
      extractValues: boolean;
      operationId?: string | null;
    }
  | { type: 'extraction_started'; operationId?: string | null }
  | { type: 'extraction_completed' }
  | { type: 'extraction_stopped' }
  | { type: 'extraction_failed' }
  | {
      type: 'status_polled';
      status?: string;
      phase?: string;
      newColumnCount?: number;
      notFound?: boolean;
    };

export const INITIAL_CONTINUE_DISCOVERY_STATE: ContinueDiscoveryActivityState = {
  phase: 'idle',
  operationId: null,
  dialogDismissedDuringDiscovery: false,
};

/**
 * Whether Continue Discovery (or its follow-up column confirmation / incremental
 * value extraction) is currently active and should guard page navigation.
 */
export function isContinueDiscoveryActive(
  state: ContinueDiscoveryActivityState,
): boolean {
  return state.phase !== 'idle';
}

/**
 * Extract the number of newly discovered columns from a `continue_discovery_completed`
 * WebSocket payload (`new_columns`), treating a missing or empty array as 0.
 */
export function extractDiscoveredColumnCount(
  data?: Record<string, unknown> | null,
): number {
  if (!data) {
    return 0;
  }
  return Array.isArray(data.new_columns) ? data.new_columns.length : 0;
}

/**
 * Map a WebSocket message (`type` + `data`) to a `ContinueDiscoveryLifecycleEvent`,
 * or `null` when the message does not affect Continue Discovery activity state.
 */
export function continueDiscoveryEventFromWebSocket(
  messageType: string,
  data?: Record<string, unknown> | null,
): ContinueDiscoveryLifecycleEvent | null {
  const operationId =
    typeof data?.operation_id === 'string' ? data.operation_id : null;

  switch (messageType) {
    case 'continue_discovery_started':
      return { type: 'discovery_started', operationId };
    case 'continue_discovery_completed':
      return {
        type: 'discovery_completed',
        newColumnCount: extractDiscoveredColumnCount(data),
        operationId,
      };
    case 'continue_discovery_stopped':
      return { type: 'discovery_stopped' };
    case 'continue_discovery_failed':
      return { type: 'discovery_failed' };
    case 'incremental_extraction_started':
      return { type: 'extraction_started', operationId };
    case 'incremental_extraction_completed':
      return { type: 'extraction_completed' };
    case 'incremental_extraction_failed':
      return { type: 'extraction_failed' };
    default:
      return null;
  }
}

/**
 * Pure state reducer for Continue Discovery activity tracking in `Visualize.tsx`:
 * - Enters `'discovering'` when Continue Discovery starts.
 * - Resets to `'idle'` immediately if `continue_discovery_completed` finds 0 new columns
 *   (or if the user already dismissed the dialog during discovery).
 * - Holds `'awaiting_confirmation'` while the user reviews discovered columns, and resets
 *   to `'idle'` if the user chooses "Add Columns Only" (`extractValues = false`) or
 *   closes/cancels the dialog without starting extraction.
 * - Transitions to `'extracting'` when incremental extraction starts, and resets to `'idle'`
 *   on any terminal extraction or stop/failure event.
 */
export function reduceContinueDiscoveryState(
  state: ContinueDiscoveryActivityState,
  event: ContinueDiscoveryLifecycleEvent,
): ContinueDiscoveryActivityState {
  switch (event.type) {
    case 'dialog_opened':
      return state;

    case 'dialog_closed': {
      if (state.phase === 'awaiting_confirmation') {
        return INITIAL_CONTINUE_DISCOVERY_STATE;
      }
      if (state.phase === 'discovering') {
        return {
          ...state,
          dialogDismissedDuringDiscovery: true,
        };
      }
      return state;
    }

    case 'discovery_started':
      return {
        phase: 'discovering',
        operationId: event.operationId ?? state.operationId,
        dialogDismissedDuringDiscovery: false,
      };

    case 'discovery_completed': {
      // Never let a late discovery completion overwrite active incremental extraction.
      if (state.phase === 'extracting') {
        return state;
      }

      if (event.newColumnCount <= 0) {
        return INITIAL_CONTINUE_DISCOVERY_STATE;
      }

      if (state.dialogDismissedDuringDiscovery && !event.fromDialog) {
        return INITIAL_CONTINUE_DISCOVERY_STATE;
      }

      if (
        state.phase === 'discovering' ||
        state.phase === 'awaiting_confirmation' ||
        event.fromDialog
      ) {
        return {
          phase: 'awaiting_confirmation',
          operationId: event.operationId ?? state.operationId,
          dialogDismissedDuringDiscovery: false,
        };
      }

      // Ignore delayed/duplicate completion messages arriving after state already reset to idle.
      return INITIAL_CONTINUE_DISCOVERY_STATE;
    }

    case 'columns_confirmed': {
      if (!event.extractValues) {
        return INITIAL_CONTINUE_DISCOVERY_STATE;
      }
      return {
        phase: 'extracting',
        operationId: event.operationId ?? state.operationId,
        dialogDismissedDuringDiscovery: false,
      };
    }

    case 'extraction_started':
      return {
        phase: 'extracting',
        operationId: event.operationId ?? state.operationId,
        dialogDismissedDuringDiscovery: false,
      };

    case 'discovery_stopped':
    case 'discovery_failed':
    case 'extraction_completed':
    case 'extraction_stopped':
    case 'extraction_failed':
      return INITIAL_CONTINUE_DISCOVERY_STATE;

    case 'status_polled': {
      if (event.notFound) {
        return INITIAL_CONTINUE_DISCOVERY_STATE;
      }

      const { status, phase, newColumnCount = 0 } = event;
      if (!status) {
        return state;
      }

      if (status === 'failed' || status === 'stopped') {
        return INITIAL_CONTINUE_DISCOVERY_STATE;
      }

      if (status === 'completed') {
        if (phase === 'extraction') {
          return INITIAL_CONTINUE_DISCOVERY_STATE;
        }
        // While incremental extraction is running, ignore a stale poll snapshot
        // from the prior discovery phase.
        if (state.phase === 'extracting') {
          return state;
        }
        if (newColumnCount <= 0 || state.dialogDismissedDuringDiscovery) {
          return INITIAL_CONTINUE_DISCOVERY_STATE;
        }
        if (
          state.phase === 'discovering' ||
          state.phase === 'awaiting_confirmation'
        ) {
          return {
            ...state,
            phase: 'awaiting_confirmation',
          };
        }
        return INITIAL_CONTINUE_DISCOVERY_STATE;
      }

      if (
        (status === 'running' || status === 'starting') &&
        phase === 'extraction'
      ) {
        return {
          ...state,
          phase: 'extracting',
          dialogDismissedDuringDiscovery: false,
        };
      }

      return state;
    }

    default:
      return state;
  }
}
