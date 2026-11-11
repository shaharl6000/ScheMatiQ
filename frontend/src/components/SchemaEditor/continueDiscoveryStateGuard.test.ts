import {
  INITIAL_CONTINUE_DISCOVERY_STATE,
  continueDiscoveryEventFromWebSocket,
  extractDiscoveredColumnCount,
  isContinueDiscoveryActive,
  reduceContinueDiscoveryState,
  type ContinueDiscoveryActivityState,
} from './continueDiscoveryStateGuard';

function applyWs(
  state: ContinueDiscoveryActivityState,
  messageType: string,
  data?: Record<string, unknown> | null,
): ContinueDiscoveryActivityState {
  const event = continueDiscoveryEventFromWebSocket(messageType, data);
  return event ? reduceContinueDiscoveryState(state, event) : state;
}

describe('continueDiscoveryStateGuard', () => {
  it('starts inactive in idle phase', () => {
    expect(isContinueDiscoveryActive(INITIAL_CONTINUE_DISCOVERY_STATE)).toBe(false);
    expect(INITIAL_CONTINUE_DISCOVERY_STATE.phase).toBe('idle');
  });

  it('activates when continue_discovery_started arrives and clears when 0 new columns are discovered', () => {
    const started = applyWs(
      INITIAL_CONTINUE_DISCOVERY_STATE,
      'continue_discovery_started',
      { operation_id: 'op-zero' },
    );
    expect(started.phase).toBe('discovering');
    expect(isContinueDiscoveryActive(started)).toBe(true);

    const completedZero = applyWs(started, 'continue_discovery_completed', {
      operation_id: 'op-zero',
      new_columns: [],
      message: 'No new columns discovered',
    });
    expect(completedZero).toEqual(INITIAL_CONTINUE_DISCOVERY_STATE);
    expect(isContinueDiscoveryActive(completedZero)).toBe(false);
  });

  it('clears when the completion payload has no new_columns', () => {
    const started = reduceContinueDiscoveryState(
      INITIAL_CONTINUE_DISCOVERY_STATE,
      { type: 'discovery_started', operationId: 'op-none', fromDialog: true },
    );

    const missing = applyWs(started, 'continue_discovery_completed', {
      operation_id: 'op-none',
    });
    expect(isContinueDiscoveryActive(missing)).toBe(false);
    expect(extractDiscoveredColumnCount({ new_columns: [{ name: 'c1' }] })).toBe(1);
    expect(extractDiscoveredColumnCount(null)).toBe(0);
  });

  it('stays active during column review when new columns are found, and clears on "Add Columns Only" (extractValues = false)', () => {
    let state = reduceContinueDiscoveryState(
      INITIAL_CONTINUE_DISCOVERY_STATE,
      { type: 'discovery_started', operationId: 'op-add-only', fromDialog: true },
    );
    expect(isContinueDiscoveryActive(state)).toBe(true);

    state = applyWs(state, 'continue_discovery_completed', {
      operation_id: 'op-add-only',
      new_columns: [{ name: 'col_a' }, { name: 'col_b' }],
    });
    expect(state.phase).toBe('awaiting_confirmation');
    expect(isContinueDiscoveryActive(state)).toBe(true);

    state = reduceContinueDiscoveryState(state, {
      type: 'columns_confirmed',
      extractValues: false,
      operationId: 'op-add-only',
    });
    expect(state).toEqual(INITIAL_CONTINUE_DISCOVERY_STATE);
    expect(isContinueDiscoveryActive(state)).toBe(false);
  });

  it('clears when user cancels or closes the dialog from column review without starting extraction', () => {
    let state = applyWs(
      INITIAL_CONTINUE_DISCOVERY_STATE,
      'continue_discovery_started',
      { operation_id: 'op-cancel' },
    );
    state = reduceContinueDiscoveryState(state, {
      type: 'discovery_completed',
      newColumnCount: 2,
      operationId: 'op-cancel',
      fromDialog: true,
    });
    expect(state.phase).toBe('awaiting_confirmation');
    expect(isContinueDiscoveryActive(state)).toBe(true);

    state = reduceContinueDiscoveryState(state, { type: 'dialog_closed' });
    expect(state).toEqual(INITIAL_CONTINUE_DISCOVERY_STATE);
    expect(isContinueDiscoveryActive(state)).toBe(false);
  });

  it('ignores a delayed continue_discovery_completed WebSocket message after dialog already resolved to idle', () => {
    let state = reduceContinueDiscoveryState(
      INITIAL_CONTINUE_DISCOVERY_STATE,
      { type: 'discovery_started', operationId: 'op-race', fromDialog: true },
    );
    state = reduceContinueDiscoveryState(state, {
      type: 'discovery_completed',
      newColumnCount: 1,
      operationId: 'op-race',
      fromDialog: true,
    });
    state = reduceContinueDiscoveryState(state, {
      type: 'columns_confirmed',
      extractValues: false,
      operationId: 'op-race',
    });
    expect(isContinueDiscoveryActive(state)).toBe(false);

    // Delayed WebSocket event arrives after user already chose Add Columns Only
    state = applyWs(state, 'continue_discovery_completed', {
      operation_id: 'op-race',
      new_columns: [{ name: 'col_a' }],
    });
    expect(isContinueDiscoveryActive(state)).toBe(false);
  });

  it('clears when discovery finishes if the dialog was already dismissed mid-discovery', () => {
    let state = reduceContinueDiscoveryState(
      INITIAL_CONTINUE_DISCOVERY_STATE,
      { type: 'discovery_started', operationId: 'op-dismissed', fromDialog: true },
    );
    state = reduceContinueDiscoveryState(state, { type: 'dialog_closed' });
    // Backend discovery is still running until terminal event arrives
    expect(state.phase).toBe('discovering');
    expect(isContinueDiscoveryActive(state)).toBe(true);

    // Discovery completes with new columns, but dialog is closed so no review step can run
    state = applyWs(state, 'continue_discovery_completed', {
      operation_id: 'op-dismissed',
      new_columns: [{ name: 'col_a' }],
    });
    expect(state).toEqual(INITIAL_CONTINUE_DISCOVERY_STATE);
    expect(isContinueDiscoveryActive(state)).toBe(false);
  });

  it('stays active throughout incremental extraction (even if dialog closes) and clears on completion, stop, or failure', () => {
    const startExtracting = (): ContinueDiscoveryActivityState => {
      let st = applyWs(
        INITIAL_CONTINUE_DISCOVERY_STATE,
        'continue_discovery_started',
        { operation_id: 'op-ext' },
      );
      st = applyWs(st, 'continue_discovery_completed', {
        operation_id: 'op-ext',
        new_columns: [{ name: 'col_a' }],
      });
      st = reduceContinueDiscoveryState(st, {
        type: 'columns_confirmed',
        extractValues: true,
        operationId: 'op-ext',
      });
      return st;
    };

    const extracting = startExtracting();
    expect(extracting.phase).toBe('extracting');
    expect(isContinueDiscoveryActive(extracting)).toBe(true);

    // Closing dialog during active extraction does not clear continueDiscoveryActive prematurely
    const afterDialogClosed = reduceContinueDiscoveryState(extracting, {
      type: 'dialog_closed',
    });
    expect(afterDialogClosed.phase).toBe('extracting');
    expect(isContinueDiscoveryActive(afterDialogClosed)).toBe(true);

    // Terminal incremental extraction events clear state
    expect(
      isContinueDiscoveryActive(
        applyWs(afterDialogClosed, 'incremental_extraction_completed', {
          operation_id: 'op-ext',
        }),
      ),
    ).toBe(false);

    expect(
      isContinueDiscoveryActive(
        applyWs(extracting, 'incremental_extraction_failed', {
          operation_id: 'op-ext',
        }),
      ),
    ).toBe(false);

    // Backend stop_operation broadcasts continue_discovery_stopped even in extraction phase
    expect(
      isContinueDiscoveryActive(
        applyWs(extracting, 'continue_discovery_stopped', {
          operation_id: 'op-ext',
          phase: 'extraction',
        }),
      ),
    ).toBe(false);
  });

  it('clears on continue_discovery_stopped and continue_discovery_failed during discovery', () => {
    const discovering = applyWs(
      INITIAL_CONTINUE_DISCOVERY_STATE,
      'continue_discovery_started',
      { operation_id: 'op-err' },
    );

    expect(
      isContinueDiscoveryActive(
        applyWs(discovering, 'continue_discovery_stopped', { operation_id: 'op-err' }),
      ),
    ).toBe(false);

    expect(
      isContinueDiscoveryActive(
        applyWs(discovering, 'continue_discovery_failed', {
          operation_id: 'op-err',
          error: 'LLM failure',
        }),
      ),
    ).toBe(false);
  });

  it('handles fallback status polling safely without regressing active extraction', () => {
    const discovering = reduceContinueDiscoveryState(
      INITIAL_CONTINUE_DISCOVERY_STATE,
      { type: 'discovery_started', operationId: 'op-poll', fromDialog: true },
    );

    // 0 new columns via fallback poll clears state
    const zeroPolled = reduceContinueDiscoveryState(discovering, {
      type: 'status_polled',
      status: 'completed',
      phase: 'discovery',
      newColumnCount: 0,
    });
    expect(isContinueDiscoveryActive(zeroPolled)).toBe(false);

    // >0 new columns via fallback poll enters awaiting_confirmation
    const reviewPolled = reduceContinueDiscoveryState(discovering, {
      type: 'status_polled',
      status: 'completed',
      phase: 'discovery',
      newColumnCount: 3,
    });
    expect(reviewPolled.phase).toBe('awaiting_confirmation');

    // Stale discovery completed poll does not regress extracting phase
    const extracting = reduceContinueDiscoveryState(reviewPolled, {
      type: 'columns_confirmed',
      extractValues: true,
      operationId: 'op-poll',
    });
    const stalePoll = reduceContinueDiscoveryState(extracting, {
      type: 'status_polled',
      status: 'completed',
      phase: 'discovery',
      newColumnCount: 3,
    });
    expect(stalePoll.phase).toBe('extracting');

    // Extraction completed or 404 clears state
    const completedExtraction = reduceContinueDiscoveryState(extracting, {
      type: 'status_polled',
      status: 'completed',
      phase: 'extraction',
    });
    expect(isContinueDiscoveryActive(completedExtraction)).toBe(false);

    const notFoundPoll = reduceContinueDiscoveryState(extracting, {
      type: 'status_polled',
      notFound: true,
    });
    expect(isContinueDiscoveryActive(notFoundPoll)).toBe(false);
  });
});
