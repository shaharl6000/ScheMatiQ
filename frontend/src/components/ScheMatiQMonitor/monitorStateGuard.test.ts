import {
  resolvePolledMonitorState,
  type PolledMonitorContext,
} from './monitorStateGuard';

const baseCtx: PolledMonitorContext = {
  currentState: 'idle',
  polledStatus: undefined,
  schemaCompleted: false,
  columnsDiscovered: 0,
  schemaProgressComplete: false,
  extractionProgressComplete: false,
  stopRequested: false,
  activeReextractionId: null,
  reviewEntryMode: null,
};

describe('resolvePolledMonitorState', () => {
  it('does not let a stale processing poll regress a stop-requested state back to extraction', () => {
    const res = resolvePolledMonitorState({
      ...baseCtx,
      currentState: 'stopped',
      polledStatus: 'processing',
      schemaCompleted: true,
      schemaProgressComplete: false,
      stopRequested: true,
    });
    expect(res).toEqual({
      nextState: 'stopped',
      isExternalRediscoveryReset: false,
    });
  });

  it('does not kick the user out of mid_run or post_run observation_unit_review on polled status updates', () => {
    const postRunRes = resolvePolledMonitorState({
      ...baseCtx,
      currentState: 'observation_unit_review',
      polledStatus: 'completed',
      reviewEntryMode: 'post_run',
    });
    expect(postRunRes.nextState).toBe('observation_unit_review');

    const midRunRes = resolvePolledMonitorState({
      ...baseCtx,
      currentState: 'observation_unit_review',
      polledStatus: 'processing',
      schemaCompleted: true,
      schemaProgressComplete: false,
      reviewEntryMode: 'mid_run',
    });
    expect(midRunRes.nextState).toBe('observation_unit_review');
  });

  it('keeps extraction state when activeReextractionId is set even if /schematiq/status reports completed or stopped', () => {
    const res = resolvePolledMonitorState({
      ...baseCtx,
      currentState: 'extraction',
      polledStatus: 'completed',
      activeReextractionId: 'op-1234',
    });
    expect(res).toEqual({
      nextState: 'extraction',
      isExternalRediscoveryReset: false,
    });
  });

  it('detects an externally triggered schema rediscovery reset after a completed run', () => {
    const res = resolvePolledMonitorState({
      ...baseCtx,
      currentState: 'completed',
      polledStatus: 'processing',
      schemaCompleted: false,
      columnsDiscovered: 0,
      schemaProgressComplete: true,
      extractionProgressComplete: true,
    });
    expect(res).toEqual({
      nextState: 'schema',
      isExternalRediscoveryReset: true,
    });
  });

  it('transitions from schema to extraction when schemaCompleted becomes true during processing', () => {
    const res = resolvePolledMonitorState({
      ...baseCtx,
      currentState: 'schema',
      polledStatus: 'processing',
      schemaCompleted: true,
      schemaProgressComplete: false,
    });
    expect(res).toEqual({
      nextState: 'extraction',
      isExternalRediscoveryReset: false,
    });
  });
});
