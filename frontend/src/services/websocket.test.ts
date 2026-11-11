/**
 * Unit tests for the reference-counted WebSocket singleton.
 *
 * jsdom does not provide a WebSocket implementation, so we install a minimal
 * mock on globalThis and reset the module (and its singleton) before each test.
 */

// websocket.ts imports ./api (which pulls in axios, an ESM-only package that
// react-scripts' jest does not transform). We only need getBackendBaseUrl, so
// mock the module to keep this unit test self-contained.
jest.mock('./api', () => ({ getBackendBaseUrl: () => 'http://localhost:8000' }));
// '@/' alias is not mapped in this project's jest config (no tests existed),
// so register debug as a virtual mock.
jest.mock('@/utils/debug', () => ({ debug: { log: jest.fn() } }), { virtual: true });

class MockWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  static instances: MockWebSocket[] = [];

  url: string;
  readyState: number = MockWebSocket.CONNECTING;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: ((event: { code: number; reason?: string }) => void) | null = null;
  onerror: ((error: unknown) => void) | null = null;
  sent: string[] = [];
  closeCalls: Array<{ code?: number; reason?: string }> = [];

  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);
  }

  send(data: string) {
    this.sent.push(data);
  }

  // Called by the service for an intentional close.
  close(code?: number, reason?: string) {
    this.closeCalls.push({ code, reason });
    this.readyState = MockWebSocket.CLOSED;
    if (this.onclose) this.onclose({ code: code ?? 1000, reason });
  }

  // Test helpers
  simulateOpen() {
    this.readyState = MockWebSocket.OPEN;
    if (this.onopen) this.onopen();
  }

  simulateServerClose(code: number) {
    this.readyState = MockWebSocket.CLOSED;
    if (this.onclose) this.onclose({ code });
  }
}

describe('webSocketService reference counting', () => {
  let webSocketService: typeof import('./websocket').webSocketService;

  beforeEach(() => {
    jest.resetModules();
    MockWebSocket.instances = [];
    (globalThis as unknown as { WebSocket: unknown }).WebSocket = MockWebSocket;
    webSocketService = require('./websocket').webSocketService;
  });

  it('reuses a single socket across connect() calls and closes only on the last disconnect()', () => {
    webSocketService.connect('session-1', 'progress');
    webSocketService.connect('session-1', 'progress');

    // Same key -> only one underlying socket.
    expect(MockWebSocket.instances).toHaveLength(1);
    const socket = MockWebSocket.instances[0];
    socket.simulateOpen();

    // First release: two holders -> one remains, socket stays open.
    webSocketService.disconnect();
    expect(socket.closeCalls).toHaveLength(0);
    expect(webSocketService.isConnected()).toBe(true);

    // Last release: socket is torn down.
    webSocketService.disconnect();
    expect(socket.closeCalls).toHaveLength(1);
    expect(socket.closeCalls[0].code).toBe(1000);
    expect(webSocketService.isConnected()).toBe(false);
  });

  it('ensureConnected() does not acquire an additional reference', () => {
    webSocketService.connect('session-1', 'progress');
    MockWebSocket.instances[0].simulateOpen();

    // Socket already live -> no-op, no new socket, no extra ref.
    webSocketService.ensureConnected('session-1', 'progress');
    expect(MockWebSocket.instances).toHaveLength(1);

    // A single disconnect from the single real holder closes it.
    webSocketService.disconnect();
    expect(MockWebSocket.instances[0].closeCalls).toHaveLength(1);
    expect(webSocketService.isConnected()).toBe(false);
  });

  it('reconnects after an abnormal close without changing the reference count', () => {
    jest.useFakeTimers();
    try {
      webSocketService.connect('session-1', 'progress');
      const first = MockWebSocket.instances[0];
      first.simulateOpen();

      // Abnormal close (not code 1000) -> service schedules a reconnect.
      first.simulateServerClose(1006);
      jest.advanceTimersByTime(5000);

      // A new socket was opened by the reconnect path.
      expect(MockWebSocket.instances.length).toBeGreaterThanOrEqual(2);
      const reconnected = MockWebSocket.instances[MockWebSocket.instances.length - 1];
      reconnected.simulateOpen();

      // The reconnect must NOT have bumped the ref count: a single disconnect
      // from the original (single) holder still tears the socket down.
      webSocketService.disconnect();
      expect(reconnected.closeCalls.length).toBeGreaterThanOrEqual(1);
      expect(webSocketService.isConnected()).toBe(false);
    } finally {
      jest.useRealTimers();
    }
  });

  it('preserves reference count when a second holder connects during reconnect backoff', () => {
    jest.useFakeTimers();
    try {
      webSocketService.connect('session-1', 'progress');
      const first = MockWebSocket.instances[0];
      first.simulateOpen();

      // Abnormal close -> socket is null while reconnectTimeout is pending.
      first.simulateServerClose(1006);
      expect(MockWebSocket.instances).toHaveLength(1);

      // Second holder connects for the same session during backoff.
      webSocketService.connect('session-1', 'progress');
      expect(MockWebSocket.instances).toHaveLength(1);

      // Scheduled reconnect fires and opens a single replacement socket.
      jest.advanceTimersByTime(2000);
      expect(MockWebSocket.instances).toHaveLength(2);
      const reconnected = MockWebSocket.instances[1];
      reconnected.simulateOpen();

      // First holder disconnects -> second holder still keeps the socket open.
      webSocketService.disconnect();
      expect(reconnected.closeCalls).toHaveLength(0);
      expect(webSocketService.isConnected()).toBe(true);

      // Second holder disconnects -> socket is closed.
      webSocketService.disconnect();
      expect(reconnected.closeCalls).toHaveLength(1);
      expect(webSocketService.isConnected()).toBe(false);
    } finally {
      jest.useRealTimers();
    }
  });

  it('cancels pending reconnect timer when ensureConnected() or session switch opens a socket early', () => {
    jest.useFakeTimers();
    try {
      webSocketService.connect('session-1', 'progress');
      const first = MockWebSocket.instances[0];
      first.simulateOpen();

      // Abnormal close schedules a 2s reconnect timer.
      first.simulateServerClose(1006);
      expect(MockWebSocket.instances).toHaveLength(1);

      // Caller triggers immediate reconnect before the timer fires.
      webSocketService.ensureConnected('session-1', 'progress');
      expect(MockWebSocket.instances).toHaveLength(2);
      MockWebSocket.instances[1].simulateOpen();

      // Advancing past the original 2s backoff must NOT open a duplicate socket.
      jest.advanceTimersByTime(5000);
      expect(MockWebSocket.instances).toHaveLength(2);

      webSocketService.disconnect();
    } finally {
      jest.useRealTimers();
    }
  });
});

export {};
