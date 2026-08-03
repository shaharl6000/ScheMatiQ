import { WebSocketMessage } from '../types';
import { getBackendBaseUrl } from './api';
import { debug } from '@/utils/debug';

type MessageHandler = (message: WebSocketMessage) => void;

// Derive WebSocket URL from the backend base URL
function getWebSocketBaseUrl(): string {
  // Check for explicit WS URL first (build-time)
  if (process.env.REACT_APP_WS_URL) {
    return process.env.REACT_APP_WS_URL + '/ws';
  }

  // Get backend URL and convert to WebSocket protocol
  const backendUrl = getBackendBaseUrl();
  // Convert http(s) to ws(s)
  return backendUrl.replace(/^http/, 'ws') + '/ws';
}

class WebSocketService {
  private socket: WebSocket | null = null;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private pingInterval: NodeJS.Timeout | null = null;
  private messageHandlers: MessageHandler[] = [];
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private baseUrl = getWebSocketBaseUrl();

  // Reference counting so multiple components can share a single socket.
  // The socket is only torn down once the last holder calls disconnect().
  private refCount = 0;
  private activeKey: string | null = null;
  private activeSessionId: string | null = null;
  private activeEndpoint: 'progress' | 'logs' = 'progress';

  constructor() {
    // Auto-recover after the reconnect budget is exhausted. Once the browser
    // regains connectivity or the tab is foregrounded, a still-needed session
    // reconnects instead of staying dead until a page reload. The singleton
    // lives for the page lifetime, so these listeners are never removed.
    if (typeof window !== 'undefined') {
      window.addEventListener('online', this.handleConnectivityRestored);
    }
    if (typeof document !== 'undefined') {
      document.addEventListener('visibilitychange', this.handleConnectivityRestored);
    }
  }

  private handleConnectivityRestored = () => {
    // Only act when a holder still needs the socket and it is fully down.
    if (this.refCount <= 0 || !this.activeSessionId || this.socket) {
      return;
    }
    if (typeof document !== 'undefined' && document.visibilityState === 'hidden') {
      return;
    }
    if (typeof navigator !== 'undefined' && navigator.onLine === false) {
      return;
    }
    // Reset the backoff and cancel any pending timer so recovery is immediate.
    this.reconnectAttempts = 0;
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }
    this.openSocket(this.activeSessionId, this.activeEndpoint);
  };

  connect(sessionId: string, endpoint: 'progress' | 'logs' = 'progress') {
    const key = `${endpoint}/${sessionId}`;

    // Reuse the live socket for the same logical connection.
    if (this.socket && this.activeKey === key) {
      this.refCount += 1;
      if (this.socket.readyState === WebSocket.OPEN) {
        // Notify the just-arrived subscriber. Deferred so that a handler added
        // in the same tick (some callers connect() before addMessageHandler())
        // is registered before this fires.
        setTimeout(() => {
          if (this.socket && this.socket.readyState === WebSocket.OPEN) {
            this.messageHandlers.forEach(handler =>
              handler({ type: 'connected', message: 'WebSocket connected' })
            );
          }
        }, 0);
      }
      return;
    }

    // Switching to a different session/endpoint, or no socket yet.
    if (this.socket && this.activeKey !== key) {
      // Not expected in current usage (single session, 'progress' endpoint only).
      debug.log('WebSocket switching active connection', this.activeKey, '->', key);
      this.closeSocket();
    }

    this.refCount = 1;
    this.openSocket(sessionId, endpoint);
  }

  // Open the socket if it is currently down WITHOUT taking a reference. For
  // callers that already hold a reference (e.g. a mount effect) and only want
  // to guarantee connectivity before kicking off background work.
  ensureConnected(sessionId: string, endpoint: 'progress' | 'logs' = 'progress') {
    if (this.socket) {
      return;
    }
    if (this.refCount === 0) {
      this.refCount = 1;
    }
    this.openSocket(sessionId, endpoint);
  }

  private openSocket(sessionId: string, endpoint: 'progress' | 'logs') {
    this.activeSessionId = sessionId;
    this.activeEndpoint = endpoint;
    this.activeKey = `${endpoint}/${sessionId}`;

    const wsUrl = `${this.baseUrl}/${endpoint}/${sessionId}`;
    this.socket = new WebSocket(wsUrl);

    this.socket.onopen = () => {
      debug.log('WebSocket connected');
      this.reconnectAttempts = 0;

      // Notify handlers that connection is established
      this.messageHandlers.forEach(handler =>
        handler({ type: 'connected', message: 'WebSocket connected' })
      );

      // Send ping to keep connection alive
      this.startPingInterval();
    };

    this.socket.onmessage = (event) => {
      try {
        const message: WebSocketMessage = JSON.parse(event.data);

        // Handle server heartbeats silently (just confirms connection is alive)
        if (message.type === 'heartbeat') {
          return;
        }

        this.messageHandlers.forEach(handler => handler(message));
      } catch (error) {
        console.error('Error parsing WebSocket message:', error);
      }
    };

    this.socket.onclose = (event) => {
      debug.log('WebSocket disconnected:', event.code, event.reason);
      this.socket = null;

      // Notify handlers that connection is lost
      this.messageHandlers.forEach(handler =>
        handler({ type: 'disconnected', message: 'WebSocket disconnected' })
      );

      // Attempt to reconnect only if this was not a normal closure and a holder
      // still needs the connection. Reconnecting reopens the same logical
      // connection; it does not acquire a new reference.
      if (
        event.code !== 1000 &&
        this.refCount > 0 &&
        this.reconnectAttempts < this.maxReconnectAttempts
      ) {
        this.scheduleReconnect();
      }
    };

    this.socket.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
  }

  private startPingInterval() {
    // Clear any interval left over from a previous connection so reconnects do
    // not leak timers. Each open starts exactly one ping loop, tracked on the
    // instance so it can also be cleared from closeSocket().
    this.clearPingInterval();
    this.pingInterval = setInterval(() => {
      if (this.socket && this.socket.readyState === WebSocket.OPEN) {
        this.socket.send(JSON.stringify({ type: 'ping' }));
      } else {
        this.clearPingInterval();
      }
    }, 15000); // Ping every 15 seconds (reduced from 30s to help keep connection alive)
  }

  private clearPingInterval() {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
  }

  private scheduleReconnect() {
    if (!this.activeSessionId) {
      return;
    }
    const sessionId = this.activeSessionId;
    const endpoint = this.activeEndpoint;

    this.reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);

    debug.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);

    // Notify handlers about reconnection attempt
    this.messageHandlers.forEach(handler =>
      handler({
        type: 'reconnecting',
        message: `Reconnecting in ${Math.round(delay / 1000)}s (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`,
      })
    );

    this.reconnectTimeout = setTimeout(() => {
      this.openSocket(sessionId, endpoint);
    }, delay);
  }

  disconnect() {
    if (this.refCount > 0) {
      this.refCount -= 1;
    }

    // Other holders still need the socket; keep it alive.
    if (this.refCount > 0) {
      return;
    }

    this.closeSocket();
  }

  private closeSocket() {
    this.clearPingInterval();

    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    this.reconnectAttempts = 0;
    this.activeKey = null;
    this.activeSessionId = null;

    if (this.socket) {
      this.socket.close(1000, 'Normal closure');
      this.socket = null;
    }
  }

  addMessageHandler(handler: MessageHandler) {
    this.messageHandlers.push(handler);

    // Return cleanup function
    return () => {
      this.messageHandlers = this.messageHandlers.filter(h => h !== handler);
    };
  }

  removeMessageHandler(handler: MessageHandler) {
    this.messageHandlers = this.messageHandlers.filter(h => h !== handler);
  }

  sendMessage(message: any) {
    if (this.socket && this.socket.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify(message));
    } else {
      console.warn('WebSocket not connected, cannot send message');
    }
  }

  isConnected(): boolean {
    return this.socket !== null && this.socket.readyState === WebSocket.OPEN;
  }
}

// Export singleton instance
export const webSocketService = new WebSocketService();

export default webSocketService;
