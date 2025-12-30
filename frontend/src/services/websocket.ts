import { WebSocketMessage } from '../types';
import { getBackendBaseUrl } from './api';

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
  private messageHandlers: MessageHandler[] = [];
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private baseUrl = getWebSocketBaseUrl();

  connect(sessionId: string, endpoint: 'progress' | 'logs' = 'progress') {
    if (this.socket) {
      this.disconnect();
    }

    const wsUrl = `${this.baseUrl}/${endpoint}/${sessionId}`;
    this.socket = new WebSocket(wsUrl);

    this.socket.onopen = () => {
      console.log('WebSocket connected');
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
      console.log('WebSocket disconnected:', event.code, event.reason);
      this.socket = null;

      // Notify handlers that connection is lost
      this.messageHandlers.forEach(handler =>
        handler({ type: 'disconnected', message: 'WebSocket disconnected' })
      );

      // Attempt to reconnect if not a normal closure
      if (event.code !== 1000 && this.reconnectAttempts < this.maxReconnectAttempts) {
        this.scheduleReconnect(sessionId, endpoint);
      }
    };

    this.socket.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
  }

  private startPingInterval() {
    const pingInterval = setInterval(() => {
      if (this.socket && this.socket.readyState === WebSocket.OPEN) {
        this.socket.send(JSON.stringify({ type: 'ping' }));
      } else {
        clearInterval(pingInterval);
      }
    }, 15000); // Ping every 15 seconds (reduced from 30s to help keep connection alive)
  }

  private scheduleReconnect(sessionId: string, endpoint: 'progress' | 'logs') {
    this.reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);

    console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);

    // Notify handlers about reconnection attempt
    this.messageHandlers.forEach(handler =>
      handler({
        type: 'reconnecting',
        message: `Reconnecting in ${Math.round(delay / 1000)}s (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`,
      })
    );

    this.reconnectTimeout = setTimeout(() => {
      this.connect(sessionId, endpoint);
    }, delay);
  }

  disconnect() {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

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