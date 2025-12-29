import { WebSocketMessage } from '../types';

type MessageHandler = (message: WebSocketMessage) => void;

class WebSocketService {
  private socket: WebSocket | null = null;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private messageHandlers: MessageHandler[] = [];
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private baseUrl = process.env.REACT_APP_WS_BASE || 'ws://localhost:8000/ws';

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
    }, 30000); // Ping every 30 seconds
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