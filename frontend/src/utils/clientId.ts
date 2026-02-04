const CLIENT_ID_KEY = 'qbsd_client_id';

export function getOrCreateClientId(): string {
  if (typeof window === 'undefined') {
    return 'server';
  }

  let clientId = localStorage.getItem(CLIENT_ID_KEY);
  if (!clientId) {
    const randomId = typeof crypto !== 'undefined' && crypto.randomUUID
      ? crypto.randomUUID()
      : `client_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    clientId = randomId;
    localStorage.setItem(CLIENT_ID_KEY, clientId);
  }

  return clientId;
}

