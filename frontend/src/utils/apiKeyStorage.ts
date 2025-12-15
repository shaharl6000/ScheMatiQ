/**
 * API Key Storage Utility
 *
 * Handles encrypted localStorage storage for API keys using Web Crypto API (AES-GCM).
 * Provides obfuscation against casual observation, not full security against determined local attacks.
 */

export type LLMProvider = 'openai' | 'together' | 'gemini';

const STORAGE_KEY_PREFIX = 'qbsd_api_key_';
const ENCRYPTION_KEY_STORAGE = 'qbsd_enc_key';
const GEMINI_KEY_TYPE_STORAGE = 'qbsd_gemini_key_type';

/**
 * Get or create the encryption key for AES-GCM.
 */
async function getOrCreateEncryptionKey(): Promise<CryptoKey> {
  const storedKeyData = localStorage.getItem(ENCRYPTION_KEY_STORAGE);

  if (storedKeyData) {
    try {
      const keyData = JSON.parse(storedKeyData);
      return await crypto.subtle.importKey(
        'jwk',
        keyData,
        { name: 'AES-GCM', length: 256 },
        true,
        ['encrypt', 'decrypt']
      );
    } catch {
      // If import fails, generate new key
      localStorage.removeItem(ENCRYPTION_KEY_STORAGE);
    }
  }

  // Generate new key
  const key = await crypto.subtle.generateKey(
    { name: 'AES-GCM', length: 256 },
    true,
    ['encrypt', 'decrypt']
  );

  // Export and store
  const exportedKey = await crypto.subtle.exportKey('jwk', key);
  localStorage.setItem(ENCRYPTION_KEY_STORAGE, JSON.stringify(exportedKey));

  return key;
}

/**
 * Encrypt and store an API key for a provider.
 */
export async function encryptAndStore(provider: string, apiKey: string): Promise<void> {
  if (!apiKey) {
    clearStoredKey(provider);
    return;
  }

  try {
    const key = await getOrCreateEncryptionKey();
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const encodedData = new TextEncoder().encode(apiKey);

    const encryptedData = await crypto.subtle.encrypt(
      { name: 'AES-GCM', iv },
      key,
      encodedData
    );

    const stored = {
      iv: Array.from(iv),
      data: Array.from(new Uint8Array(encryptedData)),
    };

    localStorage.setItem(`${STORAGE_KEY_PREFIX}${provider}`, JSON.stringify(stored));
  } catch (error) {
    console.error('Failed to encrypt and store API key:', error);
  }
}

/**
 * Retrieve and decrypt an API key for a provider.
 */
export async function retrieveAndDecrypt(provider: string): Promise<string | null> {
  const storedStr = localStorage.getItem(`${STORAGE_KEY_PREFIX}${provider}`);
  if (!storedStr) return null;

  try {
    const key = await getOrCreateEncryptionKey();
    const stored = JSON.parse(storedStr);
    const iv = new Uint8Array(stored.iv);
    const data = new Uint8Array(stored.data);

    const decryptedData = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv },
      key,
      data
    );

    return new TextDecoder().decode(decryptedData);
  } catch (error) {
    console.error('Failed to decrypt API key:', error);
    // Clear corrupted data
    localStorage.removeItem(`${STORAGE_KEY_PREFIX}${provider}`);
    return null;
  }
}

/**
 * Clear a stored API key for a provider.
 */
export function clearStoredKey(provider: string): void {
  localStorage.removeItem(`${STORAGE_KEY_PREFIX}${provider}`);
}

/**
 * Clear all stored API keys.
 */
export function clearAllStoredKeys(): void {
  const keysToRemove: string[] = [];
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (key && key.startsWith(STORAGE_KEY_PREFIX)) {
      keysToRemove.push(key);
    }
  }
  keysToRemove.forEach((key) => localStorage.removeItem(key));
}

/**
 * Store Gemini key type preference.
 */
export function storeGeminiKeyType(keyType: 'single' | 'multi'): void {
  localStorage.setItem(GEMINI_KEY_TYPE_STORAGE, keyType);
}

/**
 * Get stored Gemini key type preference.
 */
export function getGeminiKeyType(): 'single' | 'multi' {
  const stored = localStorage.getItem(GEMINI_KEY_TYPE_STORAGE);
  return (stored === 'single' || stored === 'multi') ? stored : 'single';
}

/**
 * Check if a specific provider has an API key configured.
 * For Gemini, checks both single and multi key types.
 */
export async function hasApiKey(provider: LLMProvider): Promise<boolean> {
  if (provider === 'gemini') {
    const singleKey = await retrieveAndDecrypt('gemini_single');
    const multiKey = await retrieveAndDecrypt('gemini_multi');
    return !!(singleKey || multiKey);
  }
  const key = await retrieveAndDecrypt(provider);
  return !!key;
}

/**
 * Get list of all providers that have API keys configured.
 */
export async function getConfiguredProviders(): Promise<LLMProvider[]> {
  const providers: LLMProvider[] = ['openai', 'together', 'gemini'];
  const configured: LLMProvider[] = [];

  for (const provider of providers) {
    if (await hasApiKey(provider)) {
      configured.push(provider);
    }
  }

  return configured;
}

/**
 * Check if any API keys are configured (for AI feature availability).
 */
export async function hasAnyApiKeys(): Promise<boolean> {
  const configured = await getConfiguredProviders();
  return configured.length > 0;
}

/**
 * Get the API key for a provider (handling Gemini key type).
 * Returns null if not configured.
 */
export async function getApiKeyForProvider(provider: LLMProvider): Promise<string | null> {
  if (provider === 'gemini') {
    const keyType = getGeminiKeyType();
    return retrieveAndDecrypt(`gemini_${keyType}`);
  }
  return retrieveAndDecrypt(provider);
}
