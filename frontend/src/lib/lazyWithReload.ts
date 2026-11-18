// React.lazy with one automatic page reload when a route chunk fails to load.
// After a deploy, a tab opened before it still points at the old chunk file
// names, which no longer exist. Loading such a route throws ChunkLoadError and
// the page goes blank. Reloading once fetches the new index and its chunks.
// A sessionStorage flag stops a reload loop if the chunk is truly missing;
// the flag is cleared after the next successful load.
// Used by App.tsx for the route pages.

import { lazy, type ComponentType, type LazyExoticComponent } from 'react';

export const CHUNK_RELOAD_FLAG = 'schematiq:chunk-reload';

function readFlag(): boolean {
  try {
    return window.sessionStorage.getItem(CHUNK_RELOAD_FLAG) === '1';
  } catch {
    return false;
  }
}

// Returns false when storage is unavailable (private mode, blocked). Then
// there is no loop guard, so the caller must not reload.
function writeFlag(on: boolean): boolean {
  try {
    if (on) window.sessionStorage.setItem(CHUNK_RELOAD_FLAG, '1');
    else window.sessionStorage.removeItem(CHUNK_RELOAD_FLAG);
    return true;
  } catch {
    return false;
  }
}

export function isChunkLoadError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return (
    error.name === 'ChunkLoadError' ||
    /Loading (CSS )?chunk [\w-]+ failed/i.test(error.message)
  );
}

export function lazyWithReload<T extends ComponentType<any>>(
  load: () => Promise<{ default: T }>,
  reload: () => void = () => window.location.reload(),
): LazyExoticComponent<T> {
  return lazy(async () => {
    try {
      const mod = await load();
      writeFlag(false);
      return mod;
    } catch (error) {
      if (isChunkLoadError(error) && !readFlag() && writeFlag(true)) {
        reload();
        // Keep Suspense showing its fallback until the reload takes over.
        return new Promise<{ default: T }>(() => undefined);
      }
      throw error;
    }
  });
}
