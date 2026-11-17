import React, { act, Suspense } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import { CHUNK_RELOAD_FLAG, isChunkLoadError, lazyWithReload } from './lazyWithReload';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

function chunkError(): Error {
  const error = new Error('Loading chunk 123 failed.');
  error.name = 'ChunkLoadError';
  return error;
}

class Catch extends React.Component<{ children: React.ReactNode }, { error: Error | null }> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    return this.state.error ? <span>error: {this.state.error.message}</span> : this.props.children;
  }
}

describe('lazyWithReload', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    window.sessionStorage.clear();
    container = document.createElement('div');
    root = createRoot(container);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });

  afterEach(() => {
    act(() => root.unmount());
    jest.restoreAllMocks();
  });

  async function render(Component: React.ComponentType) {
    await act(async () => {
      root.render(
        <Catch>
          <Suspense fallback={<span>loading</span>}>
            <Component />
          </Suspense>
        </Catch>,
      );
    });
    await act(async () => {
      await new Promise((r) => setTimeout(r, 0));
    });
  }

  it('recognizes webpack chunk errors only', () => {
    expect(isChunkLoadError(chunkError())).toBe(true);
    expect(isChunkLoadError(new Error('Loading CSS chunk 7 failed.'))).toBe(true);
    expect(isChunkLoadError(new Error('boom'))).toBe(false);
    expect(isChunkLoadError('Loading chunk 1 failed')).toBe(false);
  });

  it('renders the page and clears a leftover reload flag', async () => {
    window.sessionStorage.setItem(CHUNK_RELOAD_FLAG, '1');
    const Page = lazyWithReload(async () => ({ default: () => <span>page</span> }));
    await render(Page);
    expect(container.textContent).toBe('page');
    expect(window.sessionStorage.getItem(CHUNK_RELOAD_FLAG)).toBeNull();
  });

  it('reloads once on a chunk error and keeps the fallback up', async () => {
    const reload = jest.fn();
    const Page = lazyWithReload(() => Promise.reject(chunkError()), reload);
    await render(Page);
    expect(reload).toHaveBeenCalledTimes(1);
    expect(window.sessionStorage.getItem(CHUNK_RELOAD_FLAG)).toBe('1');
    expect(container.textContent).toBe('loading');
  });

  it('does not reload again after a reload already happened', async () => {
    window.sessionStorage.setItem(CHUNK_RELOAD_FLAG, '1');
    const reload = jest.fn();
    const Page = lazyWithReload(() => Promise.reject(chunkError()), reload);
    await render(Page);
    expect(reload).not.toHaveBeenCalled();
    expect(container.textContent).toContain('error: Loading chunk 123 failed.');
  });

  it('does not reload for ordinary errors', async () => {
    const reload = jest.fn();
    const Page = lazyWithReload(() => Promise.reject(new Error('boom')), reload);
    await render(Page);
    expect(reload).not.toHaveBeenCalled();
    expect(container.textContent).toBe('error: boom');
  });
});
