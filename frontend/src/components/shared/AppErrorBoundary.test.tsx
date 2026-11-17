import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import AppErrorBoundary from './AppErrorBoundary';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

function Boom(): JSX.Element {
  throw new Error('kaboom');
}

describe('AppErrorBoundary', () => {
  let container: HTMLDivElement;
  let root: Root;
  let consoleError: jest.SpyInstance;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    // React and the boundary both log caught errors; keep test output clean.
    consoleError = jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    container.remove();
    consoleError.mockRestore();
  });

  it('renders children when nothing throws', () => {
    act(() => {
      root.render(
        <AppErrorBoundary resetKey="/">
          <p>all good</p>
        </AppErrorBoundary>,
      );
    });
    expect(container.textContent).toContain('all good');
    expect(container.querySelector('[role="alert"]')).toBeNull();
  });

  it('shows a recoverable fallback instead of unmounting when a child throws', () => {
    act(() => {
      root.render(
        <AppErrorBoundary resetKey="/">
          <Boom />
        </AppErrorBoundary>,
      );
    });
    expect(container.querySelector('[role="alert"]')).not.toBeNull();
    expect(container.textContent).toContain('Something went wrong');
    expect(container.textContent).toContain('kaboom');
    expect(container.textContent).toContain('Reload');
  });

  it('clears the error when resetKey changes (navigation)', () => {
    act(() => {
      root.render(
        <AppErrorBoundary resetKey="/broken">
          <Boom />
        </AppErrorBoundary>,
      );
    });
    expect(container.querySelector('[role="alert"]')).not.toBeNull();

    act(() => {
      root.render(
        <AppErrorBoundary resetKey="/classic">
          <p>other page</p>
        </AppErrorBoundary>,
      );
    });
    expect(container.querySelector('[role="alert"]')).toBeNull();
    expect(container.textContent).toContain('other page');
  });
});
