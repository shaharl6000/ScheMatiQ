import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import { useSingleFlight } from './useSingleFlight';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

type Arg = { tags: string[] };
const merge = (a?: Arg, b?: Arg): Arg => ({ tags: [...(a?.tags ?? []), ...(b?.tags ?? [])] });

function deferred() {
  let resolve!: () => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('useSingleFlight', () => {
  let container: HTMLDivElement;
  let root: Root;
  let calls: Array<{ arg?: Arg; done: ReturnType<typeof deferred> }>;
  let run: (arg?: Arg) => Promise<void>;

  function Harness() {
    run = useSingleFlight<Arg>((arg) => {
      const done = deferred();
      calls.push({ arg, done });
      return done.promise;
    }, merge);
    return null;
  }

  beforeEach(() => {
    calls = [];
    container = document.createElement('div');
    root = createRoot(container);
    act(() => root.render(<Harness />));
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('runs one call at a time and folds waiting calls into one trailing run', async () => {
    const first = run({ tags: ['a'] });
    const second = run({ tags: ['b'] });
    const third = run({ tags: ['c'] });

    expect(calls).toHaveLength(1);
    expect(second).toBe(third);

    await act(async () => {
      calls[0].done.resolve();
      await first;
    });

    expect(calls).toHaveLength(2);
    expect(calls[1].arg).toEqual({ tags: ['b', 'c'] });

    let settled = false;
    void second.then(() => {
      settled = true;
    });
    await act(async () => {
      await Promise.resolve();
    });
    expect(settled).toBe(false);

    await act(async () => {
      calls[1].done.resolve();
      await second;
    });
    expect(settled).toBe(true);
    expect(calls).toHaveLength(2);
  });

  it('starts a fresh run once idle, and a failed run does not block the queue', async () => {
    const first = run({ tags: ['a'] });
    const queued = run({ tags: ['b'] });

    await act(async () => {
      calls[0].done.reject(new Error('network'));
      await first;
    });
    expect(calls).toHaveLength(2);

    await act(async () => {
      calls[1].done.resolve();
      await queued;
    });

    const later = run({ tags: ['c'] });
    expect(calls).toHaveLength(3);
    expect(calls[2].arg).toEqual({ tags: ['c'] });
    await act(async () => {
      calls[2].done.resolve();
      await later;
    });
  });
});
