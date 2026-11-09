/**
 * Slow or timed-out requests are logged with their URL, because the axios
 * message "timeout of 30000ms exceeded" does not say which request it was.
 */

// axios is ESM-only and not transformed by react-scripts' jest; mock it whole.
// jest.mock is hoisted above these declarations, so everything is reached lazily.
const mockHandlers: { request?: (c: any) => any; ok?: (r: any) => any; fail?: (e: any) => any } = {};
jest.mock('axios', () => ({
  __esModule: true,
  default: {
    create: () => ({
      interceptors: {
        request: { use: (fn: any) => { mockHandlers.request = fn; } },
        response: { use: (ok: any, fail: any) => { mockHandlers.ok = ok; mockHandlers.fail = fail; } },
      },
    }),
  },
}));

// require, not import: api.ts registers its interceptors at load time, and an
// import would be hoisted above mockHandlers.
require('./api');

describe('request timing', () => {
  let warn: jest.SpyInstance;
  beforeEach(() => { warn = jest.spyOn(console, 'warn').mockImplementation(() => {}); });
  afterEach(() => warn.mockRestore());

  const started = (agoMs: number) => {
    const config = mockHandlers.request!({ method: 'put', url: '/schematiq/cell/s1' });
    config.startedAt = Date.now() - agoMs;
    return config;
  };

  it('stays quiet for a fast request', () => {
    mockHandlers.ok!({ config: started(50), status: 200 });
    expect(warn).not.toHaveBeenCalled();
  });

  it('names a slow request', () => {
    mockHandlers.ok!({ config: started(12000), status: 200 });
    expect(warn.mock.calls[0][0]).toMatch(/Slow request: PUT \/schematiq\/cell\/s1 -> 200 after 12\.\ds/);
  });

  it('names a request that timed out and still rejects with the error', async () => {
    const error = { code: 'ECONNABORTED', config: started(30000), message: 'timeout of 30000ms exceeded' };
    await expect(mockHandlers.fail!(error)).rejects.toBe(error);
    expect(warn.mock.calls[0][0]).toContain('PUT /schematiq/cell/s1 -> timed out');
  });

  it('survives an error with no request config', async () => {
    const error = new Error('cancelled');
    await expect(mockHandlers.fail!(error)).rejects.toBe(error);
    expect(warn).not.toHaveBeenCalled();
  });
});

export {};
