const DEBUG_ENABLED = process.env.REACT_APP_ENABLE_DEBUG === 'true';

export const debug = {
  log: (...args: unknown[]) => {
    if (DEBUG_ENABLED) console.log(...args);
  },
};
