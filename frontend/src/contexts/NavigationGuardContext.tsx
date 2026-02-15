import React, { createContext, useContext, useCallback, useRef, useMemo, ReactNode } from 'react';

interface NavigationGuardContextValue {
  /** Register a guard — returns a cleanup function. Only one guard active at a time. */
  registerGuard: (requestNav: (fn: () => void) => void) => (() => void);
  /** Attempt navigation. If a guard is registered it will be checked; otherwise fn executes immediately. */
  requestNavigation: (fn: () => void) => void;
}

const NavigationGuardContext = createContext<NavigationGuardContextValue>({
  registerGuard: () => () => {},
  requestNavigation: (fn) => fn(),
});

export function NavigationGuardProvider({ children }: { children: ReactNode }) {
  const guardRef = useRef<((fn: () => void) => void) | null>(null);

  const registerGuard = useCallback((requestNav: (fn: () => void) => void) => {
    guardRef.current = requestNav;
    return () => {
      guardRef.current = null;
    };
  }, []);

  const requestNavigation = useCallback((fn: () => void) => {
    if (guardRef.current) {
      guardRef.current(fn);
    } else {
      fn();
    }
  }, []);

  const value = useMemo(() => ({ registerGuard, requestNavigation }), [registerGuard, requestNavigation]);

  return (
    <NavigationGuardContext.Provider value={value}>
      {children}
    </NavigationGuardContext.Provider>
  );
}

export function useNavigationGuardContext() {
  return useContext(NavigationGuardContext);
}

export default NavigationGuardContext;
