/**
 * Custom hook for managing loading states
 */
import { useState } from 'react';

export interface LoadingStateResult {
  isLoading: boolean;
  startLoading: () => void;
  stopLoading: () => void;
  withLoading: <T>(fn: () => Promise<T>) => Promise<T>;
}

export const useLoadingState = (initialState = false): LoadingStateResult => {
  const [isLoading, setIsLoading] = useState(initialState);

  const startLoading = () => {
    setIsLoading(true);
  };

  const stopLoading = () => {
    setIsLoading(false);
  };

  const withLoading = async <T>(fn: () => Promise<T>): Promise<T> => {
    try {
      setIsLoading(true);
      const result = await fn();
      return result;
    } finally {
      setIsLoading(false);
    }
  };

  return {
    isLoading,
    startLoading,
    stopLoading,
    withLoading
  };
};