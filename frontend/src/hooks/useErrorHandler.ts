/**
 * Custom hook for standardized error handling
 */
import { useState } from 'react';
import { ApiError, GenericErrorHandler } from '../types';

export interface ErrorHandlerResult {
  error: string | null;
  setError: (error: string | null) => void;
  clearError: () => void;
  handleError: GenericErrorHandler;
  hasError: boolean;
}

export const useErrorHandler = (): ErrorHandlerResult => {
  const [error, setError] = useState<string | null>(null);

  const clearError = () => {
    setError(null);
  };

  const handleError: GenericErrorHandler = (error: ApiError) => {
    if (error?.response?.data?.detail) {
      setError(error.response.data.detail);
    } else if (error?.message) {
      setError(error.message);
    } else if (typeof error === 'string') {
      setError(error);
    } else {
      setError('An unexpected error occurred');
    }
  };

  return {
    error,
    setError,
    clearError,
    handleError,
    hasError: !!error
  };
};