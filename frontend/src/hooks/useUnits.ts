/**
 * Hooks for unit view functionality.
 */

import { useState, useEffect, useCallback } from 'react';
import { unitsAPI } from '../services/api';
import {
  UnitListResponse,
  UnitSuggestionsResponse,
  MergeUnitsRequest,
  MergeUnitsResponse,
} from '../types/unit';

interface UseUnitsResult {
  /** Unit list data */
  units: UnitListResponse | null;
  /** Loading state */
  loading: boolean;
  /** Error state */
  error: string | null;
  /** Refresh units list */
  refresh: () => Promise<void>;
}

/**
 * Hook to fetch and manage observation units for a session.
 */
export function useUnits(sessionId: string | undefined): UseUnitsResult {
  const [units, setUnits] = useState<UnitListResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchUnits = useCallback(async () => {
    if (!sessionId) {
      setUnits(null);
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await unitsAPI.list(sessionId);
      setUnits(response);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to fetch units';
      setError(message);
      console.error('Error fetching units:', err);
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  useEffect(() => {
    fetchUnits();
  }, [fetchUnits]);

  return {
    units,
    loading,
    error,
    refresh: fetchUnits,
  };
}

interface UseMergeUnitsResult {
  /** Merge function */
  merge: (request: MergeUnitsRequest) => Promise<MergeUnitsResponse>;
  /** Loading state */
  loading: boolean;
  /** Error state */
  error: string | null;
  /** Last merge result */
  result: MergeUnitsResponse | null;
  /** Clear error */
  clearError: () => void;
}

/**
 * Hook to merge observation units.
 */
export function useMergeUnits(sessionId: string | undefined): UseMergeUnitsResult {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<MergeUnitsResponse | null>(null);

  const merge = useCallback(async (request: MergeUnitsRequest): Promise<MergeUnitsResponse> => {
    if (!sessionId) {
      throw new Error('No session ID provided');
    }

    setLoading(true);
    setError(null);

    try {
      const response = await unitsAPI.merge(sessionId, request);
      setResult(response);
      return response;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to merge units';
      setError(message);
      throw err;
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  const clearError = useCallback(() => {
    setError(null);
  }, []);

  return {
    merge,
    loading,
    error,
    result,
    clearError,
  };
}

interface UseUnitSuggestionsResult {
  /** Suggestions data */
  suggestions: UnitSuggestionsResponse | null;
  /** Loading state */
  loading: boolean;
  /** Error state */
  error: string | null;
  /** Fetch suggestions with threshold */
  fetchSuggestions: (threshold?: number) => Promise<void>;
}

/**
 * Hook to fetch merge suggestions for similar units.
 */
export function useUnitSuggestions(sessionId: string | undefined): UseUnitSuggestionsResult {
  const [suggestions, setSuggestions] = useState<UnitSuggestionsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchSuggestions = useCallback(async (threshold: number = 0.8) => {
    if (!sessionId) {
      setSuggestions(null);
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await unitsAPI.getSuggestions(sessionId, threshold);
      setSuggestions(response);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to fetch suggestions';
      setError(message);
      console.error('Error fetching unit suggestions:', err);
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  return {
    suggestions,
    loading,
    error,
    fetchSuggestions,
  };
}
