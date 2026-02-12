/**
 * Hooks for unit view functionality.
 */

import { useState, useEffect, useCallback, useRef } from 'react';
import { useQuery, useQueryClient } from 'react-query';
import { unitsAPI } from '../services/api';
import {
  UnitListResponse,
  UnitSuggestionsResponse,
  AutoMergeResult,
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
 * Uses React Query for shared caching — multiple components calling useUnits
 * with the same sessionId will share cached data (no redundant fetches or flicker).
 */
export function useUnits(sessionId: string | undefined): UseUnitsResult {
  const queryClient = useQueryClient();

  const { data: units, isLoading: loading, error: queryError } = useQuery(
    ['units', sessionId],
    () => unitsAPI.list(sessionId!),
    {
      enabled: !!sessionId,
      keepPreviousData: true,
    }
  );

  const error = queryError instanceof Error ? queryError.message : queryError ? String(queryError) : null;

  const refresh = useCallback(async () => {
    if (sessionId) {
      await queryClient.invalidateQueries(['units', sessionId]);
    }
  }, [sessionId, queryClient]);

  return {
    units: units ?? null,
    loading,
    error,
    refresh,
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
  /** Auto-merge results from the initial fetch */
  autoMerged: AutoMergeResult[];
  /** Fetch suggestions with threshold — returns the response */
  fetchSuggestions: (threshold?: number) => Promise<UnitSuggestionsResponse | null>;
}

/**
 * Hook to fetch merge suggestions for similar units.
 * Auto-fetches on mount with autoMerge=true to merge exact matches immediately.
 */
export function useUnitSuggestions(sessionId: string | undefined): UseUnitSuggestionsResult {
  const [suggestions, setSuggestions] = useState<UnitSuggestionsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [autoMerged, setAutoMerged] = useState<AutoMergeResult[]>([]);
  const initialFetchDone = useRef(false);

  const fetchSuggestions = useCallback(async (threshold: number = 0.8, autoMerge: boolean = false): Promise<UnitSuggestionsResponse | null> => {
    if (!sessionId) {
      setSuggestions(null);
      return null;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await unitsAPI.getSuggestions(sessionId, threshold, autoMerge);
      setSuggestions(response);
      if (autoMerge && response.autoMerged.length > 0) {
        setAutoMerged(response.autoMerged);
      }
      return response;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to fetch suggestions';
      setError(message);
      console.error('Error fetching unit suggestions:', err);
      return null;
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  // Auto-fetch on mount with autoMerge=true
  useEffect(() => {
    if (sessionId && !initialFetchDone.current) {
      initialFetchDone.current = true;
      fetchSuggestions(0.8, true);
    }
  }, [sessionId, fetchSuggestions]);

  return {
    suggestions,
    loading,
    error,
    autoMerged,
    fetchSuggestions,
  };
}
