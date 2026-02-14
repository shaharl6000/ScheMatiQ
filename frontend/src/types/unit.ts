/**
 * Types for observation unit view and merge functionality.
 */

/** View mode for the data table */
export type ViewMode = 'standard' | 'by_unit';

/** Summary information for a single observation unit */
export interface UnitSummary {
  name: string;
  rowCount: number;
  sourceDocuments: string[];
  isMerged: boolean;
  originalUnits?: string[];
}

/** Response containing list of all observation units with statistics */
export interface UnitListResponse {
  units: UnitSummary[];
  totalUnits: number;
  totalRows: number;
}

/** Request to merge multiple observation units into one */
export interface MergeUnitsRequest {
  source_units: string[];
  target_unit: string;
  strategy: 'rename' | 'combine';
}

/** Response after merging units */
export interface MergeUnitsResponse {
  success: boolean;
  message: string;
  merged_unit?: UnitSummary;
  rows_affected: number;
}

/** Suggested merge based on similarity between units */
export interface UnitSimilarity {
  units: string[];
  similarity: number;
  suggestedName: string;
  reason: string;
}

/** Result of an individual auto-merge group */
export interface AutoMergeResult {
  mergedUnits: string[];
  targetUnit: string;
  rowsAffected: number;
}

/** Response containing merge suggestions */
export interface UnitSuggestionsResponse {
  suggestions: UnitSimilarity[];
  threshold: number;
  autoMerged: AutoMergeResult[];
}

/** State for a unit group in the grouped table view */
export interface UnitGroupState {
  name: string;
  isExpanded: boolean;
  isSelected: boolean;
  rowCount: number;
}

/** Summary information for a source document */
export interface DocumentSummary {
  name: string;
  rowCount: number;
}

/** Response from the documents endpoint */
export interface DocumentListResponse {
  documents: DocumentSummary[];
  totalDocuments: number;
  totalRows: number;
}
