/**
 * Utility functions for detecting and handling empty/null values in DataTable
 */

/**
 * Determines if a value should be considered "empty" or "not found"
 * This is the single source of truth for empty value detection across the DataTable
 *
 * Values treated as empty:
 * - null, undefined
 * - "" (empty string)
 * - "None", "null", "undefined" (string literals - case insensitive)
 * - "N/A", "n/a", "Not found", "Not available" (user-requested)
 * - [] (empty array)
 * - {} (empty object)
 * - "[]", "{}" (Python-style empty collection strings)
 */
export function isEmpty(value: unknown): boolean {
  // Handle null/undefined
  if (value === null || value === undefined) return true;

  // Handle strings
  if (typeof value === 'string') {
    const trimmed = value.trim().toLowerCase();
    if (
      trimmed === '' ||
      trimmed === 'none' ||
      trimmed === 'null' ||
      trimmed === 'undefined' ||
      trimmed === 'n/a' ||
      trimmed === 'not found' ||
      trimmed === 'not available' ||
      trimmed === '[]' ||
      trimmed === '{}'
    ) {
      return true;
    }
  }

  // Handle empty arrays
  if (Array.isArray(value) && value.length === 0) return true;

  // Handle empty objects (but not Date objects or other special types)
  if (
    typeof value === 'object' &&
    value !== null &&
    !(value instanceof Date) &&
    Object.keys(value).length === 0
  ) {
    return true;
  }

  return false;
}

/**
 * Checks if a value is "complete" (i.e., not empty)
 * Inverse of isEmpty for semantic clarity
 */
export function isComplete(value: unknown): boolean {
  return !isEmpty(value);
}
