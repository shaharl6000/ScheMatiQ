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

  // Handle QBSD answer objects — check the inner answer value
  if (
    typeof value === 'object' &&
    value !== null &&
    'answer' in value
  ) {
    return isEmpty((value as { answer: unknown }).answer);
  }

  // Handle streaming value objects — check the inner value
  if (
    typeof value === 'object' &&
    value !== null &&
    'value' in value &&
    !('answer' in value)
  ) {
    return isEmpty((value as { value: unknown }).value);
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

/**
 * Parse Python-style dict/list strings to JSON objects.
 * Handles single quotes, None, True/False, and apostrophes within text content.
 */
export function parsePythonString(val: string): unknown {
  const trimmed = val.trim();
  if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return val;

  try {
    return JSON.parse(trimmed);
  } catch {
    // Try targeted parsing for {'text': '...', 'source': '...'} patterns
    // This handles apostrophes within text content correctly
    try {
      const textMatch = trimmed.match(/\{\s*'text'\s*:\s*'([\s\S]*?)'\s*(?:,\s*'source'\s*:\s*'([\s\S]*?)'\s*)?\}/);
      if (textMatch) {
        const result: Record<string, string> = { text: textMatch[1] };
        if (textMatch[2]) result.source = textMatch[2];
        return result;
      }
    } catch { /* fall through */ }

    try {
      const jsonified = trimmed
        .replace(/'/g, '"')
        .replace(/None/g, 'null')
        .replace(/True/g, 'true')
        .replace(/False/g, 'false');
      return JSON.parse(jsonified);
    } catch {
      return val;
    }
  }
}

/**
 * Extract display string from various QBSD value formats.
 * Handles: answer/excerpts, value/excerpt, text (ExcerptWithSource), arrays, etc.
 */
export function extractDisplayValue(value: unknown): string {
  if (value === null || value === undefined) return '';

  // Parse string values that look like JSON/Python objects
  if (typeof value === 'string') {
    const parsed = parsePythonString(value);
    // If parsing changed the value, recursively extract from parsed result
    if (parsed !== value && typeof parsed === 'object') {
      return extractDisplayValue(parsed);
    }
    return value;
  }

  if (typeof value === 'number' || typeof value === 'boolean') return String(value);

  if (Array.isArray(value)) {
    // If array of objects, extract from first item
    if (value.length > 0 && typeof value[0] === 'object') {
      return extractDisplayValue(value[0]);
    }
    return value.map(v => extractDisplayValue(v)).join(', ');
  }

  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    // QBSD format with answer/excerpts
    if ('answer' in obj) {
      return extractDisplayValue(obj.answer);
    }
    // Value/excerpt format (streaming cells)
    if ('value' in obj) {
      return extractDisplayValue(obj.value);
    }
    // ExcerptWithSource format
    if ('text' in obj) {
      return extractDisplayValue(obj.text);
    }
    // Generic object: collect non-null values into a readable summary
    const values = Object.values(obj)
      .filter(v => v !== null && v !== undefined && String(v).trim() !== '')
      .map(v => typeof v === 'object' ? JSON.stringify(v) : String(v));
    return values.length > 0 ? values.join(', ') : JSON.stringify(value);
  }

  return String(value);
}
