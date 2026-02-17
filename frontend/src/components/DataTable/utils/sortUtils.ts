import { DataRow, CellValue } from '../../../types';
import { SortState, SortColumn } from '../types/filters';

/**
 * Apply sorting to a set of rows
 */
export function applySort(rows: DataRow[], sortState: SortState): DataRow[] {
  if (sortState.columns.length === 0) {
    return rows;
  }

  // Sort columns by priority (lower priority = more important)
  const sortedColumns = [...sortState.columns].sort((a, b) => a.priority - b.priority);

  return [...rows].sort((a, b) => {
    for (const sortCol of sortedColumns) {
      const comparison = compareRows(a, b, sortCol);
      if (comparison !== 0) {
        return comparison;
      }
    }
    return 0;
  });
}

/**
 * Compare two rows by a single sort column
 */
function compareRows(a: DataRow, b: DataRow, sortCol: SortColumn): number {
  const aValue = getCellValue(a, sortCol.column);
  const bValue = getCellValue(b, sortCol.column);

  const comparison = compareValues(aValue, bValue);

  return sortCol.direction === 'asc' ? comparison : -comparison;
}

/**
 * Get cell value for a column, handling special columns
 */
function getCellValue(row: DataRow, columnName: string): CellValue {
  if (columnName === '_row_name') {
    return row.row_name;
  }
  if (columnName === '_papers') {
    return row.papers;
  }
  return row.data[columnName];
}

/**
 * Compare two cell values with type-aware sorting
 * Nulls are always sorted last
 */
function compareValues(a: CellValue, b: CellValue): number {
  // Handle null/undefined - nulls always go last
  const aIsNull = a === null || a === undefined || a === '';
  const bIsNull = b === null || b === undefined || b === '';

  if (aIsNull && bIsNull) return 0;
  if (aIsNull) return 1;  // a goes after b
  if (bIsNull) return -1; // b goes after a

  // Extract comparable values
  const aComp = extractComparableValue(a);
  const bComp = extractComparableValue(b);

  // If both are numbers, compare numerically
  if (typeof aComp === 'number' && typeof bComp === 'number') {
    return aComp - bComp;
  }

  // If both are dates, compare as dates
  if (aComp instanceof Date && bComp instanceof Date) {
    return aComp.getTime() - bComp.getTime();
  }

  // Default to string comparison
  const aStr = String(aComp).toLowerCase();
  const bStr = String(bComp).toLowerCase();

  return aStr.localeCompare(bStr, undefined, { numeric: true, sensitivity: 'base' });
}

/**
 * Extract a comparable value from a cell value
 */
function extractComparableValue(value: CellValue): string | number | Date {
  if (value === null || value === undefined) {
    return '';
  }

  // Handle ScheMatiQ answer format
  if (typeof value === 'object' && 'answer' in value) {
    const answer = (value as { answer: unknown }).answer;
    return extractComparableValue(answer as CellValue);
  }

  // Handle arrays - compare by first element or length
  if (Array.isArray(value)) {
    if (value.length === 0) return '';
    // Try to get a meaningful first value
    return extractComparableValue(value[0] as CellValue);
  }

  // Handle other objects
  if (typeof value === 'object') {
    // Can't meaningfully compare objects, return empty string
    return '';
  }

  // Handle numbers
  if (typeof value === 'number') {
    return value;
  }

  // Handle booleans
  if (typeof value === 'boolean') {
    return value ? 1 : 0;
  }

  // Handle strings
  if (typeof value === 'string') {
    // Try to parse as number
    const num = parseFloat(value);
    if (!isNaN(num) && isFinite(num)) {
      return num;
    }

    // Try to parse as date (ISO format)
    if (/^\d{4}-\d{2}-\d{2}/.test(value)) {
      const date = new Date(value);
      if (!isNaN(date.getTime())) {
        return date;
      }
    }

    return value;
  }

  return String(value);
}

/**
 * Get numeric range for a column (for range filters)
 */
export function getNumericRange(rows: DataRow[], columnName: string): { min: number; max: number } | null {
  let min = Infinity;
  let max = -Infinity;
  let hasNumeric = false;

  for (const row of rows) {
    const value = getCellValue(row, columnName);
    const num = extractNumber(value);

    if (num !== null) {
      hasNumeric = true;
      min = Math.min(min, num);
      max = Math.max(max, num);
    }
  }

  return hasNumeric ? { min, max } : null;
}

/**
 * Extract number from a cell value
 */
function extractNumber(value: CellValue): number | null {
  if (value === null || value === undefined) return null;

  if (typeof value === 'number') return value;

  if (typeof value === 'string') {
    const parsed = parseFloat(value);
    return isNaN(parsed) ? null : parsed;
  }

  // Handle ScheMatiQ answer format
  if (typeof value === 'object' && 'answer' in value) {
    return extractNumber((value as { answer: unknown }).answer as CellValue);
  }

  return null;
}
