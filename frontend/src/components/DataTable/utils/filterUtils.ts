import { DataRow, CellValue } from '../../../types';
import {
  FilterRule,
  FilterState,
  ColumnDataType,
  ColumnMetadata,
} from '../types/filters';
import { isEmpty } from './valueUtils';

/**
 * Apply all filter rules to a set of rows
 */
export function applyFilters(rows: DataRow[], filterState: FilterState): DataRow[] {
  if (filterState.rules.length === 0) {
    return rows;
  }

  return rows.filter(row => {
    // All rules must pass (AND logic)
    return filterState.rules.every(rule => {
      const value = getCellValue(row, rule.column);
      return evaluateFilterRule(value, rule);
    });
  });
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
 * Evaluate a single filter rule against a value
 */
function evaluateFilterRule(value: CellValue, rule: FilterRule): boolean {
  const { operator, value: filterValue, caseSensitive = false } = rule;

  // Handle null operators first - use isEmpty for consistent detection
  // isEmpty treats null, undefined, "", "None", "N/A", [], {} as empty
  if (operator === 'isNull') {
    return isEmpty(value);
  }
  if (operator === 'isNotNull') {
    return !isEmpty(value);
  }

  // For other operators, empty values don't match
  if (isEmpty(value)) {
    return false;
  }

  // Handle boolean operators
  if (operator === 'isTrue') {
    return value === true || value === 'true' || value === 'True' || value === '1';
  }
  if (operator === 'isFalse') {
    return value === false || value === 'false' || value === 'False' || value === '0';
  }

  // Text operators
  if (isTextOperator(operator)) {
    const stringValue = extractStringValue(value);
    const searchValue = String(filterValue ?? '');

    const compareValue = caseSensitive ? stringValue : stringValue.toLowerCase();
    const compareSearch = caseSensitive ? searchValue : searchValue.toLowerCase();

    switch (operator) {
      case 'contains':
        return compareValue.includes(compareSearch);
      case 'equals':
        return compareValue === compareSearch;
      case 'startsWith':
        return compareValue.startsWith(compareSearch);
      case 'endsWith':
        return compareValue.endsWith(compareSearch);
      case 'regex':
        try {
          const regex = new RegExp(searchValue, caseSensitive ? '' : 'i');
          return regex.test(stringValue);
        } catch {
          return false;
        }
      default:
        return true;
    }
  }

  // Numeric operators
  if (isNumericOperator(operator)) {
    const numValue = extractNumericValue(value);
    if (numValue === null) return false;

    switch (operator) {
      case 'eq':
        return numValue === Number(filterValue);
      case 'gt':
        return numValue > Number(filterValue);
      case 'lt':
        return numValue < Number(filterValue);
      case 'gte':
        return numValue >= Number(filterValue);
      case 'lte':
        return numValue <= Number(filterValue);
      case 'between':
        if (Array.isArray(filterValue) && filterValue.length === 2) {
          const [min, max] = filterValue as [number, number];
          return numValue >= min && numValue <= max;
        }
        return true;
      default:
        return true;
    }
  }

  // Categorical operators
  if (isCategoricalOperator(operator)) {
    const stringValue = extractStringValue(value);
    const allowedValues = Array.isArray(filterValue) ? filterValue : [filterValue];

    switch (operator) {
      case 'in':
        return allowedValues.some(v =>
          String(v).toLowerCase() === stringValue.toLowerCase()
        );
      case 'notIn':
        return !allowedValues.some(v =>
          String(v).toLowerCase() === stringValue.toLowerCase()
        );
      default:
        return true;
    }
  }

  return true;
}

/**
 * Extract string value from cell, handling complex types
 */
function extractStringValue(value: CellValue): string {
  if (value === null || value === undefined) return '';

  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);

  // Handle ScheMatiQ answer format
  if (typeof value === 'object' && 'answer' in value) {
    return String((value as { answer: unknown }).answer ?? '');
  }

  // Handle arrays
  if (Array.isArray(value)) {
    return value.map(v => String(v)).join(', ');
  }

  // Handle other objects
  try {
    return JSON.stringify(value);
  } catch {
    return '';
  }
}

/**
 * Extract numeric value from cell
 */
function extractNumericValue(value: CellValue): number | null {
  if (value === null || value === undefined) return null;

  if (typeof value === 'number') return value;

  if (typeof value === 'string') {
    const parsed = parseFloat(value);
    return isNaN(parsed) ? null : parsed;
  }

  // Handle ScheMatiQ answer format
  if (typeof value === 'object' && 'answer' in value) {
    const answer = (value as { answer: unknown }).answer;
    if (typeof answer === 'number') return answer;
    if (typeof answer === 'string') {
      const parsed = parseFloat(answer);
      return isNaN(parsed) ? null : parsed;
    }
  }

  return null;
}

function isTextOperator(op: string): boolean {
  return ['contains', 'equals', 'startsWith', 'endsWith', 'regex'].includes(op);
}

function isNumericOperator(op: string): boolean {
  return ['eq', 'gt', 'lt', 'gte', 'lte', 'between'].includes(op);
}

function isCategoricalOperator(op: string): boolean {
  return ['in', 'notIn'].includes(op);
}

/**
 * Detect column data type from sample values
 */
export function detectColumnType(
  rows: DataRow[],
  columnName: string,
  allowedValues?: string[]
): ColumnDataType {
  // If allowed_values is defined, it's categorical
  if (allowedValues && allowedValues.length > 0) {
    return 'categorical';
  }

  const values: CellValue[] = [];
  for (const row of rows.slice(0, 100)) { // Sample first 100 rows
    const val = getCellValue(row, columnName);
    if (val !== null && val !== undefined && val !== '') {
      values.push(val);
    }
  }

  if (values.length === 0) {
    return 'string';
  }

  // Check for arrays
  if (values.some(v => Array.isArray(v))) {
    return 'array';
  }

  // Check for objects (JSON)
  if (values.some(v => typeof v === 'object' && v !== null && !Array.isArray(v))) {
    // Special case: ScheMatiQ answer format - treat as string
    if (values.some(v => typeof v === 'object' && 'answer' in (v as object))) {
      return 'string';
    }
    return 'json';
  }

  // Check for booleans
  if (values.every(v => typeof v === 'boolean' || v === 'true' || v === 'false')) {
    return 'boolean';
  }

  // Check for numbers
  if (values.every(v => {
    if (typeof v === 'number') return true;
    if (typeof v === 'string') {
      const n = parseFloat(v);
      return !isNaN(n) && isFinite(n);
    }
    return false;
  })) {
    return 'number';
  }

  // Check for dates (ISO format)
  const dateRegex = /^\d{4}-\d{2}-\d{2}/;
  if (values.every(v => typeof v === 'string' && dateRegex.test(v))) {
    return 'date';
  }

  return 'string';
}

/**
 * Get unique values for a column (for categorical filters)
 */
export function getUniqueValues(rows: DataRow[], columnName: string): string[] {
  const values = new Set<string>();

  for (const row of rows) {
    const value = getCellValue(row, columnName);
    if (value !== null && value !== undefined && value !== '') {
      const strValue = extractStringValue(value);
      if (strValue) {
        values.add(strValue);
      }
    }
  }

  return Array.from(values).sort();
}

/**
 * Build column metadata for filter UI
 */
export function buildColumnMetadata(
  rows: DataRow[],
  columnName: string,
  allowedValues?: string[]
): ColumnMetadata {
  const dataType = detectColumnType(rows, columnName, allowedValues);
  const hasNulls = rows.some(row => {
    const val = getCellValue(row, columnName);
    return isEmpty(val);
  });

  return {
    name: columnName,
    dataType,
    allowedValues,
    hasNulls,
  };
}
