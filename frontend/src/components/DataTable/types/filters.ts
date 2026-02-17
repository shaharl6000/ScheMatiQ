// Filter operator types
export type TextFilterOperator = 'contains' | 'equals' | 'startsWith' | 'endsWith' | 'regex';
export type NumericFilterOperator = 'eq' | 'gt' | 'lt' | 'gte' | 'lte' | 'between';
export type CategoricalFilterOperator = 'in' | 'notIn';
export type BooleanFilterOperator = 'isTrue' | 'isFalse';
export type NullFilterOperator = 'isNull' | 'isNotNull';

export type FilterOperator =
  | TextFilterOperator
  | NumericFilterOperator
  | CategoricalFilterOperator
  | BooleanFilterOperator
  | NullFilterOperator;

// Data types for columns
export type ColumnDataType = 'string' | 'number' | 'boolean' | 'date' | 'categorical' | 'json' | 'array';

// Filter value types
export type FilterValue = string | number | boolean | string[] | [number, number] | null;

// Individual filter rule
export interface FilterRule {
  id: string;
  column: string;
  operator: FilterOperator;
  value: FilterValue;
  caseSensitive?: boolean;
}

// Sort direction
export type SortDirection = 'asc' | 'desc';

// Sort column configuration
export interface SortColumn {
  column: string;
  direction: SortDirection;
  priority: number;
}

// Sort state
export interface SortState {
  columns: SortColumn[];
}

// Filter state
export interface FilterState {
  rules: FilterRule[];
}

// Filter preset for saving/loading
export interface FilterPreset {
  id: string;
  name: string;
  description?: string;
  filters: FilterRule[];
  sort: SortColumn[];
  createdAt: string;
}

// Column visibility state
export interface ColumnVisibilityState {
  [columnName: string]: boolean;
}

// Column metadata for filter UI
export interface ColumnMetadata {
  name: string;
  dataType: ColumnDataType;
  allowedValues?: string[];
  hasNulls?: boolean;
}

// Operators available per data type
export const OPERATORS_BY_TYPE: Record<ColumnDataType, FilterOperator[]> = {
  string: ['contains', 'equals', 'in', 'notIn', 'startsWith', 'endsWith', 'regex', 'isNull', 'isNotNull'],
  number: ['eq', 'gt', 'lt', 'gte', 'lte', 'between', 'isNull', 'isNotNull'],
  boolean: ['isTrue', 'isFalse', 'isNull', 'isNotNull'],
  date: ['eq', 'gt', 'lt', 'gte', 'lte', 'between', 'isNull', 'isNotNull'],
  categorical: ['in', 'notIn', 'isNull', 'isNotNull'],
  json: ['isNull', 'isNotNull'],
  array: ['isNull', 'isNotNull'],
};

// Human-readable operator labels
export const OPERATOR_LABELS: Record<FilterOperator, string> = {
  // Text
  contains: 'Contains',
  equals: 'Equals',
  startsWith: 'Starts with',
  endsWith: 'Ends with',
  regex: 'Matches regex',
  // Numeric
  eq: 'Equals',
  gt: 'Greater than',
  lt: 'Less than',
  gte: 'Greater or equal',
  lte: 'Less or equal',
  between: 'Between',
  // Categorical
  in: 'Is one of',
  notIn: 'Is not one of',
  // Boolean
  isTrue: 'Is true',
  isFalse: 'Is false',
  // Null
  isNull: 'Is empty',
  isNotNull: 'Is not empty',
};

// Generate unique ID for filter rules
export const generateFilterId = (): string => {
  return `filter_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
};

// Generate unique ID for presets
export const generatePresetId = (): string => {
  return `preset_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
};
