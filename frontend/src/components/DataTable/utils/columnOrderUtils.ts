import { DataRow } from '../../../types';

interface ColumnInfoLike {
  name: string;
  allowed_values?: string[];
}

/**
 * Returns data columns in a consistent priority-based order.
 * Only handles schema/data columns — internal columns (prefixed with _) are excluded.
 *
 * Order: exact-match priority → fuzzy-match priority → regular → schema-only → "Document Directory" last
 */
export function getDefaultColumnOrder(
  rows: DataRow[],
  columnInfo?: ColumnInfoLike[]
): string[] {
  const priorityColumns: string[] = [];
  const regularColumns: string[] = [];

  // Collect all non-internal data columns
  const allDataColumns = new Set<string>();
  rows.forEach(row => {
    Object.keys(row.data).forEach(key => {
      if (!key.startsWith('_')) {
        allDataColumns.add(key);
      }
    });
  });

  let dataColumnArray = Array.from(allDataColumns).filter(col => !col.endsWith('_excerpt'));

  // Sort data columns by schema order upfront for deterministic results
  // regardless of row data iteration order
  if (columnInfo && columnInfo.length > 0) {
    const schemaOrder = new Map(columnInfo.map((col, idx) => [col.name, idx]));
    dataColumnArray.sort((a, b) => {
      const aIdx = schemaOrder.get(a) ?? Infinity;
      const bIdx = schemaOrder.get(b) ?? Infinity;
      return aIdx - bIdx;
    });
  }

  // Exact-match priority columns
  const exactMatches = ['row_name', 'name', 'id', 'title', 'row', 'identifier'];
  exactMatches.forEach(exactName => {
    const found = dataColumnArray.find(col => col.toLowerCase() === exactName);
    if (found && !priorityColumns.includes(found)) {
      priorityColumns.push(found);
    }
  });

  // Fuzzy-match priority columns (contain name/id/title/label)
  dataColumnArray.forEach(key => {
    const keyLower = key.toLowerCase();
    if (!priorityColumns.includes(key)) {
      if (keyLower.includes('name') || keyLower.includes('id') ||
          keyLower.includes('title') || keyLower.includes('label')) {
        priorityColumns.push(key);
      } else {
        regularColumns.push(key);
      }
    }
  });

  // If no priority columns found, promote first regular column
  if (priorityColumns.length === 0 && regularColumns.length > 0) {
    const firstColumn = regularColumns.shift();
    if (firstColumn) priorityColumns.push(firstColumn);
  }

  // Schema-only columns (in columnInfo but not in row data)
  const schemaColumns: string[] = [];
  if (columnInfo && columnInfo.length > 0) {
    columnInfo.forEach(col => {
      if (!col.name.startsWith('_') && !col.name.endsWith('_excerpt')) {
        if (!priorityColumns.includes(col.name) && !regularColumns.includes(col.name)) {
          schemaColumns.push(col.name);
        }
      }
    });
  }

  // TEMP HACK: Sort all data columns alphabetically
  priorityColumns.sort((a, b) => a.localeCompare(b));
  regularColumns.sort((a, b) => a.localeCompare(b));
  schemaColumns.sort((a, b) => a.localeCompare(b));

  // Combine all columns
  const allCols = [...priorityColumns, ...regularColumns, ...schemaColumns];

  // Move "Document Directory" (and similar patterns) to the end
  const isDocDirectoryColumn = (col: string) => {
    const colLower = col.toLowerCase().replace(/[_-]/g, ' ');
    return colLower.includes('document directory') ||
           colLower.includes('doc directory') ||
           colLower === 'directory';
  };

  const docDirectoryCols = allCols.filter(isDocDirectoryColumn);
  const otherCols = allCols.filter(col => !isDocDirectoryColumn(col));

  return [...otherCols, ...docDirectoryCols];
}
