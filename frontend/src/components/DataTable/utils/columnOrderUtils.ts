import { DataRow } from '../../../types';

interface ColumnInfoLike {
  name: string;
  allowed_values?: string[];
}

/**
 * Returns data columns in a consistent order.
 * Only handles schema/data columns — internal columns (prefixed with _) are excluded.
 *
 * When a schema (columnInfo) is provided, schema order is authoritative:
 *   schema columns (in definition order) → extra data columns not in schema (alphabetical)
 *
 * When no schema is provided, falls back to heuristic priority ordering:
 *   exact-match priority → fuzzy-match priority → regular → "Document Directory" last
 */
export function getDefaultColumnOrder(
  rows: DataRow[],
  columnInfo?: ColumnInfoLike[]
): string[] {
  // Collect all non-internal data columns present in any row
  const allDataColumns = new Set<string>();
  rows.forEach(row => {
    Object.keys(row.data).forEach(key => {
      if (!key.startsWith('_')) {
        allDataColumns.add(key);
      }
    });
  });
  const dataColumnSet = new Set(
    Array.from(allDataColumns).filter(col => !col.endsWith('_excerpt'))
  );

  let allCols: string[];

  if (columnInfo && columnInfo.length > 0) {
    // Schema order is authoritative: schema columns first (in definition order),
    // then any extra data columns not in the schema (alphabetical).
    const inSchema = new Set(columnInfo.map(c => c.name));
    const schemaOrdered = columnInfo
      .map(c => c.name)
      .filter(name => !name.startsWith('_') && !name.endsWith('_excerpt'));
    const extras = Array.from(dataColumnSet)
      .filter(col => !inSchema.has(col))
      .sort((a, b) => a.localeCompare(b));
    allCols = [...schemaOrdered, ...extras];
  } else {
    // No schema — use heuristic priority ordering
    const priorityColumns: string[] = [];
    const regularColumns: string[] = [];
    const dataColumnArray = Array.from(dataColumnSet);

    const exactMatches = ['row_name', 'name', 'id', 'title', 'row', 'identifier'];
    exactMatches.forEach(exactName => {
      const found = dataColumnArray.find(col => col.toLowerCase() === exactName);
      if (found && !priorityColumns.includes(found)) {
        priorityColumns.push(found);
      }
    });

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

    if (priorityColumns.length === 0 && regularColumns.length > 0) {
      const firstColumn = regularColumns.shift();
      if (firstColumn) priorityColumns.push(firstColumn);
    }

    priorityColumns.sort((a, b) => a.localeCompare(b));
    regularColumns.sort((a, b) => a.localeCompare(b));
    allCols = [...priorityColumns, ...regularColumns];
  }

  // Move enrichment/external columns to the end (right side of table)
  const isEnrichmentColumn = (col: string) => {
    const colLower = col.toLowerCase().replace(/[_-]/g, ' ');
    return colLower.startsWith('uniprot') ||
           colLower.startsWith('alphafold') ||
           colLower.startsWith('gene symbol') ||
           colLower.startsWith('go ') ||
           colLower === 'pdb ids' ||
           colLower.includes('subcellular localization') ||
           colLower === 'protein length';
  };

  // Move "Document Directory" (and similar patterns) to the very end
  const isDocDirectoryColumn = (col: string) => {
    const colLower = col.toLowerCase().replace(/[_-]/g, ' ');
    return colLower.includes('document directory') ||
           colLower.includes('doc directory') ||
           colLower === 'directory';
  };

  const enrichmentCols = allCols.filter(col => isEnrichmentColumn(col) && !isDocDirectoryColumn(col));
  const docDirectoryCols = allCols.filter(isDocDirectoryColumn);
  const coreCols = allCols.filter(col => !isEnrichmentColumn(col) && !isDocDirectoryColumn(col));

  return [...coreCols, ...enrichmentCols, ...docDirectoryCols];
}
