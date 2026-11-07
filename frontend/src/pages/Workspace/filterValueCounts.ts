import type Handsontable from 'handsontable';

/**
 * Occurrence counts for the column dropdown's "Filter by value" list, shown as
 * "district (12)" the way Excel does.
 *
 * Handsontable builds that list itself and exposes only one extension point:
 * the `modifyFiltersMultiSelectValue(visualValue, meta)` hook, which may return
 * a new label for each item. The label is display-only: the checked state and
 * the filter condition use the item's raw `value`, and the list's search box
 * still matches the value text inside the new label.
 *
 * Counts follow Excel's rule: a row counts when it passes the filters on every
 * OTHER column, so a value hidden by this column's own filter still shows how
 * many rows it would bring back.
 */

const BLANK_KEY = '\u0000blank';

export function filterValueKey(value: unknown): string {
  return value === null || value === undefined || value === '' ? BLANK_KEY : String(value);
}

/** Count values by key, skipping rows that `includeRow` rejects. */
export function countFilterValues(
  values: readonly unknown[],
  includeRow: (row: number) => boolean = () => true,
): Map<string, number> {
  const counts = new Map<string, number>();
  values.forEach((value, row) => {
    if (!includeRow(row)) return;
    const key = filterValueKey(value);
    counts.set(key, (counts.get(key) ?? 0) + 1);
  });
  return counts;
}

export function formatFilterValueLabel(label: string, count: number): string {
  return `${label} (${count.toLocaleString()})`;
}

function computeColumnCounts(hot: Handsontable, physicalCol: number): Map<string, number> {
  const filters = hot.getPlugin('filters');
  const own = filters.getDataMapAtColumn(physicalCol);
  const collection = filters.conditionCollection;
  const otherCols = collection
    ? collection.getFilteredColumns().filter((col) => col !== physicalCol)
    : [];
  const others = otherCols.map((col) => ({ col, data: filters.getDataMapAtColumn(col) }));
  return countFilterValues(
    own.map((cell) => cell.value),
    (row) => others.every(({ col, data }) => !data[row] || collection.isMatch(data[row], col)),
  );
}

/**
 * Build the `modifyFiltersMultiSelectValue` handler. Handsontable calls it once
 * per list item while it rebuilds the list, so counts are computed once per
 * column and filter state, and dropped after the current task. The filter state
 * is part of the cache key because applying a filter can rebuild a list in the
 * same task, after the conditions changed.
 */
export function createFilterValueCountLabeler(getHot: () => Handsontable | null | undefined) {
  let cache: Map<string, Map<string, number>> | null = null;

  return (visualValue: string, meta: Handsontable.CellProperties): string => {
    const hot = getHot();
    const physicalCol = meta?.col;
    if (!hot || typeof physicalCol !== 'number' || physicalCol < 0) return visualValue;
    try {
      if (!cache) {
        cache = new Map();
        queueMicrotask(() => { cache = null; });
      }
      const conditions = hot.getPlugin('filters').conditionCollection?.exportAllConditions() ?? [];
      const cacheKey = `${physicalCol}:${JSON.stringify(conditions)}`;
      let counts = cache.get(cacheKey);
      if (!counts) {
        counts = computeColumnCounts(hot, physicalCol);
        cache.set(cacheKey, counts);
      }
      const blankLabel = `(${hot.getTranslatedPhrase('Filters:values.blankCells', undefined) ?? 'Blank cells'})`;
      const key = visualValue === blankLabel ? BLANK_KEY : filterValueKey(visualValue);
      const count = counts.get(key);
      return count === undefined ? visualValue : formatFilterValueLabel(String(visualValue), count);
    } catch {
      // Never break the filter menu over a label.
      return visualValue;
    }
  };
}
