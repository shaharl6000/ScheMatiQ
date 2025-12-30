import { useMemo } from 'react';
import { isEmpty } from '../utils/valueUtils';
import { DataRow, CellValue } from '../../../types';

export interface ColumnStats {
  columnName: string;
  totalRows: number;
  nonEmptyRows: number;
  emptyRows: number;
  fullnessPercentage: number; // 0-100, rounded to 1 decimal
}

export interface UseColumnStatsReturn {
  columnStats: Map<string, ColumnStats>;
  getColumnFullness: (columnName: string) => number;
  getColumnsAboveThreshold: (threshold: number) => string[];
  overallCompleteness: number;
}

/**
 * Helper to get cell value from a DataRow
 * Handles special columns like _row_name and _papers
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
 * Hook to calculate column statistics including fullness percentages
 * Uses the centralized isEmpty function for consistent empty value detection
 */
export function useColumnStats(
  rows: DataRow[],
  columns: string[]
): UseColumnStatsReturn {
  const columnStats = useMemo(() => {
    const stats = new Map<string, ColumnStats>();

    if (rows.length === 0 || columns.length === 0) {
      // Return empty stats for all columns
      columns.forEach(columnName => {
        stats.set(columnName, {
          columnName,
          totalRows: 0,
          nonEmptyRows: 0,
          emptyRows: 0,
          fullnessPercentage: 0,
        });
      });
      return stats;
    }

    columns.forEach(columnName => {
      let nonEmptyCount = 0;

      rows.forEach(row => {
        const value = getCellValue(row, columnName);
        if (!isEmpty(value)) {
          nonEmptyCount++;
        }
      });

      const emptyCount = rows.length - nonEmptyCount;
      const fullness = rows.length > 0
        ? (nonEmptyCount / rows.length) * 100
        : 0;

      stats.set(columnName, {
        columnName,
        totalRows: rows.length,
        nonEmptyRows: nonEmptyCount,
        emptyRows: emptyCount,
        fullnessPercentage: Math.round(fullness * 10) / 10, // Round to 1 decimal
      });
    });

    return stats;
  }, [rows, columns]);

  const getColumnFullness = useMemo(() => {
    return (columnName: string): number => {
      return columnStats.get(columnName)?.fullnessPercentage ?? 0;
    };
  }, [columnStats]);

  const getColumnsAboveThreshold = useMemo(() => {
    return (threshold: number): string[] => {
      return Array.from(columnStats.entries())
        .filter(([_, stats]) => stats.fullnessPercentage >= threshold)
        .map(([columnName]) => columnName);
    };
  }, [columnStats]);

  // Calculate overall completeness (average of all column fullness)
  const overallCompleteness = useMemo(() => {
    if (columnStats.size === 0) return 0;
    let totalFullness = 0;
    columnStats.forEach(stats => {
      totalFullness += stats.fullnessPercentage;
    });
    return Math.round((totalFullness / columnStats.size) * 10) / 10;
  }, [columnStats]);

  return {
    columnStats,
    getColumnFullness,
    getColumnsAboveThreshold,
    overallCompleteness,
  };
}
