/**
 * Utility functions for text formatting and display
 */

/**
 * Convert snake_case string to Title Case
 * Examples: 
 * - "snake_case" -> "Snake Case"
 * - "protein_name" -> "Protein Name"
 * - "has_nuclear_export_signal" -> "Has Nuclear Export Signal"
 */
export const snakeCaseToTitleCase = (str: string): string => {
  return str
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
};

/**
 * Format column name for display, handling special cases
 */
export const formatColumnName = (columnName: string): string => {
  // Handle special metadata columns
  if (columnName.startsWith('_')) {
    const cleanName = columnName.replace('_', '');
    return snakeCaseToTitleCase(cleanName);
  }
  
  // Convert snake_case to Title Case
  return snakeCaseToTitleCase(columnName);
};

/**
 * Truncate text to specified length with ellipsis
 */
export const truncateText = (text: string, maxLength: number = 100): string => {
  if (text.length <= maxLength) return text;
  return text.substring(0, maxLength) + '...';
};

/**
 * Check if text needs truncation
 */
export const needsTruncation = (text: string, maxLength: number = 100): boolean => {
  return text.length > maxLength;
};