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
  // Handle null/undefined cases
  if (!columnName || typeof columnName !== 'string') {
    return 'Unknown Column';
  }

  // Special display names for internal columns
  if (columnName === '_row_name' || columnName === 'row_name') {
    return 'Doc Name';
  }
  if (columnName === '_unit_name') {
    return 'Observation Unit';
  }

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

/**
 * Get preview text with smart truncation
 */
export const getPreviewText = (text: string, maxLength: number = 100): string => {
  if (text.length <= maxLength) return text;
  
  // Try to truncate at word boundary
  const truncated = text.substring(0, maxLength);
  const lastSpaceIndex = truncated.lastIndexOf(' ');
  
  if (lastSpaceIndex > maxLength * 0.75) {
    return truncated.substring(0, lastSpaceIndex) + '...';
  }
  
  return truncated + '...';
};

/**
 * Check if content appears to be excerpt-like
 */
export const isExcerptContent = (columnName: string, value: unknown): boolean => {
  if (typeof value !== 'string') return false;
  
  const column = columnName.toLowerCase();
  return column.includes('excerpt') || 
         column.includes('evidence') || 
         column.includes('source') || 
         column.includes('citation');
};

/**
 * Check if text is very long
 */
export const isVeryLongText = (text: string, threshold: number = 300): boolean => {
  return text.length > threshold;
};

/**
 * Check if text has multiple lines
 */
export const hasMultipleLines = (text: string, lineThreshold: number = 5): boolean => {
  return text.split('\n').length > lineThreshold;
};

/**
 * Format session status for display
 */
export const formatSessionStatus = (status: string): string => {
  return status
    .replace(/_/g, ' ')
    .replace(/\b\w/g, char => char.toUpperCase());
};