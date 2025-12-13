/**
 * API helper utilities for common operations
 */

/**
 * Extract error message from API response
 */
export const extractApiErrorMessage = (error: unknown, fallbackMessage: string): string => {
  if (error && typeof error === 'object' && 'response' in error) {
    const apiError = error as any;
    if (apiError?.response?.data?.detail) {
      return apiError.response.data.detail;
    }
  }
  if (error && typeof error === 'object' && 'message' in error) {
    const errorWithMessage = error as { message: string };
    return errorWithMessage.message;
  }
  return fallbackMessage;
};

/**
 * Check if a file has valid extension
 */
export const hasValidFileExtension = (file: File, allowedExtensions: string[]): boolean => {
  const fileExtension = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));
  return allowedExtensions.includes(fileExtension);
};

/**
 * Format file size for display
 */
export const formatFileSize = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

/**
 * Validate file before upload
 */
export interface FileValidation {
  isValid: boolean;
  errors: string[];
}

export const validateFile = (
  file: File, 
  maxSize: number, 
  allowedTypes: string[]
): FileValidation => {
  const errors: string[] = [];
  
  // Check file size
  if (file.size > maxSize) {
    errors.push(`File size ${formatFileSize(file.size)} exceeds maximum allowed size ${formatFileSize(maxSize)}`);
  }
  
  // Check file type
  const fileExtension = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));
  if (!allowedTypes.includes(fileExtension)) {
    errors.push(`File type ${fileExtension} is not allowed. Allowed types: ${allowedTypes.join(', ')}`);
  }
  
  return {
    isValid: errors.length === 0,
    errors
  };
};

/**
 * Debounce function for search inputs
 */
export const debounce = <T extends (...args: unknown[]) => void>(
  func: T,
  delay: number
): ((...args: Parameters<T>) => void) => {
  let timeoutId: NodeJS.Timeout;
  
  return (...args: Parameters<T>) => {
    clearTimeout(timeoutId);
    timeoutId = setTimeout(() => func(...args), delay);
  };
};

/**
 * Generate a unique client-side ID
 */
export const generateClientId = (): string => {
  return `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
};