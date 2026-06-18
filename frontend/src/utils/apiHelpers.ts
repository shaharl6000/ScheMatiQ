/**
 * API helper utilities for common operations
 */

/**
 * Normalize FastAPI/Pydantic error `detail` to a display string.
 */
export const formatApiErrorDetail = (detail: unknown): string | null => {
  if (typeof detail === 'string') {
    return detail;
  }
  if (Array.isArray(detail) && detail.length > 0) {
    return detail
      .map((err: { msg?: string }) => (typeof err?.msg === 'string' ? err.msg : String(err)))
      .join('; ');
  }
  if (detail && typeof detail === 'object' && 'msg' in detail) {
    const msg = (detail as { msg?: string }).msg;
    if (typeof msg === 'string') {
      return msg;
    }
  }
  return null;
};

/**
 * Extract error message from API response
 */
export const extractApiErrorMessage = (error: unknown, fallbackMessage: string): string => {
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  const formatted = formatApiErrorDetail(detail);
  if (formatted) {
    return formatted;
  }
  if (error && typeof error === 'object' && 'message' in error) {
    const errorWithMessage = error as { message: string };
    if (errorWithMessage.message) {
      return errorWithMessage.message;
    }
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