/**
 * Custom hook for handling file upload with dropzone functionality
 */
import { useState } from 'react';
import { useDropzone, DropzoneOptions } from 'react-dropzone';
import { ALLOWED_FILE_TYPES, MAX_FILE_SIZE } from '../constants/index';

export interface FileUploadOptions {
  allowMultiple?: boolean;
  acceptedTypes?: Record<string, string[]>;
  maxSize?: number;
  onFilesSelected?: (files: File[]) => void;
  onError?: (error: string) => void;
  externalFiles?: File[];  // Sync with parent state to avoid accumulation
}

export interface FileUploadResult {
  files: File[];
  getRootProps: () => any;
  getInputProps: () => any;
  isDragActive: boolean;
  clearFiles: () => void;
  removeFile: (fileToRemove: File) => void;
  dragError: string | null;
}

export const useFileUpload = (options: FileUploadOptions = {}): FileUploadResult => {
  const {
    allowMultiple = false,
    acceptedTypes = ALLOWED_FILE_TYPES,
    maxSize = MAX_FILE_SIZE,
    onFilesSelected,
    onError,
    externalFiles
  } = options;

  const [files, setFiles] = useState<File[]>([]);
  const [dragError, setDragError] = useState<string | null>(null);

  const dropzoneOptions: DropzoneOptions = {
    onDrop: (acceptedFiles, rejectedFiles) => {
      setDragError(null);

      if (rejectedFiles.length > 0) {
        const errorMessage = 'Some files were rejected. Please check file type and size requirements.';
        setDragError(errorMessage);
        onError?.(errorMessage);
        return;
      }

      // Use externalFiles if provided (syncs with parent state), otherwise use internal state
      const currentFiles = externalFiles ?? files;
      const newFiles = allowMultiple ? [...currentFiles, ...acceptedFiles] : acceptedFiles;
      setFiles(newFiles);
      onFilesSelected?.(newFiles);
    },
    accept: acceptedTypes,
    maxSize,
    multiple: allowMultiple,
  };

  const { getRootProps, getInputProps, isDragActive } = useDropzone(dropzoneOptions);

  const clearFiles = () => {
    setFiles([]);
    setDragError(null);
  };

  const removeFile = (fileToRemove: File) => {
    const updatedFiles = files.filter(file => file !== fileToRemove);
    setFiles(updatedFiles);
    onFilesSelected?.(updatedFiles);
  };

  return {
    files,
    getRootProps,
    getInputProps,
    isDragActive,
    clearFiles,
    removeFile,
    dragError
  };
};