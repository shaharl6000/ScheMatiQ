import React, { useCallback, useRef, useState } from 'react';
import { Upload, Loader2 } from 'lucide-react';

/** Source-document formats we accept (mirrors the backend allow-list). */
const ALLOWED_EXTENSIONS = ['.txt', '.md', '.pdf', '.doc', '.docx', '.rtf', '.json'];
const ACCEPT_ATTR = ALLOWED_EXTENSIONS.join(',');

const hasAllowedExtension = (name: string): boolean => {
  const lower = name.toLowerCase();
  return ALLOWED_EXTENSIONS.some((ext) => lower.endsWith(ext));
};

/** Recursively collect File objects from a dropped FileSystemEntry (file or folder). */
const readEntry = (entry: any): Promise<File[]> =>
  new Promise((resolve) => {
    if (!entry) {
      resolve([]);
      return;
    }
    if (entry.isFile) {
      entry.file(
        (file: File) => resolve([file]),
        () => resolve([]),
      );
      return;
    }
    if (entry.isDirectory) {
      const reader = entry.createReader();
      const collected: File[] = [];
      const readBatch = () => {
        // readEntries returns results in chunks; keep calling until empty.
        reader.readEntries(
          async (entries: any[]) => {
            if (!entries.length) {
              resolve(collected);
              return;
            }
            const batches = await Promise.all(entries.map(readEntry));
            batches.forEach((b) => collected.push(...b));
            readBatch();
          },
          () => resolve(collected),
        );
      };
      readBatch();
      return;
    }
    resolve([]);
  });

interface SourceUploadButtonProps {
  /** Called with the selected/dropped files, already filtered to allowed formats. */
  onFiles: (files: File[]) => void;
  busy?: boolean;
  /** 'button' = compact (header); 'dropzone' = larger drag area (empty state). */
  variant?: 'button' | 'dropzone';
  label?: string;
}

/**
 * A single control that re-attaches source documents. Because one native
 * <input> can offer files OR a folder but not both, this combines a click
 * (opens the file picker, restricted to allowed formats) with drag-and-drop
 * that accepts loose files AND whole folders (recursed). Everything is filtered
 * to the allowed formats, so a dropped folder contributes only its matching
 * files. Both paths funnel into `onFiles`.
 */
const SourceUploadButton: React.FC<SourceUploadButtonProps> = ({
  onFiles,
  busy = false,
  variant = 'button',
  label = 'Upload',
}) => {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [dragOver, setDragOver] = useState(false);

  const emit = useCallback(
    (files: File[]) => {
      const filtered = files.filter((f) => hasAllowedExtension(f.name));
      if (filtered.length > 0) onFiles(filtered);
    },
    [onFiles],
  );

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    e.target.value = '';
    emit(files);
  };

  const handleDrop = useCallback(
    async (e: React.DragEvent) => {
      e.preventDefault();
      setDragOver(false);
      if (busy) return;
      const items = e.dataTransfer?.items;
      if (items && items.length && typeof (items[0] as any).webkitGetAsEntry === 'function') {
        const entries = Array.from(items)
          .map((it) => (it as any).webkitGetAsEntry?.())
          .filter(Boolean);
        const lists = await Promise.all(entries.map(readEntry));
        emit(lists.flat());
      } else {
        emit(Array.from(e.dataTransfer?.files || []));
      }
    },
    [busy, emit],
  );

  const openPicker = () => {
    if (!busy) inputRef.current?.click();
  };

  const dragProps = {
    onDragOver: (e: React.DragEvent) => {
      e.preventDefault();
      if (!dragOver) setDragOver(true);
    },
    onDragLeave: () => setDragOver(false),
    onDrop: handleDrop,
  };

  return (
    <>
      <input
        ref={inputRef}
        type="file"
        multiple
        accept={ACCEPT_ATTR}
        className="hidden"
        onChange={handleInputChange}
      />

      {variant === 'dropzone' ? (
        <button
          type="button"
          onClick={openPicker}
          disabled={busy}
          {...dragProps}
          className={`mt-1 w-full max-w-xs flex flex-col items-center gap-1 px-4 py-4 rounded-md border border-dashed transition-colors disabled:opacity-50 ${
            dragOver ? 'border-primary bg-primary/5' : 'border-border hover:bg-muted/50'
          }`}
        >
          {busy ? <Loader2 className="h-5 w-5 animate-spin" /> : <Upload className="h-5 w-5 opacity-70" />}
          <span className="text-sm text-foreground">{label}</span>
          <span className="text-xs text-muted-foreground">Click to choose files, or drop a folder here</span>
        </button>
      ) : (
        <button
          type="button"
          onClick={openPicker}
          disabled={busy}
          {...dragProps}
          title="Click to choose files, or drop a folder here"
          className={`inline-flex items-center gap-1 px-1.5 py-1 rounded-md border transition-colors text-foreground disabled:opacity-50 text-xs ${
            dragOver ? 'border-primary bg-primary/5' : 'border-border hover:bg-muted/50'
          }`}
        >
          {busy ? <Loader2 className="h-3 w-3 animate-spin" /> : <Upload className="h-3 w-3" />}
          <span>{label}</span>
        </button>
      )}
    </>
  );
};

export default SourceUploadButton;
