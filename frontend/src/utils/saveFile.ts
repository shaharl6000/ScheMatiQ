// "Save as" for exports: let the user pick the folder and file name.
//
// Uses the File System Access API (showSaveFilePicker) where the browser has it
// (Chrome, Edge). Elsewhere (Firefox, Safari) it falls back to a plain anchor
// download, which lands in the browser's default download folder.
//
// The picker is opened BEFORE the export request runs. Browsers only allow the
// picker during a short window after a user gesture, and a large export can
// take longer than that window. Picking first keeps the gesture valid.

type SavePickerType = { description: string; accept: Record<string, string[]> };

type WritableLike = {
  write: (data: Blob) => Promise<void>;
  close: () => Promise<void>;
  abort?: () => Promise<void>;
};

type FileHandleLike = {
  createWritable: () => Promise<WritableLike>;
  remove?: () => Promise<void>;
};

type SavePickerWindow = Window & {
  showSaveFilePicker?: (options: {
    suggestedName?: string;
    types?: SavePickerType[];
    id?: string;
  }) => Promise<FileHandleLike>;
};

const TYPES_BY_SUFFIX: Array<[string, SavePickerType]> = [
  ['.schematiq.json', { description: 'ScheMatiQ project', accept: { 'application/json': ['.json'] } }],
  ['.zip', { description: 'ZIP archive', accept: { 'application/zip': ['.zip'] } }],
  ['.csv', { description: 'CSV table', accept: { 'text/csv': ['.csv'] } }],
  ['.json', { description: 'JSON file', accept: { 'application/json': ['.json'] } }],
];

function pickerTypesFor(filename: string): SavePickerType[] | undefined {
  const lower = filename.toLowerCase();
  const match = TYPES_BY_SUFFIX.find(([suffix]) => lower.endsWith(suffix));
  return match ? [match[1]] : undefined;
}

function anchorDownload(blob: Blob, filename: string): void {
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.click();
  window.URL.revokeObjectURL(url);
}

export function supportsSavePicker(): boolean {
  return typeof (window as SavePickerWindow).showSaveFilePicker === 'function';
}

/**
 * Ask where to save, then fetch the content and write it there.
 *
 * Returns false when the user cancelled the picker (nothing was fetched or
 * written), true otherwise. Errors from `getBlob` are rethrown so callers keep
 * their existing error toasts.
 */
export async function saveBlobAs(
  suggestedName: string,
  getBlob: () => Promise<Blob>,
): Promise<boolean> {
  const picker = (window as SavePickerWindow).showSaveFilePicker;
  if (typeof picker !== 'function') {
    anchorDownload(await getBlob(), suggestedName);
    return true;
  }

  let handle: FileHandleLike;
  try {
    // `id` makes the browser reopen the folder used last time for exports.
    handle = await picker.call(window, {
      suggestedName,
      types: pickerTypesFor(suggestedName),
      id: 'schematiq-export',
    });
  } catch (err: any) {
    if (err?.name === 'AbortError') return false; // user cancelled
    // SecurityError / NotAllowedError (e.g. gesture expired, sandboxed frame):
    // fall back to a normal download rather than failing the export.
    anchorDownload(await getBlob(), suggestedName);
    return true;
  }

  let blob: Blob;
  try {
    blob = await getBlob();
  } catch (err) {
    // Don't leave an empty file behind in the chosen folder.
    try { await handle.remove?.(); } catch { /* best effort */ }
    throw err;
  }

  const writable = await handle.createWritable();
  try {
    await writable.write(blob);
    await writable.close();
  } catch (err) {
    try { await writable.abort?.(); } catch { /* best effort */ }
    throw err;
  }
  return true;
}
