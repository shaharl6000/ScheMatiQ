import React, { useState, useRef, useEffect, useCallback } from 'react';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import { Check, X, Pencil } from 'lucide-react';
import { CellValue } from '../../types';

interface EditableCellProps {
  value: CellValue;
  rowName: string;
  column: string;
  children: React.ReactNode;
  onSave: (rowName: string, column: string, value: string) => Promise<void>;
  disabled?: boolean;
}

/**
 * Extracts the editable string value from a CellValue.
 * Handles QBSD objects with 'answer' field, arrays, etc.
 */
function getEditableValue(value: CellValue): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  if (typeof value === 'object' && 'answer' in value) {
    const answer = (value as { answer: unknown }).answer;
    if (answer === null || answer === undefined) return '';
    if (typeof answer === 'object') return JSON.stringify(answer);
    return String(answer);
  }
  if (Array.isArray(value)) return value.join(', ');
  return JSON.stringify(value);
}

/**
 * A cell wrapper that enables inline editing on double-click.
 */
const EditableCell: React.FC<EditableCellProps> = ({
  value,
  rowName,
  column,
  children,
  onSave,
  disabled = false,
}) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editValue, setEditValue] = useState('');
  const [isSaving, setIsSaving] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  // Extract editable value when entering edit mode
  const startEditing = useCallback(() => {
    if (disabled) return;
    setEditValue(getEditableValue(value));
    setIsEditing(true);
  }, [value, disabled]);

  // Focus input when entering edit mode
  useEffect(() => {
    if (isEditing && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [isEditing]);

  const [error, setError] = useState<string | null>(null);

  const handleSave = async () => {
    if (isSaving) return;
    const originalValue = getEditableValue(value);
    if (editValue === originalValue) {
      setIsEditing(false);
      return;
    }
    setIsSaving(true);
    setError(null);
    try {
      await onSave(rowName, column, editValue);
      setIsEditing(false);
    } catch (err) {
      console.error('Failed to save cell:', err);
      setError('Failed to save. Please try again.');
      // Keep editing mode open on error
    } finally {
      setIsSaving(false);
    }
  };

  const handleCancel = () => {
    setIsEditing(false);
    setEditValue('');
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSave();
    } else if (e.key === 'Escape') {
      handleCancel();
    }
  };

  if (isEditing) {
    return (
      <div className="flex flex-col gap-1 min-w-[100px]">
        <div className="flex items-center gap-1">
          <Input
            ref={inputRef}
            value={editValue}
            onChange={(e) => setEditValue(e.target.value)}
            onKeyDown={handleKeyDown}
            onBlur={() => {
              // Small delay to allow button clicks to register
              setTimeout(() => {
                if (!isSaving) handleCancel();
              }, 150);
            }}
            disabled={isSaving}
            className={cn("h-7 text-sm py-0 px-2", error && "border-red-500")}
          />
          <button
            type="button"
            onClick={handleSave}
            disabled={isSaving}
            className="p-1 hover:bg-green-100 dark:hover:bg-green-900 rounded text-green-600"
            title="Save (Enter)"
          >
            <Check className="h-4 w-4" />
          </button>
          <button
            type="button"
            onClick={handleCancel}
            disabled={isSaving}
            className="p-1 hover:bg-red-100 dark:hover:bg-red-900 rounded text-red-600"
            title="Cancel (Escape)"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        {error && <span className="text-xs text-red-500">{error}</span>}
      </div>
    );
  }

  return (
    <div
      className={cn(
        "group relative",
        !disabled && "cursor-pointer"
      )}
      onDoubleClick={startEditing}
      title={disabled ? undefined : "Double-click to edit"}
    >
      {!disabled && (
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            startEditing();
          }}
          className="flex-shrink-0 p-0.5 opacity-0 group-hover:opacity-100 transition-opacity hover:bg-muted rounded mt-0.5 hidden group-hover:inline-flex absolute -left-5"
          title="Edit"
        >
          <Pencil className="h-3 w-3 text-muted-foreground" />
        </button>
      )}
      <div className="min-w-0">{children}</div>
    </div>
  );
};

export default EditableCell;

