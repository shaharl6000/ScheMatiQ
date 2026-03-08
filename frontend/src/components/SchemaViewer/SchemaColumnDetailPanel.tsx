import React from 'react';
import { X, Pencil, Trash2, AlertTriangle, Loader2, Undo2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';
import { ColumnInfo, SchemaChangeStatus } from '../../types';
import { formatColumnName } from '../../utils/formatting';

interface SchemaColumnDetailPanelProps {
  column: ColumnInfo | null;
  isOpen: boolean;
  onClose: () => void;
  onEdit: (column: ColumnInfo) => void;
  onDelete: (columnName: string) => void;
  onRevert?: (columnName: string, baseline: { definition?: string; rationale?: string; allowed_values?: string[]; old_name?: string }) => void;
  readonly: boolean;
  schemaChanges?: SchemaChangeStatus | null;
  processingColumns?: Set<string>;
  sessionType?: 'load' | 'schematiq';
}

const SchemaColumnDetailPanel: React.FC<SchemaColumnDetailPanelProps> = ({
  column,
  isOpen,
  onClose,
  onEdit,
  onDelete,
  onRevert,
  readonly,
  schemaChanges,
  processingColumns,
  sessionType
}) => {
  if (!column) return null;

  const isModified = schemaChanges?.changed_columns?.includes(column.name);
  const isNew = schemaChanges?.new_columns?.includes(column.name) && !schemaChanges?.missing_baseline;
  const isProcessing = processingColumns?.has(column.name);
  const changeDetail = schemaChanges?.column_changes?.[column.name];

  return (
    <>
      {/* Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/20 z-40"
          onClick={onClose}
        />
      )}

      {/* Panel */}
      <div
        className={cn(
          "fixed right-0 top-0 h-full w-[400px] bg-background border-l shadow-xl z-50",
          "transform transition-transform duration-300 ease-in-out",
          isOpen ? "translate-x-0" : "translate-x-full"
        )}
      >
        <div className="flex flex-col h-full">
          {/* Header */}
          <div className="flex items-center justify-between p-4 border-b">
            <div className="flex items-center gap-2 flex-wrap flex-1 mr-2">
              <h3 className="font-semibold text-lg truncate">
                {formatColumnName(column.name)}
              </h3>
              {isModified && (
                <Badge variant="outline" className="text-xs bg-amber-50 dark:bg-amber-950 text-amber-700 dark:text-amber-300 border-amber-300">
                  Modified
                </Badge>
              )}
              {isNew && (
                <Badge variant="outline" className="text-xs bg-green-50 dark:bg-green-950 text-green-700 dark:text-green-300 border-green-300">
                  New
                </Badge>
              )}
              {isProcessing && (
                <Badge variant="outline" className="text-xs bg-blue-50 dark:bg-blue-950 text-blue-700 dark:text-blue-300 border-blue-300">
                  <Loader2 className="h-3 w-3 animate-spin mr-1" />
                  Extracting...
                </Badge>
              )}
            </div>
            <Button variant="ghost" size="icon" onClick={onClose}>
              <X className="h-5 w-5" />
            </Button>
          </div>

          {/* Scrollable Content */}
          <ScrollArea className="flex-1">
            <div className="p-4 space-y-4">
              {/* Data Type */}
              {column.data_type && (
                <div>
                  <p className="text-xs font-semibold text-muted-foreground mb-1">Data Type</p>
                  <Badge>{column.data_type}</Badge>
                </div>
              )}

              {/* Definition */}
              {column.definition && (
                <div className="p-3 bg-muted rounded-md">
                  <p className="text-xs font-semibold text-primary mb-1">Definition</p>
                  <p className="text-sm">{column.definition}</p>
                </div>
              )}

              {/* Rationale */}
              {column.rationale && (
                <div className="p-3 bg-blue-50 dark:bg-blue-950 rounded-md border border-blue-200 dark:border-blue-800">
                  <p className="text-xs font-semibold text-blue-700 dark:text-blue-300 mb-1">Rationale</p>
                  <p className="text-sm">{column.rationale}</p>
                </div>
              )}

              {/* Allowed Values */}
              {column.allowed_values && column.allowed_values.length > 0 ? (
                <div className="p-3 bg-purple-50 dark:bg-purple-950 rounded-md border border-purple-200 dark:border-purple-800">
                  <p className="text-xs font-semibold text-purple-700 dark:text-purple-300 mb-2">
                    {column.allowed_values.length === 1 && column.allowed_values[0].toLowerCase() === 'number'
                      ? 'Numeric Constraint'
                      : column.allowed_values.length === 1 && /^-?\d+(\.\d+)?--?\d+(\.\d+)?$/.test(column.allowed_values[0])
                        ? 'Range Constraint'
                        : 'Allowed Values'}
                  </p>
                  <div className="flex flex-wrap gap-1">
                    {column.allowed_values.length === 1 && column.allowed_values[0].toLowerCase() === 'number' ? (
                      <Badge variant="outline" className="text-xs bg-blue-100 dark:bg-blue-900 border-blue-300 dark:border-blue-700 text-blue-700 dark:text-blue-300">
                        Any Number (int/float)
                      </Badge>
                    ) : column.allowed_values.length === 1 && /^(-?\d+(\.\d+)?)-(-?\d+(\.\d+)?)$/.test(column.allowed_values[0]) ? (
                      <Badge variant="outline" className="text-xs bg-blue-100 dark:bg-blue-900 border-blue-300 dark:border-blue-700 text-blue-700 dark:text-blue-300">
                        Range: {column.allowed_values[0]}
                      </Badge>
                    ) : (
                      column.allowed_values.map((value, idx) => (
                        <Badge key={idx} variant="outline" className="text-xs bg-purple-100 dark:bg-purple-900 border-purple-300 dark:border-purple-700">
                          {value}
                        </Badge>
                      ))
                    )}
                  </div>
                </div>
              ) : (
                <div className="p-2 bg-gray-50 dark:bg-gray-900 rounded-md border border-gray-200 dark:border-gray-700">
                  <p className="text-xs text-gray-500 dark:text-gray-400 italic">
                    Free-form (any value accepted)
                  </p>
                </div>
              )}

              {/* Pending Values */}
              {column.pending_values && column.pending_values.length > 0 && !readonly && (
                <div className="p-3 bg-amber-50 dark:bg-amber-950 rounded-md border border-amber-200 dark:border-amber-800">
                  <p className="text-xs font-semibold text-amber-700 dark:text-amber-300 mb-2 flex items-center gap-1">
                    <AlertTriangle className="h-3 w-3" />
                    New Values Detected ({column.pending_values.length})
                  </p>
                  <div className="space-y-1">
                    {column.pending_values.map((pv, idx) => (
                      <div key={idx} className="flex items-center justify-between text-xs">
                        <span className="font-medium">{pv.value}</span>
                        <span className="text-amber-600 dark:text-amber-400">
                          {pv.document_count} doc{pv.document_count > 1 ? 's' : ''}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Missing metadata warning */}
              {sessionType === 'load' && (!column.definition || !column.rationale) && (
                <div className="p-2 bg-yellow-50 dark:bg-yellow-950 rounded-md border border-yellow-200 dark:border-yellow-800">
                  <p className="text-xs text-yellow-700 dark:text-yellow-300 flex items-center gap-1">
                    <AlertTriangle className="h-3 w-3" />
                    {!column.definition && !column.rationale
                      ? 'Missing definition and rationale'
                      : !column.definition
                        ? 'Missing definition'
                        : 'Missing rationale'}
                  </p>
                </div>
              )}

              {/* Statistics */}
              {(column.non_null_count != null || column.unique_count != null) && (
                <div>
                  <p className="text-xs font-semibold text-muted-foreground mb-2">Statistics</p>
                  <div className="flex gap-2">
                    {column.non_null_count != null && (
                      <Badge variant="outline">{column.non_null_count} non-null</Badge>
                    )}
                    {column.unique_count != null && (
                      <Badge variant="outline">{column.unique_count} unique</Badge>
                    )}
                  </div>
                </div>
              )}

              {/* Change details */}
              {isModified && changeDetail && (
                <div className="p-3 bg-amber-50 dark:bg-amber-950 rounded-md border border-amber-200 dark:border-amber-800">
                  <p className="text-xs font-semibold text-amber-700 dark:text-amber-300 mb-1">Change Details</p>
                  <p className="text-sm text-amber-600 dark:text-amber-400">
                    {changeDetail.change_type === 'definition' && 'Definition was changed'}
                    {changeDetail.change_type === 'rationale' && 'Rationale was changed'}
                    {changeDetail.change_type === 'allowed_values' && 'Allowed values were changed'}
                    {!changeDetail.change_type && 'Column modified since last extraction'}
                  </p>
                </div>
              )}
            </div>
          </ScrollArea>

          {/* Footer Actions */}
          {!readonly && (
            <div className="p-4 border-t flex gap-2">
              <Button onClick={() => onEdit(column)} className="flex-1">
                <Pencil className="h-4 w-4 mr-2" />
                Edit
              </Button>
              {isModified && !isNew && onRevert && changeDetail && (changeDetail.old_definition != null || changeDetail.old_rationale != null || changeDetail.old_allowed_values != null) && (
                <Button
                  variant="outline"
                  onClick={() => onRevert(column.name, {
                    definition: changeDetail.old_definition ?? undefined,
                    rationale: changeDetail.old_rationale ?? undefined,
                    allowed_values: changeDetail.old_allowed_values ?? undefined,
                    old_name: changeDetail.old_name ?? undefined,
                  })}
                >
                  <Undo2 className="h-4 w-4" />
                </Button>
              )}
              <Button
                variant="destructive"
                onClick={() => onDelete(column.name)}
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>
          )}
        </div>
      </div>
    </>
  );
};

export default SchemaColumnDetailPanel;
