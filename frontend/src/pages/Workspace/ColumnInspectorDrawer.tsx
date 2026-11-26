// Slide-over Column Inspector Drawer for editing (or creating) a schema column
// directly from the Data tab — with full multi-line prompt editors, value_kind
// pills, interactive allowed-value tags, off-list value suggestions, and live
// column value distribution + health stats.
// Parent: Workspace (index.tsx).

import { useEffect, useMemo, useState } from 'react';
import {
  AlertTriangle,
  BarChart3,
  Check,
  Loader2,
  Plus,
  RotateCw,
  Trash2,
  X,
} from 'lucide-react';

import { extractDisplayValue } from '@/components/DataTable/utils/valueUtils';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet';
import { Textarea } from '@/components/ui/textarea';
import { useToast } from '@/components/ui/use-toast';
import { schemaAPI } from '@/services/api';
import type { ColumnInfo, PaginatedData } from '@/types';
import { formatColumnName } from '@/utils/formatting';

import { categoryAllowedKeys, isOffListValue } from './helpers';

const VALUE_KINDS: Array<{ id: string; label: string; hint: string }> = [
  { id: 'free_text', label: 'Free text', hint: 'Open-ended text or summary' },
  { id: 'category', label: 'Category', hint: 'Fixed set of allowed choices' },
  { id: 'identifier', label: 'Identifier', hint: 'Entity name, ID, or code' },
  { id: 'number', label: 'Number', hint: 'Numeric value or measurement' },
  { id: 'date', label: 'Date', hint: 'Date or year value' },
];

export function ColumnInspectorDrawer({
  open,
  mode,
  column,
  sessionId,
  data,
  onOpenChange,
  onSaved,
  onSaveAndReextract,
  onDeleteColumn,
}: {
  open: boolean;
  mode: 'create' | 'edit';
  column: ColumnInfo | null;
  sessionId?: string;
  data: PaginatedData;
  onOpenChange: (open: boolean) => void;
  onSaved: (columnName: string, isNew: boolean) => void;
  onSaveAndReextract: (columnName: string) => void;
  onDeleteColumn?: (columnName: string) => void;
}) {
  const { toast } = useToast();
  const [name, setName] = useState('');
  const [definition, setDefinition] = useState('');
  const [rationale, setRationale] = useState('');
  const [valueKind, setValueKind] = useState<string>('free_text');
  const [allowedValues, setAllowedValues] = useState<string[]>([]);
  const [newAllowedInput, setNewAllowedInput] = useState('');
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!open) return;
    if (mode === 'create' || !column) {
      setName('');
      setDefinition('');
      setRationale('');
      setValueKind('free_text');
      setAllowedValues([]);
      setNewAllowedInput('');
      return;
    }
    setName(column.display_name || column.name || '');
    setDefinition(column.definition || '');
    setRationale(column.rationale || '');
    setValueKind(column.value_kind || (column.allowed_values?.length ? 'category' : 'free_text'));
    setAllowedValues(Array.isArray(column.allowed_values) ? [...column.allowed_values] : []);
    setNewAllowedInput('');
  }, [open, mode, column]);

  // Live distribution & health stats for the edited column across loaded rows.
  const stats = useMemo(() => {
    if (mode !== 'edit' || !column) return null;
    const rows = data.rows || [];
    const total = rows.length;
    if (total === 0) return null;

    let filled = 0;
    let ungrounded = 0;
    let confirmedEmpty = 0;
    let offListCount = 0;
    const counts = new Map<string, number>();
    const offListValues = new Set<string>();
    const allowedSet = categoryAllowedKeys({
      ...column,
      value_kind: valueKind as ColumnInfo['value_kind'],
      allowed_values: allowedValues,
    });

    for (const row of rows) {
      const raw = row.data?.[column.name];
      const display = extractDisplayValue(raw).trim();
      if (raw && typeof raw === 'object') {
        if ((raw as { _ungrounded?: boolean })._ungrounded) ungrounded += 1;
        if ((raw as { _confirmed_empty?: boolean })._confirmed_empty) confirmedEmpty += 1;
      }
      if (display) {
        filled += 1;
        counts.set(display, (counts.get(display) ?? 0) + 1);
        if (allowedSet && isOffListValue(raw, allowedSet)) {
          offListCount += 1;
          offListValues.add(display);
        }
      }
    }

    const topValues = Array.from(counts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6);

    return {
      total,
      filled,
      completenessPct: Math.round((filled / total) * 100),
      uniqueCount: counts.size,
      ungrounded,
      confirmedEmpty,
      offListCount,
      offListValues: Array.from(offListValues).slice(0, 8),
      topValues,
    };
  }, [allowedValues, column, data.rows, mode, valueKind]);

  const addAllowedTag = (raw: string) => {
    const parts = raw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    if (parts.length === 0) return;
    setAllowedValues((prev) => {
      const seen = new Set(prev.map((v) => v.toLowerCase()));
      const next = [...prev];
      for (const p of parts) {
        if (!seen.has(p.toLowerCase())) {
          seen.add(p.toLowerCase());
          next.push(p);
        }
      }
      return next;
    });
    setNewAllowedInput('');
  };

  const removeAllowedTag = (val: string) => {
    setAllowedValues((prev) => prev.filter((item) => item !== val));
  };

  const persist = async (andReextract: boolean) => {
    if (!sessionId || saving) return;
    const trimmedName = name.trim();
    if (!trimmedName) {
      toast({
        title: 'Column name is required',
        variant: 'destructive',
      });
      return;
    }

    setSaving(true);
    try {
      if (mode === 'create' || !column) {
        await schemaAPI.addColumn(sessionId, {
          name: trimmedName,
          definition: definition.trim(),
          rationale: rationale.trim() || undefined,
          allowed_values: valueKind === 'category' && allowedValues.length > 0 ? allowedValues : undefined,
        });
        // Apply value_kind if non-default.
        if (valueKind && valueKind !== 'free_text') {
          try {
            await schemaAPI.editColumn(sessionId, {
              old_name: trimmedName,
              value_kind: valueKind as any,
              allowed_values: valueKind === 'category' ? allowedValues : undefined,
              reprocess: false,
            } as any);
          } catch {
            // Non-fatal: column was still created.
          }
        }
        toast({ title: 'Column added', description: trimmedName });
        onOpenChange(false);
        onSaved(trimmedName, true);
        if (andReextract) onSaveAndReextract(trimmedName);
      } else {
        const request: Record<string, any> = {
          old_name: column.name,
          definition: definition.trim(),
          rationale: rationale.trim(),
          value_kind: valueKind,
          allowed_values: valueKind === 'category' ? allowedValues : [],
          reprocess: false,
        };
        const currentDisplayed = column.display_name || column.name;
        if (trimmedName !== currentDisplayed && trimmedName !== column.name) {
          request.new_name = trimmedName;
        }
        const targetCol = request.new_name || column.name;
        await schemaAPI.editColumn(sessionId, request as any);
        toast({ title: 'Schema column updated', description: trimmedName });
        onOpenChange(false);
        onSaved(targetCol, false);
        if (andReextract) onSaveAndReextract(targetCol);
      }
    } catch (err: any) {
      toast({
        title: mode === 'create' ? 'Could not add column' : 'Could not update column',
        description: err?.response?.data?.detail || err?.message || 'Request failed.',
        variant: 'destructive',
      });
    } finally {
      setSaving(false);
    }
  };

  const selectedKindMeta = VALUE_KINDS.find((k) => k.id === valueKind) ?? VALUE_KINDS[0];

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full sm:max-w-md flex flex-col p-0 gap-0">
        <SheetHeader className="px-5 py-4 border-b">
          <SheetTitle className="text-base">
            {mode === 'create'
              ? 'New Schema Column'
              : `Column: ${column ? column.display_name || formatColumnName(column.name) : ''}`}
          </SheetTitle>
          <SheetDescription className="text-xs">
            {mode === 'create'
              ? 'Define what to extract from every document. You can extract values right after saving.'
              : 'Refine the extraction prompt, type, or allowed values without leaving the table.'}
          </SheetDescription>
        </SheetHeader>

        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
          {/* Column Name */}
          <div className="space-y-1.5">
            <Label htmlFor="col-inspector-name" className="text-xs font-semibold">
              Column name
            </Label>
            <Input
              id="col-inspector-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. Sample Size, Policy Outcome, Appointing President"
              className="h-9 text-sm"
            />
            {mode === 'edit' && column && column.display_name && column.display_name !== column.name && (
              <p className="text-[11px] text-muted-foreground">
                Canonical key: <code className="rounded bg-muted px-1">{column.name}</code>
              </p>
            )}
          </div>

          {/* Value Kind Pills */}
          <div className="space-y-1.5">
            <Label className="text-xs font-semibold">Value type</Label>
            <div className="grid grid-cols-3 gap-1.5">
              {VALUE_KINDS.map((kind) => {
                const active = valueKind === kind.id;
                return (
                  <button
                    key={kind.id}
                    type="button"
                    onClick={() => setValueKind(kind.id)}
                    className={`rounded-md border px-2.5 py-1.5 text-xs font-medium transition-colors text-left ${
                      active
                        ? 'border-primary bg-primary/10 text-primary'
                        : 'border-border bg-background text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                    }`}
                  >
                    {kind.label}
                  </button>
                );
              })}
            </div>
            <p className="text-[11px] text-muted-foreground">{selectedKindMeta.hint}</p>
          </div>

          {/* Definition */}
          <div className="space-y-1.5">
            <Label htmlFor="col-inspector-def" className="text-xs font-semibold">
              Extraction prompt (definition)
            </Label>
            <Textarea
              id="col-inspector-def"
              value={definition}
              onChange={(e) => setDefinition(e.target.value)}
              placeholder="Describe clearly what value should be extracted from each document and any edge-case rules…"
              rows={4}
              className="text-sm resize-y"
            />
          </div>

          {/* Allowed Values Tag Editor (Category) */}
          {valueKind === 'category' && (
            <div className="space-y-2 rounded-lg border bg-muted/25 p-3">
              <div className="flex items-center justify-between">
                <Label className="text-xs font-semibold">Allowed values</Label>
                <span className="text-[11px] text-muted-foreground">
                  {allowedValues.length} option{allowedValues.length === 1 ? '' : 's'}
                </span>
              </div>
              {allowedValues.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {allowedValues.map((val) => (
                    <span
                      key={val}
                      className="inline-flex items-center gap-1 rounded-full border border-border bg-background px-2.5 py-0.5 text-xs font-medium"
                    >
                      <span>{val}</span>
                      <button
                        type="button"
                        onClick={() => removeAllowedTag(val)}
                        className="text-muted-foreground hover:text-destructive"
                        aria-label={`Remove ${val}`}
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </span>
                  ))}
                </div>
              )}
              <div className="flex gap-1.5">
                <Input
                  value={newAllowedInput}
                  onChange={(e) => setNewAllowedInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ',') {
                      e.preventDefault();
                      addAllowedTag(newAllowedInput);
                    }
                  }}
                  placeholder="Type a value and press Enter…"
                  className="h-8 text-xs"
                />
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  className="h-8 px-2.5"
                  onClick={() => addAllowedTag(newAllowedInput)}
                  disabled={!newAllowedInput.trim()}
                >
                  <Plus className="h-3.5 w-3.5" />
                </Button>
              </div>
              {stats && stats.offListValues.length > 0 && (
                <div className="pt-1 border-t border-border/60 space-y-1.5">
                  <div className="flex items-center gap-1 text-[11px] font-medium text-amber-700 dark:text-amber-300">
                    <AlertTriangle className="h-3 w-3" />
                    <span>Off-list values found in table — click to allow:</span>
                  </div>
                  <div className="flex flex-wrap gap-1">
                    {stats.offListValues.map((val) => (
                      <button
                        key={val}
                        type="button"
                        onClick={() => addAllowedTag(val)}
                        className="inline-flex items-center gap-1 rounded-full border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 text-[11px] text-amber-800 dark:text-amber-200 hover:bg-amber-500/20"
                      >
                        <Plus className="h-2.5 w-2.5" />
                        <span>{val}</span>
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Rationale */}
          <div className="space-y-1.5">
            <Label htmlFor="col-inspector-rationale" className="text-xs font-semibold">
              Rationale <span className="font-normal text-muted-foreground">(optional notes)</span>
            </Label>
            <Textarea
              id="col-inspector-rationale"
              value={rationale}
              onChange={(e) => setRationale(e.target.value)}
              placeholder="Why this column matters for the research question…"
              rows={2}
              className="text-sm resize-y"
            />
          </div>

          {/* Live Value Distribution & Health Card */}
          {stats && (
            <div className="rounded-lg border bg-muted/20 p-3.5 space-y-3">
              <div className="flex items-center justify-between text-xs">
                <span className="inline-flex items-center gap-1.5 font-semibold text-foreground">
                  <BarChart3 className="h-3.5 w-3.5 text-primary" />
                  Column Health &amp; Distribution
                </span>
                <span className="font-semibold tabular-nums text-foreground">
                  {stats.completenessPct}% filled
                </span>
              </div>

              <div className="grid grid-cols-3 gap-2 text-center">
                <div className="rounded-md border bg-background px-2 py-1.5">
                  <div className="text-sm font-bold tabular-nums">{stats.filled}/{stats.total}</div>
                  <div className="text-[10px] text-muted-foreground">Filled</div>
                </div>
                <div className="rounded-md border bg-background px-2 py-1.5">
                  <div className="text-sm font-bold tabular-nums">{stats.uniqueCount}</div>
                  <div className="text-[10px] text-muted-foreground">Unique</div>
                </div>
                <div className="rounded-md border bg-background px-2 py-1.5">
                  <div className={`text-sm font-bold tabular-nums ${stats.ungrounded > 0 ? 'text-amber-600 dark:text-amber-400' : ''}`}>
                    {stats.ungrounded}
                  </div>
                  <div className="text-[10px] text-muted-foreground">Ungrounded</div>
                </div>
              </div>

              {stats.topValues.length > 0 && (
                <div className="space-y-1.5 pt-1">
                  <div className="text-[11px] font-medium text-muted-foreground">Top values</div>
                  {stats.topValues.map(([val, count]) => {
                    const pct = Math.max(4, Math.round((count / stats.filled) * 100));
                    return (
                      <div key={val} className="space-y-0.5">
                        <div className="flex items-center justify-between gap-2 text-[11px]">
                          <span className="truncate text-foreground" title={val}>{val}</span>
                          <span className="shrink-0 tabular-nums text-muted-foreground">{count}</span>
                        </div>
                        <div className="h-1.5 w-full rounded-full bg-muted overflow-hidden">
                          <div className="h-full rounded-full bg-primary/70" style={{ width: `${pct}%` }} />
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}
        </div>

        <div className="border-t px-5 py-3 flex items-center justify-between gap-2">
          {mode === 'edit' && column && onDeleteColumn ? (
            <Button
              type="button"
              size="sm"
              variant="ghost"
              className="text-destructive hover:text-destructive hover:bg-destructive/10 px-2"
              disabled={saving}
              onClick={() => {
                onOpenChange(false);
                onDeleteColumn(column.name);
              }}
              title="Delete this column"
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          ) : (
            <span />
          )}

          <div className="flex items-center gap-2">
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={saving || !name.trim()}
              onClick={() => void persist(false)}
            >
              {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Check className="h-3.5 w-3.5" />}
              Save
            </Button>
            <Button
              type="button"
              size="sm"
              disabled={saving || !name.trim()}
              onClick={() => void persist(true)}
            >
              {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RotateCw className="h-3.5 w-3.5" />}
              Save &amp; Re-extract
            </Button>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
}
