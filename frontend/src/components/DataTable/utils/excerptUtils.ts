import type { DataRow, FigureExcerpt, TextExcerpt } from '@/types';

import { extractDisplayValue, parsePythonString } from './valueUtils';

// Wider than just text now — a figure-backed citation (see FigureExcerpt) has
// no `text` at all, so every consumer of ParsedExcerpt must check
// `excerpt.type === 'figure'` before assuming `.text` exists.
export type ParsedExcerpt = TextExcerpt | FigureExcerpt;

const GROUNDING_STATUSES = new Set<TextExcerpt['grounding_status']>([
  'exact', 'case_insensitive', 'fuzzy', 'not_found',
]);

/** Runtime type guard for TextExcerpt['grounding_status'] -- an `as` cast
 *  alone would let any string through unchecked, silently lying to the
 *  type system about a value that came straight from an external payload. */
function isGroundingStatus(value: unknown): value is TextExcerpt['grounding_status'] {
  return typeof value === 'string' && GROUNDING_STATUSES.has(value as TextExcerpt['grounding_status']);
}

/**
 * Parse pipe-separated excerpt strings like: {'text': '...', 'source': '...'} | {'text': '...'}
 * Figure-typed objects (`{type: 'figure', figure_id, ...}`) are passed through
 * unchanged — they never arrive as pipe-separated/Python-dict strings (they
 * have no `text` to encode that way), only as real objects from parsed JSON.
 */
export function parseExcerpts(excerpts: unknown[]): ParsedExcerpt[] {
  const result: ParsedExcerpt[] = [];

  for (const exc of excerpts) {
    if (typeof exc === 'string') {
      if (exc.includes("'text':") || exc.includes('"text":')) {
        const parts = exc.split(/\s*\|\s*/);
        for (const part of parts) {
          const parsed = parsePythonString(part.trim());
          if (typeof parsed === 'object' && parsed !== null && 'text' in parsed) {
            const obj = parsed as Record<string, unknown>;
            result.push({
              text: String(obj.text || ''),
              source: String(obj.source || `Source ${result.length + 1}`),
            });
          } else if (typeof parsed === 'string' && parsed.trim()) {
            result.push({ text: parsed, source: `Source ${result.length + 1}` });
          }
        }
      } else if (exc.trim()) {
        result.push({ text: exc, source: `Source ${result.length + 1}` });
      }
    } else if (typeof exc === 'object' && exc !== null) {
      const obj = exc as Record<string, unknown>;
      if (obj.type === 'figure' && typeof obj.figure_id === 'string') {
        result.push({
          type: 'figure',
          figure_id: obj.figure_id,
          source: String(obj.source || `Source ${result.length + 1}`),
          ...(typeof obj.caption === 'string' ? { caption: obj.caption } : {}),
          ...(typeof obj.image_filename === 'string' ? { image_filename: obj.image_filename } : {}),
        });
      } else if ('text' in obj) {
        result.push({
          text: String(obj.text || ''),
          source: String(obj.source || `Source ${result.length + 1}`),
          ...(typeof obj.char_start === 'number' ? { char_start: obj.char_start } : {}),
          ...(typeof obj.char_end === 'number' ? { char_end: obj.char_end } : {}),
          ...(isGroundingStatus(obj.grounding_status) ? { grounding_status: obj.grounding_status } : {}),
        });
      }
    }
  }

  return result;
}

/** Map canonical column names to their parallel `{name}_excerpt` column keys. */
export function buildExcerptMapping(rows: Array<{ data?: Record<string, unknown> }>): Record<string, string> {
  const mapping: Record<string, string> = {};
  const allDataColumns = new Set<string>();

  rows.forEach((row) => {
    if (row.data) {
      Object.keys(row.data).forEach((key) => allDataColumns.add(key));
    }
  });

  Array.from(allDataColumns).forEach((col) => {
    if (col.endsWith('_excerpt')) {
      const baseColumn = col.replace('_excerpt', '');
      if (allDataColumns.has(baseColumn)) {
        mapping[baseColumn] = col;
      }
    }
  });

  return mapping;
}

/**
 * Normalize value to ScheMatiQ format with `answer` and `excerpts`.
 */
export function normalizeToScheMatiQ(val: unknown): unknown {
  if (!val || typeof val !== 'object') return val;

  if (Array.isArray(val) && val.length > 0 && typeof val[0] === 'object') {
    return normalizeToScheMatiQ(val[0]);
  }

  const obj = val as Record<string, unknown>;

  if ('answer' in obj) {
    let answerVal = obj.answer;
    let excerptsVal = obj.excerpts || [];

    if (typeof answerVal === 'string') {
      const parsed = parsePythonString(answerVal);
      if (parsed !== answerVal && typeof parsed === 'object' && parsed !== null) {
        if (Array.isArray(parsed) && parsed.length > 0) {
          const firstItem = parsed[0];
          if (typeof firstItem === 'object') {
            const item = firstItem as Record<string, unknown>;
            answerVal = item.value || item.answer || String(firstItem);
            const allExcerpts: unknown[] = [];
            for (const p of parsed) {
              const pObj = p as Record<string, unknown>;
              const exc = pObj.excerpt || pObj.excerpts;
              if (exc) {
                allExcerpts.push(...(Array.isArray(exc) ? exc : [exc]));
              }
            }
            if (allExcerpts.length > 0) {
              excerptsVal = allExcerpts;
            }
          }
        } else {
          answerVal = parsed;
        }
      }
    }

    const parsedExcerpts = parseExcerpts(excerptsVal as unknown[]);

    return {
      answer: answerVal,
      excerpts: parsedExcerpts,
      ...(obj.manually_edited ? { manually_edited: true } : {}),
    };
  }

  if ('value' in obj) {
    const excerptsRaw = obj.citation ? [obj.citation] :
      obj.excerpt ? [obj.excerpt] :
        (obj.excerpts || []);
    return {
      answer: obj.value,
      excerpts: parseExcerpts(excerptsRaw as unknown[]),
    };
  }

  if ('text' in obj) {
    return {
      answer: obj.text,
      excerpts: obj.source ? [{ text: String(obj.text), source: String(obj.source) }] : [],
    };
  }

  return val;
}

export type CellGrounding = { answer: string; excerpts: ParsedExcerpt[] };

/**
 * Resolve grounding for a data cell from inline `{ answer, excerpts }` objects
 * and/or parallel `{canonical}_excerpt` columns. Returns null when no excerpts exist.
 */
export function resolveCellGrounding(
  row: Pick<DataRow, 'data'>,
  canonicalName: string,
  mapping: Record<string, string>,
): CellGrounding | null {
  if (!row.data) return null;

  const rawValue = row.data[canonicalName];
  const excerptColumnName = mapping[canonicalName];
  const hasExcerptColumn = Boolean(excerptColumnName && row.data[excerptColumnName]);

  const getExcerptsFromColumn = (): ParsedExcerpt[] => {
    if (!hasExcerptColumn || !excerptColumnName) return [];
    return parseExcerpts([String(row.data[excerptColumnName])]);
  };

  let processedValue: unknown = typeof rawValue === 'string' ? parsePythonString(rawValue) : rawValue;

  if (typeof processedValue === 'object' && processedValue !== null) {
    processedValue = normalizeToScheMatiQ(processedValue);
  }

  let excerpts: ParsedExcerpt[] = [];
  let answer = extractDisplayValue(rawValue);

  if (typeof processedValue === 'object' && processedValue !== null && 'answer' in processedValue) {
    const obj = processedValue as { answer: unknown; excerpts?: ParsedExcerpt[] };
    answer = extractDisplayValue(obj.answer);
    excerpts = obj.excerpts || [];
    if (excerpts.length === 0 && hasExcerptColumn) {
      excerpts = getExcerptsFromColumn();
    }
  } else if (hasExcerptColumn) {
    excerpts = getExcerptsFromColumn();
  }

  if (excerpts.length === 0) return null;

  return { answer, excerpts };
}
