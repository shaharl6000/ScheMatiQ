/**
 * Value-constraint presets for dates. Backend stores tokens like `date`, `date:long`.
 */
export const DATE_PRESET_BUTTONS: {
  token: string;
  label: string;
  title: string;
}[] = [
  {
    token: 'date',
    label: 'yyyy-mm-dd',
    title: 'Saves dates like 2025-06-15. Text in documents can be normalized to this format.',
  },
  {
    token: 'date:long',
    label: 'Month day, year (e.g. May 6, 2025)',
    title: 'Saves dates with the month name spelled out, e.g. May 6, 2025.',
  },
];

/** Readable badge text for stored tokens (includes legacy presets from older UI). */
export function formatConstraintBadgeDisplay(stored: string): string {
  if (stored === 'date' || stored === 'date:iso') {
    return 'yyyy-mm-dd';
  }
  if (stored === 'date:long') {
    return 'Month day, year (e.g. May 6, 2025)';
  }
  if (stored === 'date:us') {
    return 'mm/dd/yyyy';
  }
  if (stored === 'date:eu') {
    return 'dd/mm/yyyy';
  }
  return stored;
}
