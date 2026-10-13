/**
 * Tests for the figure-citation handling added to excerptUtils.
 *
 * parseExcerpts previously silently dropped any excerpt object without a
 * `text` key — since resolveCellGrounding routes every object-shaped cell
 * value through it (via normalizeToScheMatiQ) unconditionally, a figure-only
 * cell would resolve to zero excerpts and `dataGrounding` would never see
 * it: no `›` indicator, nothing to click. These tests pin the fix and guard
 * against regressing it back to dropping figure excerpts.
 */

import { parseExcerpts, resolveCellGrounding, type ParsedExcerpt } from './excerptUtils';
import type { DataRow } from '@/types';

describe('parseExcerpts', () => {
  it('passes a figure-typed excerpt through unchanged', () => {
    const result = parseExcerpts([
      {
        type: 'figure',
        figure_id: 'paper1_fig003',
        source: 'paper1.pdf',
        caption: 'Fig 3. Accuracy by model size.',
        image_filename: 'fig003.png',
      },
    ]);

    expect(result).toEqual([
      {
        type: 'figure',
        figure_id: 'paper1_fig003',
        source: 'paper1.pdf',
        caption: 'Fig 3. Accuracy by model size.',
        image_filename: 'fig003.png',
      },
    ]);
  });

  it('still parses a plain text excerpt object (regression)', () => {
    const result = parseExcerpts([{ text: 'We propose Mamba...', source: 'paper1.pdf' }]);
    expect(result).toEqual([{ text: 'We propose Mamba...', source: 'paper1.pdf' }]);
  });

  it('still parses a plain string excerpt (regression)', () => {
    const result = parseExcerpts(['We propose Mamba...']);
    expect(result).toEqual([{ text: 'We propose Mamba...', source: 'Source 1' }]);
  });

  it('preserves backend grounding offsets (char_start/char_end/grounding_status) on a text excerpt', () => {
    const result = parseExcerpts([
      {
        text: 'We propose Mamba...',
        source: 'paper1.pdf',
        char_start: 120,
        char_end: 140,
        grounding_status: 'fuzzy',
      },
    ]);

    expect(result).toEqual([
      {
        text: 'We propose Mamba...',
        source: 'paper1.pdf',
        char_start: 120,
        char_end: 140,
        grounding_status: 'fuzzy',
      },
    ]);
  });

  it('rejects an unrecognized grounding_status value instead of trusting it blindly', () => {
    // grounding_status comes straight from an external payload -- an `as`
    // cast alone would let ANY string through as if it were one of the 4
    // known literals. Validate it instead of lying to the type system.
    const result = parseExcerpts([
      { text: 'We propose Mamba...', source: 'paper1.pdf', grounding_status: 'some_future_status' },
    ]);
    expect('grounding_status' in result[0]).toBe(false);
  });

  it('omits grounding offset fields when absent rather than inventing them', () => {
    const result = parseExcerpts([{ text: 'We propose Mamba...', source: 'paper1.pdf' }]);
    expect('char_start' in result[0]).toBe(false);
    expect('char_end' in result[0]).toBe(false);
    expect('grounding_status' in result[0]).toBe(false);
  });

  it('keeps both a text and a figure excerpt from the same array', () => {
    const result = parseExcerpts([
      { text: 'We propose Mamba...', source: 'paper1.pdf' },
      { type: 'figure', figure_id: 'paper1_fig003', source: 'paper1.pdf' },
    ]);

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({ text: 'We propose Mamba...', source: 'paper1.pdf' });
    expect((result[1] as { type?: string }).type).toBe('figure');
  });

  it('defaults the figure source label when source is missing', () => {
    const result = parseExcerpts([{ type: 'figure', figure_id: 'paper1_fig003' }]);
    expect(result[0].source).toBe('Source 1');
  });

  it('drops an object claiming type "figure" with no figure_id (malformed)', () => {
    // Falls through to the plain-object branch, which itself requires
    // `text` to keep the entry -- an object with neither is correctly
    // dropped, matching the pre-existing (intentional) behavior for any
    // excerpt object missing usable content.
    const result = parseExcerpts([{ type: 'figure', source: 'paper1.pdf' }]);
    expect(result).toEqual([]);
  });

  it('omits caption/image_filename when absent rather than inventing them', () => {
    const result = parseExcerpts([{ type: 'figure', figure_id: 'paper1_fig003', source: 'paper1.pdf' }]);
    expect(result[0]).toEqual({ type: 'figure', figure_id: 'paper1_fig003', source: 'paper1.pdf' });
    expect('caption' in result[0]).toBe(false);
    expect('image_filename' in result[0]).toBe(false);
  });
});

describe('resolveCellGrounding', () => {
  const mapping = {};

  it('returns grounding for a cell whose only excerpt is a figure', () => {
    // This is the critical regression test: before the fix, a figure-only
    // cell resolved to excerpts.length === 0 here and the function returned
    // null -- the grid's `›` has-grounding indicator would never show.
    const row: Pick<DataRow, 'data'> = {
      data: {
        model_type: {
          answer: 'mamba',
          excerpts: [{ type: 'figure', figure_id: 'paper1_fig003', source: 'paper1.pdf' }],
        },
      },
    };

    const grounding = resolveCellGrounding(row, 'model_type', mapping);

    expect(grounding).not.toBeNull();
    expect(grounding!.excerpts).toHaveLength(1);
    expect((grounding!.excerpts[0] as ParsedExcerpt & { type?: string }).type).toBe('figure');
  });

  it('returns grounding with both a text and a figure excerpt', () => {
    const row: Pick<DataRow, 'data'> = {
      data: {
        model_type: {
          answer: 'mamba',
          excerpts: [
            { text: 'We propose Mamba...', source: 'paper1.pdf' },
            { type: 'figure', figure_id: 'paper1_fig003', source: 'paper1.pdf' },
          ],
        },
      },
    };

    const grounding = resolveCellGrounding(row, 'model_type', mapping);

    expect(grounding!.excerpts).toHaveLength(2);
  });

  it('still returns null for a cell with no excerpts at all (regression)', () => {
    const row: Pick<DataRow, 'data'> = {
      data: { model_type: { answer: 'mamba', excerpts: [] } },
    };
    expect(resolveCellGrounding(row, 'model_type', mapping)).toBeNull();
  });

  it('still resolves a plain text-only cell (regression)', () => {
    const row: Pick<DataRow, 'data'> = {
      data: {
        model_type: { answer: 'mamba', excerpts: [{ text: 'We propose Mamba...', source: 'paper1.pdf' }] },
      },
    };
    const grounding = resolveCellGrounding(row, 'model_type', mapping);
    expect(grounding!.excerpts).toEqual([{ text: 'We propose Mamba...', source: 'paper1.pdf' }]);
  });
});
