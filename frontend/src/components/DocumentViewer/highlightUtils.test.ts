import { findHighlightRange, findHighlightRanges } from './highlightUtils';

describe('findHighlightRange', () => {
  it('finds an exact case-insensitive match', () => {
    const text = 'The quick brown fox jumps over the lazy dog.';
    expect(findHighlightRange(text, 'BROWN fox')).toEqual([10, 19]);
  });

  it('finds a whitespace/quote-normalized match', () => {
    const text = 'The model achieved  86.4%  accuracy on MMLU.';
    expect(findHighlightRange(text, '"achieved 86.4% accuracy"')).not.toBeNull();
  });

  it('returns null when nothing resembles the query, even via the prefix fallback', () => {
    const text = 'This paper discusses climate change and rainfall patterns.';
    expect(findHighlightRange(text, 'GPT-4 achieves 86.4% accuracy on MMLU')).toBeNull();
  });
});

describe('findHighlightRanges', () => {
  const text = 'Intro. The model achieved an accuracy of 86.4 percent on MMLU. Outro.';

  it('uses a valid char_start/char_end offset directly instead of re-searching text', () => {
    const start = text.indexOf('accuracy of 86.4 percent on');
    const end = start + 'accuracy of 86.4 percent on'.length;
    // A paraphrase ("86.4%" vs the source's "86.4 percent") that this
    // module's own text search cannot find on its own -- confirm that first.
    expect(findHighlightRange(text, 'accuracy of 86.4% on MMLU')).toBeNull();

    const ranges = findHighlightRanges(text, [
      { text: 'accuracy of 86.4% on MMLU', char_start: start, char_end: end },
    ]);
    expect(ranges).toEqual([[start, end]]);
  });

  it('falls back to text search when char_start/char_end are absent', () => {
    const ranges = findHighlightRanges(text, [{ text: 'accuracy of 86.4 percent on' }]);
    expect(ranges).toHaveLength(1);
    expect(text.slice(...ranges[0])).toBe('accuracy of 86.4 percent on');
  });

  it('falls back to text search when the offset is out of range', () => {
    const ranges = findHighlightRanges(text, [
      { text: 'accuracy of 86.4 percent on', char_start: 9999, char_end: 10010 },
    ]);
    expect(ranges).toHaveLength(1);
    expect(text.slice(...ranges[0])).toBe('accuracy of 86.4 percent on');
  });

  it('uses the offset even when the excerpt text carries a surrounding quote', () => {
    // Excerpts routinely arrive quote-wrapped, and this one is ALSO a
    // paraphrase ("86.4%" vs "86.4 percent") that findHighlightRange can
    // never locate on its own (proven in the earlier test above) -- so
    // this only passes if the offset path itself tolerates the leading
    // quote when checking "does this offset look right."
    const start = text.indexOf('accuracy of 86.4 percent on');
    const end = start + 'accuracy of 86.4 percent on'.length;
    const ranges = findHighlightRanges(text, [
      { text: '"accuracy of 86.4% on MMLU', char_start: start, char_end: end },
    ]);
    expect(ranges).toEqual([[start, end]]);
  });

  it('falls back to text search when the offset points at unrelated text (stale offset guard)', () => {
    // char_start/char_end are in-range but point at "Intro." while the
    // excerpt text is about something else entirely -- the first-word sanity
    // check should reject this offset and fall back to a real search.
    const ranges = findHighlightRanges(text, [
      { text: 'accuracy of 86.4 percent on', char_start: 0, char_end: 6 },
    ]);
    expect(ranges).toHaveLength(1);
    expect(text.slice(...ranges[0])).toBe('accuracy of 86.4 percent on');
  });

  it('falls back to the raw offset when neither the sanity check nor a fresh text search finds it', () => {
    // char_start/char_end are in-range but point at "Intro." (fails the
    // first-word sanity check), and the excerpt text itself doesn't appear
    // anywhere in the document (fragment re-search also comes up empty).
    // The stored offset should still be used rather than the citation
    // being dropped entirely.
    const ranges = findHighlightRanges(text, [
      { text: 'a phrase that never appears in the source text', char_start: 0, char_end: 6 },
    ]);
    expect(ranges).toEqual([[0, 6]]);
  });

  it('drops overlapping ranges, keeping the first', () => {
    const ranges = findHighlightRanges(text, [{ text: 'accuracy of' }, { text: 'accuracy of 86.4 percent on' }]);
    expect(ranges).toHaveLength(1);
  });

  it('returns an empty array when text or excerpts are missing', () => {
    expect(findHighlightRanges(null, [{ text: 'x' }])).toEqual([]);
    expect(findHighlightRanges(text, null)).toEqual([]);
    expect(findHighlightRanges(text, [])).toEqual([]);
  });

  it('produces identical sorted ranges across multiple normalized and ellipsis-split excerpts while lowercasing the document at most once', () => {
    const richDoc = [
      'Section 1.   The \u201cbaseline\u201d architecture   achieved 71.2% F1   on dev.',
      'Intermediate paragraph with recombina-\ntion rates and T-cell counts.',
      'Section 2.   Our proposed\u2014hybrid model reached   89.7% F1   on test, outperforming prior work significantly.',
    ].join('\n\n');

    const excerpts = [
      { text: '"baseline" architecture achieved 71.2% F1 ... proposed-hybrid model reached 89.7% F1' },
      { text: 'recombination rates and T-cell counts' },
      { text: 'Our proposed-hybrid model reached 89.7% F1 on test, outperforming divergent tail wording here' },
    ];

    const expectedUnmerged = [
      findHighlightRange(richDoc, '"baseline" architecture achieved 71.2% F1'),
      findHighlightRange(richDoc, 'proposed-hybrid model reached 89.7% F1'),
      findHighlightRange(richDoc, 'recombination rates and T-cell counts'),
      findHighlightRange(
        richDoc,
        'Our proposed-hybrid model reached 89.7% F1 on test, outperforming divergent tail wording here',
      ),
    ].filter((r): r is [number, number] => r !== null);

    expect(expectedUnmerged).toHaveLength(4);

    // Compute expected sorted + overlap-deduped ranges from individual calls
    const sorted = [...expectedUnmerged].sort((a, b) => a[0] - b[0]);
    const expected: [number, number][] = [];
    let lastEnd = -1;
    for (const [s, e] of sorted) {
      if (s >= lastEnd) {
        expected.push([s, e]);
        lastEnd = e;
      }
    }

    const toLowerSpy = jest.spyOn(String.prototype, 'toLowerCase');
    try {
      const actual = findHighlightRanges(richDoc, excerpts);
      expect(actual).toEqual(expected);
      const docLowerCalls = toLowerSpy.mock.instances.filter((inst) => String(inst) === richDoc).length;
      expect(docLowerCalls).toBe(1);
    } finally {
      toLowerSpy.mockRestore();
    }
  });
});
