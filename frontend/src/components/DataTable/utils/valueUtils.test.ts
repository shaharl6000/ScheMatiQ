/**
 * Tests for parsePythonString's last-resort conversion of a Python repr to JSON:
 * None/True/False are replaced as whole words only, so words that merely start
 * with them ("Nonetheless", "Trueman") survive.
 */

import { parsePythonString } from './valueUtils';

describe('parsePythonString', () => {
  it('parses JSON as is', () => {
    expect(parsePythonString('{"a": 1}')).toEqual({ a: 1 });
  });

  it('converts Python literals to JSON', () => {
    expect(parsePythonString("['x', None, True, False]")).toEqual(['x', null, true, false]);
  });

  it('leaves words that only start with None/True/False alone', () => {
    expect(parsePythonString("['Nonetheless', 'Trueman', 'Falsehood', None]")).toEqual([
      'Nonetheless',
      'Trueman',
      'Falsehood',
      null,
    ]);
  });

  it("reads an excerpt's whole text, quotes and braces included", () => {
    expect(parsePythonString("{'text': 'it's in the 'core'} region', 'source': 'a.pdf'}")).toEqual({
      text: "it's in the 'core'} region",
      source: 'a.pdf',
    });
    expect(parsePythonString("{'text': 'no source, it's fine'}")).toEqual({ text: "no source, it's fine" });
  });

  it('returns non-object strings unchanged', () => {
    expect(parsePythonString('plain text')).toBe('plain text');
  });
});
