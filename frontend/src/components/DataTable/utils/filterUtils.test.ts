/**
 * Tests for the 'regex' filter's guard: a user-typed pattern of the
 * catastrophic-backtracking shape, an invalid one, or an overly long one is
 * refused instead of freezing the tab.
 */

import { compileUserRegex, hasNestedQuantifier } from './filterUtils';

describe('hasNestedQuantifier', () => {
  it.each(['(a+)+', '(\\w*)*$', '(x{1,9})+', '((ab)+c)*', '(?:a|b+)+'])('flags %s', (pattern) => {
    expect(hasNestedQuantifier(pattern)).toBe(true);
  });

  it.each(['L\\d+A', '^(homo|mus) \\w+$', '(ab)+', '(a+)?', '[(a+)]+', '\\(a+\\)+', 'a{2,3}b*'])(
    'allows %s',
    (pattern) => {
      expect(hasNestedQuantifier(pattern)).toBe(false);
    },
  );
});

describe('compileUserRegex', () => {
  it('compiles an ordinary pattern, honouring case sensitivity', () => {
    expect(compileUserRegex('^homo', false)?.test('Homo sapiens')).toBe(true);
    expect(compileUserRegex('^homo', true)?.test('Homo sapiens')).toBe(false);
  });

  it('refuses invalid, overly long and backtracking patterns', () => {
    expect(compileUserRegex('(unclosed', false)).toBeNull();
    expect(compileUserRegex('a'.repeat(501), false)).toBeNull();
    expect(compileUserRegex('^(a+)+$', false)).toBeNull();
  });
});
