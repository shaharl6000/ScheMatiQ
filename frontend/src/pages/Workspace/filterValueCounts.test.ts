import { countFilterValues, filterValueKey, formatFilterValueLabel } from './filterValueCounts';

describe('filterValueCounts', () => {
  it('counts each value, with null/undefined/empty as one blank bucket', () => {
    const counts = countFilterValues(['district', 'appellate', 'district', '', null, undefined, 'district']);
    expect(counts.get(filterValueKey('district'))).toBe(3);
    expect(counts.get(filterValueKey('appellate'))).toBe(1);
    expect(counts.get(filterValueKey(''))).toBe(3);
    expect(counts.get(filterValueKey('supreme_court'))).toBeUndefined();
  });

  it('skips rows rejected by the other-column filters', () => {
    const counts = countFilterValues(['a', 'a', 'b', 'a'], (row) => row !== 1);
    expect(counts.get(filterValueKey('a'))).toBe(2);
    expect(counts.get(filterValueKey('b'))).toBe(1);
  });

  it('keys numbers and their string form together', () => {
    expect(filterValueKey(5)).toBe(filterValueKey('5'));
  });

  it('formats the label like Excel', () => {
    expect(formatFilterValueLabel('district', 12)).toBe('district (12)');
  });
});
