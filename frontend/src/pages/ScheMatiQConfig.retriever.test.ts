import type { RetrieverConfig } from '../types';

jest.mock('@/services/api', () => ({}));
jest.mock('../services/api', () => ({}));

import { normalizeRestoredRetriever } from './ScheMatiQConfig';

describe('normalizeRestoredRetriever', () => {
  it('returns undefined when no retriever config is present', () => {
    expect(normalizeRestoredRetriever(undefined)).toBeUndefined();
  });

  it('leaves restored legacy passage_chars configs unchanged for backend conversion (#675)', () => {
    const legacy: RetrieverConfig = {
      type: 'embedding',
      model_name: 'all-MiniLM-L6-v2',
      passage_chars: 1200,
      overlap: 64,
      k: 4,
      enable_dynamic_k: false,
      dynamic_k_threshold: 0.65,
      dynamic_k_minimum: 3,
    };

    const restored = normalizeRestoredRetriever(legacy);
    expect(restored).toEqual(legacy);
    expect(restored?.passage_chars).toBe(1200);
    expect(restored?.passage_words).toBeUndefined();
  });

  it('preserves explicit passage_words configs unchanged', () => {
    const modern: RetrieverConfig = {
      type: 'embedding',
      model_name: 'BAAI/bge-m3',
      passage_words: 256,
      overlap: 48,
      k: 10,
      enable_dynamic_k: true,
      dynamic_k_threshold: 0.7,
      dynamic_k_minimum: 3,
    };

    expect(normalizeRestoredRetriever(modern)).toEqual(modern);
  });
});
