import React from 'react';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';

import DocumentPreview from './DocumentPreview';

// Mock the api module so we don't pull axios/env config, and can assert which
// endpoint the reference path calls. NOTE: CRA's jest config sets resetMocks:true,
// which strips jest.fn(impl) factory implementations before every test — so the
// URL builders' implementations are (re)assigned in beforeEach, not here.
const mockGetContentText = jest.fn();
const mockGetContentUrl = jest.fn();
const mockGetDocumentContentText = jest.fn();
const mockGetDocumentContentUrl = jest.fn();

jest.mock('../../services/api', () => ({
  referenceAPI: {
    getContentText: (...args: [string, string]) => mockGetContentText(...args),
    getContentUrl: (...args: [string, string]) => mockGetContentUrl(...args),
  },
  unitsAPI: {
    getDocumentContentText: (...args: [string, string]) => mockGetDocumentContentText(...args),
    getDocumentContentUrl: (...args: [string, string]) => mockGetDocumentContentUrl(...args),
  },
}));

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  // Re-establish implementations after resetMocks wipes them.
  mockGetContentUrl.mockImplementation(
    (sessionId: string, referenceId: string) =>
      `http://test/reference/${sessionId}/${referenceId}/content`,
  );
  mockGetDocumentContentUrl.mockImplementation(
    (sessionId: string, name: string) => `http://test/units/doc/${sessionId}?name=${name}`,
  );
  // Availability probe uses fetch HEAD; make it succeed.
  (global as unknown as { fetch: jest.Mock }).fetch = jest.fn(() =>
    Promise.resolve({ ok: true } as Response),
  );
  // scrollIntoView isn't implemented in jsdom.
  (window.HTMLElement.prototype as unknown as { scrollIntoView: () => void }).scrollIntoView =
    jest.fn();
});

afterEach(() => {
  act(() => root.unmount());
  container.remove();
});

// Two async hops happen in sequence: the HEAD availability probe resolves and
// sets state, then a re-render fires the text-fetch effect. Yield to the macrotask
// queue a few times, each inside act(), so both settle and React flushes.
const flush = async () => {
  for (let i = 0; i < 5; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
  }
};

test('reference mode fetches reference text, renders it, and highlights the passage', async () => {
  const refText =
    'name,appointing_president\nWilliam Acker,Nixon\nJane Doe,Reagan';
  mockGetContentText.mockResolvedValue(refText);

  await act(async () => {
    root.render(
      <DocumentPreview
        sessionId="s1"
        documentName="ruling.pdf"
        referenceDoc={{ id: 'ref-123', filename: 'judges.csv' }}
        highlightTexts={['name,appointing_president\nWilliam Acker,Nixon']}
      />,
    );
  });
  await flush();

  // It fetched the REFERENCE, not the source document.
  expect(mockGetContentText).toHaveBeenCalledWith('s1', 'ref-123');
  expect(mockGetDocumentContentText).not.toHaveBeenCalled();

  // The reference text is on screen.
  expect(container.textContent).toContain('William Acker,Nixon');

  // The "Reference" badge is shown.
  expect(container.textContent).toContain('Reference');
  // The reference filename is shown in the header (not the source doc name).
  expect(container.textContent).toContain('judges.csv');

  // The passage is highlighted in a <mark>.
  const marks = container.querySelectorAll('mark');
  expect(marks.length).toBeGreaterThan(0);
  const marked = Array.from(marks).map((m) => m.textContent).join(' ');
  expect(marked).toContain('William Acker,Nixon');
  // The non-matching row is NOT inside a mark.
  expect(marked).not.toContain('Jane Doe');
});

test('without referenceDoc it uses the source-document text path', async () => {
  mockGetDocumentContentText.mockResolvedValue('Some source document text here.');

  await act(async () => {
    root.render(
      <DocumentPreview
        sessionId="s1"
        documentName="ruling.txt"
        highlightTexts={['source document text']}
      />,
    );
  });
  await flush();

  expect(mockGetDocumentContentText).toHaveBeenCalledWith('s1', 'ruling.txt');
  expect(mockGetContentText).not.toHaveBeenCalled();
  expect(container.textContent).toContain('Some source document text');
  // No reference badge in source mode.
  expect(container.querySelector('mark')?.textContent).toContain('source document text');
});

test('reference load failure shows an error, not a crash', async () => {
  mockGetContentText.mockRejectedValue(new Error('boom'));

  await act(async () => {
    root.render(
      <DocumentPreview
        sessionId="s1"
        documentName="ruling.pdf"
        referenceDoc={{ id: 'ref-err', filename: 'judges.csv' }}
        highlightTexts={['anything']}
      />,
    );
  });
  await flush();

  expect(container.textContent).toContain('Could not load');
});
