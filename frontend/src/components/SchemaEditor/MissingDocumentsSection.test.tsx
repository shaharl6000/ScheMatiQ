import React from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { act as legacyAct } from 'react-dom/test-utils';

import type { DocumentAvailabilityResponse } from '@/types';

const mockAddDocuments = jest.fn();

jest.mock('../../services/api', () => ({
  loadAPI: {
    addDocuments: (...args: unknown[]) => mockAddDocuments(...args),
  },
}));

import MissingDocumentsSection from './MissingDocumentsSection';

const act: typeof legacyAct = (React as Record<string, any>).act || legacyAct;

async function flushPromises(ticks = 4): Promise<void> {
  for (let i = 0; i < ticks; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve();
  }
}

describe('MissingDocumentsSection', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    (globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    jest.clearAllMocks();
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(async () => {
    await act(async () => {
      root.unmount();
    });
    container.remove();
  });

  it('renders a destructive alert and direct upload card when zero documents are available and missing_documents is empty', async () => {
    const onRefresh = jest.fn();
    const availability: DocumentAvailabilityResponse = {
      total_documents: 0,
      local_documents: [],
      cloud_documents: [],
      missing_documents: [],
      can_proceed: false,
      total_rows: 0,
      rows_with_missing_docs: 0,
    };

    await act(async () => {
      root.render(
        <MissingDocumentsSection
          sessionId="sess-empty"
          availability={availability}
          loading={false}
          onRefresh={onRefresh}
        />,
      );
      await flushPromises();
    });

    expect(container.textContent).not.toContain('All 0 source documents are available.');
    expect(container.textContent).toContain('No source documents are available on the server.');
    expect(container.textContent).toContain('Upload the original documents to enable re-extraction.');
    expect(container.textContent).not.toContain('Missing documents (0)');

    // File upload input is rendered directly without needing to expand a collapsible.
    const fileInput = container.querySelector<HTMLInputElement>('input[type="file"]');
    expect(fileInput).not.toBeNull();

    const sampleFile = new File(['paper body'], 'paper_a.txt', { type: 'text/plain' });
    mockAddDocuments.mockResolvedValue({ uploaded_files: ['paper_a.txt'] });

    await act(async () => {
      Object.defineProperty(fileInput, 'files', {
        value: [sampleFile],
        configurable: true,
      });
      fileInput?.dispatchEvent(new Event('change', { bubbles: true }));
      await flushPromises();
    });

    const uploadButton = Array.from(container.querySelectorAll<HTMLButtonElement>('button')).find(
      (btn) => btn.textContent?.includes('Upload & Re-check'),
    );
    expect(uploadButton).toBeDefined();

    await act(async () => {
      uploadButton?.click();
      await flushPromises();
    });

    expect(mockAddDocuments).toHaveBeenCalledWith('sess-empty', [sampleFile]);
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });

  it('renders the green success alert when can_proceed is true and no documents are missing', async () => {
    const availability: DocumentAvailabilityResponse = {
      total_documents: 2,
      local_documents: [{ name: 'doc1.txt', status: 'local', affected_rows: [] }],
      cloud_documents: [{ name: 'doc2.txt', status: 'cloud', cloud_path: 'ds/doc2.txt', affected_rows: [] }],
      missing_documents: [],
      can_proceed: true,
      total_rows: 0,
      rows_with_missing_docs: 0,
    };

    await act(async () => {
      root.render(
        <MissingDocumentsSection
          sessionId="sess-ok"
          availability={availability}
          loading={false}
          onRefresh={jest.fn()}
        />,
      );
      await flushPromises();
    });

    expect(container.textContent).toContain('All 2 source documents are available.');
    expect(container.textContent).toContain('(1 local, 1 from cloud)');
  });
});
