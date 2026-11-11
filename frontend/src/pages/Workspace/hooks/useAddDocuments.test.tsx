import React, { act } from 'react';
import { createRoot } from 'react-dom/client';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

jest.mock(
  '@/constants',
  () => ({
    getAvailableProviders: () => ['gemini'],
    getDefaultModelForProvider: () => 'gemini-3.1-flash-lite',
  }),
  { virtual: true },
);

jest.mock(
  '@/services/api',
  () => ({
    configAPI: { getLLMConfigStatus: jest.fn() },
    loadAPI: {
      addDocuments: jest.fn(),
      processDocuments: jest.fn(),
    },
  }),
  { virtual: true },
);

jest.mock(
  '@/utils/apiKeyStorage',
  () => ({
    getApiKeyForProvider: jest.fn(),
    getConfiguredProviders: () => [],
  }),
  { virtual: true },
);

jest.mock(
  '@/components/AdvancedSettings/AdvancedSettingsFields',
  () => ({
    observationUnitFromValue: jest.fn(),
    retrieverIsCustomized: jest.fn(),
    DEFAULT_ADVANCED_SETTINGS: {},
  }),
  { virtual: true },
);

import { useAddDocuments } from './useAddDocuments';

describe('useAddDocuments session-switch reset', () => {
  it('clears staged files and notices when sessionId changes', () => {
    const container = document.createElement('div');
    const root = createRoot(container);
    let latest: ReturnType<typeof useAddDocuments> | null = null;

    function Harness({ sessionId }: { sessionId?: string }) {
      latest = useAddDocuments({
        sessionId,
        documents: {
          documents: [{ name: 'existing.pdf', rowCount: 1 }],
          totalDocuments: 1,
          totalRows: 1,
          enrichmentPending: false,
        },
        session: null,
        refresh: jest.fn().mockResolvedValue(undefined),
        setActiveSheet: jest.fn(),
        toast: jest.fn(),
      });
      return null;
    }

    act(() => {
      root.render(<Harness sessionId="session-a" />);
    });

    const newFile = new File(['hello'], 'new-paper.pdf', { type: 'application/pdf' });
    const dupFile = new File(['dup'], 'existing.pdf', { type: 'application/pdf' });

    act(() => {
      latest!.handleAddDocsFilesChange([newFile, dupFile]);
    });

    expect(latest!.addDocsFiles).toHaveLength(1);
    expect(latest!.addDocsFiles[0].name).toBe('new-paper.pdf');
    expect(latest!.addDocsNotice).toContain('Skipped 1 document');

    act(() => {
      root.render(<Harness sessionId="session-b" />);
    });

    expect(latest!.addDocsFiles).toEqual([]);
    expect(latest!.addDocsNotice).toBeNull();
    expect(latest!.addDocsError).toBeNull();
    expect(latest!.addDocsResult).toBeNull();

    act(() => {
      root.unmount();
    });
  });
});
