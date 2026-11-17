import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

jest.mock('react-markdown', () => ({
  __esModule: true,
  default: ({ children }: { children?: React.ReactNode }) => (
    <div data-testid="markdown-body">{children}</div>
  ),
}));

jest.mock('remark-gfm', () => ({
  __esModule: true,
  default: () => {},
}));

const mockGetDocumentContentUrl = jest.fn();
const mockGetDocumentContentText = jest.fn();

jest.mock('../../../services/api', () => ({
  __esModule: true,
  unitsAPI: {
    getDocumentContentUrl: (...args: unknown[]) => mockGetDocumentContentUrl(...args),
    getDocumentContentText: (...args: unknown[]) => mockGetDocumentContentText(...args),
  },
}));

import { ChatMessageBody, MARKDOWN_COMPONENTS } from './ChatMessageBody';
import DocumentPreview from '../../../components/DocumentViewer/DocumentPreview';

const flushPromises = async (ticks = 4) => {
  for (let i = 0; i < ticks; i += 1) {
    await Promise.resolve();
  }
};

describe('RTL and bidirectional text support', () => {
  let container: HTMLDivElement;
  let root: Root;
  const originalFetch = global.fetch;
  const originalScrollIntoView = HTMLElement.prototype.scrollIntoView;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    mockGetDocumentContentUrl.mockReset();
    mockGetDocumentContentText.mockReset();
    mockGetDocumentContentUrl.mockImplementation(
      (sessionId: string, docName: string) => `/api/units/${sessionId}/doc?name=${docName}`,
    );
    mockGetDocumentContentText.mockResolvedValue('פסק הדין קבע כי הערעור נדחה.');
    global.fetch = jest.fn().mockResolvedValue({ ok: true } as Response) as unknown as typeof fetch;
    HTMLElement.prototype.scrollIntoView = jest.fn();
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    container.remove();
    global.fetch = originalFetch;
    HTMLElement.prototype.scrollIntoView = originalScrollIntoView;
  });

  it('renders Hebrew assistant markdown with dir="auto"', () => {
    act(() => {
      root.render(
        <ChatMessageBody
          message={{
            id: 'm1',
            role: 'assistant',
            content: 'סיכום פסק הדין:\n- נקודה ראשונה',
          }}
        />,
      );
    });

    const md = container.querySelector('.workspace-chat-markdown');
    expect(md).not.toBeNull();
    expect(md?.getAttribute('dir')).toBe('auto');
    expect(md?.textContent).toContain('סיכום פסק הדין');
  });

  it('gives markdown lists their own dir="auto"', () => {
    const List = MARKDOWN_COMPONENTS?.ul as React.ComponentType<{ children?: React.ReactNode }>;
    const Ordered = MARKDOWN_COMPONENTS?.ol as React.ComponentType<{ children?: React.ReactNode }>;
    act(() => {
      root.render(
        <>
          <List><li>נקודה</li></List>
          <Ordered><li>שלב</li></Ordered>
        </>,
      );
    });
    expect(container.querySelector('ul')?.getAttribute('dir')).toBe('auto');
    expect(container.querySelector('ol')?.getAttribute('dir')).toBe('auto');
  });

  it('renders plain Hebrew user messages inside a dir="auto" element', () => {
    act(() => {
      root.render(
        <ChatMessageBody
          message={{
            id: 'm2',
            role: 'user',
            content: 'מה נקבע בערעור?',
          }}
        />,
      );
    });

    const bidiEl = container.querySelector('[dir="auto"]');
    expect(bidiEl).not.toBeNull();
    expect(bidiEl?.textContent).toBe('מה נקבע בערעור?');
  });

  it('renders DocumentPreview inline text and figure captions with dir="auto"', async () => {
    await act(async () => {
      root.render(
        <DocumentPreview
          sessionId="sess-rtl"
          documentName="ruling.txt"
          highlightTexts={[{ text: 'פסק הדין', source: 'ruling.txt' }]}
          figures={[{ url: '/api/fig/1.png', caption: 'איור 1: התפלגות התביעות' }]}
        />,
      );
      await flushPromises(8);
    });
    await act(async () => {
      await flushPromises(8);
    });

    const pre = container.querySelector('pre[dir="auto"]');
    expect(pre).not.toBeNull();
    expect(pre?.textContent).toContain('פסק הדין קבע כי הערעור נדחה.');

    const caption = container.querySelector('p[dir="auto"]');
    expect(caption).not.toBeNull();
    expect(caption?.textContent).toBe('איור 1: התפלגות התביעות');
  });
});

