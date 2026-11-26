// Renders a single chat message body (Markdown for assistant, plain text otherwise).
// Parent: ChatPanel.

import { memo, useMemo, type ComponentProps } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import type { WorkspaceMessage } from '../types';

export interface ChatLinkTargets {
  columns: Map<string, string>; // lowercased canonical or display name -> canonical name
  rows: Map<string, string>;    // lowercased row name -> exact row name
  docs: Map<string, string>;    // lowercased doc name / stem -> exact doc name
}

const REMARK_PLUGINS: ComponentProps<typeof ReactMarkdown>['remarkPlugins'] = [remarkGfm];

export const MARKDOWN_COMPONENTS: ComponentProps<typeof ReactMarkdown>['components'] = {
  a: ({ node: _node, children, ...props }) => (
    <a {...props} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  ),
  // Each list resolves its own direction: a Hebrew list inside a message that
  // starts in English gets its indent and markers on the right.
  ul: ({ node: _node, ...props }) => <ul dir="auto" {...props} />,
  ol: ({ node: _node, ...props }) => <ol dir="auto" {...props} />,
};

export const ChatMessageBody = memo(function ChatMessageBody({
  message,
  linkTargets,
  onJumpToTarget,
}: {
  message: WorkspaceMessage;
  linkTargets?: ChatLinkTargets;
  onJumpToTarget?: (kind: 'column' | 'row' | 'doc', name: string) => void;
}) {
  const components = useMemo<ComponentProps<typeof ReactMarkdown>['components']>(() => {
    if (!linkTargets || !onJumpToTarget) return MARKDOWN_COMPONENTS;
    return {
      ...MARKDOWN_COMPONENTS,
      code: ({ node: _node, className, children, ...props }) => {
        // Only linkify short inline code tokens (skip fenced code blocks with language className).
        const raw = typeof children === 'string'
          ? children
          : Array.isArray(children) && children.every((c) => typeof c === 'string')
            ? children.join('')
            : null;
        if (!className && raw && raw.length > 0 && raw.length <= 120 && !raw.includes('\n')) {
          const key = raw.trim().toLowerCase();
          const colMatch = linkTargets.columns.get(key);
          if (colMatch) {
            return (
              <button
                type="button"
                className="workspace-chat-deeplink"
                onClick={() => onJumpToTarget('column', colMatch)}
                title={`Jump to column "${raw.trim()}" in table`}
              >
                {raw}
              </button>
            );
          }
          const docMatch = linkTargets.docs.get(key);
          if (docMatch) {
            return (
              <button
                type="button"
                className="workspace-chat-deeplink is-doc"
                onClick={() => onJumpToTarget('doc', docMatch)}
                title={`Open source document "${docMatch}"`}
              >
                {raw}
              </button>
            );
          }
          const rowMatch = linkTargets.rows.get(key);
          if (rowMatch) {
            return (
              <button
                type="button"
                className="workspace-chat-deeplink is-row"
                onClick={() => onJumpToTarget('row', rowMatch)}
                title={`Jump to row "${rowMatch}" in table`}
              >
                {raw}
              </button>
            );
          }
        }
        return (
          <code className={className} {...props}>
            {children}
          </code>
        );
      },
    };
  }, [linkTargets, onJumpToTarget]);

  const isMarkdown = message.role === 'assistant' && message.kind !== 'tool_log';
  if (!isMarkdown) {
    return <div dir="auto">{message.content}</div>;
  }
  return (
    <div className="workspace-chat-markdown" dir="auto">
      <ReactMarkdown remarkPlugins={REMARK_PLUGINS} components={components}>
        {message.content}
      </ReactMarkdown>
    </div>
  );
});
