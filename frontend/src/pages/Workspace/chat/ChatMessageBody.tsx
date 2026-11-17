// Renders a single chat message body (Markdown for assistant, plain text otherwise).
// Parent: ChatPanel.

import { memo, type ComponentProps } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import type { WorkspaceMessage } from '../types';

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
}: {
  message: WorkspaceMessage;
}) {
  const isMarkdown = message.role === 'assistant' && message.kind !== 'tool_log';
  if (!isMarkdown) {
    return <div dir="auto">{message.content}</div>;
  }
  return (
    <div className="workspace-chat-markdown" dir="auto">
      <ReactMarkdown remarkPlugins={REMARK_PLUGINS} components={MARKDOWN_COMPONENTS}>
        {message.content}
      </ReactMarkdown>
    </div>
  );
});
