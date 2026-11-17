// Renders a single chat message body (Markdown for assistant, plain text otherwise).
// Parent: ChatPanel.

import { memo, type ComponentProps } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import type { WorkspaceMessage } from '../types';

const REMARK_PLUGINS: ComponentProps<typeof ReactMarkdown>['remarkPlugins'] = [remarkGfm];

const MARKDOWN_COMPONENTS: ComponentProps<typeof ReactMarkdown>['components'] = {
  a: ({ node: _node, children, ...props }) => (
    <a {...props} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  ),
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
