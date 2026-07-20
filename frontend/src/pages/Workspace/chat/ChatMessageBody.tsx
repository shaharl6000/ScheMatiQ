// Renders a single chat message body (Markdown for assistant, plain text otherwise).
// Parent: ChatPanel.

import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import type { WorkspaceMessage } from '../types';

export function ChatMessageBody({ message }: { message: WorkspaceMessage }) {
  const isMarkdown = message.role === 'assistant' && message.kind !== 'tool_log';
  if (!isMarkdown) {
    return <>{message.content}</>;
  }
  return (
    <div className="workspace-chat-markdown">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          a: ({ node, children, ...props }) => (
            <a {...props} target="_blank" rel="noopener noreferrer">
              {children}
            </a>
          ),
        }}
      >
        {message.content}
      </ReactMarkdown>
    </div>
  );
}
