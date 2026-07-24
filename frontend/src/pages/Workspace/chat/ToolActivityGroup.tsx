// Collapsible summary of the tool activity within a single assistant turn.
// Parent: ChatPanel. By default it shows a one-line summary ("Used N tools")
// and expands on click to reveal each individual tool-log entry.

import { useState } from 'react';
import { ChevronRight, Loader2, Wrench } from 'lucide-react';

import type { WorkspaceMessage } from '../types';
import { ChatMessageBody } from './ChatMessageBody';

function summarize(logs: WorkspaceMessage[]): string {
  const names = Array.from(
    new Set(logs.map((log) => log.toolName).filter((name): name is string => Boolean(name))),
  );
  const count = names.length || logs.length;
  const noun = count === 1 ? 'step' : 'steps';
  if (names.length === 1) {
    return `Ran 1 step (${names[0]})`;
  }
  return `Ran ${count} ${noun}`;
}

export function ToolActivityGroup({ logs }: { logs: WorkspaceMessage[] }) {
  const [open, setOpen] = useState(false);
  const running = logs.some((log) => log.toolStatus === 'running');
  const errored = logs.some((log) => log.toolStatus === 'error');
  const status = running ? 'running' : errored ? 'error' : 'done';

  return (
    <div className="workspace-chat-tool-group" data-tool-status={status}>
      <button
        type="button"
        className="workspace-chat-tool-group-toggle"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
      >
        <ChevronRight
          className={`workspace-chat-tool-group-chevron h-3.5 w-3.5${open ? ' is-open' : ''}`}
        />
        {running ? (
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
        ) : (
          <Wrench className="h-3.5 w-3.5" />
        )}
        <span className="workspace-chat-tool-group-summary">{summarize(logs)}</span>
      </button>

      {open && (
        <div className="workspace-chat-tool-group-body">
          {logs.map((log) => (
            <div
              key={log.id}
              className="workspace-chat-tool-group-item"
              data-tool-status={log.toolStatus}
            >
              <ChatMessageBody message={log} />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
