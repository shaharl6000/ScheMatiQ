// Side-panel chat UI for inspecting and editing the project via workspace tools.
// Parent: Workspace (index.tsx).

import { useCallback, useEffect, useRef, useState } from 'react';
import { Bot, Check, X } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { chatAPI } from '@/services/api';
import type { ChatToolInfo, PaginatedData, SchemaData, ScheMatiQStatus } from '@/types';

import {
  CHAT_MUTATION_TOOLS,
  CHAT_RERUN_FOLLOWUP_TOOLS,
  CHAT_SCHEMA_FOLLOWUP_TOOLS,
  SHOW_TOOL_SUGGESTION,
} from '../constants';
import { formatToolsList, mapChatTurnMessage } from '../helpers';
import type { PendingChatAction, PendingRerunKind, WorkspaceMessage, WorkspaceSessionMode } from '../types';
import { ChatMessageBody } from './ChatMessageBody';

export function ChatPanel({
  sessionId,
  sessionMode,
  onRefresh,
  onEditFollowUp,
  onRegisterCancelPending,
}: {
  sessionId?: string;
  sessionMode: WorkspaceSessionMode;
  status: ScheMatiQStatus | null;
  schema: SchemaData | null;
  data: PaginatedData;
  onRefresh: () => void;
  onEditFollowUp: (kind: PendingRerunKind, columns?: string[]) => void;
  onRegisterCancelPending?: (cancel: (() => Promise<boolean>) | null) => void;
}) {
  const [messages, setMessages] = useState<WorkspaceMessage[]>([
    {
      id: 'hello',
      role: 'assistant',
      content:
        'Ask me to inspect or edit this project. I use workspace tools to read schema and data before making changes. Type /tools to list available tools.',
    },
  ]);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const [chatId, setChatId] = useState<string | null>(null);
  const [pinnedTool, setPinnedTool] = useState<string | null>(null);
  const [availableTools, setAvailableTools] = useState<ChatToolInfo[]>([]);
  const [pendingAction, setPendingAction] = useState<PendingChatAction | null>(null);
  const messagesRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const container = messagesRef.current;
    if (!container) return;

    container.scrollTo({
      top: container.scrollHeight,
      behavior: 'smooth',
    });
  }, [messages, pendingAction]);

  const loadTools = useCallback(async () => {
    const tools = await chatAPI.getTools(sessionId, sessionMode);
    setAvailableTools(tools.filter((tool) => tool.available));
    return tools;
  }, [sessionId, sessionMode]);

  useEffect(() => {
    loadTools().catch(() => {
      setAvailableTools([]);
    });
  }, [loadTools]);

  const appendMessages = useCallback((next: WorkspaceMessage[]) => {
    setMessages((current) => [...current, ...next]);
  }, []);

  const applyChatResponse = useCallback((response: Awaited<ReturnType<typeof chatAPI.sendMessage>>) => {
    setChatId(response.chat_id);
    appendMessages(response.messages.map(mapChatTurnMessage));
    if (response.pending_action) {
      setPendingAction({
        id: response.pending_action.tool_name,
        label: response.pending_action.label,
        description: response.pending_action.description,
        chatId: response.chat_id,
      });
    } else {
      setPendingAction(null);
    }
    const completedTools = response.messages.filter(
      (message) =>
        message.kind === 'tool_log'
        && message.tool_status === 'done'
        && message.tool_name,
    );

    if (completedTools.some((message) => CHAT_MUTATION_TOOLS.has(message.tool_name!))) {
      onRefresh();
    }

    const alreadyFollowedUp =
      (response.pending_action != null
        && CHAT_RERUN_FOLLOWUP_TOOLS.has(response.pending_action.tool_name))
      || completedTools.some((message) =>
        CHAT_RERUN_FOLLOWUP_TOOLS.has(message.tool_name!),
      );

    if (!alreadyFollowedUp && completedTools.some((message) => message.tool_name === 'edit_observation_unit')) {
      onEditFollowUp('unit');
    } else if (!alreadyFollowedUp && completedTools.some((message) => CHAT_SCHEMA_FOLLOWUP_TOOLS.has(message.tool_name!))) {
      const editedColumns = completedTools
        .filter((message) => CHAT_SCHEMA_FOLLOWUP_TOOLS.has(message.tool_name!))
        .flatMap((message) => message.columns ?? []);
      // Only prompt a re-extract when a column was added/edited/merged; a
      // delete-only change yields no columns and needs no re-extraction.
      if (editedColumns.length > 0) {
        onEditFollowUp('schema', editedColumns);
      }
    }
  }, [appendMessages, onEditFollowUp, onRefresh]);

  const showToolsList = useCallback(async () => {
    setBusy(true);
    try {
      const tools = await loadTools();
      appendMessages([
        {
          id: `${Date.now()}-tools`,
          role: 'assistant',
          content: `Available tools:\n\n${formatToolsList(tools)}`,
        },
      ]);
    } catch (err: any) {
      appendMessages([
        {
          id: `${Date.now()}-tools-error`,
          role: 'assistant',
          content: err?.response?.data?.detail || err?.message || 'Could not load tools.',
        },
      ]);
    } finally {
      setBusy(false);
    }
  }, [appendMessages, loadTools]);

  const ask = useCallback(async () => {
    const text = input.trim();
    if (!text || busy) return;
    setInput('');
    appendMessages([{ id: `${Date.now()}-user`, role: 'user', content: text }]);

    if (text.toLowerCase().startsWith('/tools')) {
      await showToolsList();
      return;
    }

    if (!sessionId) {
      appendMessages([
        {
          id: `${Date.now()}-no-session`,
          role: 'assistant',
          content: 'Open > New Project or Import Project to get started. Once a project exists I can inspect and edit it.',
        },
      ]);
      return;
    }

    setBusy(true);
    try {
      const response = await chatAPI.sendMessage(sessionId, {
        message: text,
        chat_id: chatId || undefined,
        session_mode: sessionMode,
        pinned_tool: pinnedTool || undefined,
      });
      applyChatResponse(response);
    } catch (err: any) {
      appendMessages([
        {
          id: `${Date.now()}-error`,
          role: 'assistant',
          content: err?.response?.data?.detail || err?.message || 'That workspace action failed.',
        },
      ]);
    } finally {
      setBusy(false);
    }
  }, [
    appendMessages,
    applyChatResponse,
    busy,
    chatId,
    input,
    pinnedTool,
    sessionId,
    sessionMode,
    showToolsList,
  ]);

  const confirmPendingAction = useCallback(async () => {
    if (!pendingAction || !sessionId) return;
    setBusy(true);
    try {
      const response = await chatAPI.confirmAction(sessionId, pendingAction.chatId);
      applyChatResponse(response);
    } catch (err: any) {
      appendMessages([
        {
          id: `${Date.now()}-confirm-error`,
          role: 'assistant',
          content: err?.response?.data?.detail || err?.message || 'The confirmed action failed.',
        },
      ]);
    } finally {
      setBusy(false);
    }
  }, [appendMessages, applyChatResponse, pendingAction, sessionId]);

  const cancelPendingAction = useCallback(async (): Promise<boolean> => {
    if (!pendingAction || !sessionId) return true;
    setBusy(true);
    try {
      const response = await chatAPI.cancelAction(sessionId, pendingAction.chatId);
      setPendingAction(null);
      applyChatResponse(response);
      return true;
    } catch (err: any) {
      appendMessages([
        {
          id: `${Date.now()}-cancel-error`,
          role: 'assistant',
          content:
            err?.response?.data?.detail
            || err?.message
            || 'Could not cancel the pending action. Try again.',
        },
      ]);
      return false;
    } finally {
      setBusy(false);
    }
  }, [appendMessages, applyChatResponse, pendingAction, sessionId]);

  useEffect(() => {
    if (!onRegisterCancelPending) return;
    if (pendingAction) {
      onRegisterCancelPending(cancelPendingAction);
    } else {
      onRegisterCancelPending(null);
    }
    return () => onRegisterCancelPending(null);
  }, [cancelPendingAction, onRegisterCancelPending, pendingAction]);

  return (
    <aside className="workspace-chat">
      <div className="workspace-chat-header">
        <div className="flex items-center gap-2 text-sm font-medium">
          <Bot className="h-4 w-4" />
          Chat
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="outline">gemini-3.1-flash-lite</Badge>
          <Button size="sm" variant="outline" onClick={showToolsList} disabled={busy}>
            Tools
          </Button>
        </div>
      </div>

      {SHOW_TOOL_SUGGESTION && availableTools.length > 0 && (
        <div className="workspace-chat-tools px-3 pb-2">
          <select
            className="w-full rounded-md border bg-background px-2 py-1 text-xs"
            value={pinnedTool || ''}
            onChange={(event) => setPinnedTool(event.target.value || null)}
          >
            <option value="">Suggest a tool (optional)</option>
            {availableTools.map((tool) => (
              <option key={tool.name} value={tool.name}>
                {tool.name}
                {tool.cost_class === 'expensive' ? ' [cost]' : ''}
              </option>
            ))}
          </select>
        </div>
      )}

      <div className="workspace-chat-messages" ref={messagesRef}>
        {messages.map((message) => (
          <div
            key={message.id}
            className={`workspace-chat-message${message.kind === 'tool_log' ? ' workspace-chat-tool-log' : ''}`}
            data-role={message.role}
            data-tool-status={message.toolStatus}
          >
            <ChatMessageBody message={message} />
          </div>
        ))}

        {pendingAction && (
          <div className="rounded-md border bg-muted/30 p-3 text-sm">
            <div className="font-medium">{pendingAction.label}</div>
            <div className="mt-1 text-muted-foreground">{pendingAction.description}</div>
            <div className="mt-3 flex gap-2">
              <Button size="sm" onClick={confirmPendingAction} disabled={busy}>
                <Check className="h-4 w-4" />
                Confirm
              </Button>
              <Button size="sm" variant="outline" onClick={cancelPendingAction} disabled={busy}>
                <X className="h-4 w-4" />
                Cancel
              </Button>
            </div>
          </div>
        )}
      </div>

      <div className="workspace-chat-input">
        <Textarea
          value={input}
          onChange={(event) => setInput(event.target.value)}
          placeholder="Ask ScheMatiQ or type /tools"
          rows={3}
          disabled={busy}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
              event.preventDefault();
              ask();
            }
          }}
        />
      </div>
    </aside>
  );
}
