// Side-panel chat UI for inspecting and editing the project via workspace tools.
// Parent: Workspace (index.tsx).

import { type ChangeEvent, type DragEvent, useCallback, useEffect, useRef, useState } from 'react';
import { ArrowUp, Bot, Check, FileText, Loader2, Paperclip, Square, X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { ModelSelector } from '@/components/ModelSelector';
import { getDefaultModelForProvider } from '@/constants';
import { chatAPI, referenceAPI, type ReferenceDocumentInfo } from '@/services/api';
import webSocketService from '@/services/websocket';
import type {
  ChatToolInfo,
  ChatTurnMessage,
  PaginatedData,
  SchemaData,
  ScheMatiQStatus,
  WebSocketMessage,
} from '@/types';

import {
  CHAT_MUTATION_TOOLS,
  CHAT_RERUN_FOLLOWUP_TOOLS,
  CHAT_SCHEMA_FOLLOWUP_TOOLS,
  SHOW_TOOL_SUGGESTION,
} from '../constants';
import { formatToolsList, mapChatTurnMessage } from '../helpers';
import type { PendingChatAction, PendingRerunKind, WorkspaceMessage, WorkspaceSessionMode } from '../types';
import { ChatMessageBody } from './ChatMessageBody';
import { ToolActivityGroup } from './ToolActivityGroup';
import { useChatTurn } from './hooks/useChatTurn';

// Fold a flat message list into render groups so that consecutive tool_log
// messages collapse into a single expandable block, while text/user messages
// pass through untouched.
type ChatRenderItem =
  | { kind: 'message'; message: WorkspaceMessage }
  | { kind: 'tool_group'; id: string; logs: WorkspaceMessage[] };

function groupChatMessages(messages: WorkspaceMessage[]): ChatRenderItem[] {
  const items: ChatRenderItem[] = [];
  let pending: WorkspaceMessage[] = [];

  const flush = () => {
    if (pending.length === 0) return;
    items.push({ kind: 'tool_group', id: `tools-${pending[0].id}`, logs: pending });
    pending = [];
  };

  for (const message of messages) {
    if (message.kind === 'tool_log') {
      pending.push(message);
    } else {
      flush();
      items.push({ kind: 'message', message });
    }
  }
  flush();
  return items;
}

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
  // The chat turn lifecycle (busy flag + abort wiring) lives in its own hook so
  // Stop can cancel the in-flight request and every handler shares one busy
  // source instead of toggling it by hand.
  const { busy, runTurn, stop } = useChatTurn();
  const [chatId, setChatId] = useState<string | null>(null);
  const [chatModel, setChatModel] = useState<string>(() => getDefaultModelForProvider('gemini'));
  const [pinnedTool, setPinnedTool] = useState<string | null>(null);
  const [availableTools, setAvailableTools] = useState<ChatToolInfo[]>([]);
  const [pendingAction, setPendingAction] = useState<PendingChatAction | null>(null);
  const [references, setReferences] = useState<ReferenceDocumentInfo[]>([]);
  const [uploadingReference, setUploadingReference] = useState(false);
  const [referenceError, setReferenceError] = useState<string | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const [fillRunning, setFillRunning] = useState<{ column: string } | null>(null);
  const [reextractionRunning, setReextractionRunning] = useState<{ columns: string[] } | null>(null);
  const messagesRef = useRef<HTMLDivElement | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  // Which session's persisted transcript we've already tried to load, so a model
  // switch (which resets chatId) or a re-render doesn't refetch and repaint.
  const historyLoadedForRef = useRef<string | null>(null);
  // Flips true once the user drives the conversation, so a late-arriving history
  // load can't clobber messages they've already produced this session.
  const conversationStartedRef = useRef(false);
  // Set when the user presses Stop: the backend may still emit a message or two
  // from the step that was already running, so drop live chat_message events
  // until the next turn starts rather than letting the stopped reply trickle in.
  const suppressStreamRef = useRef(false);

  useEffect(() => {
    const container = messagesRef.current;
    if (!container) return;

    container.scrollTo({
      top: container.scrollHeight,
      behavior: 'smooth',
    });
  }, [messages, pendingAction, busy]);

  // On entering a project, repaint any persisted conversation so a refresh or a
  // backend redeploy resumes where the user left off instead of showing only the
  // seed. Runs once per session id; keeps the seed when there is no history, and
  // never overwrites a conversation the user has already started this session.
  useEffect(() => {
    if (!sessionId) return;
    if (historyLoadedForRef.current === sessionId) return;
    historyLoadedForRef.current = sessionId;
    let cancelled = false;
    (async () => {
      try {
        const history = await chatAPI.getMessages(sessionId);
        if (cancelled || conversationStartedRef.current) return;
        if (history.messages.length === 0) return;
        setMessages(history.messages.map(mapChatTurnMessage));
        // Adopt the resumable chat id so the next send continues this thread.
        if (history.chat_id) setChatId(history.chat_id);
      } catch {
        // Best-effort: on any failure keep the local seed, exactly as before.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [sessionId]);

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

  const loadReferences = useCallback(async () => {
    if (!sessionId) {
      setReferences([]);
      return;
    }
    try {
      setReferences(await referenceAPI.list(sessionId));
    } catch {
      setReferences([]);
    }
  }, [sessionId]);

  useEffect(() => {
    loadReferences();
  }, [loadReferences]);

  const handleAttachClick = useCallback(() => {
    setReferenceError(null);
    fileInputRef.current?.click();
  }, []);

  const uploadReferenceFile = useCallback(
    async (file: File) => {
      if (!sessionId) return;
      setUploadingReference(true);
      setReferenceError(null);
      try {
        const created = await referenceAPI.upload(sessionId, file);
        // The backend dedups by filename (re-attaching the same name replaces
        // the existing entry), so mirror that here: swap a same-named chip in
        // place instead of appending a duplicate.
        setReferences((current) => {
          const withoutSameName = current.filter(
            (ref) => ref.filename !== created.filename,
          );
          return [...withoutSameName, created];
        });
      } catch (error) {
        const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data
          ?.detail;
        setReferenceError(
          typeof detail === 'string' ? detail : `Could not attach ${file.name}.`,
        );
      } finally {
        setUploadingReference(false);
      }
    },
    [sessionId],
  );

  const handleReferenceSelected = useCallback(
    async (event: ChangeEvent<HTMLInputElement>) => {
      const file = event.target.files?.[0];
      // Reset so selecting the same file again still fires onChange.
      event.target.value = '';
      if (!file) return;
      await uploadReferenceFile(file);
    },
    [uploadReferenceFile],
  );

  const handleChatDragOver = useCallback(
    (event: DragEvent<HTMLElement>) => {
      if (!sessionId || uploadingReference) return;
      if (!Array.from(event.dataTransfer.types).includes('Files')) return;
      event.preventDefault();
      event.dataTransfer.dropEffect = 'copy';
      setDragActive(true);
    },
    [sessionId, uploadingReference],
  );

  const handleChatDragLeave = useCallback((event: DragEvent<HTMLElement>) => {
    // Only clear when the pointer leaves the panel, not when moving between children.
    if (event.currentTarget.contains(event.relatedTarget as Node | null)) return;
    setDragActive(false);
  }, []);

  const handleChatDrop = useCallback(
    async (event: DragEvent<HTMLElement>) => {
      if (!sessionId || uploadingReference) return;
      event.preventDefault();
      setDragActive(false);
      const file = event.dataTransfer.files?.[0];
      if (!file) return;
      await uploadReferenceFile(file);
    },
    [sessionId, uploadingReference, uploadReferenceFile],
  );

  const handleRemoveReference = useCallback(
    async (referenceId: string) => {
      if (!sessionId) return;
      const previous = references;
      setReferences((current) => current.filter((ref) => ref.id !== referenceId));
      try {
        await referenceAPI.remove(sessionId, referenceId);
      } catch {
        setReferences(previous); // restore on failure
      }
    },
    [sessionId, references],
  );

  // Ids are the deduplication key. Assistant and tool messages now arrive over
  // two channels -- live over the WebSocket as the agent produces them, and
  // again in the HTTP response at the end of the turn -- so whichever lands
  // first wins and the other copy is dropped. The HTTP response stays the source
  // of truth: if the socket is down, nothing is lost.
  const appendMessages = useCallback((next: WorkspaceMessage[]) => {
    setMessages((current) => {
      const seen = new Set(current.map((message) => message.id));
      const fresh = next.filter((message) => {
        if (seen.has(message.id)) return false;
        seen.add(message.id);
        return true;
      });
      return fresh.length ? [...current, ...fresh] : current;
    });
  }, []);

  // A column fill runs in the background after the chat turn ends, streaming cells
  // into the table. Show a spinner while it runs, then post the model's recap as an
  // assistant message so the user learns what was filled vs left empty.
  useEffect(() => {
    if (!sessionId) return undefined;
    const handler = (message: WebSocketMessage) => {
      if (message.type === 'chat_message' && message.data) {
        // Emitted by agent_service as each assistant/tool message is produced,
        // so the panel fills in while the turn is still running instead of
        // staying empty until the HTTP response returns. After a Stop we ignore
        // these so the halted turn's reply does not keep landing.
        if (suppressStreamRef.current) return;
        appendMessages([mapChatTurnMessage(message.data as unknown as ChatTurnMessage)]);
      } else if (message.type === 'reference_fill_started') {
        const data = (message.data ?? {}) as unknown as { column?: string };
        setFillRunning({ column: data.column ?? '' });
      } else if (message.type === 'reference_fill_completed') {
        const data = (message.data ?? {}) as unknown as {
          column?: string; message?: string; filled?: number; total?: number;
        };
        setFillRunning(null);
        appendMessages([
          {
            id: `${Date.now()}-fill-summary`,
            role: 'assistant',
            content:
              data.message
              || `Finished filling '${data.column ?? ''}' (${data.filled ?? 0} of ${data.total ?? 0} rows).`,
          },
        ]);
      } else if (message.type === 'reextraction_started') {
        // A chat-triggered reextract runs in the background after the turn ends;
        // show a spinner until the completion event posts the model's recap.
        const data = (message.data ?? {}) as unknown as { columns?: string[] };
        setReextractionRunning({ columns: data.columns ?? [] });
      } else if (message.type === 'reextraction_completed') {
        const data = (message.data ?? {}) as unknown as {
          columns?: string[]; summary?: string;
          coverage?: { column: string; filled: number; empty: number }[];
          total_rows?: number;
        };
        setReextractionRunning(null);
        const fallback = () => {
          const cols = data.coverage ?? [];
          if (cols.length) {
            return `Re-extraction finished. ${cols
              .map((c) => `'${c.column}': ${c.filled} of ${data.total_rows ?? c.filled + c.empty} rows filled`)
              .join('; ')}.`;
          }
          const names = data.columns?.length ? ` for ${data.columns.map((c) => `'${c}'`).join(', ')}` : '';
          return `Re-extraction finished${names}.`;
        };
        appendMessages([
          {
            id: `${Date.now()}-reextract-summary`,
            role: 'assistant',
            content: data.summary || fallback(),
          },
        ]);
      } else if (message.type === 'reextraction_failed') {
        const data = (message.data ?? {}) as unknown as { error?: string };
        setReextractionRunning(null);
        appendMessages([
          {
            id: `${Date.now()}-reextract-failed`,
            role: 'assistant',
            content: `Re-extraction failed: ${data.error ?? 'unknown error'}.`,
          },
        ]);
      } else if (message.type === 'reextraction_stopped') {
        // A user-halted run still merges whatever it processed before stopping,
        // so report partial progress rather than treating it as a failure. This
        // is the third and final terminal event that must clear the spinner.
        const data = (message.data ?? {}) as unknown as {
          processed_documents?: number; total_documents?: number; message?: string;
        };
        setReextractionRunning(null);
        const progress =
          data.processed_documents != null && data.total_documents != null
            ? ` Kept results from ${data.processed_documents} of ${data.total_documents} documents processed so far.`
            : '';
        appendMessages([
          {
            id: `${Date.now()}-reextract-stopped`,
            role: 'assistant',
            content: `${data.message ?? 'Re-extraction stopped'}.${progress}`,
          },
        ]);
      }
    };
    webSocketService.addMessageHandler(handler);
    return () => webSocketService.removeMessageHandler(handler);
  }, [sessionId, appendMessages]);

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
    try {
      const tools = await runTurn(() => loadTools());
      if (!tools) return; // stopped
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
    }
  }, [appendMessages, loadTools, runTurn]);

  const ask = useCallback(async () => {
    const text = input.trim();
    if (!text || busy) return;
    // The user is now driving this conversation; block any in-flight history
    // restore from overwriting what they produce.
    conversationStartedRef.current = true;
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
          content: 'Use File > New project or File > Import project to get started. Once a project exists I can inspect and edit it.',
        },
      ]);
      return;
    }

    // A new turn re-opens the live stream that a prior Stop had muted.
    suppressStreamRef.current = false;
    try {
      const response = await runTurn((signal) =>
        chatAPI.sendMessage(
          sessionId,
          {
            message: text,
            chat_id: chatId || undefined,
            session_mode: sessionMode,
            pinned_tool: pinnedTool || undefined,
            model: chatModel || undefined,
          },
          signal,
        ),
      );
      // null means the user pressed Stop; leave the transcript as-is.
      if (response) applyChatResponse(response);
    } catch (err: any) {
      appendMessages([
        {
          id: `${Date.now()}-error`,
          role: 'assistant',
          content: err?.response?.data?.detail || err?.message || 'That workspace action failed.',
        },
      ]);
    }
  }, [
    appendMessages,
    applyChatResponse,
    busy,
    chatId,
    chatModel,
    input,
    pinnedTool,
    runTurn,
    sessionId,
    sessionMode,
    showToolsList,
  ]);

  const confirmPendingAction = useCallback(async () => {
    if (!pendingAction || !sessionId) return;
    suppressStreamRef.current = false;
    try {
      const response = await runTurn((signal) =>
        chatAPI.confirmAction(sessionId, pendingAction.chatId, signal),
      );
      if (response) applyChatResponse(response); // null => stopped
    } catch (err: any) {
      appendMessages([
        {
          id: `${Date.now()}-confirm-error`,
          role: 'assistant',
          content: err?.response?.data?.detail || err?.message || 'The confirmed action failed.',
        },
      ]);
    }
  }, [appendMessages, applyChatResponse, pendingAction, runTurn, sessionId]);

  const cancelPendingAction = useCallback(async (): Promise<boolean> => {
    if (!pendingAction || !sessionId) return true;
    try {
      const response = await runTurn(() =>
        chatAPI.cancelAction(sessionId, pendingAction.chatId),
      );
      if (!response) return false; // stopped; the pending action still stands
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
    }
  }, [appendMessages, applyChatResponse, pendingAction, runTurn, sessionId]);

  // Stop the running turn: mute the live stream so the halted reply stops
  // landing, abort the client's wait so the composer frees up immediately, and
  // tell the backend to bail and discard the turn. The backend call is
  // best-effort -- the UI is already unblocked regardless of its outcome.
  const handleStop = useCallback(() => {
    suppressStreamRef.current = true;
    stop();
    if (sessionId) {
      chatAPI.stopChat(sessionId).catch(() => {
        // Best-effort: the turn ends server-side on its own if this misses.
      });
    }
  }, [sessionId, stop]);

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
    <aside
      className="workspace-chat"
      data-drag-active={dragActive ? 'true' : undefined}
      onDragOver={handleChatDragOver}
      onDragLeave={handleChatDragLeave}
      onDrop={handleChatDrop}
    >
      {dragActive && (
        <div className="workspace-chat-drop-overlay">
          <Paperclip className="h-5 w-5" />
          Drop to attach reference
        </div>
      )}
      <div className="workspace-chat-header">
        <div className="flex min-w-0 items-center gap-2">
          <Bot className="h-4 w-4 shrink-0" />
          <ModelSelector
            provider="gemini"
            value={chatModel}
            onChange={(nextModel) => {
              setChatModel(nextModel);
              // Chat sessions are bound to a model at creation, so start a
              // fresh session to make the switch take effect on the next turn.
              setChatId(null);
            }}
            disabled={busy}
            showDetails={false}
            triggerClassName="h-8 w-auto max-w-[240px] focus:ring-offset-0"
          />
        </div>
        <Button size="sm" variant="outline" className="ml-2 shrink-0" onClick={showToolsList} disabled={busy}>
          Tools
        </Button>
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
        {groupChatMessages(messages).map((item) =>
          item.kind === 'tool_group' ? (
            <ToolActivityGroup key={item.id} logs={item.logs} />
          ) : (
            <div
              key={item.message.id}
              className="workspace-chat-message"
              data-role={item.message.role}
            >
              <ChatMessageBody message={item.message} />
            </div>
          ),
        )}

        {busy && (
          <div className="workspace-chat-message" data-role="assistant">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <img
                src="/icon.png"
                alt=""
                aria-hidden="true"
                className="workspace-chat-thinking-logo h-5 w-5"
              />
              <span>Thinking…</span>
            </div>
          </div>
        )}

        {fillRunning && (
          <div className="workspace-chat-message" data-role="assistant">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              <span>
                {fillRunning.column
                  ? `Filling '${fillRunning.column}' in the background…`
                  : 'Filling column in the background…'}
              </span>
            </div>
          </div>
        )}

        {reextractionRunning && (
          <div className="workspace-chat-message" data-role="assistant">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              <span>
                {reextractionRunning.columns.length
                  ? `Re-extracting ${reextractionRunning.columns
                      .map((c) => `'${c}'`)
                      .join(', ')} in the background…`
                  : 'Re-extracting in the background…'}
              </span>
            </div>
          </div>
        )}

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
        {(references.length > 0 || referenceError) && (
          <div className="mb-2 flex flex-col gap-1">
            {references.length > 0 && (
              <div className="text-[11px] text-muted-foreground">
                Stays available to the assistant for this whole conversation
                until you remove it.
              </div>
            )}
            {references.map((ref) => (
              <div
                key={ref.id}
                className="flex items-center gap-2 rounded-md border bg-muted/30 px-2 py-1 text-xs"
                title={`External reference document${ref.truncated ? ' (truncated)' : ''}`}
              >
                <FileText className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                <span className="truncate">{ref.filename}</span>
                {ref.truncated && (
                  <span className="shrink-0 text-muted-foreground">(truncated)</span>
                )}
                <button
                  type="button"
                  className="ml-auto shrink-0 text-muted-foreground hover:text-foreground"
                  onClick={() => handleRemoveReference(ref.id)}
                  aria-label={`Remove ${ref.filename}`}
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
            ))}
            {referenceError && (
              <div className="text-xs text-destructive">{referenceError}</div>
            )}
          </div>
        )}
        <Textarea
          value={input}
          onChange={(event) => setInput(event.target.value)}
          placeholder="Ask ScheMatiQ or type /tools"
          rows={3}
          disabled={busy}
          onKeyDown={(event) => {
            // Enter sends; Shift+Enter inserts a newline.
            if (event.key === 'Enter' && !event.shiftKey) {
              event.preventDefault();
              ask();
            }
          }}
        />
        <div className="mt-2 flex items-center gap-2">
          <input
            ref={fileInputRef}
            type="file"
            className="hidden"
            accept=".txt,.md,.markdown,.json,.csv,.tsv,.xlsx,.xls,.pdf,.docx"
            onChange={handleReferenceSelected}
          />
          <Button
            type="button"
            size="sm"
            variant="outline"
            className="h-8 px-3"
            onClick={handleAttachClick}
            disabled={!sessionId || uploadingReference}
            title="Attach an external reference document (e.g. a lookup spreadsheet). It is not treated as a source document."
          >
            {uploadingReference ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Paperclip className="h-4 w-4" />
            )}
            Attach reference
          </Button>
          {busy ? (
            <Button
              type="button"
              size="icon"
              variant="destructive"
              onClick={handleStop}
              className="ml-auto h-8 w-8 rounded-lg"
              aria-label="Stop"
              title="Stop"
            >
              <Square className="h-4 w-4" />
            </Button>
          ) : (
            <Button
              type="button"
              size="icon"
              onClick={ask}
              disabled={!input.trim()}
              className="ml-auto h-8 w-8 rounded-lg"
              aria-label="Send message"
              title="Send message (Enter)"
            >
              <ArrowUp className="h-4 w-4" />
            </Button>
          )}
        </div>
      </div>
    </aside>
  );
}
