# ScheMatiQ Workspace + Chat Agent — Conceptual Spec & Verification Checklist

This is a conceptual map of everything the `feature/chat-tool-calling` branch should support, organized so each line can be checked as working or not. It covers two coupled systems: the spreadsheet Workspace and the Gemini tool-calling chat agent. It describes *what must be true*, not how to build it.

Legend for the status column when you walk through it: ☐ not verified, ✔ verified working, ✘ broken, N/A not applicable.

> **Verification pass — 2026-06-08.** Every line below was checked against the actual code with `file:line` evidence. Sections 2a / 3a / 3c / 4b reflect the fixes in commit `39a8ddb`. The verification pass found five defects (**3b.4, 6c.1, 7.8, 9.1, 9.5**) which have now been **fixed** (see lines, marked ✔ *(Fixed.)*). Remaining non-✔: hygiene gate **§12 ruff/black** (✘, pre-existing, not from this work); and items needing a live run (§2.8, §9.4, §10.3, plus §12 lint-deps/branch-base/PR-desc) left ☐ with a note.

---

## 0. The cascade (the mental model behind everything)

The three sheets are not independent. There is a one-directional dependency chain, and every re-run decision follows from it:

**Observation unit → Schema → Data.**

- The **observation unit** defines what each row *is*. Changing it changes the granularity of the whole table, so it can invalidate both the schema and all the data.
- The **schema** defines the columns. Changing a column changes only that column's values.
- The **data** is the extracted values. A manual data edit is a leaf: it changes nothing upstream.

This is why the re-run scope differs by edit type (see sections 3 and 4): a unit edit cascades down the whole chain, a schema-column edit touches only its column, and a data-cell edit triggers nothing.

---

## 1. Session modes (the spine everything hangs off)

Every behavior below has to be correct in both modes, so this is the first axis to fix in your head.

- **schematiq mode**: a live project with source documents. Schema discovery, value extraction, re-extraction, and rediscovery are all possible because the documents exist.
- **load mode**: an imported/saved static project. Data and schema are present, but there may be no source documents, so extraction-style operations may be limited or impossible.

Checklist:
- ✔ Mode is determined correctly on load (URL `?mode=load` vs default, plus the fallback that detects a load session when a schematiq fetch fails). — `Workspace.tsx:1663` (URL param), `Workspace.tsx:1750-1763` (fallback `setSessionMode('load')` when schematiq fetch throws).
- ✔ Every read path uses the right API per mode (schematiq endpoints vs `loadAPI`). — `Workspace.tsx:1723-1735` (loadAPI), `1738-1744` (schematiqAPI).
- ✔ Every write path uses the right API per mode (this is where the known cell-edit bug lived). — Data cell now passes `_row_index` for keyless load rows (`Workspace.tsx:837-856`, `api.ts:360-371`); schema/unit writes (`schemaAPI`/`observationUnitAPI`) edit metadata only and work in both modes.
- ✔ Every expensive action is correctly enabled/disabled per mode, with a reason shown when disabled. — Rediscovery guard `Workspace.tsx:2118-2125`; cost-estimate guard `~1893-1898`; re-extract precheck `2051-2064`.

---

## 2. Spreadsheet rendering and sizing

- ✔ Grid renders on first load in a fresh project (no blank/zero-size table). — `Workspace.tsx:703-736` (`useLayoutEffect` + `measureGrid` + rAF retries + min 320×260).
- ✔ Grid fills the pane and resizes when the chat divider is dragged. — `layoutRevision` from `chatWidth` (`Workspace.tsx:2198`, passed `~2298`), in the measure effect dep list (`736`).
- ✔ Grid resizes on window resize. — `window.addEventListener('resize', measureGrid)` `Workspace.tsx:729`.
- ✔ Switching sheets (Data / Observation Unit / Schema) re-measures and renders correctly. — `activeSheet` in measure-effect deps `Workspace.tsx:736`.
- ✔ Table is interactive: a cell can enter edit mode on a fresh project. — HotTable has no global `readOnly`; only per-column/cell (`Workspace.tsx:1027-1056`).
- ✔ Read-only columns are actually read-only (`unit_name` in Data, `field` in Unit, non-editable observation-unit fields). — `unit_name` readOnly `Workspace.tsx:817`; `field` readOnly `808`; unit `value` readOnly unless in `EDITABLE_OBSERVATION_UNIT_FIELDS` (`239`, `1054-1056`).
- ✔ Re-mount on schema/format change does not swallow an in-progress edit. — HotTable `key` is `${activeSheet}-${cols}-${formatVersion}` (`Workspace.tsx:1011`); `formatVersion` bumps only on explicit format apply (`~1947`), not on schema state updates.
- ☐ The 5-second polling refresh does not reset the table mid-edit. — Polling uses `refresh({silent:true})` (`Workspace.tsx:~1792`) which still calls `setData` (`1731/1747`); there is **no `isEditing()` guard**. Needs a live check (Handsontable may preserve the open editor across data set).

### 2a. Loading / progress indicator (so the user knows something is happening)
- ✔ When a saved project is being loaded, a centered overlay/window appears over the board saying the data is loading. — Added: `Workspace.tsx` overlay (`.workspace-loading-overlay`) gated on `loading && sessionId`; styles `Workspace.css` `.workspace-loading-overlay/.workspace-loading-card`.
- ✔ The indicator shows during the initial fetch of a loaded project, and clears once data is on screen. — `loading` set in `refresh` non-silent path (`Workspace.tsx:1719-1721`), cleared in `finally` (`1765-1767`).
- ✔ A long-running operation (re-extraction, rediscovery, run) also shows visible progress. — Bottom progress bar from `status`/`reextraction` (`Workspace.tsx:~2200-2203`, `~2340`).
- ✔ The indicator does not linger or flicker once the table is populated. — Silent polling/WS refresh (`refresh({silent:true})`) never raises the overlay (`1719`).

---

## 3. Manual editing in the spreadsheet

Three sheets, three editing contracts, following the cascade in section 0. The core principle: **a manual cell edit is a final human correction; a schema edit re-extracts one column; a unit edit cascades through schema and then data.**

### 3a. Data sheet — cell edits (a leaf, no cascade)
- ✔ Editing a data cell persists the value (schematiq mode). — `Workspace.tsx:837-847` → `schematiqAPI.updateCell` → `schematiq.py:455-477` → `data_editor.update_cell` (`data_editor.py:37-134`).
- ✔ Editing a data cell persists the value (**load mode** — the reported bug). — Now sends `_row_index` fallback when `row_name` absent: frontend `Workspace.tsx:834-856`, backend match-by-index `data_editor.py:74-90`, route `schematiq.py:456-470`. Index is the absolute non-blank line stamped in `file_parser.py:get_paginated_data`/`_load_all_rows`.
- ✔ Edited value survives a page reload in both modes. — `data_editor` rewrites the same `data.jsonl` (`data_editor.py:131-134`); reload re-reads it; index order is stable.
- ✔ A manual cell edit does **not** trigger any re-extract or re-run. — Data branch calls only `updateCell` + `onRefresh`, never `onEditFollowUp` (`Workspace.tsx:837-866`).
- ✔ Editing the `unit_name` column is blocked. — Data `unit_name` column `readOnly:true` (`Workspace.tsx:817`); `_`-prefixed props skipped (`832`).
- ✔ A failed cell update shows a clear error and does not silently drop the edit. — Unidentifiable row now errors instead of `continue` (`Workspace.tsx:838-848`); API failure → destructive toast (`849-855`); backend raises `ValueError`→400 (`data_editor.py:128-130`, `schematiq.py:474-475`).

### 3b. Schema sheet — column edits (re-extract that column only)
- ✔ Renaming a column flags re-extraction **for that column only**. — `affectedColumn` = new name; `onEditFollowUp('schema', [affectedColumn])` (`Workspace.tsx:910-915`).
- ✔ The re-extraction is scoped to the single affected column (not units, not whole table). — Frontend filters to requested cols (`Workspace.tsx:2027-2036`); `schemaAPI.startReextraction({columns})` → `reextraction_service` filters `target_columns` to `operation.columns` (`reextraction_service.py:971-988`, merge `1384-1389`).
- ✔ Editing a definition flags re-extraction for that column only. — `request.definition` + `affectedColumn=existing.name` (`Workspace.tsx:906-915`).
- ✔ Editing rationale / allowed_values / auto_expand_threshold flags re-extraction for that column only. — rationale/allowed_values (`Workspace.tsx:907-915`); `auto_expand_threshold` now also calls `onEditFollowUp('schema', [existing.name])` (`Workspace.tsx:898-903`). *(Fixed.)*
- ✔ Adding a new column (typing a name in the spare row) creates it and flags only it for extraction. — `schemaAPI.addColumn` then `onEditFollowUp('schema', [newName])` (`Workspace.tsx:863-872`).
- ✔ Excerpt/derived columns are excluded from re-extraction targets. — `.filter(name => !name.toLowerCase().endsWith('_excerpt'))` (`Workspace.tsx:2034`).

### 3c. Observation Unit sheet — definition edits (cascade: schema, then data)
- ✔ The observation unit is editable (name / definition / example_names save). — `EDITABLE_OBSERVATION_UNIT_FIELDS` (`Workspace.tsx:239`) → `observationUnitAPI.updateDefinition` (`951-955`).
- ✔ Editing the unit triggers, in order: **re-discover the whole schema**, then **re-extract all the data**. — Single cascade action calls `startSchemaRediscovery` → `schematiqAPI.resume` → `run_schematiq` runs Schema Discovery then Value Extraction in order (`schematiq_runner.py:~342-343`).
- ✔ The cascade order is respected (schema rediscovery feeds the data re-extraction). — Same single `run_schematiq` pipeline; not two independent calls anymore.
- ✔ Validation enforced (name required, definition min length) with a clear message. — name required + definition ≥10 chars else destructive toast (`Workspace.tsx:941-948`).
- ✔ The follow-up surface for a unit edit offers the cascade action (rediscover + full re-extract), not a single-column re-extract. — Banner unit kind = single "Rediscover schema & re-extract" (`Workspace.tsx:1408-1432`); toast action → `startSchemaRediscovery` (`~2154`).
- ✔ The cascade is gated behind explicit confirmation. — Runs only on the user clicking the banner/toast/topbar action; no auto-run.
- ✔ In load mode, the unit-edit cascade is disabled / clearly limited with a message. — Banner button disabled + tooltip when `sessionMode!=='schematiq'` (`Workspace.tsx:1424-1429`); `startSchemaRediscovery` guards + toasts (`2118-2125`); toast omits action in load mode (`~2152`).

---

## 4. The re-run follow-up surface

- ✔ A banner appears summarizing what changed and the scope. — `PendingRerunBanner` shows "Schema changed"/"Observation unit changed" + column summary (`Workspace.tsx:1408-1418`).
- ✔ A toast appears with an inline action button. — `notifyEditFollowUp` toasts with `ToastAction` (`Workspace.tsx:~2150-2173`).
- ✔ The toolbar "Re-extract" button enables when something is pending and disables otherwise. — `rerunDisabled={!sessionId || !pendingRerunKind || rerunStarting}` (`Workspace.tsx:~2251`).
- ✔ The "Data" menu item mirrors the toolbar button state. — Menu "Re-extract table" disabled via same `rerunDisabled` (`Workspace.tsx:~1503-1508`).
- ✔ All of these trigger the *same* underlying action, with the same scope. — Banner/toast/topbar all funnel to `startReextraction`/`startSchemaRediscovery` with the same column scoping (`Workspace.tsx:2024-2113`, `runPendingEdits:2176-2183`).
- ✔ Pending state clears after a successful start and after switching sessions. — `clearPendingRerun()` after start (`Workspace.tsx:2090`); reset on `sessionId` change (`1778-1783`).
- ✔ A "Dismiss" path exists. — Banner Dismiss → `onDismiss={clearPendingRerun}` (`Workspace.tsx:~1442`, `~2262`).

### 4a. Schema-column re-extraction (narrow scope)
- ✔ Uses the existing re-extraction API (same as classic Visualize), not a parallel mechanism. — `schemaAPI.startReextraction` → `POST /schema/reextract/{id}` → `reextraction_service.start_reextraction`.
- ✔ Targets exactly the affected column(s), nothing else. — see 3b.2; `reextraction_service.py:971-988`.
- ✔ Resolves provider / model / API key correctly, including the server-keys-only case. — `Workspace.tsx:2066-2077` (falls back to gemini; omits `llm_config` when no key).
- ✔ Handles "no source documents" gracefully (clear message, no crash). — `precheckDocuments` + zero-doc guard (`Workspace.tsx:2051-2088`).
- ✔ Reports how many columns and documents are being processed. — toast + bottombar (`Workspace.tsx:2100-2103`, `2200-2202`).
- ✔ Refreshes the table when extraction progress/completion arrives. — reextraction WS handlers call `onRefresh` (`Workspace.tsx:~1808-1864`).

### 4b. Observation-unit cascade (widest scope)
- ✔ Re-discovers the whole schema from the updated unit, then re-extracts all data. — `run_schematiq` pipeline order (`schematiq_runner.py:~342-343`).
- ✔ Runs the steps in the correct dependency order. — single pipeline; discovery precedes extraction.
- ✔ Blocked with explanation outside schematiq mode. — `startSchemaRediscovery:2118-2125`; banner disabled `1424-1429`.
- ✔ Clearly the most expensive action; explicit confirmation required before it starts. — user-initiated action only; backend chat path also estimates cost for `run_schematiq` (`tool_executor.py:61-76`).

---

## 5. Chat agent — model in the loop

- ✔ A user message plus the tool list goes to Gemini in AUTO mode. — FunctionDeclarations + `FunctionCallingConfig(mode="AUTO")` (`agent_service.py:198-210`).
- ✔ The loop executes the chosen tool server-side, feeds the result back, and continues until the model returns text. — `_continue_after_tool` loop (`agent_service.py:213-301`); function response sent back (`284-288`); ends on text (`291-294`).
- ✔ Execution is **manual** (schemas declared, code runs them). — `AutomaticFunctionCallingConfig(disable=True)` (`agent_service.py:203`).
- ✔ Conversation history kept in the SDK session (thought signatures survive). — `ChatSessionState.chat` SDK object reused (`session_store.py:17-25`, `agent_service.py:303-320`).
- ✔ The loop terminates correctly and has a sane cap. — `MAX_TOOL_ITERATIONS=10` (`agent_service.py:24`, `228`); fallback text if no completion (`298-301`).
- ✔ Single-message tool-use scope; no autonomous multi-step planning. — loop bounded to one `send_message`/`_run_loop` call; processes only current response's `function_calls` (`agent_service.py:228-296`).

---

## 6. Tool registry (one source of truth)

- ✔ A single declarative registry lists every tool. — `_all_tools()` of `ToolSpec` (`tool_registry.py:33-324`); `TOOL_BY_NAME` (`327`).
- ✔ The same registry feeds both the `/tools` view and the FunctionDeclaration list. — `to_public_tool_list` (`tool_registry.py:351-361`) and `to_function_declarations` (`364-375`); both from `get_tools_for_context`.
- ✔ Model-visible inputs are only language-level parameters. — e.g. `update_cell` params row/column/value (`tool_registry.py:184-191`).
- ✔ Infrastructure identifiers injected server-side, never exposed. — `server_injects` (`tool_registry.py:24`, `195`); executor uses `state.workspace_session_id` (`agent_service.py:261-267`).
- ✔ The model reads current state via cheap read tools before editing. — system prompt instructs read-before-edit (`agent_service.py:34-35`); read tools at `tool_registry.py:35-114`.
- ✔ Adding a new tool later requires only one registry entry. — single `ToolSpec` flows to all consumers (`tool_registry.py:33-375`).

### 6a. Cheap tools (run immediately, no confirm)
- ✔ get_status, get_schema, get_observation_unit, preview_data, get_validation (reads). — `tool_registry.py:35,42,49,95,109` (all `cost_class="cheap"`).
- ✔ add_column, edit_column, delete_column, merge_columns (schema edits). — `tool_registry.py:116,130,150,163`.
- ✔ update_cell (cell edit). — `tool_registry.py:181`.
- ✔ add_unit / remove_unit. — `tool_registry.py:197,210` (plus `edit_observation_unit` `60`).
- ✔ export_table. — `tool_registry.py:223`.

### 6b. Expensive tools (pause loop, require confirm)
- ✔ run_schematiq — `tool_registry.py:~240` `cost_class="expensive"`.
- ✔ reextract (column-scoped re-extraction) — `tool_registry.py:248-265` (but see 6c.1 on scoping).
- ✔ continue_discovery — `tool_registry.py:266-274`.
- ✔ reprocess — `tool_registry.py:275-290`.
- ✔ The observation-unit cascade (rediscover + re-extract all) is the most expensive, explicitly gated. — surfaced as `run_schematiq` with cost estimate (`tool_executor.py:61-76`).
- ✔ Any "edit then reprocess" path treats reprocess as a separate expensive step. — `edit_observation_unit`/`edit_column` are cheap and only *suggest* the expensive step; reprocess/reextract are separate expensive tools.

### 6c. Tool input scoping (so the model doesn't over-extract)
- ✔ A schema-column re-extract tool accepts a column scope and the model must pass the specific column(s). — `reextract` now exposes an explicit `columns` array (`tool_registry.py:248-280`); `_handle_reextract` prefers `columns`, validates them against the schema, strips `_excerpt` targets, and **no longer silently widens — `edited_only` with no changes now raises** instead of re-extracting all (`tool_executor.py:466-489`). Covered by `tests/test_chat_executor.py` (explicit-scope / unknown-column / no-widen). *(Fixed.)*
- ✔ The unit-edit tool is the only one that legitimately triggers the full cascade; no other tool should. — `edit_observation_unit` is cheap and only suggests (`tool_registry.py:60-93`, `tool_executor.py:126-161`); the cascade itself is `run_schematiq`; no tool auto-triggers a unit re-discovery.

### 6d. Planned / stubbed
- ✔ web_search declared with `available: false`. — `tool_registry.py:292-305` (`available=False`); shown by `to_public_tool_list` but filtered out of `to_function_declarations` (`374`); handler raises "not available yet" (`tool_executor.py:528-531`).

---

## 7. Cost safety / confirmation gate

- ✔ Cost class is a property of each tool in the registry. — `ToolSpec.cost_class` (`tool_registry.py:21`); gate reads `tool.cost_class` (`agent_service.py:242`).
- ✔ When the model asks for an expensive tool, the loop pauses and a confirm card appears. — returns `pending_confirmation` + `pending_action` (`agent_service.py:242-259`); surfaced to client (`chat.py:43-51`).
- ✔ The action runs only after the user clicks Confirm. — `/confirm` endpoint executes `state.pending` (`chat.py:59-79`, `agent_service.py:106-153`).
- ✔ A Cancel path exists and cleanly abandons the pending action. — Frontend Cancel clears the card (`Workspace.tsx` `setPendingAction(null)`, `~1349`); expensive tools never auto-run. *Caveat:* there is no backend cancel endpoint, so `state.pending` lingers server-side until overwritten by the next expensive call (`agent_service.py:106-118`) — harmless (unreachable without a confirm card) but not explicitly cleared.
- ✔ Cheap tools never trigger the gate. — gate guarded by `cost_class=="expensive"` (`agent_service.py:242`); cheap tools execute inline (`261-288`).
- ✔ The gate cannot be bypassed by phrasing/model/document content. — structural on registry `cost_class`, not prompt-derived (`agent_service.py:242`, `tool_registry.py:13`).
- ✔ The widest action (unit cascade) makes its full cost clear before confirm. — `run_schematiq` estimate computes USD + API calls (`tool_executor.py:61-76`). *Note:* reextract/reprocess/continue_discovery return a generic estimate string only (`77-78`).
- ✔ Which tool the model selected is logged. — `logger.info("Chat tool selected: %s (cost=%s, session=%s)", ...)` in the loop after the tool is resolved (`agent_service.py:~242`). *(Fixed.)*

---

## 8. Chat UX

- ✔ When a tool runs, a tool-log line appears and updates to a result. — `kind:'tool_log'` + `data-tool-status` (`Workspace.tsx:1340-1347`); CSS states (`Workspace.css:~529-535`).
- ✔ Tool logs read as one continuous conversation. — all roles rendered in one `messages.map` (`Workspace.tsx:~1339`).
- ✔ `/tools` lists tools available in the current context (session + mode aware). — `chatAPI.getTools(sessionId, sessionMode)` (`Workspace.tsx:~1154`, `api.ts:981-991`); `/tools` command (`~1237`).
- ✔ Expensive tools show a cost badge in the list. — `' [cost]'` (`Workspace.tsx:~1106`, dropdown `~1320`).
- ✔ Planned tools show as planned. — `' (planned)'` when `!available` (`Workspace.tsx:~1107`).
- ✔ The user can pin a tool. — `pinnedTool` state + dropdown → `pinned_tool` (`Workspace.tsx:~1138`, `~1249`).
- ✔ Errors surface as readable chat messages. — failed send/confirm/load append messages (`Workspace.tsx:~1219-1225`, `~1264-1269`, `~1292-1299`).
- ✔ After a mutating tool the spreadsheet refreshes; after a read tool it does not. — `CHAT_MUTATION_TOOLS` gate (`Workspace.tsx:1067-1080`, `1189-1191`).
- ✔ If the agent already ran a re-extract/cascade itself, the UI does not also nag. — `alreadyFollowedUp` (`Workspace.tsx:1193-1204`).
- ✔ The "no session yet" case guides the user. — "Open > New Project or Import Project…" (`Workspace.tsx:~1242-1251`).

---

## 9. Consistency between chat edits and manual edits

- ✔ A schema-column edit via chat flags the same single-column re-extract as a manual schema edit. — the backend now stamps the affected column(s) on the tool_log (`agent_service._affected_columns`, `ChatTurnMessage.columns` in `models/chat.py`), and the chat follow-up passes them: `onEditFollowUp('schema', editedColumns)` (`Workspace.tsx:1202-1210`). Matches manual `onEditFollowUp('schema', [affectedColumn])` (`915`). *(Fixed.)*
- ✔ A unit edit via chat triggers the same full cascade as a manual unit edit. — both call `onEditFollowUp('unit')` (`Workspace.tsx:1200-1201` vs `957`).
- ✔ A cell edit via chat behaves like a manual cell edit (no re-run). — `update_cell` only refreshes (`CHAT_MUTATION_TOOLS`), not in `CHAT_SCHEMA_FOLLOWUP_TOOLS` (`Workspace.tsx:1072,1082-1087`).
- ☐ Both paths respect the cost gate identically. — Chat expensive tools hit the confirm card (`agent_service.py:242-259`); manual re-extract is itself a deliberate click but has no cost-confirm card. Different mechanisms; both require an explicit user action. Needs a product call on whether they must match.
- ✔ Neither path lets a column edit silently widen into a full re-extract or a unit re-discovery. — chat now propagates the edited column(s) to the follow-up (`Workspace.tsx:1202-1210`) so the banner re-extract stays scoped; delete-only changes intentionally skip the prompt. Server-side, the `reextract` tool no longer widens under `edited_only` (see 6c.1). *(Fixed.)*

---

## 10. Real-time updates

- ✔ WebSocket messages for progress / schema / extraction / reprocessing / unit updates trigger a refresh. — handler maps those types to `refresh({silent:true})` (`Workspace.tsx:~1867-1876`, reextraction handlers `~1808-1864`).
- ✔ Connection/heartbeat/pong noise is ignored. — early-returns on `connected`/`heartbeat`/`pong` (`Workspace.tsx:~1800-1806`).
- ☐ Polling and WebSocket together don't cause flicker or mid-edit resets. — both call silent refresh; no debounce/dedupe between in-flight polling and WS (`Workspace.tsx:~1792`, `~1867`). React batching likely hides flicker; needs a live check (related to 2.8).
- ✔ During the cascade, progress for both phases is reflected. — `schema_progress` and `reprocessing_progress` both refresh (`Workspace.tsx:~1873-1874`); status bar shows progress.

---

## 11. Security / safety boundaries

- ✔ API keys stay server-side; the model and the browser never receive them. — `get_gemini_api_key()` reads env, used only server-side (`deps.py:71-75`, `agent_service.py:~192`); never returned in responses.
- ✔ Instructions inside documents/tool results are treated as data, not commands. — tool results sent as function responses `{"result": ...}` (`agent_service.py:~317`); system instruction fixed at chat creation (`207`).
- ✔ No expensive/LLM flow runs without an explicit user click. — expensive tools return `pending_confirmation` and only run via `/confirm` (`agent_service.py:242-259`, `chat.py:59-79`).
- ✔ Permissions are per-action, not generalized from one approval. — `confirm_pending` clears `state.pending` immediately; single `Optional[PendingToolCall]` (`agent_service.py:117-118`, `session_store.py:24`).

---

## 12. Engineering hygiene (gate to merge)

- ✘ `cd schematiq-lib && ruff check . && black --check .` passes. — ruff reports 114 errors and black aborts on a Python 3.12.5 AST-safety issue. **Pre-existing** (none in the three-fix diff; the touched backend files have only pre-existing import/unused-var warnings). Gate as written does not currently pass.
- ✔ Chat tool registry tests pass. — `tests/test_chat_tool_registry.py` + `tests/test_chat_executor.py`: 8 passed (run with the backend `.venv` + `pytest-asyncio`).
- ✔ `npm run build` passes with no type errors. — `tsc --noEmit` exit 0 after the fixes.
- ☐ No unused/incorrect `useCallback`/`useEffect` dependency lists (lint clean). — eslint not run in this pass.
- ☐ Branch is based on `experimental-excel` with latest `main` merged in; nothing lost. — not verified against git history this pass.
- ☐ PR description documents the rerun policy. — no PR opened yet (branch pushed as `feature/chat-tool-calling`).

---

## Known open items as of this review

1. **Load-mode cell editing** — FIXED (commit `39a8ddb`): keyless load rows now edit via an absolute `_row_index` fallback; silent drop replaced with a clear error. (§1.3, §3a.)
2. **Column-scoped re-extraction in code** — Manual schema edits are correctly column-scoped (§3b.2, §4a.2). The chat `reextract` tool now takes explicit `columns` and no longer widens under `edited_only` (§6c.1), and chat schema edits now carry column scope to the follow-up (§9.1/§9.5). *(Closed.)*
3. **Observation-unit cascade** — FIXED: one gated action runs rediscover→re-extract in order via `run_schematiq` (§3c, §4b).
4. **Load-progress indicator** — FIXED: centered overlay added, gated on non-silent loading (§2a).
5. **Browser verification** — still pending live: cell-edit persistence in a loaded keyless project, the chat-vs-manual scope bugs above, mid-edit polling/WS behavior (§2.8, §10.3), and the unit cascade running end-to-end (LLM run — needs explicit go-ahead).
6. **Backend agent code** — REVIEWED this pass (§5–§8, §11 mostly ✔). Defects surfaced and now fixed: §6c.1 (reextract scoping/fallback), §7.8 (tool-selection logging), §3b.4 (auto_expand_threshold re-extract flag). Remaining caveat: §7.4 (no backend cancel endpoint — frontend Cancel clears the card; server `state.pending` lingers harmlessly until overwritten).

### Defects found this pass — all fixed
- **§3b.4** — ✔ `auto_expand_threshold` now flags a single-column re-extract (`Workspace.tsx:898-903`).
- **§6c.1** — ✔ `reextract` takes explicit `columns`, validates them, and no longer widens under `edited_only` (`tool_registry.py:248-280`, `tool_executor.py:466-489`; tests in `test_chat_executor.py`).
- **§7.8** — ✔ selected tool logged server-side (`agent_service.py:~242`).
- **§9.1 / §9.5** — ✔ chat schema edits carry the edited column(s) to the follow-up (`agent_service._affected_columns`, `models/chat.py` `ChatTurnMessage.columns`, `Workspace.tsx:1202-1210`).
