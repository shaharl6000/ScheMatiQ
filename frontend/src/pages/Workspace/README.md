# `pages/Workspace/` — module map

The Workspace is ScheMatiQ's default flow (route `/`, and `/workspace/:sessionId`
for a single project): one spreadsheet-style surface with the toolbar chrome, an
editable data grid, a chat panel, and a document viewer, all bound to one
session. This directory is the result of splitting a single ~4200-line
`Workspace.tsx` into per-component modules. Read this file before editing so you
know where things live and what depends on what.

## Files

| File | Role |
|------|------|
| `index.tsx` | The main `Workspace()` component (default export). Owns the project-data layer (`status`, `session`, `schema`, `data`, `refresh`, etc.), view-local state, and the top-level layout. This is the orchestrator; it wires the child components and the hooks together. |
| `hooks/` | Custom hooks extracted from `Workspace()`: `useAddDocuments`, `useWorkspaceLayout`, `useReextraction`, `useWorkspaceSocket`. Each owns one cohesive cluster of state + effects and returns the values/handlers `index.tsx` needs. See "State ownership" below. |
| `types.ts` | Shared TypeScript types used across the module (`SheetId`, `WorkspaceMessage`, `SheetSelection`, `TableDisplayOptions`, `CellFormat`, etc.). No logic. |
| `constants.ts` | Shared constants and static config (`SHEETS`, `WORKSPACE_MENUS`, tooltip tables, `DEFAULT_PROVIDER`, feature flags like `SHOW_API_KEY_FIELD`). No logic. |
| `helpers.ts` | Shared pure functions (`buildConfig`, `dataEquals`, `formatCost`, `buildExportFilename`, `schemaFromLoadSession`, HTML-escaping and markup helpers, etc.). Pure and side-effect-free — safe to import anywhere. |
| `SpreadsheetSurface.tsx` | The Handsontable data grid. **Owns the sole `registerAllModules()` call** — do not call it elsewhere. Receives data/schema/selection via props from `index.tsx`. |
| `SpreadsheetChrome.tsx` | The top toolbar (File/Edit/View menus, font/format controls, source-document toggle). |
| `NewProjectDialog.tsx` | Dialog for creating/opening a project (the entry point when no session is loaded). |
| `ProjectDetailsDialog.tsx` | Dialog showing project metadata/details. |
| `PendingRerunBanner.tsx` | Inline banner prompting the user to confirm a schema/unit re-run. |
| `chat/ChatPanel.tsx` | The chat side panel: message list, input, tool logs, pending-action UI. |
| `chat/ChatMessageBody.tsx` | Renders one chat message (Markdown for assistant turns, plain text otherwise). Used only by `ChatPanel`. |
| `Workspace.css` | All styles for the module. |

## Dependency direction

```
index.tsx (Workspace)
├── hooks/useAddDocuments     ← state + effects, returns handlers
├── hooks/useWorkspaceLayout  ← chat divider drag
├── hooks/useReextraction     ← re-extraction lifecycle
├── hooks/useWorkspaceSocket  ← WebSocket lifecycle (receives refresh + setters)
├── SpreadsheetSurface   ← props only
├── SpreadsheetChrome    ← props only
├── NewProjectDialog     ← props only
├── ProjectDetailsDialog ← props only
├── PendingRerunBanner   ← props only
└── chat/ChatPanel       ← props only
        └── chat/ChatMessageBody

all of the above import from → types.ts · constants.ts · helpers.ts
```

Rules that keep this clean:

- **State lives in `index.tsx` or a hook in `hooks/` — never in a child
  component.** The child components are presentational: they take data and
  callbacks as props and render. Shared state belongs either in `index.tsx` or a
  hook it calls; don't introduce cross-imports between siblings.
- **`types.ts` / `constants.ts` / `helpers.ts` are leaves.** They must not import
  from any of the component files. Anything shared by two or more files goes
  here.
- **No sibling-to-sibling imports** except `ChatPanel → ChatMessageBody`, which
  is an internal detail of the chat subfolder.
- **`registerAllModules()` runs once**, in `SpreadsheetSurface.tsx`.

## Where to make a change

- Grid rendering / cell formatting / selection behavior → `SpreadsheetSurface.tsx`
- Toolbar menus, fonts, export buttons → `SpreadsheetChrome.tsx`
- Chat UI, message rendering, tool logs → `chat/`
- Add-documents upload flow → `hooks/useAddDocuments.ts`
- Re-extraction / rerun banner / confirm dialog → `hooks/useReextraction.ts`
- WebSocket connection and message routing → `hooks/useWorkspaceSocket.ts`
- Chat divider drag / layout → `hooks/useWorkspaceLayout.ts`
- Data fetching (`refresh`), project lifecycle, and view-local state → `index.tsx`
- A new shared type/constant/pure helper → the matching leaf file

## State ownership

`index.tsx` retains the **project-data layer** (`status`, `session`, `schema`,
`data`, `unitData`, `documents`, `config`, `refresh`, `refreshSilent`, loading
flags) and **view-local state** (`activeSheet`, `tableDisplay`, `cellFormats`,
grounding, dialog flags, JSX-bound refs). The data layer stays here on purpose:
it is tightly coupled to the WebSocket and re-extraction flows, so extracting it
would require shared refs or dependency-array changes that risk behavior drift.

The four hooks each own one cohesive cluster and take `refresh` plus any needed
setters (e.g. `setReextraction`, `setActiveSheet`) as arguments, so `index.tsx`
remains the single wiring point. `useReextraction` returns `setReextraction`,
which is passed into `useWorkspaceSocket` (progress messages) and used by the
monitor's optimistic banner update — declare `useReextraction` before
`useWorkspaceSocket` so that setter exists when the socket hook is wired.

If you add a new cohesive cluster of state + effects, prefer a new hook in
`hooks/` over growing `Workspace()`.
