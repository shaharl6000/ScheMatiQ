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
| `index.tsx` | The main `Workspace()` component (default export). Owns nearly all state, effects, data fetching, and the top-level layout. This is the orchestrator; the other components are presentational children it wires together. |
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

- **State lives in `index.tsx`.** The child components are presentational: they
  take data and callbacks as props and render. If you need new shared state, add
  it to `index.tsx` and pass it down — don't introduce cross-imports between
  siblings.
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
- Data fetching, WebSocket handling, re-extraction, add-documents, project
  lifecycle → `index.tsx` (this is the heavy logic; a future refactor may extract
  it into hooks — see the note below)
- A new shared type/constant/pure helper → the matching leaf file

## Known follow-up

`index.tsx` still holds the full `Workspace()` body (~1400 lines of state and
effects). A planned Stage 2 will extract this into custom hooks
(`useProjectData`, `useReextraction`, `useAddDocuments`, `useWorkspaceLayout`,
etc.) to shrink the orchestrator. Until then, treat `index.tsx` as the single
source of Workspace state.
