# Design record: availability-based extraction gating

**Status:** Implemented (see "As shipped" below).
**Scope:** Backend — chat tool gating, project export/import, config persistence.

This records why the chat agent gates extraction tools on *document
availability* rather than *session type*, and how a downloaded project carries
what it needs to be re-run after import. It is a design record: the plan below
is already merged; the "Open items" section is the remaining work.

## Problem

The chat agent originally decided which tools it could call from the *session
mode* (`schematiq` vs `load`), not from whether the session could actually be
extracted. In `load` mode every extraction tool was hidden except `reprocess`,
which by design keeps skipped documents skipped. So once a project was imported,
the chat could not re-extract or re-discover a document — even after the user
re-attached its source file via "Show source document".

The `load`/`schematiq` split was a proxy for "is there an extraction pipeline to
run here". That proxy broke once imported sessions could carry or re-attach
source documents. The correct gate is **document availability**, which is what
the frontend already used: `canRediscoverSchema = sessionMode === 'schematiq' ||
hasSourceDocuments`.

## Design

1. **Availability predicate, not session type.** Extraction tools are flagged
   `requires_documents` on their `ToolSpec`. `get_tools_for_context` takes an
   `extraction_capable` flag; a `requires_documents` tool is offered outside its
   declared `session_modes` only when the session is extraction-capable
   (`schematiq` always; `load` once source documents are reachable). Only
   `requires_documents` tools are affected — every other tool keeps its existing
   mode-based gating, so no existing flow changes.

2. **Cheap capability signal.** `ChatAgentService._extraction_capable` runs per
   message and must stay cheap and synchronous: a local-disk check
   (`has_local_source_documents`) plus a cloud-dataset check for imported
   projects whose files live only in Supabase (`session.metadata.cloud_dataset`).
   The downstream gated path still runs the full local+cloud
   `precheck_document_availability` and returns a precise error if the files turn
   out unreachable — an over-eager offer degrades to a clear message, never a
   silent no-op.

3. **Config + documents travel with the project.** Full round-trip needs both
   the source files and a runnable config to survive export/import:
   - The bundle export gathers the union of row-referenced, skipped, and on-disk
     source files (not just row-backed ones), so skipped and re-attached files
     travel too.
   - `export-complete` emits a consolidated config; import reassembles a runnable
     `schematiq_config.json`. Old exports without it fall back to run-time
     synthesis from `RELEASE_CONFIG`, so imports never fail on a missing block.
   The plain JSON export stays data-only by design; a JSON must not embed
   document bytes.

4. **Rediscovery from chat.** A `rediscover` tool (expensive, availability-gated
   like the others) triggers a full schema + observation-unit rebuild from the
   source documents, re-evaluating previously-skipped documents. It mirrors the
   proven `POST /load/rediscover` sequence
   (`pending_observation_unit_rediscovery` → `prepare_resume` → `run_schematiq`).
   For UPLOAD sessions it synthesizes a `config.json`; for SCHEMATIQ it reuses the
   existing one.

## Backward compatibility

The only behavioral change is a capability being *unlocked* for load sessions
that have documents — a strict superset of prior behavior.

| Session | Before | After |
|---|---|---|
| `schematiq` (any) | full extraction tools | unchanged |
| `load`, no documents | only `reprocess` | unchanged |
| `load`, documents available | only `reprocess` | extraction tools now offered |
| Old export (no config block) | synthesis at rediscover | synthesis fallback (unchanged) |

## As shipped

- Availability gating with `requires_documents` / `extraction_capable` (#446).
- Bundle includes skipped and on-disk source documents (#447).
- Import reassembles a runnable `schematiq_config.json` (#448).
- `rediscover` chat tool for full rediscovery, both session types (#449).
- `only_empty` no longer blocks re-extracting a skipped document (#443).
- `continue_discovery` gated on document availability like the other extraction
  tools (its service is session-type-agnostic: no `SessionType` gate, schema
  built from `session.columns`, documents resolved from session-local dirs,
  raises clearly when none are found).
- Cloud-only imported sessions are extraction-capable via `cloud_dataset`.

The four document-backed extraction tools — `reextract`, `extract_cells`,
`rediscover`, `continue_discovery` — now share one gate: document availability.

## Open items

- **`run_schematiq`** stays SCHEMATIQ-only: its handler hard-requires a
  SCHEMATIQ session (an initial run from a query), so it is intentionally not
  document-gated. Re-discovery for imported sessions is `rediscover`'s job.
- **Automated end-to-end coverage** of the UPLOAD extraction/rediscovery path
  (currently smoke-tested manually; the pipeline run needs an LLM).
- **`continue_discovery` inline precheck** — its "no documents" error surfaces
  from the background task rather than synchronously in the handler; a cheap
  handler precheck would give an immediate message. Minor UX.
