# CLAUDE.md

## Project Overview

ScheMatiQ implements query-based schema discovery — takes a user query + documents, iteratively discovers a table schema, then extracts values into a structured table. Production-deployed on Railway with Supabase storage.

## Architecture

```
ScheMatiQ/
├── frontend/        # React 18 + TypeScript + Tailwind/shadcn (Railway Service 1)
├── backend/         # FastAPI + WebSocket (Railway Service 2)
├── schematiq-lib/   # Core ScheMatiQ algorithms (Python package, imported by backend)
├── research/        # Datasets, experiments, evaluation results
└── shared/          # (currently empty)
```

**Request flow:** Frontend → Backend route (`app/api/routes/`) → Service (`app/services/`) → schematiq-lib (`schematiq/`) → LLM API. Real-time progress via WebSocket.

### Frontend
- Pages: **Landing, ScheMatiQConfig, Load, Visualize**
- Key dirs: `src/pages/`, `src/components/ui/`, `src/contexts/`, `src/hooks/`, `src/types/`, `src/services/`, `src/constants/`

### Backend
- **Routes** (`app/api/routes/`): `schematiq.py`, `schema.py`, `load.py`, `observation_unit.py`, `units.py`, `cloud_data.py`, `websocket.py`
- **Services** (`app/services/`):
  - Core pipeline: `schematiq_runner`, `continue_discovery_service`, `reextraction_service`
  - Data management: `session_manager`, `schema_manager`, `observation_unit_manager`, `unit_view_service`, `data_editor`, `file_parser`, `upload_document_processor`
  - Infrastructure: `websocket_manager`, `websocket_mixin`, `pdf_utils`
  - Research: `data_collection_service` (release-mode Google Drive archival)
- **Storage** (`app/storage/`): `StorageInterface` ABC → `LocalStorage` (dev) / `SupabaseStorage` (prod), via `StorageFactory`. Also `google_drive.py` and `google_sheets.py` for research data collection.
- **Models** (`app/models/`): `session`, `schematiq`, `upload`, `modification`, `unit`
- **Core** (`app/core/`): `config.py` (RELEASE_CONFIG, env vars), `exceptions.py`, `logging_utils.py`
- **Working dirs** (gitignored, runtime): `sessions/`, `schematiq_work/`, `data/`, `templates/`, `initial_schemas/`

### schematiq-lib
- `schematiq/core/` — Schema discovery pipeline, observation unit discovery, LLM backends, retrievers, cost estimation, document preprocessing, prompts, model specs
- `schematiq/value_extraction/` — TableBuilder, PaperProcessor, LLMCache (subpackages: `core/`, `utils/`, `config/`)
- `schematiq/evaluation/` — Schema/row evaluation, ground truth comparison, data quality metrics
- See `schematiq/__init__.py` for all public exports

## Key Concepts

- **Observation Unit**: The entity each table row describes (e.g., "research paper", "patient"). Discovered automatically from query + documents BEFORE schema generation. Defined in `schematiq.core.schema.ObservationUnit`.
- **Schema Discovery**: Embedding-based retrieval → LLM schema generation → semantic merging → convergence check. Iterative across document batches.
- **Value Extraction**: Groups documents by observation unit, extracts column values per document, assembles rows. Writes incrementally to prevent data loss.
- **Continue Discovery**: Extends schema after initial convergence by processing more documents. Managed by `ContinueDiscoveryService`.
- **Reextraction**: Re-runs value extraction with current/edited schema. Managed by `ReextractionService`.
- **Cost Estimation**: Estimates API costs before expensive LLM ops. `schematiq.core.cost_estimator` → `/api/schematiq/cost-estimate` endpoint.

## Development Commands

```bash
# Frontend
cd frontend && npm install --legacy-peer-deps && npm start    # Dev server :3000

# Backend
cd backend && pip install -r requirements.txt && uvicorn app.main:app --reload --port 8000

# schematiq-lib (editable install)
cd schematiq-lib && pip install -e .
```

## Environment Variables

**Backend** — MUST set at least one LLM key:
- `OPENAI_API_KEY`, `TOGETHER_API_KEY`, `GEMINI_API_KEY`
- `ALLOWED_ORIGINS` (default: http://localhost:3000)
- `SUPABASE_URL`, `SUPABASE_KEY` (optional, for prod storage)
- `MAX_CONCURRENT_SESSIONS` (default: 5), `SCHEMATIQ_THREAD_POOL_SIZE` (default: 6)
- `DEVELOPER_MODE=true` to unlock all features (see below)

**Frontend** — `REACT_APP_API_URL`, `REACT_APP_WS_URL`, `REACT_APP_ENABLE_DEBUG`

## Release Mode vs Developer Mode

Release mode (default) restricts features for public use. Set `DEVELOPER_MODE=true` to unlock.

| Setting                  | Release Mode           | Developer Mode          |
|--------------------------|------------------------|-------------------------|
| Document limit           | 20                     | 10,000                  |
| Bypass UI toggle         | Hidden                 | Visible                 |
| LLM configuration        | Locked (Gemini only)   | User-configurable       |
| Schema creation model    | gemini-2.5-flash       | User's choice           |
| Value extraction model   | gemini-2.5-flash-lite  | User's choice           |
| Research data collection | Enabled (if configured)| Disabled                |

All mode settings in `RELEASE_CONFIG` in `backend/app/core/config.py`. Frontend adapts via `/api/config`.

### Research Data Collection (Release Mode Only)

In release mode, completed sessions are automatically archived to Google Drive for research. This is fire-and-forget — zero impact on user latency, failures are only logged.

**How it works:** After ScheMatiQ completion, reextraction, or continue discovery finishes, `DataCollectionService` bundles session data (metadata, schema, extracted table, documents, sanitized config) into a ZIP and uploads it to a shared Google Drive folder. Optionally logs a summary row to a Google Sheet.

**Where data is saved:** Controlled by `GOOGLE_DRIVE_FOLDER_ID` — the ID from the Google Drive folder URL. For example, if the folder URL is `https://drive.google.com/drive/folders/1AbCdEfGhIjKlMnOpQrStUv`, then `GOOGLE_DRIVE_FOLDER_ID=1AbCdEfGhIjKlMnOpQrStUv`.

**Setup:**
1. Create a GCP project → Enable Drive API (and Sheets API if using summary logging)
2. Create a service account → Download JSON key
3. Create a Google Drive folder → Share it with the service account email (as Editor)
4. (Optional) Create a Google Sheet → Share it with the service account email (as Editor)
5. Set environment variables in Railway (see below)

**Environment variables (all optional — feature disabled if not set):**

| Variable | Required | Description |
|---|---|---|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Yes* | Service account key as JSON string (preferred on Railway) |
| `GOOGLE_SERVICE_ACCOUNT_FILE` | Alt* | Path to JSON key file (alternative to JSON string) |
| `GOOGLE_DRIVE_FOLDER_ID` | Yes | Target folder ID from Drive URL |
| `GOOGLE_SHEETS_SPREADSHEET_ID` | No | Google Sheet ID for summary log |

\*One of `GOOGLE_SERVICE_ACCOUNT_JSON` or `GOOGLE_SERVICE_ACCOUNT_FILE` is required to enable the feature.

**Code:** `DataCollectionService` in `app/services/data_collection_service.py`, `GoogleDriveUploader` in `app/storage/google_drive.py`, `GoogleSheetsLogger` in `app/storage/google_sheets.py`. Config in `app/core/config.py` (`DATA_COLLECTION_ENABLED`). Hooks in `schematiq_runner.py`, `reextraction_service.py`, `continue_discovery_service.py`.

## Concurrency

- All blocking LLM/embedding calls offloaded via `loop.run_in_executor(schematiq_thread_pool, ...)` with `functools.partial`
- `ConcurrencyLimiter` in `app/services/__init__.py` tracks active sessions; exceeding capacity → HTTP 503 (frontend shows amber banner, not red error)
- Thread safety: `SessionManager` uses `threading.Lock`, `WebSocketManager` uses `asyncio.Lock`, services use `threading.Lock` for stop flags

## Deployment

Both services use **Dockerfile-based** builds (NOT NIXPACKS):
- `frontend/Dockerfile` — Multi-stage Node 18 → Nginx. Has `railway.json` with `builder: "DOCKERFILE"`.
- `backend/Dockerfile` — Python 3.11-slim, CPU-only PyTorch, copies `schematiq-lib/`. **No `railway.json`**.

## Important Rules

- **NEVER run LLM flows without explicit user permission** — API costs
- MUST use `--legacy-peer-deps` for frontend npm install
- MUST run backend from the `backend/` directory for correct path resolution
- Research data in `research/` is gitignored — do not commit large data files
