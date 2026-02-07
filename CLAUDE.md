# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QueryDiscovery is a production-ready application and research project implementing Query-Based Schema Discovery (QBSD), a modern approach to information extraction. The system takes a user query and a collection of documents, then iteratively discovers a table schema (column headers + rationales) that best captures information needed to answer the query.

## Architecture

The project is structured for production deployment on Railway with Supabase storage:

```
QueryDiscovery/
├── frontend/           # React Application (Railway Service 1)
├── backend/            # FastAPI Application (Railway Service 2)
├── qbsd-lib/           # QBSD Core Library (Python Package)
├── research/           # Research Data & Experiments
├── shared/             # Shared types and constants
├── docs/               # Documentation
├── .venv/              # Python virtual environment
├── CLAUDE.md           # Project documentation
├── README.md           # Project readme
├── requirements.txt    # Root Python dependencies
└── .gitignore          # Git ignore rules
```

### frontend/ - React Application
- Modern React 18 with TypeScript
- Tailwind CSS + shadcn/ui components
- Real-time WebSocket updates for QBSD creation progress
- Pages: Landing, CreateQBSD, LoadQBSD, Results

**Key Files:**
- `src/App.tsx` - Main app router
- `src/pages/` - Page components
- `src/components/ui/` - shadcn/ui components
- `src/services/api.ts` - Backend API client
- `src/constants/index.ts` - App configuration and constants
- `package.json` - Dependencies and scripts
- `railway.json` - Railway deployment configuration
- `.env.example` - Environment variables template

### backend/ - FastAPI Application
- FastAPI with CORS and WebSocket support
- Modular route structure in `app/api/routes/`
- Service layer for business logic
- Imports QBSD core from `qbsd-lib/`

**Key Files:**
- `app/main.py` - FastAPI application entry point
- `app/api/routes/` - API endpoints (qbsd.py, load.py, schema.py, websocket.py)
- `app/services/` - Business logic:
  - `qbsd_runner.py` - QBSD pipeline orchestration
  - `file_parser.py` - File parsing and validation
  - `websocket_manager.py` - WebSocket connection management
  - `schema_manager.py` - Schema editing operations
  - `upload_document_processor.py` - Document processing
  - `session_manager.py` - Session state management
- `app/core/config.py` - Configuration and environment variables
- `app/models/` - Pydantic models (session.py, qbsd.py, upload.py)
- `requirements.txt` - Python dependencies
- `railway.json` - Railway deployment configuration
- `.env.example` - Environment variables template

### qbsd-lib/ - QBSD Core Library
Self-contained Python package with all QBSD algorithms. The backend imports from this package.

**Package Structure:**
- `qbsd/` - Main package
  - `__init__.py` - Package exports (Schema, Column, LLMInterface, etc.)
  - `core/` - Core QBSD implementation
    - `qbsd.py` - Main pipeline (select_relevant_content, generate_schema, merge_schemas)
    - `schema.py` - Schema data structures (Column, Schema classes)
    - `llm_backends.py` - LLM interfaces (TogetherLLM, OpenAILLM, GeminiLLM)
    - `retrievers.py` - Embedding-based content retrieval (EmbeddingRetriever)
    - `utils.py` - Utility functions for text processing
  - `value_extraction/` - Modular value extraction framework
    - `main.py` - Entry point (build_table_jsonl function)
    - `core/` - Business logic (TableBuilder, PaperProcessor, JSONResponseParser, LLMCache, RowDataManager)
    - `utils/` - Utilities (TextProcessor, PromptBuilder)
    - `config/` - Configuration constants and system prompts
  - `evaluation/` - Evaluation metrics (from arxivDIGESTables)
    - `metrics.py` - SchemaRecallMetric, BaseMetric
    - `metrics_utils.py` - Featurizers and scorers
    - `run_eval.py` - Evaluation runner
  - `scripts/` - Data processing scripts (run.py, create_data.py, generate_tables.py)
- `pyproject.toml` - Package configuration
- `tests/` - Test directory

### research/ - Research Data & Experiments
All research datasets and experimental configurations consolidated here:

- `data/` - Research datasets
  - `abstracts/` - NES research paper abstracts
  - `full_text/` - Full research papers
  - `manually/` - Manual annotations
  - `arxivDIGESTables/` - ArxivDIGESTables evaluation data
  - `NES_DATA/` - NES database files
  - Various `.jsonl`, `.csv` data files
- `experiments/` - Experiment configurations
  - `configurations/` - JSON config files for QBSD experiments
  - Example configs for evaluation and extraction
- `results/` - Evaluation outputs and predictions
- `notebooks/` - Jupyter notebooks (if any)

### shared/ - Shared Types and Constants
- `types/` - Shared TypeScript type definitions
- `constants/` - Shared constants

## Development Commands

### Frontend Development
```bash
cd frontend
npm install --legacy-peer-deps
npm start                    # Development server on :3000
npm run build               # Production build
```

### Backend Development
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### Running Both Locally
```bash
# Terminal 1 - Backend
cd backend && uvicorn app.main:app --reload --port 8000

# Terminal 2 - Frontend
cd frontend && npm start
```

### Environment Variables

**Backend (.env):**
```bash
# LLM API Keys (at least one required)
OPENAI_API_KEY=your-key
TOGETHER_API_KEY=your-key
GEMINI_API_KEY=your-key

# Server Configuration
ALLOWED_ORIGINS=http://localhost:3000
DEBUG=true

# Supabase (optional)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key

# QBSD Defaults
DEFAULT_MAX_TOKENS=4096
DEFAULT_TEMPERATURE=0.7
DEFAULT_RETRIEVAL_K=5
```

**Frontend (.env):**
```bash
REACT_APP_API_URL=http://localhost:8000
REACT_APP_WS_URL=ws://localhost:8000
REACT_APP_ENABLE_DEBUG=true
```

### Release Mode vs Developer Mode

The backend runs in **release mode** by default, which restricts certain features
for public/production use. Set `DEVELOPER_MODE=true` to unlock all features.

| Setting            | Release Mode (default) | Developer Mode          |
|--------------------|------------------------|-------------------------|
| Document limit     | 20                     | 10,000 (effectively unlimited) |
| Bypass UI toggle   | Hidden                 | Visible                 |
| LLM configuration  | Locked (Gemini only)   | User-configurable       |
| Schema creation model | gemini-2.5-flash    | User's choice           |
| Value extraction model | gemini-2.5-flash-lite | User's choice        |

**How it works:**
- All mode-dependent settings are centralized in `RELEASE_CONFIG` in `backend/app/core/config.py`
- The `/api/config` endpoint exposes the active configuration to the frontend
- The frontend adapts its UI based on the `developer_mode` flag (e.g., showing/hiding the limit bypass toggle)

**Adding a new release restriction:**
1. Add the default (release) value to `RELEASE_CONFIG` in `backend/app/core/config.py`
2. Resolve the effective value using `DEVELOPER_MODE` (same pattern as `MAX_DOCUMENTS`)
3. Return it in `/api/config` if the frontend needs it
4. Update this table

### Using qbsd-lib Directly
```bash
cd qbsd-lib
pip install -e .

# In Python
from qbsd import Schema, Column, EmbeddingRetriever
from qbsd.core.llm_backends import OpenAILLM, TogetherLLM, GeminiLLM
from qbsd.value_extraction.main import build_table_jsonl
```

## Key Concepts

**Schema Discovery Pipeline:**
1. Content retrieval using embedding-based similarity
2. LLM-based schema generation with iterative refinement
3. Schema merging using semantic similarity
4. Convergence evaluation to determine completion

**Value Extraction Pipeline:**
1. Paper Grouping by row name
2. Resume Logic for incomplete extractions
3. Paper Processing with retrieval-based text selection
4. Row Assembly from multiple papers
5. Incremental Writing to prevent data loss

**Core Classes:**
- `Column` - Individual schema column with name, rationale, definition
- `Schema` - Collection of columns with merging operations
- `LLMInterface` - Abstract base for LLM providers
- `TogetherLLM`, `OpenAILLM`, `GeminiLLM` - Concrete LLM implementations
- `EmbeddingRetriever` - Document passage retrieval
- `TableBuilder` - Value extraction orchestrator
- `PaperProcessor` - Per-paper value extraction

## Deployment

### Railway Configuration

**Frontend Service (frontend/railway.json):**
```json
{
  "build": {
    "builder": "NIXPACKS",
    "buildCommand": "npm install --legacy-peer-deps && npm run build"
  },
  "deploy": {
    "startCommand": "npm run serve",
    "healthcheckPath": "/",
    "restartPolicyType": "ON_FAILURE"
  }
}
```

**Backend Service (backend/railway.json):**
```json
{
  "build": {
    "builder": "NIXPACKS",
    "buildCommand": "pip install -r requirements.txt"
  },
  "deploy": {
    "startCommand": "uvicorn app.main:app --host 0.0.0.0 --port $PORT",
    "healthcheckPath": "/health",
    "restartPolicyType": "ON_FAILURE"
  }
}
```

### Supabase Storage (Optional)
- `documents/` bucket - User uploaded files
- `exports/` bucket - Generated exports

## API Endpoints

The backend exposes the following main routes:

- `GET /` - Root endpoint with version info
- `GET /health` - Health check
- `POST /api/load/file` - Upload file for loading existing QBSD
- `GET /api/load/data/{session_id}` - Get session data
- `POST /api/qbsd/configure` - Configure new QBSD session
- `POST /api/qbsd/start/{session_id}` - Start QBSD processing
- `GET /api/qbsd/status/{session_id}` - Get processing status
- `PUT /api/schema/edit-column/{session_id}` - Edit schema column
- `DELETE /api/schema/delete-column/{session_id}/{column_name}` - Delete column
- `POST /api/schema/add-column/{session_id}` - Add new column
- `WS /ws/progress/{session_id}` - WebSocket for real-time updates

## Important Notes

- **NEVER run LLM flows without explicit user permission due to API costs**
- Frontend and backend are independent services - start both for full functionality
- WebSocket connections provide real-time progress updates during QBSD creation
- All API keys must be set as environment variables
- Use `--legacy-peer-deps` when installing frontend dependencies due to react-json-view peer dependency
- The backend imports QBSD core from `qbsd-lib/` - ensure the path is correct
- Research data in `research/` is gitignored to avoid committing large files
- Run backend from the `backend/` directory for correct path resolution
