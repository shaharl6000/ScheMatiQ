# QueryDiscovery

**Query-Based Schema Discovery (QBSD) — Give a research query and documents, get a structured table back.**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![React 18](https://img.shields.io/badge/react-18-61dafb.svg)](https://reactjs.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104-009688.svg)](https://fastapi.tiangolo.com/)

QueryDiscovery takes a natural-language query and a collection of documents, automatically discovers the optimal table schema to answer it, then extracts values from each document into a structured table. No predefined schema needed — the system figures out what columns matter.

**Try it live:** [querydiscovery-production.up.railway.app](https://querydiscovery-production.up.railway.app/)

---

## Architecture

```
QueryDiscovery/
├── frontend/     # React 18 + TypeScript + Tailwind/shadcn (Railway Service 1)
├── backend/      # FastAPI + WebSocket server (Railway Service 2)
├── qbsd-lib/     # Core QBSD algorithms (Python package, imported by backend)
└── research/     # Datasets, experiments, evaluation results
```

**Request flow:** Frontend → Backend routes (`app/api/routes/`) → Services (`app/services/`) → qbsd-lib (`qbsd/`) → LLM API. Real-time progress via WebSocket.

### System Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Research   │     │ Observation │     │   Schema    │     │    Value    │     │  Structured │
│    Query     │ ──▶ │    Unit     │ ──▶ │  Discovery  │ ──▶ │  Extraction │ ──▶ │    Table    │
│ + Documents  │     │  Discovery  │     │   (QBSD)    │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

The **Observation Unit** (e.g., "research paper", "patient") is discovered first — it determines what each row in the final table represents.

---

## Quick Start

**Prerequisites:** Node.js 18+, Python 3.10+, at least one LLM API key (OpenAI, Gemini, or Together AI)

```bash
# Backend
cd backend
pip install -r requirements.txt
cd ../qbsd-lib && pip install -e . && cd ../backend
# Set OPENAI_API_KEY, GEMINI_API_KEY, or TOGETHER_API_KEY in environment
uvicorn app.main:app --reload --port 8000
```

```bash
# Frontend (separate terminal)
cd frontend
npm install --legacy-peer-deps
npm start
# Opens at http://localhost:3000
```

---

## Features

### Web Application

| Feature | Description |
|---------|-------------|
| **Real-time Progress** | WebSocket-based live updates during discovery and extraction |
| **Interactive Schema Editor** | Edit, add, or remove columns after discovery |
| **Continue Discovery** | Extend schema after initial convergence by processing more documents |
| **Reextraction** | Re-run value extraction with the current or edited schema |
| **Cost Estimation** | Preview estimated LLM API costs before running expensive operations |
| **Document Upload** | PDF and TXT support with automatic preprocessing |
| **Export** | Download results as CSV, JSON, or JSONL |

**Pages:** Landing → QBSDConfig → Load → Visualize

### Core Library (qbsd-lib)

| Feature | Description |
|---------|-------------|
| **Multi-LLM Support** | OpenAI, Google Gemini, Together AI |
| **Observation Unit Discovery** | Automatically determines what entity each row represents |
| **Embedding Retrieval** | Passage-level retrieval for long documents (sentence-transformers) |
| **Iterative Schema Discovery** | Retrieval → LLM generation → semantic merging → convergence check |
| **Parallel Extraction** | Multi-threaded document processing with incremental writes |
| **Evaluation** | Schema and row-level evaluation against ground truth |

```bash
# Standalone install
cd qbsd-lib && pip install -e .
```

```python
from qbsd import Schema, Column, EmbeddingRetriever
from qbsd.core.llm_backends import GeminiLLM
from qbsd.core import qbsd as QBSD
from qbsd.value_extraction.main import build_table_jsonl
```

---

## Configuration

### Environment Variables

**Backend** (at least one LLM key required):

| Variable | Description |
|----------|-------------|
| `OPENAI_API_KEY` | OpenAI API key |
| `GEMINI_API_KEY` | Google Gemini API key |
| `TOGETHER_API_KEY` | Together AI API key |
| `ALLOWED_ORIGINS` | CORS origins (default: `http://localhost:3000`) |
| `SUPABASE_URL` / `SUPABASE_KEY` | Cloud storage (production) |
| `MAX_CONCURRENT_SESSIONS` | Concurrent session limit (default: 5) |
| `DEVELOPER_MODE` | Set `true` to unlock all features (see below) |

**Frontend:**

| Variable | Description |
|----------|-------------|
| `REACT_APP_API_URL` | Backend URL (default: `http://localhost:8000`) |
| `REACT_APP_WS_URL` | WebSocket URL (default: `ws://localhost:8000`) |

### Release Mode vs Developer Mode

Release mode (default) restricts features for public use. Set `DEVELOPER_MODE=true` to unlock.

| Setting | Release Mode | Developer Mode |
|---------|-------------|----------------|
| Document limit | 20 | 10,000 |
| LLM configuration | Locked (Gemini only) | User-configurable |
| Schema creation model | gemini-2.5-flash | User's choice |
| Value extraction model | gemini-2.5-flash-lite | User's choice |
| Research data collection | Enabled (if configured) | Disabled |

---

## Deployment

Both services deploy on **Railway** using **Dockerfile-based** builds:

- **Frontend** — Multi-stage Node 18 → Nginx (`frontend/Dockerfile`, `frontend/railway.json`)
- **Backend** — Python 3.11-slim, CPU-only PyTorch, copies `qbsd-lib/` at build time (`backend/Dockerfile`, no `railway.json`)

### Concurrency

- Blocking LLM/embedding calls offloaded via `run_in_executor` with a configurable thread pool
- `ConcurrencyLimiter` tracks active sessions; exceeding capacity returns HTTP 503
- Thread-safe session and WebSocket management

---

## License

MIT License — see [LICENSE](LICENSE).

