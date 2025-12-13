# QueryDiscovery

**Query-Based Schema Discovery (QBSD) - Automatically discover table schemas from document collections to answer research queries**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![React 18](https://img.shields.io/badge/react-18-61dafb.svg)](https://reactjs.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104-009688.svg)](https://fastapi.tiangolo.com/)

---

## Overview

QueryDiscovery implements a novel approach to information extraction: instead of pre-defining what to extract, simply ask a question and let the system **discover the optimal table structure** to answer it.

**Example**: Given the query *"What factors affect neural network training stability?"* and a collection of ML papers, QBSD will:
1. **Discover** relevant columns like `learning_rate`, `batch_size`, `optimizer`, `convergence_time`
2. **Extract** values from each document into a structured table
3. **Evaluate** schema quality against ground truth

---

## Architecture

```
QueryDiscovery/
├── frontend/           # React 18 + TypeScript Web Application
├── backend/            # FastAPI REST API & WebSocket Server
├── qbsd-lib/           # Core QBSD Python Library (pip installable)
├── research/           # Research datasets & experiments
├── shared/             # Shared types and constants
└── docs/               # Documentation
```

### System Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Research  │     │   Schema    │     │    Value    │     │  Structured │
│    Query    │ ──▶ │  Discovery  │ ──▶ │  Extraction │ ──▶ │    Table    │
│ + Documents │     │   (QBSD)    │     │             │     │   (JSONL)   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

---

## Quick Start

### Option 1: Web Application (Recommended)

The easiest way to use QueryDiscovery is through the web interface.

**Prerequisites:**
- Node.js 18+
- Python 3.10+
- At least one LLM API key (OpenAI, Gemini, or Together AI)

```bash
# Clone the repository
git clone https://github.com/your-org/QueryDiscovery.git
cd QueryDiscovery

# Setup Backend
cd backend
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and add your API keys

# Start backend server
uvicorn app.main:app --reload --port 8000
```

```bash
# In a new terminal - Setup Frontend
cd frontend
npm install --legacy-peer-deps
npm start
```

Open http://localhost:3000 in your browser.

### Option 2: Python Library

Use `qbsd-lib` directly for programmatic access or integration into existing pipelines.

```bash
cd qbsd-lib
pip install -e .
```

```python
from qbsd import Schema, Column, EmbeddingRetriever
from qbsd.core.llm_backends import GeminiLLM
from qbsd.core import qbsd as QBSD
from qbsd.value_extraction.main import build_table_jsonl
from pathlib import Path

# Initialize LLM
llm = GeminiLLM(
    model="gemini-2.0-flash",
    max_tokens=4096,
    max_context_tokens=1000000
)

# Initialize retriever for long documents
retriever = EmbeddingRetriever(
    model_name="all-MiniLM-L6-v2",
    k=15
)

# Step 1: Discover schema from documents
schema = QBSD.generate_schema(
    query="What factors influence model performance?",
    documents=["path/to/papers"],
    llm=llm,
    retriever=retriever,
    max_keys=15
)

# Step 2: Extract values using discovered schema
build_table_jsonl(
    schema_path=Path("schema.json"),
    docs_directories=[Path("path/to/papers")],
    output_path=Path("extracted_data.jsonl"),
    llm=llm,
    retriever=retriever
)
```

---

## Features

### Web Application
- **Real-time Progress**: WebSocket-based live updates during schema discovery
- **Interactive Schema Editor**: Edit, add, or remove columns after discovery
- **Document Upload**: Support for PDF, TXT, and other document formats
- **Dark Mode**: Full dark/light theme support
- **Export Options**: Download results as JSONL, CSV, or JSON

### Core Library (qbsd-lib)
- **Multi-LLM Support**: OpenAI (GPT-4), Google Gemini, Together AI
- **Intelligent Retrieval**: Embedding-based passage retrieval for long documents
- **Resume Capability**: Continue interrupted extraction jobs
- **Parallel Processing**: Multi-threaded document processing
- **Flexible Schema**: Column definitions with rationales and data types

---

## Components

### Frontend (`frontend/`)

Modern React application with TypeScript and Tailwind CSS.

| Technology | Purpose |
|------------|---------|
| React 18 | UI Framework |
| TypeScript | Type Safety |
| Tailwind CSS | Styling |
| shadcn/ui | Component Library |
| AG Grid | Data Table Display |
| React Query | Server State Management |

**Key Pages:**
- **Landing** - Project introduction and navigation
- **Create QBSD** - Upload documents, configure query, run discovery
- **Load QBSD** - Load existing QBSD results
- **Results** - Interactive table view with schema editing

### Backend (`backend/`)

FastAPI application providing REST API and WebSocket endpoints.

| Component | Description |
|-----------|-------------|
| `app/api/routes/` | API endpoint definitions |
| `app/services/` | Business logic layer |
| `app/models/` | Pydantic data models |
| `app/core/` | Configuration and utilities |

**Key Endpoints:**
```
POST /api/qbsd/configure     # Configure new QBSD session
POST /api/qbsd/start/{id}    # Start schema discovery
GET  /api/qbsd/status/{id}   # Get processing status
POST /api/load/file          # Upload existing QBSD data
WS   /ws/progress/{id}       # Real-time progress updates
```

### QBSD Library (`qbsd-lib/`)

Self-contained Python package with all core algorithms.

```
qbsd/
├── core/
│   ├── qbsd.py           # Main discovery pipeline
│   ├── schema.py         # Schema data structures
│   ├── llm_backends.py   # LLM provider interfaces
│   ├── retrievers.py     # Document retrieval
│   └── utils.py          # Utility functions
├── value_extraction/
│   ├── main.py           # Extraction entry point
│   └── core/             # Extraction logic
└── evaluation/
    └── metrics.py        # Evaluation metrics
```

---

## Configuration

### Environment Variables

**Backend (`backend/.env`):**
```bash
# LLM API Keys (at least one required)
OPENAI_API_KEY=sk-...
GEMINI_API_KEY=AIza...
TOGETHER_API_KEY=...

# Server Configuration
ALLOWED_ORIGINS=http://localhost:3000
DEBUG=true

# Optional: Supabase for cloud storage
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key

# QBSD Defaults
DEFAULT_MAX_TOKENS=4096
DEFAULT_TEMPERATURE=0.7
DEFAULT_RETRIEVAL_K=5
```

**Frontend (`frontend/.env`):**
```bash
REACT_APP_API_URL=http://localhost:8000
REACT_APP_WS_URL=ws://localhost:8000
REACT_APP_ENABLE_DEBUG=true
```

### Example Configuration Files

**Schema Discovery (`config.json`):**
```json
{
  "query": "What factors influence protein folding stability?",
  "docs_path": "data/papers",
  "max_keys_schema": 20,
  "backend": {
    "provider": "gemini",
    "model": "gemini-2.0-flash",
    "max_context_tokens": 1000000
  },
  "retriever": {
    "type": "embedding",
    "model_name": "all-MiniLM-L6-v2",
    "k": 15
  },
  "output_path": "outputs/schema.json"
}
```

**Value Extraction (`extraction_config.json`):**
```json
{
  "schema_path": "outputs/schema.json",
  "docs_directories": ["data/papers", "data/reviews"],
  "mode": "all",
  "resume": true,
  "backend_cfg": {
    "provider": "gemini",
    "model": "gemini-2.0-flash"
  },
  "output_path": "outputs/extracted_data.jsonl"
}
```

---

## Deployment

### Railway (Recommended)

The project is configured for Railway deployment with two services:

**Frontend Service:**
```json
{
  "build": {
    "builder": "NIXPACKS",
    "buildCommand": "npm install --legacy-peer-deps && npm run build"
  },
  "deploy": {
    "startCommand": "npm run serve",
    "healthcheckPath": "/"
  }
}
```

**Backend Service:**
```json
{
  "build": {
    "builder": "NIXPACKS",
    "buildCommand": "pip install -r requirements.txt"
  },
  "deploy": {
    "startCommand": "uvicorn app.main:app --host 0.0.0.0 --port $PORT",
    "healthcheckPath": "/health"
  }
}
```

### Docker (Alternative)

```dockerfile
# Backend Dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY backend/requirements.txt .
RUN pip install -r requirements.txt
COPY backend/ .
COPY qbsd-lib/ /qbsd-lib/
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## Development

### Running Tests

```bash
# Backend tests
cd backend
pytest

# Frontend tests
cd frontend
npm test
```

### Code Structure

```
# Backend imports from qbsd-lib
from qbsd.core import qbsd as QBSD
from qbsd.core.schema import Schema, Column
from qbsd.core.llm_backends import GeminiLLM, OpenAILLM
from qbsd.value_extraction.main import build_table_jsonl
```

### Adding New LLM Providers

Implement the `LLMInterface` abstract class in `qbsd-lib/qbsd/core/llm_backends.py`:

```python
class NewProviderLLM(LLMInterface):
    def __init__(self, model: str, max_tokens: int, temperature: float):
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature

    def generate(self, messages: list[dict]) -> str:
        # Implement API call
        pass
```

---

## Research

The `research/` directory contains datasets and experiment configurations:

```
research/
├── data/
│   ├── abstracts/          # Paper abstracts
│   ├── full_text/          # Full papers
│   ├── arxivDIGESTables/   # Evaluation dataset
│   └── NES_DATA/           # Domain-specific data
├── experiments/
│   └── configurations/     # Experiment configs
└── results/                # Evaluation outputs
```

---

## Troubleshooting

### Common Issues

**Frontend: Peer dependency warnings**
```bash
npm install --legacy-peer-deps
```

**Backend: Import errors**
Ensure you're running from the `backend/` directory and `qbsd-lib` is accessible:
```bash
cd backend
PYTHONPATH=$PYTHONPATH:../qbsd-lib uvicorn app.main:app --reload
```

**LLM Rate Limits**
Use multiple API keys with rotation:
```bash
GEMINI_API_KEYS="key1,key2,key3"
```

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- Built on research from arxivDIGESTables
- Uses sentence-transformers for embedding-based retrieval
- UI components from shadcn/ui
