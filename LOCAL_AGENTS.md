# Local notes for Cursor / coding agents (do not commit)

Personal cheat sheet so agents use the right Python and paths on this machine. The public [README.md](README.md) stays generic; this file is gitignored.

## Python version

- **Use Python 3.11+** (`schematiq-lib` requires `>=3.11` in `pyproject.toml`).
- Default `pyenv` here may be **3.9** — agents must not rely on bare `python` / `pip`.
- Prefer: `PYENV_VERSION=3.11.9` (or `python3.11`) for backend, tests, and one-off scripts.

```bash
pyenv versions          # confirm 3.11.9 is installed
PYENV_VERSION=3.11.9 python --version
```

## One-time local env (backend)

```bash
cd /Users/ehabba/PycharmProjects/QueryDiscovery

# Optional: dedicated venv (recommended for agents)
PYENV_VERSION=3.11.9 python -m venv backend/.venv
source backend/.venv/bin/activate

pip install -r backend/requirements.txt
pip install -e schematiq-lib/
```

After that, agents can `source backend/.venv/bin/activate` and skip `PYENV_VERSION=...` if the venv was created with 3.11.

## Running the app

Always start the **backend from `backend/`** (path resolution depends on it):

```bash
cd backend
source .venv/bin/activate   # if using venv
# export GEMINI_API_KEY=...  (or OPENAI / TOGETHER)
uvicorn app.main:app --reload --port 8000
```

Frontend (separate terminal):

```bash
cd frontend
npm install --legacy-peer-deps
npm start
```

## Commands agents should use

| Task | Command |
|------|---------|
| Backend tests (example) | `cd backend && PYENV_VERSION=3.11.9 PYTHONPATH=. pytest tests/test_json_document_preprocessor.py -v` |
| schematiq-lib tests | `cd schematiq-lib && PYENV_VERSION=3.11.9 pip install -e ".[dev]" && pytest tests/` |
| Lint (library) | `cd schematiq-lib && ruff check . && black --check .` |

Note: root `.gitignore` ignores `test_*.py` — local test files under `backend/tests/` will not be committed unless renamed or gitignore changes.

## Agent rules (this repo)

- **Do not run full ScheMatiQ / LLM pipelines** unless the user explicitly asks (API cost).
- Frontend npm: always `--legacy-peer-deps`.
- Document uploads: binary formats → `.txt` via `document_preprocessor`; `.json` is read as UTF-8 text and renamed to `.txt` (not parsed).
- See [CLAUDE.md](CLAUDE.md) for architecture and env vars.

## Quick sanity check (no LLM)

```bash
cd backend
PYENV_VERSION=3.11.9 PYTHONPATH=. python -c "from app.services.document_preprocessor import preprocess_uploaded_file; print('ok')"
```
