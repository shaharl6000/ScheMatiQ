#!/usr/bin/env bash
set -euo pipefail

# ============================================================
#  dev.sh — run backend + frontend together from one terminal.
#  Each instance is isolated: separate local storage + ports.
#
#  One-time setup (if uploads fail with multipart error):
#    cd backend && .venv/bin/python -m pip install python-multipart
#
#  Usage:
#    ./dev.sh        -> instance 0: backend 8000, frontend 3000
#    ./dev.sh 1      -> instance 1: backend 8001, frontend 3001
#    ./dev.sh 2      -> instance 2: backend 8002, frontend 3002
#  Ctrl+C stops both processes for that instance.
#
#  Storage backend:
#    By default the backend honors STORAGE_BACKEND from backend/.env, so if
#    that file selects Supabase you get the shared cloud datasets. Pass --local
#    (or LOCAL=1) to force local, isolated per-instance storage instead — use
#    that when running several instances at once so they don't share state.
#    ./dev.sh --local     -> instance 0, forced local storage
#    ./dev.sh 1 --local   -> instance 1, forced local storage
#
#  Backend log file:
#    BACKEND_LOG=path writes backend output to that file instead of the shared
#    terminal (follow it with `tail -f path`). Use it for any run whose logs you
#    need complete. Piping `./dev.sh | tee` shares one pipe with npm, which can
#    switch it to non-blocking mode; Python writes then fail with
#    BlockingIOError (Errno 35) and log lines are lost.
#    BACKEND_LOG=~/run.log ./dev.sh 1
#
#  Parallel extraction:
#    Local runs extract 4 documents at once (SCHEMATIQ_EXTRACTION_WORKERS).
#    Production keeps the backend default of 1. To change it locally, set
#    SCHEMATIQ_EXTRACTION_WORKERS in the shell or in backend/.env (both win
#    over this default). Lower it if Gemini returns 429 (rate limit), e.g.
#    when running several instances at once.
# ============================================================

# Parse args: an optional instance number (positional) and an optional --local
# flag, in any order. LOCAL=1 in the environment is equivalent to --local.
N=0
FORCE_LOCAL="${LOCAL:-0}"
for arg in "$@"; do
  case "$arg" in
    --local) FORCE_LOCAL=1 ;;
    *[!0-9]*) echo "error: unknown argument '$arg'" >&2; exit 1 ;;
    *) N="$arg" ;;
  esac
done
BACK_PORT="${BACK_PORT:-$((8000 + N))}"
FRONT_PORT="${FRONT_PORT:-$((3000 + N))}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/backend/.venv"

if [[ ! -x "$VENV/bin/python" ]]; then
  echo "error: backend venv not found at $VENV" >&2
  echo "  cd backend && python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

if ! "$VENV/bin/python" -c "import multipart" 2>/dev/null; then
  echo "warning: python-multipart not installed — file uploads will fail." >&2
  echo "  cd backend && .venv/bin/python -m pip install python-multipart" >&2
fi

# Per-instance CWD: backend code uses ./sessions, ./data, ./schematiq_work (CWD-relative).
DATA_DIR="$ROOT/.dev-data/instance-$N"
mkdir -p "$DATA_DIR"

# Shared read-only research datasets: LocalStorageBackend resolves ../research/data from CWD.
if [[ -d "$ROOT/research" ]] && [[ ! -e "$ROOT/.dev-data/research" ]]; then
  ln -sfn "$ROOT/research" "$ROOT/.dev-data/research"
fi

# PIDs listening on a TCP port (empty if free).
port_pids() {
  lsof -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null || true
}

# Refuse to start when a port is taken (usually a leftover instance that did not
# shut down). Otherwise uvicorn fails with "Address already in use" while the
# frontend starts anyway and talks to the old, possibly hung, backend.
for port in "$BACK_PORT" "$FRONT_PORT"; do
  busy="$(port_pids "$port")"
  if [[ -n "$busy" ]]; then
    echo "error: port $port is already in use (instance $N not started):" >&2
    for pid in $busy; do
      echo "  $(ps -p "$pid" -o pid=,lstart=,command= 2>/dev/null | cut -c1-160)" >&2
    done
    echo "Stop it first, e.g.: kill $(echo $busy)   (if it does not exit: kill -9 $(echo $busy))" >&2
    exit 1
  fi
done

cleanup() {
  echo
  echo "shutting down instance $N..."
  local pids
  pids="$(jobs -p)" || true
  if [[ -n "$pids" ]]; then
    kill $pids 2>/dev/null || true
  fi
  # uvicorn --reload and npm spawn extra processes; stop anything still bound to our ports.
  local port pid
  for port in "$BACK_PORT" "$FRONT_PORT"; do
    while read -r pid; do
      [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
    done < <(port_pids "$port")
  done
  # A backend stuck in a long LLM call or a worker thread can ignore SIGTERM and
  # keep the port, which breaks the next start of this instance. Give processes
  # a few seconds to exit, then force-stop whatever still holds our ports.
  local _
  for _ in 1 2 3 4 5; do
    [[ -z "$(port_pids "$BACK_PORT")$(port_pids "$FRONT_PORT")" ]] && break
    sleep 1
  done
  for port in "$BACK_PORT" "$FRONT_PORT"; do
    while read -r pid; do
      [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null || true
    done < <(port_pids "$port")
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Parallel extraction by default for local runs only; an explicit value in the
# shell or in backend/.env wins (load_dotenv does not override the environment,
# so we must not export a value when .env sets one).
if [[ -z "${SCHEMATIQ_EXTRACTION_WORKERS:-}" ]] \
   && ! grep -q '^SCHEMATIQ_EXTRACTION_WORKERS=' "$ROOT/backend/.env" 2>/dev/null; then
  export SCHEMATIQ_EXTRACTION_WORKERS=4
fi

BRANCH="$(git -C "$ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?')"
echo "=================================================="
echo " instance $N   (branch: $BRANCH)"
echo "   backend  -> http://localhost:$BACK_PORT"
echo "   frontend -> http://localhost:$FRONT_PORT"
if [[ -n "${BACKEND_LOG:-}" ]]; then
  echo "   backend log -> $BACKEND_LOG (tail -f it)"
fi
echo "   extraction workers -> ${SCHEMATIQ_EXTRACTION_WORKERS:-from backend/.env}"
if [[ "$FORCE_LOCAL" == "1" ]]; then
  echo "   storage  -> local (isolated, forced): $DATA_DIR"
else
  echo "   storage  -> from backend/.env (Supabase datasets if configured)"
fi
echo "=================================================="

# Backend: run from isolated DATA_DIR; .env still loads from backend/ via __file__ in main.py.
# By default we leave STORAGE_BACKEND unset here so backend/.env decides (Supabase
# gives shared cloud datasets). With --local we export STORAGE_BACKEND=local to
# override .env and keep per-instance isolation.
#
# NOTE ON ISOLATION: --local isolates the writable state (sessions/data/exports)
# per instance. Honoring a Supabase .env means multiple instances share one
# cloud backend and therefore share that state; run --local when you need
# several isolated instances at once.
(
  cd "$DATA_DIR"
  if [[ "$FORCE_LOCAL" == "1" ]]; then
    export STORAGE_BACKEND="local"
  fi
  if [[ -n "${BACKEND_LOG:-}" ]]; then
    # A regular file never blocks, so no log line is dropped. Unbuffered so
    # print() output stays in order with logging output.
    export PYTHONUNBUFFERED=1
    exec >>"$BACKEND_LOG" 2>&1
  fi
  PATH="$VENV/bin:$PATH" \
  PYTHONPATH="$ROOT/backend" \
  ALLOWED_ORIGINS="http://localhost:$FRONT_PORT,http://127.0.0.1:$FRONT_PORT" \
  "$VENV/bin/python" -m uvicorn app.main:app --reload --reload-dir "$ROOT/backend/app" --port "$BACK_PORT"
) &

# Frontend: talk directly to this backend (bypasses package.json proxy).
(
  cd "$ROOT/frontend"
  PORT="$FRONT_PORT" \
  REACT_APP_API_URL="http://localhost:$BACK_PORT" \
  REACT_APP_WS_URL="http://localhost:$BACK_PORT" \
  npm start
) &

wait
