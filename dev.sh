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
    done < <(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

BRANCH="$(git -C "$ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?')"
echo "=================================================="
echo " instance $N   (branch: $BRANCH)"
echo "   backend  -> http://localhost:$BACK_PORT"
echo "   frontend -> http://localhost:$FRONT_PORT"
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
