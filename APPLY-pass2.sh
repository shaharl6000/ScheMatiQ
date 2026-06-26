#!/usr/bin/env bash
# Second-pass lint cleanup — two branches, two PRs.
# Run from repo root on a clean working tree, with gh authenticated.
#   bash APPLY-pass2.sh
# Patches live next to this script.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${BASE:-main}"

git checkout "$BASE" && git pull --ff-only origin "$BASE"

# --- Branch 1: backend ---
git checkout "$BASE" && git checkout -b chore/lint-backend-unused
git apply "$HERE/lint-backend.patch"
git commit -am "chore: remove unused imports and fix placeholder-less f-strings (backend)

Mechanical lint cleanup in backend/app via ruff (F401 unused imports, F811
duplicate imports, F541 f-strings with no placeholders). No logic changes;
every touched file compiles and pyflakes reports no undefined names."
git push -u origin chore/lint-backend-unused
gh pr create --base "$BASE" --head chore/lint-backend-unused \
  --title "chore: lint cleanup in backend (unused imports, f-strings)" \
  --body "Ruff autofix over backend/app: removes unused/duplicate imports and converts placeholder-less f-strings to plain strings. Mechanical, no behavior change. Verified: all changed files compile, no new undefined names."

# --- Branch 2: schematiq-lib ---
git checkout "$BASE" && git checkout -b chore/lint-lib-unused
git apply "$HERE/lint-schematiq-lib.patch"
git commit -am "chore: lint cleanup in schematiq-lib (unused imports, f-strings, regex escapes)

Mechanical lint cleanup via ruff (F401, F811, F541, W605). Notably fixes invalid
regex escape sequences in evaluation/metrics_utils.py (were raising SyntaxWarning
and would break on future Python) by making them raw strings. No logic changes."
git push -u origin chore/lint-lib-unused
gh pr create --base "$BASE" --head chore/lint-lib-unused \
  --title "chore: lint cleanup in schematiq-lib (unused imports, f-strings, regex escapes)" \
  --body "Ruff autofix over schematiq-lib: unused/duplicate imports, placeholder-less f-strings, and invalid regex escape sequences (now raw strings; they were raising SyntaxWarning). Mechanical, no behavior change."

git checkout "$BASE"
echo "Done: 2 branches pushed and PRs opened."
