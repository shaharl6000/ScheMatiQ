"""Guard: session data files are rewritten only by modules that take the lock.

Every read-modify-write of a session data JSONL must hold
``session_write_lock`` and persist the result (see session_data_store). This
test fails when a new module starts calling ``write_jsonl_atomic`` or defines
its own JSONL writer, so the new writer gets reviewed against that rule
instead of silently racing the others.
"""

import ast
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[1] / "app"

# Modules allowed to call write_jsonl_atomic. Each one rewrites session data
# files only under session_write_lock (directly or via save_session_data_file).
ALLOWED_CALLERS = {
    "services/session_data_store.py",
    "services/data_editor.py",
    "services/unit_view_service.py",
    "services/reextraction_service.py",
}
WRITER_MODULE = "services/atomic_jsonl.py"


def _modules():
    for path in sorted(APP_DIR.rglob("*.py")):
        rel = path.relative_to(APP_DIR).as_posix()
        yield rel, path.read_text(encoding="utf-8")


def _called_name(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def test_only_locked_modules_call_write_jsonl_atomic():
    offenders = []
    for rel, source in _modules():
        if rel == WRITER_MODULE:
            continue
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Call) and _called_name(node).endswith("write_jsonl_atomic"):
                if rel not in ALLOWED_CALLERS:
                    offenders.append(f"{rel}:{node.lineno}")
    assert not offenders, (
        "New caller of write_jsonl_atomic. Rewrite session data files with "
        "save_session_data_file under session_write_lock, then add the module "
        f"to ALLOWED_CALLERS: {offenders}"
    )


def test_allowed_callers_use_the_session_write_lock():
    sources = dict(_modules())
    missing = [rel for rel in ALLOWED_CALLERS if "session_write_lock" not in sources[rel]]
    assert not missing, f"Allowed writer without session_write_lock: {missing}"


def test_no_private_jsonl_writers():
    defined = []
    for rel, source in _modules():
        if rel == WRITER_MODULE:
            continue
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and "write_jsonl" in node.name:
                defined.append(f"{rel}:{node.lineno} {node.name}")
    assert not defined, f"Use atomic_jsonl / session_data_store instead of: {defined}"
