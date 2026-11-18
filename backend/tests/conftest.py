"""Pytest fixtures for backend tests."""

import os
import shutil
import tempfile

# Run the whole test session inside a throwaway directory.
#
# The local storage backend uses CWD-relative paths (./sessions, ./data,
# ./schematiq_work, ./templates, ./initial_schemas) and creates them when
# app.services is first imported. Without this, every pytest run leaves those
# directories, plus any session files the tests write, in whatever directory
# pytest was started from (backend/ or the repo root).
#
# This must happen before the first `import app...` below, so it lives at
# module level rather than in a fixture. Paths stay relative, so tests that
# monkeypatch.chdir(tmp_path) still get their own per-test directory.
_ORIGINAL_CWD = os.getcwd()
_TEST_CWD = tempfile.mkdtemp(prefix="schematiq-tests-")
os.chdir(_TEST_CWD)

# Tests call LLM-starting endpoints freely; rate limits are tested on their own
# in test_rate_limit.py, which enables them explicitly.
os.environ.setdefault("RATE_LIMIT_ENABLED", "false")

import pytest  # noqa: E402

from app.services import session_manager  # noqa: E402


def pytest_unconfigure(config):
    os.chdir(_ORIGINAL_CWD)
    shutil.rmtree(_TEST_CWD, ignore_errors=True)


@pytest.fixture
def session_manager_fixture():
    return session_manager
