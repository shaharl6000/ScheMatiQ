"""Pytest fixtures for backend tests."""

import pytest

from app.services import session_manager


@pytest.fixture
def session_manager_fixture():
    return session_manager
