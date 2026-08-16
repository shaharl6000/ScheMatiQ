"""Tests for the chat extraction-capability gate.

`ChatAgentService._extraction_capable` decides whether the document-backed
extraction tools are offered. schematiq sessions are always capable; a load
(imported) session is capable when its source documents are reachable — either
present on local disk, or only in the session's Supabase dataset
(`cloud_dataset`), which the pipeline reads from directly.
"""

from types import SimpleNamespace

import pytest

from app.services.chat import agent_service as agent_module
from app.services.chat.agent_service import ChatAgentService

cap = ChatAgentService._extraction_capable


def test_schematiq_is_always_capable():
    assert cap("s1", "schematiq") is True
    # Even with no session id, schematiq is capable (no session-doc dependency).
    assert cap(None, "schematiq") is True


def test_load_without_session_is_not_capable():
    assert cap(None, "load") is False


def test_load_with_local_documents_is_capable(monkeypatch):
    monkeypatch.setattr(
        agent_module.reextraction_service,
        "has_local_source_documents",
        lambda sid: True,
    )
    assert cap("s1", "load") is True


def test_load_cloud_only_is_capable(monkeypatch):
    # No local files, but the session has a Supabase dataset the pipeline reads.
    monkeypatch.setattr(
        agent_module.reextraction_service,
        "has_local_source_documents",
        lambda sid: False,
    )
    session = SimpleNamespace(metadata=SimpleNamespace(cloud_dataset="nes_full_text"))
    monkeypatch.setattr(
        agent_module.session_manager, "get_session", lambda sid: session
    )
    assert cap("s1", "load") is True


def test_load_without_any_documents_is_not_capable(monkeypatch):
    monkeypatch.setattr(
        agent_module.reextraction_service,
        "has_local_source_documents",
        lambda sid: False,
    )
    session = SimpleNamespace(metadata=SimpleNamespace(cloud_dataset=None))
    monkeypatch.setattr(
        agent_module.session_manager, "get_session", lambda sid: session
    )
    assert cap("s1", "load") is False


def test_gate_never_crashes_chat(monkeypatch):
    # Any failure in the capability check must degrade to "not capable", never raise.
    def _boom(sid):
        raise RuntimeError("storage down")

    monkeypatch.setattr(
        agent_module.reextraction_service, "has_local_source_documents", _boom
    )
    assert cap("s1", "load") is False
