"""A failed reference-fill summary save is logged, and the live event still goes out (#710)."""

import logging
from types import SimpleNamespace

from app.services import reference_fill_service as rfs


async def test_summary_save_failure_is_logged_and_event_still_sent(monkeypatch, caplog):
    async def failing_save(session_id, fill_id, message):
        raise RuntimeError("storage down")

    monkeypatch.setattr("app.services.chat.chat_summary_store.save_summary", failing_save)
    sent = []

    class FakeWs:
        async def broadcast_to_session(self, session_id, event):
            sent.append(event)

    service = rfs.ReferenceFillService.__new__(rfs.ReferenceFillService)
    service._ws = FakeWs()
    op = SimpleNamespace(
        session_id="sid", fill_id="f1", message="recap", column="c",
        status="completed", filled=1, skipped=0, total=1,
    )

    with caplog.at_level(logging.WARNING, logger=rfs.logger.name):
        await service._broadcast_complete(op)

    assert any("reference-fill summary f1" in r.getMessage() for r in caplog.records)
    assert sent, "the live completion event must still be broadcast"
