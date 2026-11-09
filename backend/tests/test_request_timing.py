"""Slow requests are logged with their path and outcome; fast ones are not."""

import asyncio
import logging

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core import request_timing


@pytest.fixture
def client():
    app = FastAPI()
    app.middleware("http")(request_timing.log_slow_requests)

    # The sleeps outlast the Windows clock tick, which would measure an instant
    # request as taking no time at all.
    @app.get("/ok")
    async def ok():
        await asyncio.sleep(0.05)
        return {}

    @app.get("/boom")
    async def boom():
        await asyncio.sleep(0.05)
        raise RuntimeError("boom")

    return TestClient(app, raise_server_exceptions=False)


def _slow_lines(caplog):
    return [r.getMessage() for r in caplog.records if r.getMessage().startswith("Slow request")]


def test_slow_request_is_logged_with_path_and_status(client, caplog, monkeypatch):
    monkeypatch.setattr(request_timing, "SLOW_REQUEST_SECONDS", 0.01)
    with caplog.at_level(logging.WARNING, logger=request_timing.__name__):
        client.get("/ok")
    assert len(_slow_lines(caplog)) == 1
    assert "GET /ok -> 200 in" in _slow_lines(caplog)[0]


def test_slow_request_that_raises_is_still_logged(client, caplog, monkeypatch):
    monkeypatch.setattr(request_timing, "SLOW_REQUEST_SECONDS", 0.01)
    with caplog.at_level(logging.WARNING, logger=request_timing.__name__):
        client.get("/boom")
    assert "GET /boom -> failed in" in _slow_lines(caplog)[0]


@pytest.mark.parametrize("threshold", [3600, 0], ids=["fast", "disabled"])
def test_not_logged_when_fast_or_disabled(client, caplog, monkeypatch, threshold):
    monkeypatch.setattr(request_timing, "SLOW_REQUEST_SECONDS", threshold)
    with caplog.at_level(logging.WARNING, logger=request_timing.__name__):
        client.get("/ok")
    assert _slow_lines(caplog) == []
