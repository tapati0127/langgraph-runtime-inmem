from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest
from langgraph_runtime_inmem import thread_ttl


@pytest.mark.asyncio
async def test_background_loop_invokes_sweeper(monkeypatch) -> None:
    from langgraph_api import config as api_config

    config = {
        "strategy": "keep_latest",
        "default_ttl": 43200,
        "sweep_interval_minutes": 1,
        "sweep_limit": 7,
    }
    monkeypatch.setattr(api_config, "THREAD_TTL", config)

    connection = object()

    @asynccontextmanager
    async def fake_connect(*args, **kwargs):
        yield connection

    calls = []

    async def fake_sweep(conn, *, limit, batch_size):
        calls.append((conn, limit, batch_size))
        return (1, 0)

    sleep_calls = 0

    async def fake_sleep(seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        assert seconds == 60
        if sleep_calls > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(thread_ttl, "connect", fake_connect)
    monkeypatch.setattr(thread_ttl.Threads, "sweep_ttl", fake_sweep)
    monkeypatch.setattr(thread_ttl.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await thread_ttl.thread_ttl_sweep_loop()

    assert calls == [(connection, 7, 100)]
