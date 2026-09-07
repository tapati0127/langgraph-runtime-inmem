from __future__ import annotations

import importlib.util
import json
import asyncio
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


ROOT = Path(__file__).parents[1]


def test_example_config_uses_postgres_checkpointer() -> None:
    config = json.loads((ROOT / "example" / "langgraph.json").read_text())

    assert config["checkpointer"]["path"] == (
        "./postgres_checkpointer.py:checkpointer"
    )
    assert "langgraph-checkpoint-postgres" in config["dependencies"]
    assert "psycopg[binary,pool]>=3.2" in config["dependencies"]


def test_postgres_checkpointer_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[Any] = []

    class FakeSaver:
        @classmethod
        def from_conn_string(cls, connection_string: str) -> FakeSaver:
            events.append(("connect", connection_string))
            return cls()

        async def __aenter__(self) -> FakeSaver:
            events.append("enter")
            return self

        async def __aexit__(self, *args: object) -> None:
            events.append("exit")

        async def setup(self) -> None:
            events.append("setup")

    postgres_module = ModuleType("langgraph.checkpoint.postgres")
    aio_module = ModuleType("langgraph.checkpoint.postgres.aio")
    aio_module.AsyncPostgresSaver = FakeSaver  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "langgraph.checkpoint.postgres", postgres_module)
    monkeypatch.setitem(sys.modules, "langgraph.checkpoint.postgres.aio", aio_module)
    monkeypatch.setenv("DATABASE_URL", "postgresql://test/database")

    module_path = ROOT / "example" / "postgres_checkpointer.py"
    spec = importlib.util.spec_from_file_location("postgres_checkpointer", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    async def exercise_checkpointer() -> None:
        async with module.checkpointer() as saver:
            assert isinstance(saver, FakeSaver)
            assert events == [
                ("connect", "postgresql://test/database"),
                "enter",
                "setup",
            ]

    asyncio.run(exercise_checkpointer())
    assert events[-1] == "exit"
