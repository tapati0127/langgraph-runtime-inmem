"""PostgreSQL-backed Store for the otherwise in-memory API runtime."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from langgraph.store.postgres.aio import AsyncPostgresStore


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def store_ttl_config() -> dict[str, Any]:
    """Build Store TTL settings from environment variables."""
    return {
        "default_ttl": float(os.getenv("STORE_TTL_DEFAULT_MINUTES", "10080")),
        "refresh_on_read": _as_bool(
            os.getenv("STORE_TTL_REFRESH_ON_READ", "true")
        ),
        "sweep_interval_minutes": float(
            os.getenv("STORE_TTL_SWEEP_INTERVAL_MINUTES", "1")
        ),
    }


@asynccontextmanager
async def store() -> AsyncIterator[AsyncPostgresStore]:
    """Create the PostgreSQL Store and run its TTL sweeper for the API lifespan."""
    from langgraph.store.postgres.aio import AsyncPostgresStore

    database_url = os.getenv("STORE_DATABASE_URL") or os.environ["DATABASE_URL"]
    async with AsyncPostgresStore.from_conn_string(
        database_url,
        ttl=store_ttl_config(),
    ) as postgres_store:
        await postgres_store.setup()
        await postgres_store.start_ttl_sweeper()
        try:
            yield postgres_store
        finally:
            await postgres_store.stop_ttl_sweeper()
