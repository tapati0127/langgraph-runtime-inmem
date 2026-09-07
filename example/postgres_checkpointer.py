"""PostgreSQL-backed checkpointer for the otherwise in-memory API runtime."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver


@asynccontextmanager
async def checkpointer() -> AsyncIterator[AsyncPostgresSaver]:
    """Create, initialize, and close the API's PostgreSQL checkpointer."""
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    database_url = os.environ["DATABASE_URL"]
    async with AsyncPostgresSaver.from_conn_string(database_url) as saver:
        await saver.setup()
        yield saver
