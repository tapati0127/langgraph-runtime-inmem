"""Background thread TTL sweeping for the in-memory runtime."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog

from langgraph_runtime_inmem.database import connect
from langgraph_runtime_inmem.ops import Threads

logger = structlog.stdlib.get_logger(__name__)


async def sweep_once(config: dict[str, Any]) -> tuple[int, int]:
    """Run one TTL sweep with the configured limit and batch size."""
    async with connect() as conn:
        return await Threads.sweep_ttl(
            conn,
            limit=config.get("sweep_limit", 10000),
            batch_size=config.get("batch_size", 100),
        )


async def thread_ttl_sweep_loop() -> None:
    """Periodically prune or delete expired in-memory threads."""
    from langgraph_api.config import THREAD_TTL  # noqa: PLC0415

    config = THREAD_TTL or {}
    strategy = str(config.get("strategy", "delete")).lower()
    if strategy not in {"delete", "keep_latest"}:
        raise ValueError(
            f"Invalid thread TTL strategy: {strategy}. "
            "Expected 'delete' or 'keep_latest'."
        )

    interval_minutes = float(config.get("sweep_interval_minutes", 5))
    # Zero is useful for integration tests but must not create a hot loop.
    interval_seconds = max(interval_minutes * 60, 0.1)
    await logger.ainfo(
        f"Starting thread TTL sweeper with interval {interval_minutes:g} minutes",
        strategy=strategy,
        interval_minutes=interval_minutes,
    )
    loop = asyncio.get_running_loop()

    while True:
        await asyncio.sleep(interval_seconds)
        sweep_start = loop.time()
        try:
            threads_processed, threads_deleted = await sweep_once(config)
            if threads_processed > 0:
                await logger.ainfo(
                    f"Thread TTL sweep completed. Processed {threads_processed}",
                    threads_processed=threads_processed,
                    threads_deleted=threads_deleted,
                    duration=loop.time() - sweep_start,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Thread TTL sweep iteration failed", exc_info=exc)
