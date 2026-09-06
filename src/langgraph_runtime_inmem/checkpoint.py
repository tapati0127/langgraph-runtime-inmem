from __future__ import annotations

import logging
import os
import threading
import typing
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any

from langgraph.checkpoint.memory import (
    InMemorySaver as InMemorySaverBase,
)
from langgraph.checkpoint.memory import (
    PersistentDict,
)

from langgraph_runtime_inmem._persistence import (
    register_persistent_dict,
    stop_flush_loop,
)

if typing.TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig
    from langgraph.checkpoint.base import (
        Checkpoint,
        CheckpointMetadata,
        CheckpointTuple,
        SerializerProtocol,
    )

logger = logging.getLogger(__name__)

_EXCLUDED_KEYS = {"checkpoint_ns", "checkpoint_id", "run_id", "thread_id"}

# Configurable keys that are transient (per-request) and should not be persisted in checkpoints
_TRANSIENT_CONFIGURABLE_KEYS = frozenset(
    {
        "langgraph_request_id",
        "langgraph_auth_user",
        "langgraph_auth_user_id",
        "langgraph_auth_permissions",
    }
)

# Not in public docs: internal, disables pickle file persistence for inmem runtime
DISABLE_FILE_PERSISTENCE = (
    os.getenv("LANGGRAPH_DISABLE_FILE_PERSISTENCE", "false").lower() == "true"
)


def _component_size(value: Any) -> int:
    """Measure already-serialized components without serializing them again."""
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.encode())
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    return len(str(value).encode())


def _typed_payload_size(value: tuple[str, bytes]) -> int:
    return _component_size(value[0]) + _component_size(value[1])


def _checkpoint_entry_size(
    checkpoint_id: Any,
    entry: tuple[tuple[str, bytes], tuple[str, bytes], str | None],
) -> int:
    checkpoint, metadata, parent_id = entry
    return (
        _component_size(checkpoint_id)
        + _typed_payload_size(checkpoint)
        + _typed_payload_size(metadata)
        + _component_size(parent_id)
    )


def _write_group_size(
    checkpoint_id: Any,
    writes: dict[tuple[str, int], tuple[str, str, tuple[str, bytes], str]],
) -> int:
    size = _component_size(checkpoint_id)
    for inner_key, (task_id, channel, value, task_path) in writes.items():
        size += sum(_component_size(part) for part in inner_key)
        size += _component_size(task_id)
        size += _component_size(channel)
        size += _typed_payload_size(value)
        size += _component_size(task_path)
    return size


def _blob_entry_size(
    channel: str,
    version: Any,
    value: tuple[str, bytes],
) -> int:
    return (
        _component_size(channel) + _component_size(version) + _typed_payload_size(value)
    )


class _CheckpointSizeState:
    """Small shared aggregate tracker: two integers per checkpoint thread."""

    def __init__(self) -> None:
        self._by_thread: dict[str, tuple[int, int]] = {}
        self._total_bytes = 0
        self._total_checkpoints = 0
        self._lock = threading.RLock()

    @property
    def total_bytes(self) -> int:
        with self._lock:
            return self._total_bytes

    @property
    def total_checkpoints(self) -> int:
        with self._lock:
            return self._total_checkpoints

    def thread_bytes(self, thread_id: str) -> int:
        with self._lock:
            return self._by_thread.get(thread_id, (0, 0))[0]

    def thread_checkpoints(self, thread_id: str) -> int:
        with self._lock:
            return self._by_thread.get(thread_id, (0, 0))[1]

    def adjust(
        self,
        thread_id: str,
        size_delta: int,
        checkpoint_delta: int = 0,
    ) -> None:
        with self._lock:
            old_size, old_count = self._by_thread.get(thread_id, (0, 0))
            new_size = max(0, old_size + size_delta)
            new_count = max(0, old_count + checkpoint_delta)
            self._total_bytes += new_size - old_size
            self._total_checkpoints += new_count - old_count
            if new_size or new_count:
                self._by_thread[thread_id] = (new_size, new_count)
            else:
                self._by_thread.pop(thread_id, None)

    def remove_thread(self, thread_id: str) -> int:
        with self._lock:
            size_bytes, checkpoint_count = self._by_thread.pop(thread_id, (0, 0))
            self._total_bytes -= size_bytes
            self._total_checkpoints -= checkpoint_count
            return size_bytes

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        with self._lock:
            old_size, old_count = self._by_thread.get(target_thread_id, (0, 0))
            new_size, new_count = self._by_thread.get(source_thread_id, (0, 0))
            self._total_bytes += new_size - old_size
            self._total_checkpoints += new_count - old_count
            if new_size or new_count:
                self._by_thread[target_thread_id] = (new_size, new_count)
            else:
                self._by_thread.pop(target_thread_id, None)

    def clear(self) -> None:
        with self._lock:
            self._by_thread.clear()
            self._total_bytes = 0
            self._total_checkpoints = 0


class InMemorySaver(InMemorySaverBase):
    def __init__(
        self,
        *,
        serde: SerializerProtocol | None = None,
        __persistence_hook__: Callable[[PersistentDict], None] | None = None,
    ) -> None:
        self.filename = os.path.join(".langgraph_api", ".langgraph_checkpoint.")
        self.latest_iter: AsyncIterator[CheckpointTuple] | None = None
        i = 0

        def factory(*args):
            nonlocal i
            i += 1

            if not os.path.exists(".langgraph_api"):
                os.mkdir(".langgraph_api")
            thisfname = self.filename + str(i) + ".pckl"
            d = PersistentDict(*args, filename=thisfname)
            if __persistence_hook__:
                __persistence_hook__(d)

            try:
                d.load()
            except FileNotFoundError:
                pass
            except ModuleNotFoundError:
                logger.error(
                    "Unable to load cached data - your code has changed in a way that's incompatible with the cache."
                    "\nThis usually happens when you've:"
                    "\n  - Renamed or moved classes"
                    "\n  - Changed class structures"
                    "\n  - Pulled updates that modified class definitions in a way that's incompatible with the cache"
                    "\n\nRemoving invalid cache data stored at path: .langgraph_api"
                )
                try:
                    os.remove(self.filename)
                except Exception:
                    pass
            except Exception as e:
                logger.error("Failed to load cached data: %s", str(e))
                try:
                    os.remove(self.filename)
                except Exception:
                    pass
            return d

        from langgraph_api.serde import Serializer  # noqa: PLC0415

        super().__init__(
            serde=serde if serde is not None else Serializer(),
            factory=factory if not DISABLE_FILE_PERSISTENCE else defaultdict,
        )
        self._size_state = _CheckpointSizeState()
        self._rebuild_size_tracking()

    @property
    def estimated_size_bytes(self) -> int:
        """Estimated serialized bytes held by all checkpoint data."""
        return self._size_state.total_bytes

    @property
    def estimated_checkpoint_count(self) -> int:
        """Number of checkpoint records included in size tracking."""
        return self._size_state.total_checkpoints

    def estimated_size_for_thread(self, thread_id: str | uuid.UUID) -> int:
        return self._size_state.thread_bytes(str(thread_id))

    def estimated_checkpoint_count_for_thread(self, thread_id: str | uuid.UUID) -> int:
        return self._size_state.thread_checkpoints(str(thread_id))

    def _rebuild_size_tracking(self) -> None:
        """Rebuild estimates once after loading the persistent dictionaries."""
        self._size_state.clear()
        for thread_id, namespaces in self.storage.items():
            for _checkpoint_ns, checkpoints in namespaces.items():
                for checkpoint_id, entry in checkpoints.items():
                    self._size_state.adjust(
                        str(thread_id),
                        _checkpoint_entry_size(checkpoint_id, entry),
                        1,
                    )
        for (thread_id, _checkpoint_ns, checkpoint_id), writes in self.writes.items():
            self._size_state.adjust(
                str(thread_id), _write_group_size(checkpoint_id, writes)
            )
        for (thread_id, _checkpoint_ns, channel, version), value in self.blobs.items():
            self._size_state.adjust(
                str(thread_id), _blob_entry_size(channel, version, value)
            )

    def copy_thread_size(
        self,
        source_thread_id: str | uuid.UUID,
        target_thread_id: str | uuid.UUID,
    ) -> None:
        """Copy aggregate accounting after the runtime copies a thread."""
        self._size_state.copy_thread(str(source_thread_id), str(target_thread_id))

    def discard_checkpoint_size(
        self,
        thread_id: str | uuid.UUID,
        _checkpoint_ns: str,
        checkpoint_id: Any,
        entry: Any,
    ) -> int:
        """Discard the estimate for one checkpoint removed by internal code."""
        size_bytes = _checkpoint_entry_size(checkpoint_id, entry)
        self._size_state.adjust(str(thread_id), -size_bytes, -1)
        return size_bytes

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: dict[str, str | int | float],
    ) -> RunnableConfig:
        # TODO: Should this be done in OSS as well?
        # Filter out transient fields that are request-scoped, not checkpoint-scoped
        config_metadata = config.get("metadata", {})
        metadata = {
            **{
                k: v
                for k, v in config["configurable"].items()
                if not k.startswith("__")
                and k not in _EXCLUDED_KEYS
                and k not in _TRANSIENT_CONFIGURABLE_KEYS
            },
            **{
                k: v
                for k, v in config_metadata.items()
                if k not in _TRANSIENT_CONFIGURABLE_KEYS
            },
            **{
                k: v
                for k, v in metadata.items()
                if k not in _TRANSIENT_CONFIGURABLE_KEYS
            },
        }
        if not isinstance(checkpoint["id"], uuid.UUID):
            # Avoid type inconsistencies
            checkpoint = checkpoint.copy()
            checkpoint["id"] = str(checkpoint["id"])
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        checkpoint_id = checkpoint["id"]
        old_entry = (
            self.storage.get(thread_id, {}).get(checkpoint_ns, {}).get(checkpoint_id)
        )
        old_size = (
            _checkpoint_entry_size(checkpoint_id, old_entry)
            if old_entry is not None
            else 0
        )
        old_size += sum(
            _blob_entry_size(channel, version, self.blobs[blob_key])
            for channel, version in new_versions.items()
            if (blob_key := (thread_id, checkpoint_ns, channel, version)) in self.blobs
        )

        result = super().put(config, checkpoint, metadata, new_versions)
        new_size = _checkpoint_entry_size(
            checkpoint_id,
            self.storage[thread_id][checkpoint_ns][checkpoint_id],
        )
        new_size += sum(
            _blob_entry_size(
                channel,
                version,
                self.blobs[(thread_id, checkpoint_ns, channel, version)],
            )
            for channel, version in new_versions.items()
        )
        self._size_state.adjust(
            str(thread_id), new_size - old_size, int(old_entry is None)
        )
        return result

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"]["checkpoint_id"]
        outer_key = (thread_id, checkpoint_ns, checkpoint_id)
        old_writes = self.writes.get(outer_key)
        old_size = (
            _write_group_size(checkpoint_id, old_writes)
            if old_writes is not None
            else 0
        )
        super().put_writes(config, writes, task_id, task_path)
        new_writes = self.writes.get(outer_key)
        new_size = (
            _write_group_size(checkpoint_id, new_writes)
            if new_writes is not None
            else 0
        )
        self._size_state.adjust(str(thread_id), new_size - old_size)

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        if isinstance(config["configurable"].get("checkpoint_id"), uuid.UUID):
            # Avoid type inconsistencies....
            config = config.copy()

            config["configurable"] = {
                **config["configurable"],
                "checkpoint_id": str(config["configurable"]["checkpoint_id"]),
            }
        return super().get_tuple(config)

    def clear(self):
        self.storage.clear()
        self.writes.clear()
        self.blobs.clear()
        self._size_state.clear()
        for suffix in ["1", "2", "3"]:
            file_path = f"{self.filename}{suffix}.pckl"
            if os.path.exists(file_path):
                os.remove(file_path)

    def delete_thread(self, thread_id: str) -> None:
        super().delete_thread(thread_id)
        self._size_state.remove_thread(str(thread_id))

    def prune(
        self,
        thread_ids: Sequence[str],
        *,
        strategy: str = "keep_latest",
    ) -> None:
        """Prune checkpoint history for one or more threads.

        ``keep_latest`` retains the newest checkpoint in every checkpoint
        namespace.  If a channel's value is not materialized at that newest
        checkpoint (the representation used by ``DeltaChannel``), the parent
        chain is retained until the nearest materialized snapshot.  This keeps
        delta-backed state reconstructable while still removing old branches,
        obsolete checkpoints, writes, and blobs.
        """
        if strategy in {"delete", "delete_all"}:
            for thread_id in thread_ids:
                self.delete_thread(str(thread_id))
            return
        if strategy != "keep_latest":
            raise ValueError(
                "Unsupported prune strategy: "
                f"{strategy!r}. Expected 'keep_latest' or 'delete'."
            )

        for raw_thread_id in thread_ids:
            thread_id = str(raw_thread_id)
            namespaces = self.storage.get(thread_id)
            if not namespaces:
                continue

            kept_write_keys: set[tuple[str, str, str]] = set()
            kept_blob_keys: set[tuple[Any, ...]] = set()
            removed_size = 0
            removed_checkpoints = 0

            for checkpoint_ns, checkpoints in list(namespaces.items()):
                if not checkpoints:
                    del namespaces[checkpoint_ns]
                    continue

                # Checkpoint IDs are monotonically sortable strings in
                # LangGraph.  ``key=str`` also tolerates legacy UUID objects.
                latest_id = max(checkpoints, key=str)
                keep_ids = {latest_id}
                latest_entry = checkpoints[latest_id]
                latest_checkpoint = self.serde.loads_typed(latest_entry[0])

                # Missing/empty values need the ancestor write chain.  This is
                # how DeltaChannel stores non-snapshot checkpoints.
                needed_channels = {
                    channel
                    for channel, version in latest_checkpoint.get(
                        "channel_versions", {}
                    ).items()
                    if (
                        (
                            blob := self.blobs.get(
                                (thread_id, checkpoint_ns, channel, version)
                            )
                        )
                        is None
                        or blob[0] == "empty"
                    )
                }

                parent_id = latest_entry[2]
                while parent_id is not None and needed_channels:
                    parent_entry = checkpoints.get(parent_id)
                    if parent_entry is None:
                        break
                    keep_ids.add(parent_id)
                    parent_checkpoint = self.serde.loads_typed(parent_entry[0])
                    parent_versions = parent_checkpoint.get("channel_versions", {})
                    for channel in tuple(needed_channels):
                        version = parent_versions.get(channel)
                        if version is None:
                            continue
                        blob = self.blobs.get(
                            (thread_id, checkpoint_ns, channel, version)
                        )
                        if blob is not None and blob[0] != "empty":
                            needed_channels.remove(channel)
                    parent_id = parent_entry[2]

                for checkpoint_id in checkpoints.keys() - keep_ids:
                    removed_size += _checkpoint_entry_size(
                        checkpoint_id,
                        checkpoints[checkpoint_id],
                    )
                    removed_checkpoints += 1
                namespaces[checkpoint_ns] = {
                    checkpoint_id: entry
                    for checkpoint_id, entry in checkpoints.items()
                    if checkpoint_id in keep_ids
                }

                for checkpoint_id in keep_ids:
                    kept_write_keys.add((thread_id, checkpoint_ns, str(checkpoint_id)))
                    checkpoint = self.serde.loads_typed(checkpoints[checkpoint_id][0])
                    for channel, version in checkpoint.get(
                        "channel_versions", {}
                    ).items():
                        kept_blob_keys.add((thread_id, checkpoint_ns, channel, version))

            for key in list(self.writes):
                normalized_key = (str(key[0]), key[1], str(key[2]))
                if str(key[0]) == thread_id and normalized_key not in kept_write_keys:
                    removed_size += _write_group_size(key[2], self.writes[key])
                    del self.writes[key]

            for key in list(self.blobs):
                if str(key[0]) == thread_id and key not in kept_blob_keys:
                    removed_size += _blob_entry_size(
                        key[2],
                        key[3],
                        self.blobs[key],
                    )
                    del self.blobs[key]

            self._size_state.adjust(
                thread_id,
                -removed_size,
                -removed_checkpoints,
            )

    async def aprune(
        self,
        thread_ids: Sequence[str],
        *,
        strategy: str = "keep_latest",
    ) -> None:
        self.prune(thread_ids, strategy=strategy)

    async def _decrypt_json(self, data: dict[str, Any]) -> dict[str, Any]:
        """Decrypt a dict if custom encryption is configured."""
        from langgraph_api import config as api_config  # noqa: PLC0415

        if not api_config.LANGGRAPH_ENCRYPTION:
            return data
        from langgraph_api.encryption import get_encryption  # noqa: PLC0415
        from langgraph_api.encryption.middleware import (  # noqa: PLC0415
            decrypt_json_if_needed,
        )

        result = await decrypt_json_if_needed(data, get_encryption(), "checkpoint")
        if result is None:
            raise ValueError("decrypt_json_if_needed returned None for non-None input")
        return result

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Get checkpoint tuple with decrypted metadata."""
        tuple_ = self.get_tuple(config)
        if tuple_ is None:
            return None

        # Decrypt metadata if encryption is enabled
        decrypted_metadata = await self._decrypt_json(tuple_.metadata)

        from langgraph.checkpoint.base import (  # noqa: PLC0415
            CheckpointTuple as CPTuple,
        )

        return CPTuple(
            config=tuple_.config,
            checkpoint=tuple_.checkpoint,
            metadata=decrypted_metadata,
            parent_config=tuple_.parent_config,
            pending_writes=tuple_.pending_writes,
        )

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """List checkpoints with decrypted metadata."""
        from langgraph.checkpoint.base import (  # noqa: PLC0415
            CheckpointTuple as CPTuple,
        )

        for tuple_ in self.list(config, filter=filter, before=before, limit=limit):
            # Decrypt metadata if encryption is enabled
            decrypted_metadata = await self._decrypt_json(tuple_.metadata)

            yield CPTuple(
                config=tuple_.config,
                checkpoint=tuple_.checkpoint,
                metadata=decrypted_metadata,
                parent_config=tuple_.parent_config,
                pending_writes=tuple_.pending_writes,
            )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        stop_flush_loop()
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def aget_iter(self, config: RunnableConfig) -> AsyncIterator[CheckpointTuple]:
        tup = await self.aget_tuple(config)
        if tup is not None:
            yield tup


MEMORY = None


def Checkpointer(*args, unpack_hook=None, **kwargs):
    global MEMORY
    if MEMORY is None:
        MEMORY = InMemorySaver(
            __persistence_hook__=register_persistent_dict,
        )
    if unpack_hook is not None:
        from langgraph_api.serde import Serializer  # noqa: PLC0415

        # Prefer the API-level feature flag when available; older
        # langgraph-api versions may not define it yet.
        try:
            from langgraph_api.feature_flags import (  # noqa: PLC0415
                DELTA_CHANNEL_SUPPORT,
            )
        except ImportError:
            DELTA_CHANNEL_SUPPORT = False

        # DeltaChannel snapshots only exist on langgraph >= 1.2; on older
        # installs the ``EXT_DELTA_SNAPSHOT`` codepoint can never appear in
        # serialized payloads, so the bare ``unpack_hook`` is sufficient.
        if DELTA_CHANNEL_SUPPORT:
            from langgraph.checkpoint.serde.jsonplus import (  # noqa: PLC0415
                EXT_DELTA_SNAPSHOT,  # ty: ignore[unresolved-import]
            )
            from langgraph.checkpoint.serde.types import (  # noqa: PLC0415
                _DeltaSnapshot,  # ty: ignore[unresolved-import]
            )

            _inner_hook = unpack_hook

            def _delta_aware_hook(code: int, data: bytes) -> Any:
                if code == EXT_DELTA_SNAPSHOT:
                    import ormsgpack  # noqa: PLC0415

                    return _DeltaSnapshot(
                        ormsgpack.unpackb(
                            data,
                            ext_hook=_delta_aware_hook,
                            option=ormsgpack.OPT_NON_STR_KEYS,
                        )
                    )
                return _inner_hook(code, data)

            ext_hook = _delta_aware_hook
        else:
            ext_hook = unpack_hook

        saver = InMemorySaver(
            serde=Serializer(__unpack_ext_hook__=ext_hook),
            __persistence_hook__=register_persistent_dict,
            **kwargs,
        )
        saver.writes = MEMORY.writes
        saver.blobs = MEMORY.blobs
        saver.storage = MEMORY.storage
        saver._size_state = MEMORY._size_state
        return saver
    return MEMORY


__all__ = ["Checkpointer"]
