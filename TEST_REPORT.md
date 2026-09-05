# Test report

## Environment

- Python 3.12
- `langgraph==1.2.11`
- `langgraph-api==0.13.3`
- `langgraph-cli[inmem]==0.4.31`
- `langgraph-checkpoint==4.2.0`
- Patched runtime: `langgraph-runtime-inmem==0.33.3.post2`

An additional compatibility run replaced `langgraph-api==0.13.3` with
`langgraph-api==0.13.2`; all 21 tests passed there as well.

## Automated tests

```text
.....................                                                    [100%]
21 passed in 0.92s
```

Compatibility run:

```text
langgraph-api=0.13.2
langgraph-runtime-inmem=0.33.3.post2
21 passed in 0.86s
```

The seven thread/checkpointer TTL tests cover:

1. Per-thread TTL overrides and `include=ttl` visibility.
2. Cascade deletion of threads, runs, crons, checkpoint storage, writes,
   blobs, and TTL metadata.
3. Global `keep_latest` pruning and rearming after later activity.
4. DeltaChannel ancestor-chain preservation while obsolete forks are removed.
5. Sweep limits and protection for pending/running runs.
6. Manual `keep_latest` pruning.
7. Background-loop interval and sweep-limit propagation.

The fourteen Store TTL test cases cover:

1. `default_ttl`, per-item overrides, and `ttl=None`.
2. Default and per-operation `get` refresh behavior.
3. `search` refresh limited to returned items.
4. TTL reset on writes and metadata removal on deletes.
5. Non-retroactive defaults and per-item TTL without a global default.
6. Atomic removal of item data, vectors, and TTL metadata.
7. Background sweeper startup, deduplication, deletion, and shutdown.
8. TTL metadata persistence across a disk-backed restart.
9. Propagation of TTL configuration through the Agent Server `BatchedStore`.
10. No-op behavior without TTL configuration and validation of sweep intervals.

`pytest -W error`, Python bytecode compilation, targeted Ruff checks, Ruff
format checks, and `git diff --check` all passed. Files inherited from upstream
outside this change still contain pre-existing warnings under Ruff 0.16.6, so
the lint command was scoped to the modified Store implementation and its tests.

## Wheel verification

The final wheel was force-installed into the test environment, replacing the
editable source. Tests were then run from `/tmp` with a separate pytest root so
the repository's `pythonpath = ["src"]` setting could not shadow the wheel.

```text
version=0.33.3.post2
loaded_from=.../site-packages/langgraph_runtime_inmem/__init__.py
21 passed in 0.83s
```

Wheel SHA-256:

```text
d1bfd695a1f2f7ab194a8b69d8ffdc86a92bded3ca11a9cb89685f2dcabb6931
```

## `langgraph dev` startup smoke test

`langgraph dev --no-browser --no-reload --port 8123` reached application-ready
state with the combined thread and Store TTL example configuration. Relevant
startup logs:

```text
Starting In-Memory runtime with langgraph-api=0.13.3 and in-memory runtime=0.33.3.post2
Starting store TTL sweeper with interval 1.0 minutes
Starting thread TTL sweeper with interval 1 minutes strategy=keep_latest
Application started up
```

The deterministic automated tests use a controlled clock and immediate expiry
to verify actual deletion without waiting for production-length TTL windows.
