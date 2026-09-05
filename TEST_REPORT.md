# Test report

## Environment

- Python 3.12
- `langgraph==1.2.11`
- `langgraph-api==0.13.3`
- `langgraph-cli[inmem]==0.4.31`
- `langgraph-checkpoint==4.2.0`
- Patched runtime: `langgraph-runtime-inmem==0.33.3.post1`

An additional compatibility run replaced `langgraph-api==0.13.3` with
`langgraph-api==0.13.2`; all seven tests passed there as well.

## Automated tests

```text
.......                                                                  [100%]
7 passed in 0.23s
```

Compatibility run:

```text
langgraph-api=0.13.2
langgraph-runtime-inmem=0.33.3.post1
7 passed in 0.24s
```

Covered cases:

1. Per-thread TTL overrides the global TTL and is returned by `include=ttl`.
2. `delete` removes thread, runs, crons, checkpoint storage, writes, blobs, and
   TTL metadata.
3. Global `keep_latest` keeps one checkpoint per namespace.
4. An unchanged thread is not processed on every subsequent sweep; activity
   rearms the TTL.
5. DeltaChannel ancestor checkpoints are retained back to the nearest stored
   snapshot while an obsolete fork is deleted.
6. `limit` is honored and threads with a pending/running run are skipped.
7. The background loop uses the configured one-minute interval and invokes
   `Threads.sweep_ttl()` with the configured sweep limit.

Targeted Ruff syntax/import checks and format checks also passed.

## Wheel verification

The final wheel was force-installed into the clean test environment (replacing
the editable source) and the seven tests were rerun successfully against the
installed package.

```text
version=0.33.3.post1
loaded_from=.../site-packages/langgraph_runtime_inmem/__init__.py
7 passed in 0.23s
```

Wheel SHA-256:

```text
50663ea154b858a67e4724572e4c2d1f8be500b1477ce214afe53661d7c25399
```

## `langgraph dev` smoke test

`langgraph dev --no-browser --no-reload --port 8123` started successfully with
the included example and the requested configuration.  Relevant startup logs:

```text
Starting In-Memory runtime with langgraph-api=0.13.3 and in-memory runtime=0.33.3.post1
Starting thread TTL sweeper with interval 1 minutes strategy=keep_latest
Application started up
```

With `default_ttl=43200`, expiry is 30 days after the latest thread update, so a
short smoke test intentionally verifies activation/configuration rather than
waiting for production expiry.  Immediate expiration and both sweep strategies
are exercised by the automated tests.
