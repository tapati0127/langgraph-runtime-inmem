# langgraph-runtime-inmem 0.33.3 TTL fix

This is a source-compatible patch for `langgraph-runtime-inmem==0.33.3`.  The
wheel version is `0.33.3.post1` so pip can distinguish it from the unpatched
upstream wheel.

## What is implemented

- Persists per-thread TTL overrides and applies the global
  `checkpointer.ttl.default_ttl` to every thread, including threads implicitly
  created by a run.
- Implements `Threads.sweep_ttl()` for both `delete` and `keep_latest`.
- Implements `InMemorySaver.prune()/aprune()` and preserves the ancestor chain
  required to reconstruct `DeltaChannel` state.
- Restores a background TTL loop to the in-memory runtime lifespan, including
  the `Starting thread TTL sweeper ...` and sweep-completion logs.
- Makes thread deletion release checkpoint storage, pending writes, and blobs;
  the upstream 0.33.3 deletion path can leave blobs allocated.
- Supports `GET /threads/{thread_id}?include=ttl` for the in-memory backend.
- Does not prune a thread while it has a pending or running run.

For `keep_latest`, an expired inactive thread is pruned once.  It becomes
eligible again only after its `updated_at` advances and another full TTL period
passes.  This prevents the same retained checkpoint from being processed every
sweep interval.

## Install

```bash
pip install --force-reinstall --no-deps \
  release/langgraph_runtime_inmem-0.33.3.post1-py3-none-any.whl
```

Verify that `langgraph dev` uses the patched runtime:

```bash
python -c "import langgraph_runtime_inmem as m; print(m.__version__)"
```

Expected output: `0.33.3.post1`.

The primary tested dependency set is:

```text
langgraph==1.2.11
langgraph-api==0.13.3
langgraph-cli[inmem]==0.4.31
langgraph-checkpoint==4.2.0
langgraph-runtime-inmem==0.33.3.post1
```

The same seven tests also pass with `langgraph-api==0.13.2`, so the patch can
be installed into that environment without requiring an API upgrade.

## Configuration

The requested `langgraph.json` configuration works unchanged:

```json
{
  "checkpointer": {
    "ttl": {
      "strategy": "keep_latest",
      "sweep_interval_minutes": 1,
      "default_ttl": 43200
    }
  }
}
```

All TTL values are in minutes, so `43200` is 30 days.  The first sweep happens
after one full sweep interval.  A startup log confirms that the loop is active.
With this production-like value, a newly active thread is intentionally not
pruned during a short smoke test; use a per-thread TTL of `0` in a test request
or the included unit tests to exercise expiration immediately.

## Test

From this directory, with the LangGraph dev dependencies installed:

```bash
pytest -q
```

The tests cover TTL visibility and overrides, cascade deletion, blob release,
`keep_latest`, DeltaChannel safety, repeat-sweep prevention, sweep limits,
active-run protection, manual pruning, and the background loop.
