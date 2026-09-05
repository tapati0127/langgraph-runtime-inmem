# langgraph-runtime-inmem 0.33.3 TTL fixes

This is a source-compatible patch for `langgraph-runtime-inmem==0.33.3`.  The
wheel version is `0.33.3.post2` so pip can distinguish it from the unpatched
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
- Implements the standard LangGraph `BaseStore` item TTL contract, including
  `store.ttl.default_ttl`, per-item `ttl` overrides, and disabling expiration
  with `ttl=None`.
- Refreshes item expiration on `get` and `search` according to
  `refresh_on_read` and each operation's `refresh_ttl` override.
- Persists item TTL metadata alongside the disk-backed data and vector files,
  and deletes the item, its vectors, and its TTL metadata in one sweep.
- Runs the store sweeper in the background and emits the standard
  `Starting store TTL sweeper ...` and `Store swept ...` logs.

For `keep_latest`, an expired inactive thread is pruned once.  It becomes
eligible again only after its `updated_at` advances and another full TTL period
passes.  This prevents the same retained checkpoint from being processed every
sweep interval.

## Install

```bash
pip install --force-reinstall --no-deps \
  release/langgraph_runtime_inmem-0.33.3.post2-py3-none-any.whl
```

Verify that `langgraph dev` uses the patched runtime:

```bash
python -c "import langgraph_runtime_inmem as m; print(m.__version__)"
```

Expected output: `0.33.3.post2`.

The primary tested dependency set is:

```text
langgraph==1.2.11
langgraph-api==0.13.3
langgraph-cli[inmem]==0.4.31
langgraph-checkpoint==4.2.0
langgraph-runtime-inmem==0.33.3.post2
```

All tests also pass with `langgraph-api==0.13.2`, so the patch can be installed
into that environment without requiring an API upgrade.

## Configuration

The behavior follows LangGraph's official
[store item TTL configuration](https://docs.langchain.com/langsmith/configure-ttl#configuring-store-item-ttl).
The requested `langgraph.json` configuration works unchanged and can be
combined with Store TTL:

```json
{
  "checkpointer": {
    "ttl": {
      "strategy": "keep_latest",
      "sweep_interval_minutes": 1,
      "default_ttl": 43200
    }
  },
  "store": {
    "ttl": {
      "refresh_on_read": true,
      "sweep_interval_minutes": 60,
      "default_ttl": 10080
    }
  }
}
```

All TTL values are in minutes, so `43200` is 30 days and `10080` is 7 days.
Both sweepers first run after one full sweep interval, and startup logs confirm
that the loops are active.  Store reads refresh an item's expiration by default;
pass `refresh_ttl=False` to `get` or `search` to suppress that refresh.  Passing
`ttl` to `put` overrides the default for that item, while `ttl=None` makes it
non-expiring.  Enabling a default does not retroactively add TTL metadata to
items that already exist.

With production-like TTL values, newly active data is intentionally not removed
during a short smoke test.  Use a per-thread or per-item TTL of `0` in a test
request, or run the included unit tests, to exercise expiration immediately.

## Test

From this directory, with the LangGraph dev dependencies installed:

```bash
pytest -q
```

The tests cover thread TTL visibility and overrides, cascade deletion, blob
release, `keep_latest`, DeltaChannel safety, repeat-sweep prevention, sweep
limits, active-run protection, and the thread background loop.  Store tests
cover default and per-item TTLs, read/search refresh, write reset, non-expiring
items, item/vector cleanup, non-retroactive defaults, background sweeping,
disk-backed restart persistence, and the Agent Server store wrapper.
