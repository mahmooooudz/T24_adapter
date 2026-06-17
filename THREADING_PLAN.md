# Multi-Threading — Implementation Plan (Rewritten, measured)

**Date:** 2026-06-17
**Goal:** Parallelize the per-application path so wall-clock scales with N tables
instead of summing them, **without** breaking correctness, observability, or
the lossless single-pass guarantees we just shipped.

---

## 0. Measured baseline (this isn't a guess)

10k × 2 tables, sequential, 3 consecutive runs:

| Run | Single pass | Pass 2 (write) | Total |
|---|---|---|---|
| 1 | 2.90 s | 3.14 s | **6.38 s** |
| 2 | 2.88 s | 3.10 s | **6.34 s** |
| 3 | 2.78 s | 3.04 s | **6.20 s** |

**Per-table attribution** (isolated):

| Per table | CUSTOMER | ACCOUNT |
|---|---|---|
| Read + parse + normalize | 0.65 s | 0.61 s |
| (of which) fetchall network | 0.63 s | 0.20 s |
| (of which) parse + normalize CPU | ~0.50 s | ~0.50 s |

**Decomposition of the 6.3 s run:**
- **Per-table work (parallelizable):** 0.65 + 0.61 = **~1.27 s**
- **Per-run fixed overhead** (metadata, discovery, DDL, sweep, write coordination): **~5.0 s**

Threading can only attack the ~1.3 s. The fixed ~5 s is *unchanged by parallelism*
and needs the warm-cache lever (separate work) to move.

---

## 1. Realistic targets — set BEFORE implementing

**For today's dataset (10k × 2 tables):**

| Config | Expected | Speedup vs 6.3 s |
|---|---|---|
| K=1 (sequential, today) | **6.3 s** | 1.0× |
| K=2 | **5.0–5.3 s** | ~1.2× |
| K=4 (same 2 tables) | ~5.0 s | no further gain |

**Projected for larger schemas (estimate, must be re-measured when reached):**

| Scale | Sequential | K=4 | Speedup |
|---|---|---|---|
| 5 tables × 10k | ~10 s | ~7 s | ~1.4× |
| 10 tables × 10k | ~15 s | ~8.5 s | ~1.8× |
| 20 tables × 10k | ~25 s | ~9 s | **~2.8×** |
| 20 tables × 100k | ~135 s | ~38 s | **~3.5×** |

**Why these and not "20×":** Python GIL limits CPU concurrency; the workload is
~40% I/O wait + ~60% CPU after the recent optimizations. The theoretical ceiling
from threading alone is ~2.5–3.5× regardless of K beyond ~4.

**Honest framing for stakeholders:** for the current 2-table case the absolute
saving is ~1 s. The value is **shape, not speed** — the system becomes ready for
the N-table case before N grows.

---

## 2. Architecture (verified against the codebase, 2026-06-17)

What's already in place that the plan leverages:

| Building block | Where | What it gives us |
|---|---|---|
| `config.include_applications` (mutable Tuple) | `config.py:197`, `pipeline.py:424`, `app.py:261`, `db_writer.py:189` | Per-worker scoping: each worker runs one app's full pipeline via this filter |
| `_AppSink` per-app sink | `db_writer.py:390` | Already the right unit of write parallelism |
| `_estimate_rows` (`pg_class.reltuples`, no scan) | `db_reader.py:159` | Cheap largest-first scheduling |
| `T24GenericPipeline.__init__(config)` builds its own connections | `pipeline.py:305-336` | Each worker = own pipeline instance = own connections (no shared-conn refactor) |

What needs to change:

| Obstacle | Why it blocks threading | Plan |
|---|---|---|
| `RunMetrics` non-atomic increment, no lock, single global span model | Two workers timing `pass_single` would double-count vs wall-clock | New "per-app spans + aggregate model" |
| Console SSE: `_THREAD_RUN[get_ident()]` set only on the run thread | Worker threads' adapter logs vanish from the live stream | Workers register/deregister on enter/exit |
| No worker-pool layer exists | — | New `T24WorkerPool` over apps |

---

## 3. The levers (each with target, debug, test)

### Lever T1 — Parallelism-aware `RunMetrics`  (FOUNDATION)

**Problem.** Today: `span("pass_single")` accumulates a single global key. With
K workers each timing 3 s, the global key would read ~3K s — meaningless against
wall-clock. Also no `Lock` → torn updates on increments.

**Fix.**
- Add `threading.Lock` around `add`/`incr`/`span`.
- New per-app sub-keys: `metrics.span("pass_single", app="ACCOUNT")` records under
  `pass_single::ACCOUNT`. The report aggregates them as **MAX** (because they run
  in parallel and gate wall-clock) for phases, **SUM** for counters (round-trips).
- A new `metrics.parallel_phase("pass_single", apps=[...])` helper computes the
  MAX over a group at report time.

**Debug.** The report adds a "parallel detail" sub-section: per-app span time +
which apps were assigned to which worker (anonymous worker ids).

**Tests (new, network-free):**
- Concurrent `incr` from N=64 threads → exact final count (no torn updates).
- Two `span("pass_single", app=X)` running in parallel threads → MAX reported,
  not SUM.
- Existing single-app behavior unchanged (no `app=` kwarg → legacy path).

**Latency target.** Foundation change — no measurable speedup expected. The
8-run regression check must hold sequential at **6.3 ± 0.2 s**.

---

### Lever T2 — `T24WorkerPool` over applications

**Problem.** No fan-out layer exists.

**Fix.** New `t24_adapter/parallel.py` with one class:

```python
class T24WorkerPool:
    """Bounded ThreadPoolExecutor over applications. Each worker builds its
    OWN T24GenericPipeline + WideDatabaseWriter from the config, with
    include_applications=(app,), so connections are per-worker and the
    rest of the pipeline (single-pass, adaptive read, bulk metadata,
    everything) runs unchanged inside each worker."""
    def __init__(self, config, *, max_workers: int): ...
    def run(self, apps: list[str], *, failure_policy="independent") -> dict: ...
```

- `max_workers=1` reproduces today's sequential run byte-for-byte.
- `failure_policy="independent"` (default): each worker is its own transaction;
  on failure others continue (UPSERT keeps the run re-runnable).
- `failure_policy="fail-fast"`: first worker exception cancels pending submits.
- Each worker calls `SET LOCAL statement_timeout = <ms>` at session start
  (Postgres-side query kill) — the **only** reliable way to bound a hung query,
  since psycopg2 blocked inside libpq cannot be cancelled from Python.

**Wiring.**
- `main.py`: if `config.db_max_workers > 1`, call the pool; else today's path.
- `app.py` (console): same dispatch, with each worker registering in
  `_THREAD_RUN` on enter and popping on exit so SSE log routing keeps working.

**Debug.** Per-worker log prefix `[<APP>]` so interleaved lines stay readable;
`metrics.parallel_phase("pass_single", apps=...)` records the parallel block.

**Tests:**
- *Equivalence (the critical one).* Same `_wide` rows from K=1 and K=4 runs
  (snapshot + diff). Output must be byte-identical.
- *Connection isolation.* A wrapper around `_connect_from_env` records the
  connection's `id()` and the calling thread; assert no two threads share the
  same connection id at any moment.
- *failure-policy contract.* A fake app that raises in pass 2: `independent`
  → other apps still commit and the bad one's `_wide` is rolled back; `fail-fast`
  → pending submits cancelled.
- *Pool size honored.* Submit 10 apps with K=3, assert at most 3 are "running"
  at any instant (via an atomic counter in a fake worker).

**Latency target (measured against the 6.3 s baseline):**

| K | Target wall-clock | Acceptance band |
|---|---|---|
| 1 | 6.3 s | 6.1 – 6.5 s (regression guard) |
| 2 | **5.0 s** | 4.8 – 5.4 s |
| 4 | 5.0 s | 4.8 – 5.4 s |

If K=2 doesn't hit ≤ 5.4 s, the work overlap is broken — investigate before
moving on.

---

### Lever T3 — Largest-first scheduling

**Problem.** With heterogenous table sizes, a naive K=4 over 20 tables where one
table dominates spends most of the run waiting for that one. Wall-clock = the
biggest table's time, regardless of pool size.

**Fix.** After `discover_tables()`, the pool calls `_estimate_rows()` once per
app (uses the existing helper — `pg_class.reltuples`, no scan) and submits
**largest first**. Big tables start at t=0, small tables fill the tail.

**Debug.** Log the dispatch order with estimated sizes.

**Tests:**
- Mixed sizes (10, 1000, 100, 50000): assert dispatch order is descending by
  estimate.
- Missing estimate (`-1`) sorts to last (defensive).

**Latency target.** No measurable change on 2 balanced tables (current dataset);
on a skewed 20-table set, expect **15–25% improvement** of wall-clock vs random
ordering — re-measure when that case exists.

---

### Lever T4 — Safety: cap workers vs server capacity

**Problem.** The shared DB on `213.136.75.9` is hit by `siacoredb` and other
consumers. K workers × 2 connections each (read + write) could exhaust
`max_connections`.

**Fix.** At pool start, one query: `SHOW max_connections;` and
`SELECT count(*) FROM pg_stat_activity;`. If `2*K > 0.25 * headroom`, **lower
effective K** and warn. Pure defensive cap; never raises K.

**Debug.** Warning line with the chosen effective K and the math.

**Tests:**
- Fake `headroom=10, K=8` → effective K reduced and warning logged.
- Healthy `headroom=200, K=4` → no change.

**Latency target.** No improvement; safety only.

---

## 4. Implementation sequence & estimated effort

| Step | Lever | Effort | Expected wall-clock after (10k×2) | Cumulative speedup |
|---|---|---|---|---|
| 1 | T1: thread-safe metrics + per-app spans | 0.5 day | 6.3 s (unchanged) | 1.0× (foundation) |
| 2 | T2: worker pool + K=2 e2e at 10k×2 | 1 day | **5.0 s** | ~1.25× |
| 3 | T2: K=4 / fan-out validated on a synthetic N-table run | 0.5 day | 5.0 s (10k×2) / ~9 s (20×10k projected) | 2.8× (20-table) |
| 4 | T3: largest-first scheduling | 0.5 day | unchanged 10k×2; up to 25% on skewed | — |
| 5 | T4: connection-headroom cap + statement_timeout | 0.5 day | unchanged; safety | — |

**Total effort:** ~3 days, broken into incremental commits that each pass the
existing 28-test suite + their own new tests.

---

## 5. Acceptance gates (each step must pass before the next)

1. **Existing 28 tests still pass** at every commit.
2. **3-run sequential baseline** stays in `6.1 – 6.5 s` (no regression).
3. **K=2 wall-clock** lands in `4.8 – 5.4 s` (the I/O-overlap proof point).
4. **Output equivalence**: snapshot of `ACCOUNT_wide` + `CUSTOMER_wide` columns
   and row hashes — byte-identical to the K=1 run.
5. **Connection isolation test** asserts no two workers share a connection.
6. **Latency report** under K>1 sums coherently: per-phase MAX + per-component
   SUM = wall-clock ± measurement noise.

---

## 6. What this plan deliberately does NOT do

- **Process-based parallelism** (multiprocessing) — wrong tool here; CPU share
  is only ~60% and the cost (pickling, more connections) outweighs the gain at
  this scale.
- **Intra-table sharding** (splitting one big table across N workers) — defers
  the harder schema-merge problem; reach for it only if a single huge table
  becomes the bottleneck.
- **Warm cross-run cache** — a separate, higher-impact lever (~−2 s on every
  run) that's *more* valuable than threading for the 2-table case. Tracked
  separately so each can be evaluated on its own merits.
- **Hard Python-side thread kill** — not reliably possible while blocked in
  libpq. We use `SET LOCAL statement_timeout` so the *server* kills the query.

---

## 7. Re-measurement protocol (used at every step)

```bash
# Sequential baseline (regression check)
for i in 1 2 3; do python main.py | grep TOTAL; done

# Parallel measurement (after T2)
DB_MAX_WORKERS=2 python main.py | grep -E "TOTAL|parallel"
DB_MAX_WORKERS=4 python main.py | grep -E "TOTAL|parallel"

# Output equivalence (each step)
pytest tests/test_parallel_equivalence.py -q
```

Numbers go into `PERFORMANCE_REPORT.md` immediately, with the same care as the
existing report (mark *measured* vs *projected*, note variance).
