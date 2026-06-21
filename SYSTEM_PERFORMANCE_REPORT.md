# T24 Generic Adapter: System Performance Report

**Date:** 2026-06-21
**Branch:** `fix/wide_tables`
**Environment:** PostgreSQL at `213.136.75.9:15433` (remote, shared), round-trip
latency ≈ 70 ms. All numbers below are **measured** against the live DB unless
marked *(estimate)*.

---

## 1. Executive summary

The pipeline flattens T24 XML records into wide relational tables. Over a series
of measured optimizations it went from effectively unusable to fast and
scalable, and the web console now hides most of the remaining wait behind the
user's configuration time.


| Milestone                  | Dataset              | End-to-end           | Note                                            |
| -------------------------- | -------------------- | -------------------- | ----------------------------------------------- |
| Original (per-row write)   | 2K rows              | **≈ 140 s**          | one network round-trip per row                  |
| After core perf work       | 2K rows              | **≈ 6 s**            | bulk write + single-pass + lxml + adaptive read |
| Scaled up                  | 20K rows (10K × 2)   | **≈ 6.3 s** (cold)   | still fixed-overhead-bound                      |
| Scaled up                  | 200K rows (100K × 2) | **≈ 25–28 s** (cold) | write-bound                                     |
| **+ speculative prefetch** | 200K rows            | **≈ 14.7 s** (warm)  | read moved off the critical path                |


---

## 2. How a run works — the two phases

Every database run is two heavy phases, in sequence:

**Phase 1 — READ + normalize**

1. Stream rows from the source table. Tables under the buffer cap use a **plain
  client-side cursor** (one round-trip) instead of a server-side streaming
   cursor (~2× faster for tables that fit).
2. Parse each record's XML with **lxml** (falls back to stdlib ElementTree).
3. **Normalize** into flat fields (position → field-name resolution, multi/sub
  values, local refs) via the in-memory metadata registry (O(1) lookups).
4. **Buffer** each record compactly and discover the exact wide-column shape in
  the *same* pass (single-pass; the source is read once, not twice).

**Phase 2 — WRITE**
5. Bulk-UPSERT the buffered wide rows (`INSERT … ON CONFLICT DO UPDATE`),
   **8,000 rows per round-trip**, into the `<APP>_wide` tables.

Cold run = Phase 1 + Phase 2, sequentially.

---

## 3. The optimization levers (measured)


| Lever                              | What it changed                                            | Measured impact                                  |
| ---------------------------------- | ---------------------------------------------------------- | ------------------------------------------------ |
| **Bulk-batched UPSERT**            | one round-trip per *batch*, not per *row*                  | write ~~134 s → ~0.7 s at 2K (~~180×)            |
| **Single-pass pivot**              | read + parse + normalize once (was twice)                  | removed a full second read pass                  |
| **Bulk metadata prefetch**         | one query per metadata table, not per app                  | metadata round-trips 12 → 6 → 3                  |
| **lxml parser**                    | C-based XML parsing                                        | faster Phase 1 (parse)                           |
| `**__slots__` on NormalizedField** | cheaper allocation of the hot object                       | trims Phase 1 CPU                                |
| **Adaptive read**                  | plain cursor for tables that fit                           | ~2× faster read vs server-side cursor            |
| **Write batch 1K → 8K**            | fewer write round-trips (200 → 26 at 100K)                 | write ~~9K → ~16.7K rows/s (~~1.7×)              |
| **Connection + metadata warming**  | prefetch metadata/counts during the wizard                 | run skips validation/discovery/metadata (~1–2 s) |
| **Speculative data prefetch**      | read the data during the wizard                            | run becomes write-only (see §5)                  |
| **Parallel auto-gate**             | sequential below N tables (parallel adds contention there) | avoids slower-with-more-workers at few tables    |


---

## 4. Current performance at scale

### Scale curve (sequential, cold runs, measured)


| Dataset (rows)    | Read + normalize | Write      | **Total (cold)** | What dominates         |
| ----------------- | ---------------- | ---------- | ---------------- | ---------------------- |
| 2K (1K × 2)       | ~3 s*            | ~0.7 s     | **≈ 6.2 s**      | fixed per-run overhead |
| **20K (10K × 2)** | **~4–5.5 s**     | **~1–2 s** | **≈ 6.3 s**      | fixed per-run overhead |
| 200K (100K × 2)   | ~14 s            | ~12 s      | **≈ 25–28 s**    | the data work itself   |


 the 2K figure was taken on the earlier **two-pass** build; the 20K and 200K
rows are on the current **single-pass** engine.

**Key insight — the fixed-overhead floor.** Below ~~20K rows the run is dominated
by **~~5–6 s of per-run overhead** (connection, metadata, discovery, DDL, the
bulk write of a small set). That's why **2K and 20K land at nearly the same
~6 s** — 10× the data barely moves the needle when overhead dominates. Only
around 100K does the data work (read + write) overtake the overhead and the
total climbs to ~26 s. This is also why parallel workers don't help small sets
and why **warming + prefetch** (which attack overhead and read) matter most.

#### 20K (10K × 2) detail

- Sequential end-to-end: **~6.05–6.80 s** across 5 runs (median ≈ 6.3 s).
- Per-table read + normalize: ACCOUNT ~1.7–2.2 s, CUSTOMER ~2.4–3.5 s.
- Parallel (2 workers) at this size: **no meaningful gain** — the per-table work
is too small relative to fixed overhead + GIL/write contention (hence the
auto-gate keeps few-table runs sequential).
- *Measurement note:* taken at the 10K-row dataset stage (single-pass + lxml +
adaptive read). The write portion is marginally faster now under the 8K batch,
and prefetch applies equally — but the absolute saving is small here because
the read is only ~4–5 s.

### 100K × 2 tables = 200K records (3.1M fields), 8K write batch

```
Phase                         Time
Read + normalize (both)       ~14 s   (6 s ACCOUNT + 7.5 s CUSTOMER, sequential)
Write (200K UPSERTs)          ~12 s   (26 round-trips, ~16.7K rows/s)
TOTAL (cold)                  ~25–28 s
```

**On parallelism (honest):** at 100K, running the two tables across 2 worker
threads only saved ~12% — the **GIL serializes** the CPU-heavy normalize, so the
reads time-slice rather than truly run at once (one table's read ballooned
6 s → 16 s under contention), and the writes contend on the shared DB. Threading
is therefore **not** the lever at this table count; it is gated off below 3
tables by default.

---

## 5. The big win — speculative prefetch (25 s → ~14.7 s)

**Idea:** the READ phase doesn't depend on the format/filters/options the user
picks *after* selecting tables — and the user spends real time on those steps,
during which the machine is idle. So we do the read then.

**Mechanism:**

1. Leaving the **Tables** step fires `POST /api/prefetch` with the selected tables.
2. A background thread runs **all of Phase 1** for those tables (reusing warmed
  metadata) and stores the buffered rows + schema + stats in an in-memory
   cache, keyed by *connection signature + table set*.
3. The user configures (Configuration → Filters → Execute) while the read runs.
4. At **Run**:
  - **ready** → skip Phase 1, write directly from the buffer;
  - **still reading** → *join* the in-flight read (never double-read);
  - **absent/failed/too-big** → read normally (safe fallback).

**Measured (100K × 2, via the console):**


| Scenario                            | Run wall-clock | Saved vs cold |
| ----------------------------------- | -------------- | ------------- |
| Cold (no prefetch)                  | **27.4 s**     | —             |
| Prefetch completes (config ≥ ~14 s) | **14.7 s**     | **−12.7 s**   |
| Prefetch partial (config ~8 s)      | 22.9 s         | −2.8 s        |


**Expected vs actual:** model said "run ≈ write-only ≈ read time saved (~14 s)";
measured 27.4 → 14.7 s, **−12.7 s** — matches.

**Caveat (honest):** the saving equals *how much of the read finished before Run
was pressed*. It cannot hide read time that hasn't elapsed; it never double-reads
or risks correctness. Output is byte-identical to a cold run.

---

## 6. Where the time goes now, and what's left

After prefetch, the run is **write-bound**: ~12 s of bulk UPSERTs to a remote
shared database. That is the current floor for DB output.

**Remaining levers (not yet done):**

- **COPY-into-staging write** *(estimate ~1.4×)* — `COPY` into a staging table
then one `INSERT … SELECT … ON CONFLICT`. Would take the prefetched run from
~14.7 s toward ~10 s.
- **Multiprocessing** — would give *true* CPU parallelism for normalize (no GIL),
but the dominant write is shared-DB-bound and wouldn't parallelize; only worth
it at much larger scale.

**Note:** CSV/JSONL output is roughly half the time of DB output, because the
write is local disk rather than a network UPSERT to a shared server.

---

## 7. Correctness & safety

- **Output equivalence** is enforced by an opt-in integration test
(`pytest -m integration`): K=1 vs K=2 and cold vs prefetched produce
byte-identical `_wide` tables (SHA-256).
- **Lossless wide shape:** the single-pass buffers the whole app before writing,
so indexed columns (`NAME_1..N`) are sized to the true max; nothing is dropped.
- **74 unit tests** (network-free) cover the write batching, progress phases,
warming, the parallel gate, and the prefetch cache/join/staleness logic.

---

## 8. Configuration knobs (`settings.py` / `config.py`)


| Setting                   | Default | Effect                                                |
| ------------------------- | ------- | ----------------------------------------------------- |
| `db_batch_size`           | 8000    | rows per bulk UPSERT round-trip                       |
| `db_single_pass`          | True    | read once + buffer (vs two-pass)                      |
| `db_single_pass_max_rows` | 200000  | per-app buffer cap (over it → two-pass / no prefetch) |
| `db_max_workers`          | 1       | per-application parallelism (1 = sequential)          |
| `db_parallel_min_tables`  | 3       | parallel engages only at/above this table count       |
| `db_statement_timeout_s`  | 600     | server-side query timeout per worker                  |


---

## 9. How to reproduce

Every `python main.py` run prints a per-phase latency breakdown (`RunMetrics`).
The web console (`python app.py`) shows live two-phase progress (Reading… →
Saving…) with rows/s + ETA, and logs whether warming / prefetch were used.
Numbers in this report came from this machine against the live remote DB on
2026-06-21.