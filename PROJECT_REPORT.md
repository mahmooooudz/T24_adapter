# T24 Generic Adapter — Project Report

**Date:** 2026-06-21
**Branch:** `fix/wide_tables`
**Status:** Functionally complete and load-tested to 5M source rows. One known
performance limitation at ≥1M-row tables (see §8), with a recommended fix (§9).

---

## 1. What the project is

The T24 Generic Adapter is a **metadata-driven pipeline** that flattens Temenos
T24 XML records (the classic `RECID + XMLRECORD` layout) into **wide relational
tables** — one output table per T24 application, one column per business field.

It is **fully dynamic**: no application names, table names, field names, or T24
positions are hardcoded. All business meaning comes from T24 metadata
(`STANDARD.SELECTION`, `LOCAL.REF`, `CUSTOMIZATION`) loaded at runtime. New
applications are discovered automatically.

Two entry points:
- **CLI** — `python main.py` (settings in `settings.py`), prints a per-step
  latency breakdown.
- **Web console** — `python app.py`, a guided 6-step wizard (Connection →
  Tables → Configuration → Filters → Execute → Results) with live progress.

---

## 2. Architecture

### Pipeline stages (`t24_adapter/pipeline.py`)
1. **Validate** the package / connection.
2. **Discover** applications (every base table in the schema, minus metadata and
   `_wide` output tables).
3. **Load metadata** per app into an in-memory registry (O(1) field lookups).
4. **Read** records (streaming) → **5. Normalize** each into flat fields →
   **6. Pivot** to wide rows → **7. Write** to the sink.

### Key modules
| Module | Responsibility |
|---|---|
| `pipeline.py` | Orchestration; per-app stage loop; metadata cache; `preload()` |
| `metadata_loaders.py` / `metadata_registry.py` | Parse + index T24 metadata |
| `normalizer.py` | XML `<row>` → `NormalizedField`s (multi/sub values, local refs) |
| `db_reader.py` | Stream records from Postgres (adaptive plain vs server cursor) |
| `wide_writer.py` | Long→wide pivot; single-pass buffering + lossless shape |
| `db_writer.py` | Bulk UPSERT sink (`_wide` tables); `write_prebuilt` for prefetch |
| `parallel.py` | Bounded per-application worker pool (`T24WorkerPool`) |
| `metrics.py` | Run-scoped latency instrumentation (`RunMetrics`) |
| `_xml_backend.py` | lxml-if-available, else stdlib ElementTree |
| `app.py` | Flask web console: SSE progress, warming, prefetch, run dispatch |

### Output model
For application `ACCOUNT`, the result is `t24_adaptor.ACCOUNT_wide` (TEXT
columns, `recordId` + `app_name` + one column per field; multi/sub-valued fields
expand to `NAME_1..N`). Rows are **UPSERTed** (insert new / update changed); an
optional, safety-gated full-sync sweep mirrors source deletions.

---

## 3. Data model (current contents)

Schema `t24_adaptor`, **5 applications × 1,000,000 rows = 5,000,000 source rows**:

| Application | Rows | FK relationships |
|---|---|---|
| `ACCOUNT` | 1,000,000 | → CUSTOMER, CATEGORY, CURRENCY |
| `CUSTOMER` | 1,000,000 | (root entity) |
| `FUNDS_TRANSFER` | 1,000,000 | → ACCOUNT, CUSTOMER, CURRENCY |
| `STANDING_ORDER` | 1,000,000 | → ACCOUNT, CUSTOMER, CURRENCY |
| `LD_LOANS_AND_DEPOSITS` | 1,000,000 | → CUSTOMER, CATEGORY, CURRENCY |

Metadata tables (`STANDARD_SELECTION`, `LOCAL_REFERENCE`, `CUSTOMIZATION`) hold
one row per application. All five apps resolve to **real field names** with zero
`FIELD_*` fallbacks, and exercise multi-values, local-ref sub-values, and
customization fields.

---

## 4. Features delivered (the optimization journey)

| Area | Feature |
|---|---|
| **Write** | Bulk-batched UPSERT (was one round-trip per row); `db_batch_size=8000` |
| **Read/pivot** | Single-pass buffered pivot (read once, lossless shape); two-pass fallback for tables over the buffer cap |
| **Parsing** | Optional lxml backend; `__slots__` on the hot `NormalizedField` |
| **Read cursor** | Adaptive: plain client-side cursor for tables that fit, server-side streaming for large ones |
| **Metadata** | Bulk prefetch (one query per metadata table); cross-pass registry cache |
| **Concurrency** | Bounded `T24WorkerPool` over applications; auto-gated below `db_parallel_min_tables` |
| **Latency hiding** | Background **connection + metadata warming**; **speculative data prefetch** during the wizard |
| **UX** | Two-segment live progress (Reading… → Saving…) with rows/s + ETA; persisted connections; deterministic port |
| **Observability** | `RunMetrics` per-phase + per-component latency report with DB round-trip counts |

---

## 5. Performance summary (measured)

| Dataset | Cold | Notes |
|---|---|---|
| 2K (1K × 2) | ~6 s (was ~140 s) | bulk write killed the original per-row bottleneck |
| 20K (10K × 2) | ~6.3 s | still fixed-overhead-bound |
| 200K (100K × 2) | ~25–28 s; **~14.7 s prefetched** | write-bound; prefetch hides the read |
| **5M (1M × 5)** | **~1,970 s (~33 min)** | **two-pass re-read at scale — see §8** |

Detailed breakdowns and methodology are in `SYSTEM_PERFORMANCE_REPORT.md`.

---

## 6. Testing

- **75 unit tests** (network-free): pivot equivalence, write batching, progress
  phases, warming, parallel gate, prefetch cache/join/staleness, files sink.
- **1 opt-in integration test** (`pytest -m integration`): K=1 vs K=2 produce
  **byte-identical** `_wide` output (SHA-256) — the parallelism safety contract.
- Default `pytest` run is fully network-free and fast (~3 s).

---

## 7. Configuration knobs (`settings.py` / `config.py`)

| Setting | Default | Effect |
|---|---|---|
| `db_batch_size` | 8000 | rows per bulk UPSERT round-trip |
| `db_single_pass` | True | read once + buffer (vs two-pass) |
| `db_single_pass_max_rows` | 200000 | per-app buffer cap; over it → two-pass |
| `db_max_workers` | 1 | per-application parallelism (1 = sequential) |
| `db_parallel_min_tables` | 3 | parallel engages only at/above this table count |
| `db_statement_timeout_s` | 600 | server-side query timeout per worker |

---

## 8. ⚠️ CURRENT STATE & KNOWN ISSUE (read this)

### Current state
The system is functionally complete and correct at all tested scales (verified
to 5M rows, 0 errors, output equivalence enforced by tests). Everything works;
the issue below is **performance, not correctness**.

### The issue — two-pass re-read at ≥1M-row tables
A full run over the 5 × 1M dataset took **~1,970 s (~32 min 50 s)**. Root cause,
confirmed from the run log:

> `[WARN] Apps exceeding the single-pass buffer cap (200000) will be re-streamed
> (two-pass): ['ACCOUNT', 'CUSTOMER', 'FUNDS_TRANSFER', ...]`

Each table is 1,000,000 rows but `db_single_pass_max_rows = 200,000`. So the
single-pass buffered pivot **overflows on every table** and falls back to the
**two-pass path**: it reads + parses + normalizes each table **once to discover
the column shape, then again to write**. Measured from the log:

| | Pass 1 (discover) | Pass 2 (re-read + write) |
|---|---|---|
| 5 tables, read+normalize | **~767 s** | **~1,040 s** |

**~1,807 s of the ~1,970 s is read+normalize, roughly half of it the redundant
second read.** Two compounding factors:
1. **Prefetch is disabled** at this size — it uses the same buffer cap and bails
   (`prefetch failed ... tables too large to buffer`), so the read is fully on
   the critical path.
2. **The slower cursor is used** — tables over `buffered_read_max_rows` (200K)
   read via the server-side streaming cursor (~2× slower than the plain cursor).

The 200K cap is the single lever behind all three symptoms.

---

## 9. ✅ RECOMMENDED APPROACH TO FIX

Ranked, with honest trade-offs:

1. **Promote-on-overflow single-pass write — the proper fix.**
   Stream-write in one pass and rename columns on overflow (`NAME` → `NAME_1`,
   add `NAME_2`, …) instead of buffering the whole table to learn the shape.
   Result: **one read, constant memory, lossless — for any table size.**
   Eliminates the second read *and* the memory ceiling.
   *Expected:* ~33 min → **~18 min**; scales unbounded. *Cost:* the most
   involved change (rename-on-2nd-occurrence + seed widths from the existing
   table on re-runs).

2. **Quick win — raise `db_single_pass_max_rows`** above the table size (e.g.
   1.2M). Tables buffer in one pass (no re-read) and prefetch works again.
   *Expected:* halves the read → **~18 min**, with prefetch hiding most of it.
   *Cost:* memory — buffering 1M records ≈ **~2–3 GB RAM per table** (sequential,
   freed between tables). Fine on a workstation; risky on constrained infra.

3. **Faster large-table read — raise `buffered_read_max_rows`** so the plain
   cursor (one round-trip, ~2× faster) is used even for big tables. Pairs with #2.

4. **Parallel workers (`db_max_workers ≥ 2`).** At 5 tables × 1M, per-table work
   dominates fixed overhead, so threading genuinely overlaps reads/writes across
   tables (gate already allows ≥3 tables). Bounded by the GIL on CPU-heavy
   normalize, but a real overlap on I/O. *Expected:* another ~1.5–2×.

5. **COPY-into-staging write** — `COPY` into a staging table then one
   `INSERT … SELECT … ON CONFLICT`. *Expected:* ~1.4× on the 5M-row write.

**Recommendation:** #1 is the correct long-term answer (removes the re-read with
no memory trade-off). For a fast measured win now, **#2 + #3 + #4** combined
should take ~33 min toward **~10–12 min**, at the cost of higher RAM during the
run. Either path should be re-measured against the live DB after implementing.

---

## 10. How to run

```bash
python main.py          # CLI run; prints the RunMetrics latency breakdown
python app.py           # web console at http://127.0.0.1:5000 (auto-picks a free port)
pytest -q               # 75 unit tests (network-free)
pytest -q -m integration   # opt-in K=1 vs K=2 equivalence (needs the live DB)
```

DB credentials come from `.env` (git-ignored). Saved console connections persist
in `connections.json` (git-ignored). Optional accelerator: `pip install lxml`.
