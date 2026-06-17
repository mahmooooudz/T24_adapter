# Latency Reduction — Implementation Plan (Levers 1–4)

**Goal:** ~6.2 s → ~2 s cold (and lower warm). The bottleneck is *not* network
(~0.7 s of round-trips); it is (a) parse+normalize done **twice** (two-pass) and
(b) ~2.1 s of per-run fixed overhead (metadata + discovery + DDL).

Each lever ships with **debugging** (metrics spans / debug logs so the gain is
visible in the `RunMetrics` report) and **test units** (correctness gates,
network-free where possible).

---

## Lever 1 — Single-pass, in-memory-buffered pivot (adaptive)  🔴 biggest win

**Idea.** For tables that fit in memory, read+parse+normalize **once**: buffer
each app's records compactly while computing the exact column shape in the same
pass, then bulk-write from the buffer. Records arrive app-grouped and
record-contiguous, so the buffer holds **one app at a time**.

**Lossless?** Yes — the full app is seen before any row is written, so
`NAME_1..N` is sized to the true max. Output is byte-identical to two-pass.

**Memory guard (adaptive).** Per-app buffer cap (`db_single_pass_max_rows`,
default 200 000). If an app exceeds it, its schema is still completed (cheap),
its buffered data is dropped, and that app alone falls back to the existing
two-pass streaming re-read. Small/normal tables → one read; huge tables → safe.

**Code:**
- `wide_writer.py`: new `stream_buffer_and_schema(rows, max_buffer_rows)` →
  `(schemas, buffers, overflow)`; helper `_wide_row_from_buffer(...)`.
- `db_writer.py`: split current body into `_write_two_pass`; add single-pass
  path (default). Console path (passes pre-discovered `schemas=`) keeps two-pass.
- `config.py` / `settings.py`: `db_single_pass=True`, `db_single_pass_max_rows`.
- `metrics.py`: new phase `pass_single`.

**Debugging:** `metrics.span("pass_single")`; debug log of buffered vs overflow
apps and buffered row counts.

**Tests (network-free):**
- *Equivalence*: single-pass buffered wide rows == two-pass `iter_all_wide_rows`
  for multi-app, repeating, padded, special-char streams.
- *Overflow fallback*: with `max_buffer_rows=1`, the app is flagged overflow and
  the schema is still correct.

**Expected:** −~1.8 s (removes 2nd read+parse+normalize+discovery).

---

## Lever 2 — Bulk metadata prefetch + slim DDL  🔴 kills fixed overhead

**Idea.** Metadata tables hold one tiny row per app. Replace 6 lazy per-app
fetches with **one `SELECT *` per metadata table** (3 round-trips total,
regardless of app count), prefetched once. Merge the DDL "table exists?" +
"columns?" probes into a single `information_schema.columns` query.

**Code:**
- `db_reader.py`: `T24DatabaseMetadataReader.fetch_all()` → `{key: xml}`.
- `pipeline.py` (`T24MetadataOrchestrator`): lazy `_prefetch()` of all three
  metadata maps; `_load_*` read from the maps instead of `fetch_xml(app)`.
- `db_writer.py` (`_ensure_table`): one columns query decides create-vs-alter.

**Debugging:** existing `metadata_roundtrips` counter (should drop 6 → 3);
`ddl` span should shrink.

**Tests (network-free):** orchestrator prefetch uses a fake reader exposing
`fetch_all`; assert exactly one `fetch_all` per table and zero per-app
`fetch_xml`. DDL SQL composition unchanged (existing tests still pass).

**Expected:** −~0.7 s (metadata) + −~0.3 s (DDL).

---

## Lever 3 — Optional fast XML backend (lxml)  🟡 utility replacement

**Idea.** `xml.etree.ElementTree.fromstring` per record is a real CPU chunk.
Use `lxml` when installed (3–5× faster parse), else fall back to ElementTree —
**no hard dependency**. Combined with Lever 1, parsing happens once.

**Code:**
- New `t24_adapter/_xml_backend.py`: selects `lxml.etree` or `ElementTree`,
  exposes `fromstring`, `iterparse`, and a unified `XMLParseError`.
- `db_reader.py` / `reader.py`: parse via the backend; catch `XMLParseError`.
- `requirements.txt`: `lxml` as an optional accelerator (commented).

**Debugging:** log once at startup which backend is active.

**Tests (network-free):** backend exposes `fromstring`/`XMLParseError`; parsing
a known T24 `<DATA><row>…</row></DATA>` yields the same normalized fields under
whichever backend is active (and both, when lxml is present).

**Expected:** −~0.5–0.8 s (only when lxml installed).

---

## Lever 4 — Bigger write batch  🟢 micro

`db_batch_size` 500 → 1000: one flush per 1 000 rows (fewer round-trips).

**Tests:** existing `test_db_writer` batch-size assertions updated.

**Expected:** −~0.3 s.

---

## Rollout & verification

Order: 1 → 2 → 4 → 3. After each, run `python main.py` and read the
`RunMetrics` breakdown (before/after per step), plus the full `pytest` suite.
Targets: **~2.5–3 s** after 1+2+4, **~2 s** with lxml, byte-identical output.

`db_single_pass=False` (config) reverts to the verified two-pass path at any time.

---

## Results (measured, 2×1k rows, remote DB ~70 ms RTT)

| Stage | Total | DB round-trips | Notes |
|---|---|---|---|
| Baseline (start of this round) | ~6.19 s | 10 | two-pass, server-side cursor |
| + Lever 1 (single-pass) + Lever 4 (batch 1000) | ~5.54 s | 8 | 2nd read removed |
| + Lever 2 (bulk metadata + slim DDL) | ~4.87 s | 5 | metadata 6→3 RT |
| + Lever 3 (lxml) + `__slots__` | ~4.17 s | 5 | faster parse/alloc |
| + Lever 5 (adaptive plain-cursor read) | **~3.9 s** | 5 | plain read for tables that fit |

**Net: ~6.2 s → ~3.9 s (~1.6×), byte-identical output, 28 tests passing.**

### Bonus levers added during measurement
- **Lever 5 — adaptive read.** Tables estimated (via `pg_class.reltuples`, no
  scan) at/under `db_single_pass_max_rows` are read with a PLAIN client-side
  cursor (one round-trip) instead of a server-side streaming cursor (~2× faster
  in isolation). Larger tables still stream server-side (constant memory).
- **`__slots__`** on `NormalizedField` (the high-volume hot object).

### Why not 1–2 s (yet)
After these levers the run is no longer dominated by any single cost — it is
spread across irreducible per-run work against a 70 ms-RTT remote DB:
metadata (~0.68 s) + discovery (~0.25 s) + DDL (~0.40 s) + sweep (~0.14 s) +
bulk write (~0.66 s) + read+normalize of ~31k fields (~1.5 s). Reaching ~2–2.5 s
needs a **warm cross-run cache** (skip discovery/DDL/metadata on repeat runs —
DDL/discovery caching is safe; metadata caching needs a staleness flag).
Sub-2 s on a *cold* run is bounded by network + the per-record normalize.
