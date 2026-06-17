"""
Tests for RunMetrics (Lever T1) — thread-safety + per-app phase aggregation.

These run network-free. They lock in the contract that future parallel
execution depends on:

- concurrent incr/add from many threads → no torn updates
- span(name, app=X) goes under a per-app sub-key
- the reporter aggregates parallel phases as MAX (not SUM), so the line items
  add up coherently against wall-clock under parallel execution
- legacy sequential calls (no `app=`) still work the same way
"""

import threading
import time
import logging
from io import StringIO

from t24_adapter.metrics import RunMetrics


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------

def test_incr_is_atomic_under_64_threads():
    m = RunMetrics()
    N_THREADS = 64
    N_INCR = 1000

    def hammer():
        for _ in range(N_INCR):
            m.incr("db_roundtrips")

    threads = [threading.Thread(target=hammer) for _ in range(N_THREADS)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert m.count("db_roundtrips") == N_THREADS * N_INCR  # no torn updates


def test_add_is_atomic_under_threads():
    m = RunMetrics()

    def hammer():
        for _ in range(500):
            m.add("ddl", 0.001)

    threads = [threading.Thread(target=hammer) for _ in range(16)]
    for t in threads: t.start()
    for t in threads: t.join()

    # 16 * 500 * 0.001 = 8.0 — allow tiny float jitter
    assert abs(m.get("ddl") - 8.0) < 1e-6


# ---------------------------------------------------------------------------
# Per-app spans + MAX aggregation for parallel phases
# ---------------------------------------------------------------------------

def test_span_with_app_stores_under_per_app_key():
    m = RunMetrics()
    with m.span("pass_single", app="CUSTOMER"):
        time.sleep(0.01)
    # The reporter treats this as a parallel phase; the per-app value is fetchable.
    assert m.get("pass_single", app="CUSTOMER") >= 0.01
    # And the un-tagged key is untouched (legacy callers wouldn't see this).
    assert m.get("pass_single") == 0.0


def test_legacy_span_without_app_still_works():
    m = RunMetrics()
    with m.span("pass2_write"):
        time.sleep(0.005)
    assert m.get("pass2_write") >= 0.005


def test_phase_with_concurrent_apps_reports_max_not_sum():
    """Two workers each timing pass_single for ~80ms in parallel. The reporter
    must show ~80ms for the phase (MAX), not ~160ms (SUM)."""
    m = RunMetrics()

    def work(app):
        with m.span("pass_single", app=app):
            time.sleep(0.08)

    t0 = time.perf_counter()
    threads = [threading.Thread(target=work, args=(a,)) for a in ("A", "B")]
    for t in threads: t.start()
    for t in threads: t.join()
    wall = time.perf_counter() - t0

    # Sanity: workers really overlapped (wall ~= max, not ~= sum).
    assert wall < 0.16, f"workers ran serially: wall={wall:.3f}"

    aggregated, per_app = m._phase_time(dict(m._spans), "pass_single")
    assert set(per_app) == {"A", "B"}
    # MAX aggregation: matches the slower of the two, not their sum.
    expected_max = max(per_app.values())
    assert abs(aggregated - expected_max) < 1e-9
    assert aggregated < 0.16   # NOT ~0.16 (that would be SUM-aggregated)


def test_phase_mixes_legacy_and_per_app_taking_max():
    m = RunMetrics()
    m.add("pass_single", 0.05)                       # legacy sequential
    m.add("pass_single", 0.20, app="CUSTOMER")       # parallel worker A
    m.add("pass_single", 0.30, app="ACCOUNT")        # parallel worker B
    agg, per_app = m._phase_time(dict(m._spans), "pass_single")
    # MAX(legacy=0.05, max(per_app)=0.30) = 0.30
    assert abs(agg - 0.30) < 1e-9
    assert per_app == {"CUSTOMER": 0.20, "ACCOUNT": 0.30}


# ---------------------------------------------------------------------------
# Component aggregation = SUM (diagnostic totals across tables)
# ---------------------------------------------------------------------------

def test_component_aggregated_as_sum_across_apps():
    """DDL is a per-app cost; total DDL time across the run = sum of per-app
    DDL times. Components aggregate as SUM, in contrast to phases (MAX)."""
    m = RunMetrics()
    m.add("ddl", 0.10, app="A")
    m.add("ddl", 0.20, app="B")
    assert abs(m._component_time(dict(m._spans), "ddl") - 0.30) < 1e-9


# ---------------------------------------------------------------------------
# Reporter output — smoke check that parallel detail surfaces
# ---------------------------------------------------------------------------

def test_report_surfaces_parallel_detail():
    m = RunMetrics()
    m.add("pass_single", 0.20, app="CUSTOMER")
    m.add("pass_single", 0.30, app="ACCOUNT")
    m.incr("db_roundtrips", 5)

    buf = StringIO()
    handler = logging.StreamHandler(buf)
    logger = logging.getLogger("metrics_test")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    m.report(logger)
    text = buf.getvalue()

    assert "Parallel detail" in text
    assert "[CUSTOMER]" in text and "[ACCOUNT]" in text
    assert "DB round-trips this run: 5" in text
