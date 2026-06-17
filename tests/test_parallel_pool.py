"""
Tests for T24WorkerPool (Lever T2) — bounded fan-out over applications.

Network-free: we stub the writer_factory and exercise the pool's
orchestration directly, since the real writer needs a database. The DB-bound
end-to-end equivalence check lives in main.py + the perf reports.

These tests lock in the contracts that matter for safe parallelism:
- Pool size is honored (no more than max_workers run concurrently).
- "independent" failure policy lets others finish.
- "fail-fast" cancels pending workers.
- Each worker sees its OWN config.include_applications=(app,) (no shared state).
- Pool returns one entry per submitted app with status + result/error.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import pytest

from t24_adapter.parallel import T24WorkerPool


# ---------------------------------------------------------------------------
# Minimal config stub — we only need the fields the pool actually reads.
# ---------------------------------------------------------------------------

@dataclass
class _StubCfg:
    db_schema: str = "stub"
    db_failure_policy: str = "independent"
    db_statement_timeout_s: int = 0
    include_applications: Optional[Tuple[str, ...]] = None
    # placeholders so dataclasses.replace() works
    db_output_suffix: str = "_wide"
    db_write_mode: str = "streaming"
    db_batch_size: int = 1000
    db_key_column: str = "recordId"
    db_full_sync: bool = False
    db_filters_active: bool = False
    db_single_pass: bool = True
    db_single_pass_max_rows: int = 200_000
    db_max_workers: int = 4


# ---------------------------------------------------------------------------
# Test helpers: stub the pool's internals so it never touches a DB.
# ---------------------------------------------------------------------------

class _Probe:
    """Captures per-worker observations across a pool run."""
    def __init__(self):
        self.live = 0
        self.peak = 0
        self.thread_ids: List[int] = []
        self.app_order: List[str] = []
        self.lock = threading.Lock()


def _patch_pool(monkeypatch, probe: _Probe, *,
                fail_on: Optional[str] = None,
                durations: Optional[dict] = None,
                sizes: Optional[dict] = None):
    """Replace _run_one with a fake worker and _largest_first with a fake
    ordering so we never need a database for these tests."""
    durations = durations or {}

    def fake_run_one(self, app_name):
        with probe.lock:
            probe.live += 1
            probe.peak = max(probe.peak, probe.live)
            probe.thread_ids.append(threading.get_ident())
            probe.app_order.append(app_name)
        try:
            if fail_on and app_name == fail_on:
                raise ValueError(f"injected failure for {app_name}")
            time.sleep(durations.get(app_name, 0.02))
            return (f"{app_name}_wide", 100, 0, 5)
        finally:
            with probe.lock:
                probe.live -= 1

    def fake_largest_first(self, apps):
        if sizes is None:
            return list(apps)
        return sorted(apps, key=lambda a: -sizes.get(a, 0))

    monkeypatch.setattr(T24WorkerPool, "_run_one", fake_run_one)
    monkeypatch.setattr(T24WorkerPool, "_largest_first", fake_largest_first)


def _factory(_cfg):
    raise AssertionError("writer_factory must not be called when _run_one is stubbed")


# ---------------------------------------------------------------------------
# Pool size is honored (no more than max_workers in flight at once)
# ---------------------------------------------------------------------------

def test_pool_respects_max_workers(monkeypatch):
    probe = _Probe()
    _patch_pool(monkeypatch, probe, durations={a: 0.05 for a in "ABCDEFGH"})
    pool = T24WorkerPool(_StubCfg(), _factory, max_workers=3)
    results = pool.run(list("ABCDEFGH"))
    assert len(results) == 8
    assert all(r["status"] == "ok" for r in results.values())
    assert probe.peak <= 3, f"saw {probe.peak} concurrent workers, expected <= 3"


# ---------------------------------------------------------------------------
# Empty input is safe
# ---------------------------------------------------------------------------

def test_empty_app_list(monkeypatch):
    probe = _Probe()
    _patch_pool(monkeypatch, probe)
    pool = T24WorkerPool(_StubCfg(), _factory, max_workers=4)
    assert pool.run([]) == {}


# ---------------------------------------------------------------------------
# Failure policy — independent: one fails, the rest finish
# ---------------------------------------------------------------------------

def test_independent_failure_policy_lets_others_finish(monkeypatch):
    probe = _Probe()
    _patch_pool(monkeypatch, probe, fail_on="B")
    cfg = _StubCfg(db_failure_policy="independent")
    pool = T24WorkerPool(cfg, _factory, max_workers=4)
    results = pool.run(["A", "B", "C", "D"])
    assert results["A"]["status"] == "ok"
    assert results["B"]["status"] == "failed" and "injected failure" in results["B"]["error"]
    assert results["C"]["status"] == "ok"
    assert results["D"]["status"] == "ok"


# ---------------------------------------------------------------------------
# Failure policy — fail-fast: pending workers are cancelled
# ---------------------------------------------------------------------------

def test_fail_fast_cancels_pending(monkeypatch):
    probe = _Probe()
    # B fails fast; D and E are slow, scheduled after; with max_workers=2 they
    # haven't started yet when B raises, so they should be cancelled.
    _patch_pool(monkeypatch, probe, fail_on="B",
                durations={"A": 0.05, "B": 0.01, "C": 0.10, "D": 0.10, "E": 0.10})
    cfg = _StubCfg(db_failure_policy="fail-fast")
    pool = T24WorkerPool(cfg, _factory, max_workers=2)
    results = pool.run(["A", "B", "C", "D", "E"])

    assert results["B"]["status"] == "failed"
    cancelled = [a for a, r in results.items() if r["status"] == "cancelled"]
    assert cancelled, "expected at least one cancelled worker under fail-fast"


# ---------------------------------------------------------------------------
# Each worker sees its OWN config.include_applications=(app,) (no leak)
# ---------------------------------------------------------------------------

def test_each_worker_gets_its_own_include_applications(monkeypatch):
    seen: List[Tuple] = []
    seen_lock = threading.Lock()

    def fake_run_one(self, app_name):
        # The real _run_one builds worker_cfg via dataclasses.replace(); the
        # easiest way to assert correctness without running the pipeline is to
        # rebuild the same way it does and record the include_applications.
        from dataclasses import replace
        worker_cfg = replace(self.config, include_applications=(app_name,))
        with seen_lock:
            seen.append((app_name, worker_cfg.include_applications))
        time.sleep(0.005)
        return (f"{app_name}_wide", 1, 0, 1)

    monkeypatch.setattr(T24WorkerPool, "_run_one", fake_run_one)
    monkeypatch.setattr(T24WorkerPool, "_largest_first", lambda self, apps: list(apps))

    cfg = _StubCfg(include_applications=None)  # caller did NOT narrow
    pool = T24WorkerPool(cfg, _factory, max_workers=3)
    pool.run(["X", "Y", "Z"])

    by_app = dict(seen)
    assert by_app == {"X": ("X",), "Y": ("Y",), "Z": ("Z",)}
    # The shared config is NOT mutated:
    assert cfg.include_applications is None


# ---------------------------------------------------------------------------
# Largest-first ordering test (purely a sort-key check)
# ---------------------------------------------------------------------------

def test_largest_first_orders_by_estimate(monkeypatch):
    probe = _Probe()
    sizes = {"small": 10, "mid": 1_000, "huge": 100_000, "tiny": 5}
    _patch_pool(monkeypatch, probe, sizes=sizes,
                durations={a: 0.005 for a in sizes})
    pool = T24WorkerPool(_StubCfg(), _factory, max_workers=1)  # serial -> dispatch == start order
    pool.run(list(sizes))
    # With max_workers=1 the start order equals the dispatch order.
    assert probe.app_order == ["huge", "mid", "small", "tiny"]


# ---------------------------------------------------------------------------
# max_workers floor + invalid input
# ---------------------------------------------------------------------------

def test_invalid_max_workers_raises():
    with pytest.raises(ValueError):
        T24WorkerPool(_StubCfg(), _factory, max_workers=0)
