"""
Tests for the speculative data prefetch (background read during the wizard).

Network-free: exercises the cache key, the stats accumulator, and the
get/join/staleness logic by driving the PREFETCH dict directly.
"""

import threading
import time

import app
from conftest import nf


class _Cfg:
    db_schema = "t24_adaptor"
    db_metadata_table = "STANDARD_SELECTION"
    db_local_ref_table = "LOCAL_REFERENCE"
    db_customization_table = "CUSTOMIZATION"


def _set_env(monkeypatch):
    monkeypatch.setenv("DB_HOST", "h"); monkeypatch.setenv("DB_PORT", "1")
    monkeypatch.setenv("DB_NAME", "db"); monkeypatch.setenv("DB_USER", "u")
    monkeypatch.setenv("DB_PASSWORD", "SECRET")


# ---------------------------------------------------------------------------
# Cache key
# ---------------------------------------------------------------------------

def test_prefetch_key_is_table_order_independent(monkeypatch):
    _set_env(monkeypatch)
    k1 = app._prefetch_key(_Cfg(), ["ACCOUNT", "CUSTOMER"])
    k2 = app._prefetch_key(_Cfg(), ["customer", "account"])  # different order/case
    assert k1 == k2


def test_prefetch_key_changes_with_selection_and_hides_password(monkeypatch):
    _set_env(monkeypatch)
    k_two = app._prefetch_key(_Cfg(), ["ACCOUNT", "CUSTOMER"])
    k_one = app._prefetch_key(_Cfg(), ["ACCOUNT"])
    assert k_two != k_one                       # selection change -> new key
    assert "SECRET" not in k_two                # password never in the key


# ---------------------------------------------------------------------------
# Stats accumulator (passthrough + tally)
# ---------------------------------------------------------------------------

def test_stats_accumulator_counts_and_passes_through():
    stats = {}
    fields = [
        nf("ACCOUNT", "r1", "1", "KEY", "r1"),
        nf("ACCOUNT", "r1", "2", "X", "v", mapped=True),
        nf("ACCOUNT", "r2", "1", "KEY", "r2", mapped=False),  # unmapped
    ]
    fields[2].warnings = ["UNMAPPED_FIELD"]
    out = list(app._stats_accumulator(iter(fields), stats))
    assert out == fields                        # passthrough unchanged
    s = stats["ACCOUNT"]
    assert s["records"] == 2 and s["fields"] == 3
    assert s["mapped"] == 2 and s["warnings"] == 1


# ---------------------------------------------------------------------------
# get / staleness / join
# ---------------------------------------------------------------------------

def test_get_prefetch_ready_fresh():
    key = "pf-ready"
    with app._PREFETCH_LOCK:
        app.PREFETCH[key] = {"status": "ready", "schemas": {}, "buffers": {},
                             "stats": {}, "ts": time.time(), "event": threading.Event()}
    try:
        assert app._get_prefetch(key) is not None
    finally:
        app.PREFETCH.pop(key, None)


def test_get_prefetch_stale_and_error_and_absent():
    with app._PREFETCH_LOCK:
        app.PREFETCH["pf-stale"] = {"status": "ready", "ts": time.time() - app.PREFETCH_TTL - 1,
                                    "event": threading.Event()}
        app.PREFETCH["pf-err"] = {"status": "error", "ts": time.time(),
                                  "event": threading.Event()}
    try:
        assert app._get_prefetch("pf-stale") is None
        assert app._get_prefetch("pf-err") is None
        assert app._get_prefetch("pf-absent") is None
    finally:
        app.PREFETCH.pop("pf-stale", None); app.PREFETCH.pop("pf-err", None)


def test_get_prefetch_joins_in_flight_read():
    """A run that arrives while the prefetch is still reading should WAIT for it
    (join), not return None and re-read."""
    key = "pf-joining"
    ev = threading.Event()
    with app._PREFETCH_LOCK:
        app.PREFETCH[key] = {"status": "reading", "ts": time.time(), "event": ev}

    def finish():
        time.sleep(0.2)
        with app._PREFETCH_LOCK:
            app.PREFETCH[key] = {"status": "ready", "schemas": {}, "buffers": {},
                                 "stats": {}, "ts": time.time(), "event": ev}
        ev.set()

    t = threading.Thread(target=finish); t.start()
    try:
        result = app._get_prefetch(key, wait_timeout=2.0)   # should block ~0.2s then succeed
        assert result is not None and result["status"] == "ready"
    finally:
        t.join(); app.PREFETCH.pop(key, None)


def test_get_prefetch_join_times_out_gracefully():
    key = "pf-timeout"
    with app._PREFETCH_LOCK:
        app.PREFETCH[key] = {"status": "reading", "ts": time.time(), "event": threading.Event()}
    try:
        # Never gets set; short timeout -> falls back to None (run reads normally).
        assert app._get_prefetch(key, wait_timeout=0.1) is None
    finally:
        app.PREFETCH.pop(key, None)


def test_files_sink_writes_from_buffers(tmp_path):
    """Regression for the CSV-ignores-prefetch bug: the shared files writer
    must produce correct CSV directly from prefetched buffers (no re-read)."""
    from t24_adapter.wide_writer import WidePivotWriter
    from conftest import FakePipeline

    class _Run:
        def __init__(self, outdir):
            self.output_dir = outdir
            self.apps = {"A": {"total": 2}}
            self.results = {}
            self.events = []
            self.stop = threading.Event()
        def emit(self, kind, **d): self.events.append((kind, d))
        def log(self, *a, **k): pass

    fields = [
        nf("A", "r1", "1", "KEY", "r1"), nf("A", "r1", "2", "NAME", "Alice"),
        nf("A", "r2", "1", "KEY", "r2"), nf("A", "r2", "2", "NAME", "Bob"),
    ]
    pivot = WidePivotWriter()
    schemas, buffers, overflow = pivot.stream_buffer_and_schema(FakePipeline(fields).run())

    run = _Run(tmp_path)
    app._write_buffers_to_files(
        run, ["A"], {"A": 2},
        {"A": {"records": 2, "fields": 4, "mapped": 4, "warnings": 0}},
        pivot, schemas, buffers, overflow, "csv",
    )
    out = tmp_path / "A_wide.csv"
    assert out.exists()
    text = out.read_text()
    assert "KEY" in text and "NAME" in text          # header
    assert "Alice" in text and "Bob" in text          # both records written
    assert run.results["A"]["records"] == 2
    # write-phase progress + a final done event were emitted
    assert any(e[1].get("phase") == "done" for e in run.events if e[0] == "progress")


def test_trigger_prefetch_dedupes(monkeypatch):
    spawned = []
    class _FakeThread:
        def __init__(self, target=None, args=(), daemon=None): spawned.append(args)
        def start(self): pass
    monkeypatch.setattr(app.threading, "Thread", _FakeThread)
    _set_env(monkeypatch)
    key = app._prefetch_key(_Cfg(), ["ACCOUNT"])
    app.PREFETCH.pop(key, None)
    try:
        app._trigger_prefetch(_Cfg(), ["ACCOUNT"])
        assert app.PREFETCH[key]["status"] == "reading"
        assert len(spawned) == 1
        app._trigger_prefetch(_Cfg(), ["ACCOUNT"])   # already in flight
        assert len(spawned) == 1                     # no duplicate read
    finally:
        app.PREFETCH.pop(key, None)
