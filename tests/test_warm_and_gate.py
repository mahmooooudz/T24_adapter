"""
Tests for connection/metadata warming (#1) and the parallel auto-gate (#4).

Network-free: warm helpers are exercised by manipulating the WARM dict and a
stub cfg; the gate is a pure function.
"""

import time
import os

import app


# ---------------------------------------------------------------------------
# #4 — parallel auto-gate
# ---------------------------------------------------------------------------

def test_gate_off_when_single_worker():
    assert app._should_parallelize(1, 10, 3) is False


def test_gate_off_below_min_tables():
    assert app._should_parallelize(2, 2, 3) is False   # the reported case
    assert app._should_parallelize(4, 1, 3) is False


def test_gate_on_at_or_above_min_tables():
    assert app._should_parallelize(2, 3, 3) is True
    assert app._should_parallelize(4, 20, 3) is True


# ---------------------------------------------------------------------------
# #1 — warm signature (no secrets) + freshness
# ---------------------------------------------------------------------------

class _Cfg:
    db_schema = "t24_adaptor"
    db_metadata_table = "STANDARD_SELECTION"
    db_local_ref_table = "LOCAL_REFERENCE"
    db_customization_table = "CUSTOMIZATION"


def test_signature_is_stable_and_excludes_password(monkeypatch):
    monkeypatch.setenv("DB_HOST", "h"); monkeypatch.setenv("DB_PORT", "1")
    monkeypatch.setenv("DB_NAME", "db"); monkeypatch.setenv("DB_USER", "u")
    monkeypatch.setenv("DB_PASSWORD", "SECRET")
    sig1 = app._warm_signature(_Cfg())
    # Changing ONLY the password must not change the signature.
    monkeypatch.setenv("DB_PASSWORD", "DIFFERENT")
    sig2 = app._warm_signature(_Cfg())
    assert sig1 == sig2
    assert "SECRET" not in sig1 and "DIFFERENT" not in sig1
    assert "t24_adaptor" in sig1 and "h" in sig1


def test_get_warm_returns_ready_fresh_entry():
    sig = "sig-ready"
    with app._WARM_LOCK:
        app.WARM[sig] = {"status": "ready", "registries": {}, "counts": {},
                         "apps": [], "ts": time.time()}
    try:
        assert app._get_warm(sig) is not None
    finally:
        app.WARM.pop(sig, None)


def test_get_warm_ignores_stale_entry():
    sig = "sig-stale"
    with app._WARM_LOCK:
        app.WARM[sig] = {"status": "ready", "ts": time.time() - app.WARM_TTL - 1}
    try:
        assert app._get_warm(sig) is None       # expired
    finally:
        app.WARM.pop(sig, None)


def test_get_warm_ignores_warming_or_error():
    for st in ("warming", "error"):
        sig = f"sig-{st}"
        with app._WARM_LOCK:
            app.WARM[sig] = {"status": st, "ts": time.time()}
        try:
            assert app._get_warm(sig) is None
        finally:
            app.WARM.pop(sig, None)


def test_trigger_warm_marks_warming_and_does_not_double_fire(monkeypatch):
    started = []
    # Replace the real background thread with a recorder so no DB is touched.
    class _FakeThread:
        def __init__(self, target=None, args=(), daemon=None):
            started.append(args)
        def start(self): pass
    monkeypatch.setattr(app.threading, "Thread", _FakeThread)
    monkeypatch.setenv("DB_HOST", "h"); monkeypatch.setenv("DB_PORT", "1")
    monkeypatch.setenv("DB_NAME", "db"); monkeypatch.setenv("DB_USER", "u")

    sig = app._warm_signature(_Cfg())
    app.WARM.pop(sig, None)
    try:
        app._trigger_warm(_Cfg())
        assert app.WARM[sig]["status"] == "warming"
        assert len(started) == 1
        # A second trigger while "warming" is fresh must NOT spawn again.
        app._trigger_warm(_Cfg())
        assert len(started) == 1
    finally:
        app.WARM.pop(sig, None)
