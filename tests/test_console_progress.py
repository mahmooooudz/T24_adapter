"""
Tests for the console progress phases (Fix #1 "Saving…" + Fix #2 live write).

Network-free: a fake RunState captures emitted SSE events. We assert the read
wrapper tags events phase="read" and the write emitter tags them
status="saving"/phase="write" — the data the two-segment bar relies on.
"""

import threading

import app
from conftest import nf


class FakeRun:
    def __init__(self):
        self.apps = {}
        self.events = []
        self.stop = threading.Event()

    def emit(self, kind, **data):
        self.events.append((kind, data))

    def log(self, *a, **k):
        pass


def _progress_events(run):
    return [d for (k, d) in run.events if k == "progress"]


# ---------------------------------------------------------------------------
# Fix #2 — write-progress emitter tags status="saving", phase="write"
# ---------------------------------------------------------------------------

def test_write_progress_emitter_tags_saving_write_phase():
    run = FakeRun()
    cb = app._write_progress_emitter(run, {"ACCOUNT": 10000})
    cb("ACCOUNT", 3000)
    cb("ACCOUNT", 10000)

    evs = _progress_events(run)
    assert evs[0] == {"app": "ACCOUNT", "processed": 3000, "total": 10000,
                      "status": "saving", "phase": "write"}
    assert evs[1]["processed"] == 10000 and evs[1]["status"] == "saving"
    assert run.apps["ACCOUNT"]["status"] == "saving"


def test_write_progress_emitter_uppercases_app():
    run = FakeRun()
    cb = app._write_progress_emitter(run, {"ACCOUNT": 5})
    cb("account", 2)               # writer may pass any case
    assert _progress_events(run)[0]["app"] == "ACCOUNT"


# ---------------------------------------------------------------------------
# Fix #1 — read wrapper tags phase="read" and passes fields through unchanged
# ---------------------------------------------------------------------------

def test_streaming_wrapper_tags_read_phase_and_is_passthrough():
    run = FakeRun()
    run.apps["ACCOUNT"] = {"total": 3, "processed": 0, "status": "pending"}
    stats = {}
    totals = {"ACCOUNT": 3}
    wrap = app._streaming_progress_wrapper(run, totals, stats)

    fields = [
        nf("ACCOUNT", "r1", "1", "KEY", "r1"),
        nf("ACCOUNT", "r2", "1", "KEY", "r2"),
        nf("ACCOUNT", "r3", "1", "KEY", "r3"),
    ]
    out = list(wrap(iter(fields)))

    assert out == fields                                   # passthrough, unchanged
    phases = {e.get("phase") for e in _progress_events(run)}
    assert phases == {"read"}                              # never "write"
    assert all(e["status"] == "running" for e in _progress_events(run))
    assert stats["ACCOUNT"]["records"] == 3               # stats accumulated


def test_streaming_wrapper_honours_stop_flag():
    run = FakeRun()
    run.stop.set()                                         # user pressed stop
    wrap = app._streaming_progress_wrapper(run, {"ACCOUNT": 1}, {})
    import pytest
    with pytest.raises(app._Stopped):
        list(wrap(iter([nf("ACCOUNT", "r1", "1", "KEY", "r1")])))


# ---------------------------------------------------------------------------
# The two phases never collide: read tops out before write begins
# ---------------------------------------------------------------------------

def test_read_then_write_phase_sequence():
    """Read events (phase=read) then write events (phase=write) — the bar's
    two segments fill in order, never backwards."""
    run = FakeRun()
    run.apps["ACCOUNT"] = {"total": 2, "processed": 0, "status": "pending"}
    stats = {}
    wrap = app._streaming_progress_wrapper(run, {"ACCOUNT": 2}, stats)
    list(wrap(iter([nf("ACCOUNT", "r1", "1", "KEY", "r1"),
                    nf("ACCOUNT", "r2", "1", "KEY", "r2")])))
    on_write = app._write_progress_emitter(run, {"ACCOUNT": 2})
    on_write("ACCOUNT", 2)

    phases = [e.get("phase") for e in _progress_events(run)]
    # All read events come before all write events.
    assert "read" in phases and "write" in phases
    assert phases.index("write") == len(phases) - 1
    assert phases[: phases.index("write")] == ["read"] * phases.index("write")
