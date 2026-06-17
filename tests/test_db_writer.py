"""
Tests for the database sink logic that can run without a database:
- the full-sync delete-sweep guard (_should_sweep)
- the streaming/batching flush-size selection

The SQL/DDL path (UPSERT, CREATE, ALTER, DELETE) requires a live PostgreSQL
connection; it is exercised by the opt-in integration test and was validated
end-to-end manually. Here we lock down the pure, dangerous-if-wrong logic:
when the destructive sweep is allowed to run.
"""

from t24_adapter.db_writer import WideDatabaseWriter


def _w(**kw):
    base = dict(schema="s", full_sync=True, filters_active=False)
    base.update(kw)
    return WideDatabaseWriter(**base)


def test_sweep_allowed_on_full_clean_unfiltered_run():
    assert _w()._should_sweep("t", {"k1", "k2"}) is True


def test_sweep_blocked_when_full_sync_off():
    assert _w(full_sync=False)._should_sweep("t", {"k1"}) is False


def test_sweep_blocked_when_filters_active():
    # A partial/filtered run must never delete rows that were merely filtered out.
    assert _w(filters_active=True)._should_sweep("t", {"k1"}) is False


def test_sweep_blocked_on_empty_run():
    # Zero records seen must NOT wipe the whole table (glitch safety).
    assert _w()._should_sweep("t", set()) is False


def test_both_modes_bulk_upsert_never_per_row():
    # Regression guard: "streaming" must NOT collapse to one row per round-trip
    # (the old batch=1 behaviour was ~one network RTT per row on a remote DB).
    assert _w(write_mode="streaming", batch_size=500).batch_size == 500
    assert _w(write_mode="batching", batch_size=250).batch_size == 250
    # Streaming with no explicit size falls back to a sane bulk default, not 1.
    assert _w(write_mode="streaming", batch_size=0).batch_size > 1


def test_batch_size_floor_is_one():
    # Defensive: a negative/zero batching size never produces a non-positive flush.
    assert _w(write_mode="batching", batch_size=0).batch_size == 1


def test_single_pass_defaults_on_with_cap():
    w = _w()
    assert w.single_pass is True
    assert w.single_pass_max_rows == 200_000


def test_single_pass_can_be_disabled():
    assert _w(single_pass=False).single_pass is False
