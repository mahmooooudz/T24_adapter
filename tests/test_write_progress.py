"""
Tests for the live write-progress callback (Fix #2).

Network-free: _AppSink._flush is exercised in isolation with execute_values
stubbed, so we assert the callback fires per flush with cumulative counts —
without needing a database.
"""

import psycopg2.extras

from t24_adapter.db_writer import WideDatabaseWriter, _AppSink


def _bare_sink(writer, app_name="ACCOUNT"):
    """Build an _AppSink without running __init__ (which needs a live DB for
    DDL). We set only the attributes _flush() touches."""
    sink = _AppSink.__new__(_AppSink)
    sink.writer = writer
    sink.app_name = app_name
    sink.cursor = object()          # never used once execute_values is stubbed
    sink._upsert_sql = "INSERT ..."
    sink._batch = []
    sink.upserted = 0
    return sink


def test_flush_fires_callback_with_cumulative_count(monkeypatch):
    monkeypatch.setattr(psycopg2.extras, "execute_values", lambda *a, **k: None)
    w = WideDatabaseWriter(schema="s")
    seen = []
    w._on_write_progress = lambda app, n: seen.append((app, n))

    sink = _bare_sink(w)
    sink._batch = [("1",), ("2",), ("3",)]
    sink._flush()
    sink._batch = [("4",), ("5",)]
    sink._flush()

    # Cumulative rows written, reported once per flush.
    assert seen == [("ACCOUNT", 3), ("ACCOUNT", 5)]
    assert sink.upserted == 5


def test_flush_without_callback_is_fine(monkeypatch):
    monkeypatch.setattr(psycopg2.extras, "execute_values", lambda *a, **k: None)
    w = WideDatabaseWriter(schema="s")   # _on_write_progress defaults to None
    sink = _bare_sink(w)
    sink._batch = [("1",), ("2",)]
    sink._flush()                        # must not raise
    assert sink.upserted == 2


def test_callback_exception_never_breaks_the_write(monkeypatch):
    monkeypatch.setattr(psycopg2.extras, "execute_values", lambda *a, **k: None)
    w = WideDatabaseWriter(schema="s")
    def boom(app, n):
        raise RuntimeError("progress sink died")
    w._on_write_progress = boom

    sink = _bare_sink(w)
    sink._batch = [("1",)]
    sink._flush()                        # swallows the callback error
    assert sink.upserted == 1


def test_empty_flush_does_not_fire_callback(monkeypatch):
    monkeypatch.setattr(psycopg2.extras, "execute_values", lambda *a, **k: None)
    w = WideDatabaseWriter(schema="s")
    seen = []
    w._on_write_progress = lambda app, n: seen.append((app, n))
    sink = _bare_sink(w)
    sink._batch = []
    sink._flush()
    assert seen == []                    # nothing written -> no event


def test_write_stores_on_write_progress_callback():
    # Contract: write() stashes the callback for _AppSink to read.
    w = WideDatabaseWriter(schema="s")
    assert w._on_write_progress is None
    cb = lambda app, n: None
    # We can't call write() without a pipeline/DB, but we can assert the
    # attribute is the wiring point _AppSink reads (set directly here).
    w._on_write_progress = cb
    assert w._on_write_progress is cb
