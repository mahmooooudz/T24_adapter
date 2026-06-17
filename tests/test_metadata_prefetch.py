"""
Lever 2 — bulk metadata prefetch.

Network-free: a fake connection/cursor proves fetch_all() turns N per-app
round-trips into one query and builds the {key: xml} map, skipping empty rows.
"""

from t24_adapter.db_reader import T24DatabaseMetadataReader


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, query, params=None):
        self.executed.append(query)

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows
        self.cursors = []
        self.rolled_back = False

    def cursor(self):
        c = _FakeCursor(self._rows)
        self.cursors.append(c)
        return c

    def rollback(self):
        self.rolled_back = True


def test_fetch_all_builds_map_skips_empty_and_uses_one_query():
    conn = _FakeConn([("CUSTOMER", "<x1/>"), ("ACCOUNT", "<x2/>"), ("BLANK", None)])
    reader = T24DatabaseMetadataReader(
        schema="t24_adaptor", table="STANDARD_SELECTION", connection=conn
    )
    out = reader.fetch_all()

    assert out == {"CUSTOMER": "<x1/>", "ACCOUNT": "<x2/>"}   # blank row dropped
    assert len(conn.cursors) == 1 and len(conn.cursors[0].executed) == 1  # one query
    assert conn.rolled_back is True   # shared-conn read txn released


def test_fetch_all_returns_empty_on_unreadable_table():
    class _BoomConn(_FakeConn):
        def cursor(self):
            raise RuntimeError("relation does not exist")

    reader = T24DatabaseMetadataReader(
        schema="s", table="MISSING", connection=_BoomConn([])
    )
    assert reader.fetch_all() == {}   # caller falls back to file metadata
