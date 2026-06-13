"""
db_writer.py
------------
T24 Generic Adapter - Wide-Table Database Sink

Writes the wide (pivoted) result back into PostgreSQL, one relational table
per application, completing the migration: source data, metadata, AND output
all live in the database.

Output layout
-------------
For application "ACCOUNT" the result table is "ACCOUNT_wide" in the same
schema, with one TEXT column per wide-table column (recordId, app_name,
CUSTOMER, ACCOUNT.NO, ACCOUNT.OFFICER_1, ...). The "_wide" suffix is excluded
from input discovery, so result tables are never re-ingested as source data.

Write policy (UPSERT — no truncation)
-------------------------------------
Per application, per run:
1. Ensure the table exists with the current columns; ADD COLUMN for any new
   ones (dynamic schema growth). Ensure a UNIQUE constraint on the key column
   (required for ON CONFLICT).
2. UPSERT every row: INSERT ... ON CONFLICT (key) DO UPDATE SET <cols>.
   New keys are inserted, existing keys are updated in place. Re-runs are
   idempotent; changed values are reflected. Nothing is truncated.

Write modes (same upsert, different flush size):
- "streaming" (default): upsert each row as produced — low, constant memory.
- "batching": accumulate db_batch_size rows, then bulk-upsert.

Full sync (optional, safety-gated)
----------------------------------
When config.db_full_sync is on, after upserting, rows whose key was NOT seen
this run are DELETEd, so source deletions are mirrored. The sweep only runs
when the run is safe to treat as a complete snapshot (see _should_sweep):
the upserts + sweep happen in one per-app transaction, so the table is never
left half-synced.

All columns are TEXT (values kept verbatim). Blank cells are written as NULL.
"""

from __future__ import annotations

import logging
from typing import Dict, List

from .db_reader import _connect_from_env
from .wide_writer import WidePivotWriter

logger = logging.getLogger("t24-adapter.db-writer")


class WideDatabaseWriter:
    """
    Writes per-application wide tables into a PostgreSQL schema via UPSERT,
    with an optional, safety-gated full-sync delete-sweep.

    Usage
    -----
    writer = WideDatabaseWriter(
        schema="t24_adaptor", suffix="_wide",
        write_mode="streaming", batch_size=500,
        key_column="recordId", full_sync=True,
    )
    written = writer.write(pipeline)
    # written -> {"ACCOUNT": (table, upserted, deleted, col_count), ...}
    """

    def __init__(
        self,
        schema: str,
        suffix: str = "_wide",
        write_mode: str = "streaming",
        batch_size: int = 500,
        key_column: str = "recordId",
        full_sync: bool = False,
        filters_active: bool = False,
    ):
        self.schema = schema
        self.suffix = suffix
        self.write_mode = write_mode
        self.batch_size = batch_size if write_mode == "batching" else 1
        self.key_column = key_column
        self.full_sync = full_sync
        # When the (future) Filters feature narrows a run, this must be True so
        # the delete-sweep is disabled (a partial run must never delete rows).
        self.filters_active = filters_active
        self._pivot = WidePivotWriter()

    def write(self, pipeline) -> Dict[str, tuple]:
        """
        Pivot every application and upsert each into its own result table.

        The source is read exactly twice, regardless of how many applications
        there are: once to discover the wide schema, once to write. Pass 2
        streams every app's rows in a single traversal and dispatches each to
        its own per-app transaction (committed at the app boundary).

        Returns
        -------
        Dict[app_name, (table_name, upserted, deleted, column_count)]
        """
        schemas = self._pivot.discover_schema(pipeline.run())          # pass 1
        if not schemas:
            logger.warning("No records discovered; no result tables written.")
            return {}

        results: Dict[str, tuple] = {}
        conn = _connect_from_env()
        sink = None
        try:
            # pass 2 — single traversal of the whole source, app-grouped.
            for app_name, row in self._pivot.iter_all_wide_rows(pipeline.run(), schemas):
                if sink is None or sink.app_name != app_name:
                    if sink is not None:
                        results[sink.app_name] = sink.finalize()
                    sink = _AppSink(self, conn, app_name, schemas[app_name])
                sink.add(row)
            if sink is not None:
                results[sink.app_name] = sink.finalize()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return results

    def _build_upsert_sql(self, cursor, table: str, columns: List[str]) -> str:
        """Compose the INSERT … ON CONFLICT DO UPDATE statement for one table."""
        from psycopg2 import sql

        non_key = [c for c in columns if c != self.key_column]
        return sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}"
        ).format(
            sql.Identifier(self.schema), sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
            sql.Identifier(self.key_column),
            sql.SQL(", ").join(
                sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in non_key
            ),
        ).as_string(cursor)

    # ------------------------------------------------------------------ #
    # DDL
    # ------------------------------------------------------------------ #

    def _ensure_table(self, cursor, table: str, columns: List[str]) -> None:
        """Create the table if missing; add new columns; ensure the key is UNIQUE."""
        from psycopg2 import sql

        cursor.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (self.schema, table),
        )
        if cursor.fetchone() is None:
            col_defs = sql.SQL(", ").join(
                sql.SQL("{} text").format(sql.Identifier(c)) for c in columns
            )
            cursor.execute(
                sql.SQL("CREATE TABLE {}.{} ({}, UNIQUE ({}))").format(
                    sql.Identifier(self.schema),
                    sql.Identifier(table),
                    col_defs,
                    sql.Identifier(self.key_column),
                )
            )
            logger.info(f"Created table {self.schema}.{table} ({len(columns)} columns)")
            return

        # Exists: add any columns present now but missing in the table.
        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (self.schema, table),
        )
        existing = {r[0] for r in cursor.fetchall()}
        for c in columns:
            if c not in existing:
                cursor.execute(
                    sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} text").format(
                        sql.Identifier(self.schema), sql.Identifier(table),
                        sql.Identifier(c),
                    )
                )
                logger.info(f"Added column {c!r} to {self.schema}.{table}")

        self._ensure_unique(cursor, table)

    def _ensure_unique(self, cursor, table: str) -> None:
        """Ensure a UNIQUE constraint exists on the key column (needed for upsert)."""
        from psycopg2 import sql

        cursor.execute(
            """SELECT 1
                 FROM pg_index i
                 JOIN pg_class c   ON c.oid = i.indrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
                WHERE n.nspname = %s AND c.relname = %s
                  AND i.indisunique AND a.attname = %s
                  AND array_length(i.indkey, 1) = 1""",
            (self.schema, table, self.key_column),
        )
        if cursor.fetchone():
            return
        try:
            cursor.execute(
                sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} UNIQUE ({})").format(
                    sql.Identifier(self.schema), sql.Identifier(table),
                    sql.Identifier(f"{table}_{self.key_column}_key"),
                    sql.Identifier(self.key_column),
                )
            )
            logger.info(f"Added UNIQUE({self.key_column}) on {self.schema}.{table}")
        except Exception as exc:
            raise RuntimeError(
                f"Cannot add UNIQUE({self.key_column}) on {self.schema}.{table} — "
                f"existing data likely has duplicate keys. Upsert requires a "
                f"unique key. Underlying error: {exc}"
            ) from exc

    # ------------------------------------------------------------------ #
    # Full-sync delete-sweep (safety-gated)
    # ------------------------------------------------------------------ #

    def _should_sweep(self, table: str, seen: set) -> bool:
        """
        Decide whether the delete-sweep may run. Safe ONLY when:
        - full sync is enabled,
        - no filters narrowed the run (a partial run must never delete),
        - at least one key was seen (empty result would wipe the whole table;
          we refuse and warn rather than risk a glitch-driven full wipe).
        Clean completion is structurally guaranteed: the sweep is the last step
        in the per-app transaction, so any earlier error rolls back instead.
        """
        if not self.full_sync:
            return False
        if self.filters_active:
            logger.warning(
                f"[{table}] full sync skipped: filters are active "
                f"(a partial run must not delete rows)."
            )
            return False
        if not seen:
            logger.warning(
                f"[{table}] full sync skipped: zero records seen this run "
                f"(refusing to wipe the whole table as a safety measure)."
            )
            return False
        return True

    def _sweep(self, cursor, table: str, seen: set) -> int:
        """Delete result rows whose key was not seen this run. Returns count."""
        if not self._should_sweep(table, seen):
            return 0
        from psycopg2 import sql

        cursor.execute(
            sql.SQL("DELETE FROM {}.{} WHERE NOT ({} = ANY(%s))").format(
                sql.Identifier(self.schema), sql.Identifier(table),
                sql.Identifier(self.key_column),
            ),
            (list(seen),),
        )
        deleted = cursor.rowcount
        if deleted:
            logger.info(f"Full sync: deleted {deleted} stale row(s) from {self.schema}.{table}")
        return deleted


class _AppSink:
    """
    Per-application writer used during the single write pass. Holds one
    transaction (its own cursor), batches upserts, and on finalize() runs the
    delete-sweep and commits — so each application's table is written
    atomically, exactly as before, but the source is streamed only once
    across all apps.
    """

    def __init__(self, writer: "WideDatabaseWriter", conn, app_name: str, schema):
        self.writer = writer
        self.conn = conn
        self.app_name = app_name
        self.table = f"{app_name}{writer.suffix}"
        self.columns = schema.columns
        self.cursor = conn.cursor()
        writer._ensure_table(self.cursor, self.table, self.columns)
        self._upsert_sql = writer._build_upsert_sql(self.cursor, self.table, self.columns)
        self._batch: List[tuple] = []
        self.seen: set = set()
        self.upserted = 0

    def add(self, row: Dict[str, str]) -> None:
        self.seen.add(row[self.writer.key_column])
        self._batch.append(
            tuple(None if row[c] == "" else row[c] for c in self.columns)
        )
        if len(self._batch) >= self.writer.batch_size:
            self._flush()

    def _flush(self) -> None:
        if not self._batch:
            return
        from psycopg2.extras import execute_values
        execute_values(self.cursor, self._upsert_sql, self._batch)
        self.upserted += len(self._batch)
        self._batch = []

    def finalize(self) -> tuple:
        self._flush()
        deleted = self.writer._sweep(self.cursor, self.table, self.seen)
        self.cursor.close()
        self.conn.commit()
        logger.info(
            f"Wrote {self.writer.schema}.{self.table}: {self.upserted} upserted, "
            f"{deleted} deleted, {len(self.columns)} columns "
            f"(mode={self.writer.write_mode})"
        )
        return (self.table, self.upserted, deleted, len(self.columns))
