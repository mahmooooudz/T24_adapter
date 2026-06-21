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

Write modes (both bulk-upsert; differ only in flush size):
- "streaming" (default): bounded-memory bulk upsert — flush every db_batch_size
  rows so only one batch is ever in flight. Memory is constant regardless of
  total row count.
- "batching": identical mechanism; an explicit alias for callers that want to
  state "throughput-oriented" intent.

Both flush each batch in a SINGLE bulk statement (one network round-trip per
batch via execute_values page_size), never one round-trip per row — the latter
is pathological against a remote database (one ~RTT per row).

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
from .metrics import metrics
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
        single_pass: bool = True,
        single_pass_max_rows: int = 200_000,
    ):
        self.schema = schema
        self.suffix = suffix
        self.write_mode = write_mode
        # Single-pass: read+parse+normalize once, buffering per app, instead of
        # the two-pass discover-then-write. Bounded by single_pass_max_rows per
        # app (above it, that app falls back to two-pass streaming).
        self.single_pass = single_pass
        self.single_pass_max_rows = single_pass_max_rows
        # Both modes bulk-upsert in batches of `batch_size` rows (one network
        # round-trip per batch). "streaming" used to mean batch=1 — pathological
        # on a remote DB — so it now behaves identically to "batching". The
        # floor is 1; pass a sane positive batch_size from config (default 1000).
        self.batch_size = max(1, int(batch_size)) if batch_size else 500
        self.key_column = key_column
        self.full_sync = full_sync
        # When the (future) Filters feature narrows a run, this must be True so
        # the delete-sweep is disabled (a partial run must never delete rows).
        self.filters_active = filters_active
        self._pivot = WidePivotWriter()
        # Optional callback fired after each batch flush: (app_name, rows_written).
        # Lets a caller (e.g. the web console) show LIVE write progress instead
        # of a single jump at the end. Set per-run by write(); default no-op.
        self._on_write_progress = None

    def write(self, pipeline, schemas=None, rows_wrapper=None,
              on_write_progress=None) -> Dict[str, tuple]:
        """
        Pivot every application and upsert each into its own result table.

        The source is read twice: once to discover the wide schema (column set +
        per-field max occurrences, which determines the exact bare-vs-indexed
        shape without ever losing a value), once to write. Both passes share the
        pipeline's single read connection, so the cost is I/O, not connection
        setup. Pass 2 streams every app's rows in a single traversal.

        Parameters
        ----------
        schemas : optional pre-discovered {app_name: _AppSchema}. When provided
            (e.g. the web console already ran a discovery pass to compute live
            stats), the discovery pass here is SKIPPED — the whole job is one
            discovery + one write instead of two discoveries + one write.
        rows_wrapper : optional Callable[[Iterator[NormalizedField]],
            Iterator[NormalizedField]] that wraps the pipeline.run() stream
            BEFORE it goes into the single-pass scan. The web console uses this
            to emit per-record progress events and accumulate per-app stats as
            records flow through. Must yield every field unchanged (or raise to
            signal a user-requested stop). Ignored on the two-pass path.

        on_write_progress : optional Callable[[str, int], None] invoked after
            each batch flush with (app_name, cumulative rows written). Lets a
            caller render live write-phase progress. No-op if not provided.

        Returns
        -------
        Dict[app_name, (table_name, upserted, deleted, column_count)]
        """
        self._on_write_progress = on_write_progress
        # Single-pass is used only when this writer discovers the schema itself.
        # When the caller pre-discovered schemas (e.g. the web console), keep the
        # two-pass write that consumes those schemas.
        if schemas is None and self.single_pass:
            return self._write_single_pass(pipeline, rows_wrapper=rows_wrapper)
        return self._write_two_pass(pipeline, schemas)

    def _write_single_pass(self, pipeline, rows_wrapper=None) -> Dict[str, tuple]:
        """
        One traversal: buffer per app while discovering the shape, then bulk
        write from the buffer — no second read. Apps over single_pass_max_rows
        fall back to a two-pass re-stream (memory-bounded).
        """
        rows = pipeline.run()
        if rows_wrapper is not None:
            rows = rows_wrapper(rows)
        with metrics.span("pass_single"):
            schemas, buffers, overflow = self._pivot.stream_buffer_and_schema(
                rows, max_buffer_rows=self.single_pass_max_rows
            )
        if not schemas:
            logger.warning("No records discovered; no result tables written.")
            pipeline.close()
            return {}

        results: Dict[str, tuple] = {}
        conn = _connect_from_env()      # dedicated WRITE connection
        try:
            with metrics.span("pass2_write"):
                # Buffered apps: write directly, no re-read.
                self._write_buffered(schemas, buffers, overflow, conn, results)
                # Overflow apps (too large to buffer): re-stream just those.
                if overflow:
                    results.update(self._write_overflow(pipeline, schemas, overflow, conn))
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
            pipeline.close()
        return results

    def _write_buffered(self, schemas, buffers, overflow, conn, results) -> None:
        """Write the non-overflow buffered apps from their {position:[values]}
        buffers. Shared by single-pass and the prebuilt (prefetched) path."""
        for app_name, schema in schemas.items():
            if app_name in overflow:
                continue
            sink = _AppSink(self, conn, app_name, schema)
            for rid, values in buffers.get(app_name, []):
                sink.add(self._pivot.wide_row_from_buffer(rid, values, schema, app_name))
            results[app_name] = sink.finalize()

    def write_prebuilt(self, schemas, buffers, on_write_progress=None) -> Dict[str, tuple]:
        """Write rows that were already read, normalized, buffered and
        schema-discovered elsewhere — e.g. a speculative background prefetch
        that ran while the user was still configuring. NO source read happens
        here; only the DB write. Assumes the prefetch covered only bufferable
        apps (no overflow)."""
        self._on_write_progress = on_write_progress
        if not schemas:
            return {}
        results: Dict[str, tuple] = {}
        conn = _connect_from_env()
        try:
            with metrics.span("pass2_write"):
                self._write_buffered(schemas, buffers, set(), conn, results)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return results

    def _write_overflow(self, pipeline, schemas, overflow, conn) -> Dict[str, tuple]:
        """Two-pass write for the (rare) apps that exceeded the buffer cap.
        Narrows the re-stream to only the overflow apps via include_applications."""
        results: Dict[str, tuple] = {}
        prev_include = pipeline.config.include_applications
        sink = None
        try:
            pipeline.config.include_applications = tuple(sorted(overflow))
            for app_name, row in self._pivot.iter_all_wide_rows(pipeline.run(), schemas):
                if sink is None or sink.app_name != app_name:
                    if sink is not None:
                        results[sink.app_name] = sink.finalize()
                    sink = _AppSink(self, conn, app_name, schemas[app_name])
                sink.add(row)
            if sink is not None:
                results[sink.app_name] = sink.finalize()
        finally:
            pipeline.config.include_applications = prev_include
        return results

    def _write_two_pass(self, pipeline, schemas=None) -> Dict[str, tuple]:
        """Original two-pass write: discover the schema (unless given), then a
        single traversal of the whole source, writing app by app."""
        if schemas is None:
            with metrics.span("pass1_discover_schema"):
                schemas = self._pivot.discover_schema(pipeline.run())  # pass 1
        if not schemas:
            logger.warning("No records discovered; no result tables written.")
            pipeline.close()
            return {}

        results: Dict[str, tuple] = {}
        conn = _connect_from_env()      # dedicated WRITE connection
        sink = None
        try:
            # pass 2 — single traversal of the whole source, app-grouped.
            with metrics.span("pass2_write"):
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
            # Release the shared READ connection owned by the pipeline.
            pipeline.close()
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
        """Create the table if missing; add new columns; ensure the key is UNIQUE.

        One introspection query: information_schema.columns returns the table's
        columns (empty set => the table does not exist), so the separate
        "does the table exist?" round-trip is no longer needed.
        """
        from psycopg2 import sql

        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (self.schema, table),
        )
        existing = {r[0] for r in cursor.fetchall()}

        if not existing:
            # No columns reported => table does not exist yet. Create it with the
            # UNIQUE key in the same statement (so no follow-up _ensure_unique).
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
        self._add_columns(cursor, table, [c for c in columns if c not in existing])
        self._ensure_unique(cursor, table)

    def _add_columns(self, cursor, table: str, columns: List[str]) -> None:
        """Add the given TEXT columns to an existing table (idempotent)."""
        from psycopg2 import sql

        for c in columns:
            cursor.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} text").format(
                    sql.Identifier(self.schema), sql.Identifier(table),
                    sql.Identifier(c),
                )
            )
            logger.info(f"Added column {c!r} to {self.schema}.{table}")

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
        with metrics.span("ddl"):
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
        from time import perf_counter

        from psycopg2.extras import execute_values
        start = perf_counter()
        # page_size = batch size => the whole batch goes in ONE round-trip
        # (execute_values' default page_size of 100 would split a larger batch).
        execute_values(
            self.cursor, self._upsert_sql, self._batch,
            page_size=max(100, len(self._batch)),
        )
        metrics.add("db_upsert", perf_counter() - start)
        metrics.incr("db_roundtrips")
        self.upserted += len(self._batch)
        self._batch = []
        # Live write-phase progress: report cumulative rows written for this app.
        cb = self.writer._on_write_progress
        if cb is not None:
            try:
                cb(self.app_name, self.upserted)
            except Exception:
                pass  # progress reporting must never break the write

    def finalize(self) -> tuple:
        self._flush()
        with metrics.span("db_sweep"):
            deleted = self.writer._sweep(self.cursor, self.table, self.seen)
        self.cursor.close()
        self.conn.commit()
        logger.info(
            f"Wrote {self.writer.schema}.{self.table}: {self.upserted} upserted, "
            f"{deleted} deleted, {len(self.columns)} columns "
            f"(mode={self.writer.write_mode})"
        )
        return (self.table, self.upserted, deleted, len(self.columns))
