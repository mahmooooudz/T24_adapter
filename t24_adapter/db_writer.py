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

DDL / refresh policy (create-if-missing + truncate + insert)
------------------------------------------------------------
Per application, per run:
1. If the result table does not exist -> CREATE TABLE with the current columns.
2. If it exists -> ADD COLUMN for any new columns (e.g. a newly appearing
   INTEREST.RATE_3), so the table keeps up with schema growth.
3. TRUNCATE the table, then batch-INSERT all current rows.

All columns are TEXT (values are kept verbatim, as elsewhere in the adapter).
Blank cells are written as NULL.

Memory
------
Rows are streamed from the pivot and inserted in batches via
psycopg2.extras.execute_values, so memory stays bounded.
"""

from __future__ import annotations

import logging
from typing import Dict

from .db_reader import _connect_from_env
from .wide_writer import WidePivotWriter

logger = logging.getLogger("t24-adapter.db-writer")


class WideDatabaseWriter:
    """
    Writes per-application wide tables into a PostgreSQL schema.

    Usage
    -----
    writer = WideDatabaseWriter(schema="t24_adaptor", suffix="_wide")
    written = writer.write(pipeline)
    # written -> {"ACCOUNT": ("ACCOUNT_wide", row_count, col_count), ...}
    """

    def __init__(self, schema: str, suffix: str = "_wide", batch_size: int = 500):
        self.schema = schema
        self.suffix = suffix
        self.batch_size = batch_size
        self._pivot = WidePivotWriter()

    def write(self, pipeline) -> Dict[str, tuple]:
        """
        Pivot every application and write each to its own result table.

        Returns
        -------
        Dict[app_name, (table_name, rows_written, column_count)]
        """
        schemas = self._pivot.discover_schema(pipeline.run())
        if not schemas:
            logger.warning("No records discovered; no result tables written.")
            return {}

        results: Dict[str, tuple] = {}
        conn = _connect_from_env()
        try:
            for app_name, app_schema in schemas.items():
                table = f"{app_name}{self.suffix}"
                columns = app_schema.columns
                with conn.cursor() as cursor:
                    self._ensure_table(cursor, table, columns)
                    self._truncate(cursor, table)
                    rows_written = self._insert_rows(
                        cursor, pipeline, app_schema, app_name, table, columns
                    )
                conn.commit()
                results[app_name] = (table, rows_written, len(columns))
                logger.info(
                    f"Wrote {self.schema}.{table}: "
                    f"{rows_written} rows x {len(columns)} columns"
                )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return results

    # ------------------------------------------------------------------ #
    # DDL
    # ------------------------------------------------------------------ #

    def _ensure_table(self, cursor, table: str, columns) -> None:
        """Create the table if missing; otherwise add any new columns."""
        from psycopg2 import sql

        cursor.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (self.schema, table),
        )
        exists = cursor.fetchone() is not None

        if not exists:
            col_defs = sql.SQL(", ").join(
                sql.SQL("{} text").format(sql.Identifier(c)) for c in columns
            )
            cursor.execute(
                sql.SQL("CREATE TABLE {}.{} ({})").format(
                    sql.Identifier(self.schema),
                    sql.Identifier(table),
                    col_defs,
                )
            )
            logger.info(f"Created table {self.schema}.{table} ({len(columns)} columns)")
            return

        # Table exists: add any columns present now but missing in the table.
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
                        sql.Identifier(self.schema),
                        sql.Identifier(table),
                        sql.Identifier(c),
                    )
                )
                logger.info(f"Added column {c!r} to {self.schema}.{table}")

    def _truncate(self, cursor, table: str) -> None:
        from psycopg2 import sql

        cursor.execute(
            sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(self.schema), sql.Identifier(table)
            )
        )

    # ------------------------------------------------------------------ #
    # DML
    # ------------------------------------------------------------------ #

    def _insert_rows(self, cursor, pipeline, app_schema, app_name, table, columns) -> int:
        """Stream wide rows for one app and batch-insert them."""
        from psycopg2 import sql
        from psycopg2.extras import execute_values

        insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(self.schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        ).as_string(cursor)

        def to_tuple(row):
            # Blank cells -> NULL.
            return tuple(None if row[c] == "" else row[c] for c in columns)

        batch = []
        total = 0
        for row in self._pivot.iter_wide_rows(pipeline.run(), app_schema, app_name):
            batch.append(to_tuple(row))
            if len(batch) >= self.batch_size:
                execute_values(cursor, insert_sql, batch)
                total += len(batch)
                batch = []
        if batch:
            execute_values(cursor, insert_sql, batch)
            total += len(batch)
        return total
