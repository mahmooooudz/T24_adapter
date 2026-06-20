"""
db_reader.py
------------
T24 Generic Adapter - Streaming Database Data Reader

Reads T24 records from a PostgreSQL database instead of XML files.

In a T24-shaped database table, each row holds one record as XML in a
single column (the classic T24 "RECID + XMLRECORD" layout). For example:

    schema:  t24_adaptor
    table:   FBANK_Account
    columns: recordId (text) | xmlRecord (text)
                                └─ <?xml ...><DATA><row><c1>..</c1>..</row></DATA>

This reader yields the SAME <row> ET.Element objects that the file-based
T24StreamingDataReader yields, so the downstream normalizer and wide-table
writer are completely unchanged. Only the *source* differs.

Memory model
------------
A PostgreSQL server-side (named) cursor streams rows in batches via
`itersize`, so the full result set is never loaded into memory. Each
xmlRecord is small (one record), parsed and yielded one at a time.

Connection
----------
Credentials come from environment variables (loaded from .env by
env_loader). DB_URL (a libpq URI) is used if present; otherwise the
connection is assembled from DB_HOST / DB_PORT / DB_NAME / DB_USER /
DB_PASSWORD. The reader only ever issues SELECT statements.
"""

from __future__ import annotations

import logging
import os
from typing import Iterator

from ._xml_backend import XMLParseError, fromstring, log_backend_once
from .metrics import metrics
from .xml_utils import XmlUtils

logger = logging.getLogger("t24-adapter.db-reader")


def _connect_from_env():
    """
    Open a PostgreSQL connection using environment variables.
    Prefers DB_URL (a full libpq URI); otherwise builds from
    DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD.
    Only SELECT statements are ever issued by this module.
    """
    try:
        import psycopg2
    except ImportError as exc:
        raise ImportError(
            "psycopg2 is required for database mode. "
            "Install it with: pip install psycopg2-binary"
        ) from exc

    url = os.environ.get("DB_URL")
    if url:
        return psycopg2.connect(url)

    return psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT"),
        dbname=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
    )


class T24DatabaseDataReader:
    """
    Streams T24 records from a PostgreSQL table whose record column holds
    the T24 XML for each record.

    Usage
    -----
    reader = T24DatabaseDataReader(schema="t24_adaptor")
    for row_element in reader.stream_records("FBANK_Account"):
        # row_element is the same <row> ET.Element the file reader yields
        pass
    """

    def __init__(
        self,
        schema: str,
        record_column: str = "xmlRecord",
        record_id_column: str = "recordId",
        batch_size: int = 1000,
        connection=None,
        buffered_read_max_rows: int = 0,
    ):
        self.schema = schema
        self.record_column = record_column
        self.record_id_column = record_id_column
        self.batch_size = batch_size
        # When > 0, tables estimated at or below this many rows are read with a
        # PLAIN client-side cursor (one round-trip, no server-cursor protocol
        # overhead) instead of a server-side streaming cursor — ~2x faster for
        # tables that fit in memory. Larger tables still stream server-side so
        # memory stays constant. 0 = always server-side (original behaviour).
        self.buffered_read_max_rows = buffered_read_max_rows
        # Optional shared connection. When set, this reader reuses it for every
        # operation and never closes it (the owner — the pipeline — does). When
        # None, each operation opens and closes its own short-lived connection.
        self._conn = connection

    # ------------------------------------------------------------------ #
    # Connection
    # ------------------------------------------------------------------ #

    def _acquire(self):
        """Return (connection, owns_it). owns_it=False for a shared connection."""
        if self._conn is not None:
            return self._conn, False
        return _connect_from_env(), True

    def discover_tables(self, exclude=(), exclude_suffixes=()) -> list:
        """
        Return the names of all base tables in the configured schema,
        excluding any in `exclude` (e.g. the metadata table) and any whose
        name ends with one of `exclude_suffixes` (e.g. the output "_wide"
        result tables, so they are never re-ingested as input).

        This is how data applications are discovered dynamically: each table
        in the schema (other than the metadata/output tables) is one
        application, keyed by its own name. No application names are hardcoded.
        """
        exclude_set = set(exclude or ())
        suffixes = tuple(exclude_suffixes or ())
        conn, owns = self._acquire()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                    "ORDER BY table_name",
                    (self.schema,),
                )
                names = [r[0] for r in cursor.fetchall()]
        finally:
            if owns:
                conn.close()
            else:
                conn.rollback()  # release the read txn on the shared connection
        return [
            n for n in names
            if n not in exclude_set and not (suffixes and n.endswith(suffixes))
        ]

    # ------------------------------------------------------------------ #
    # Streaming
    # ------------------------------------------------------------------ #

    def _estimate_rows(self, conn, table: str) -> int:
        """Cheap, instant row-count estimate via pg_class.reltuples (no scan).
        Returns -1 when unknown (e.g. never analyzed, or table missing)."""
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT reltuples::bigint FROM pg_class WHERE oid = %s::regclass",
                    (f'"{self.schema}"."{table}"',),
                )
                row = cursor.fetchone()
            if row and row[0] is not None and row[0] >= 0:
                return int(row[0])
        except Exception:
            if self._conn is not None:
                self._conn.rollback()  # clear the aborted txn on the shared conn
        return -1

    def _emit_rows(self, cursor, table: str):
        """Yield (record_id, <row> element) for each record from `cursor`.
        Returns the record count (consumed via `yield from`)."""
        count = 0
        for record_id, xml_value in cursor:
            if not xml_value or not str(xml_value).strip():
                logger.warning(
                    f"Empty {self.record_column} for recordId="
                    f"{record_id!r} in {self.schema}.{table}; skipping."
                )
                continue
            root = self._parse(xml_value)
            if root is None:
                continue
            rid = None if record_id is None else str(record_id)
            for element in root.iter():
                if XmlUtils.strip_namespace(element.tag).lower() == "row":
                    count += 1
                    yield (rid, element)
        return count

    def stream_records(self, table: str) -> Iterator[tuple]:
        """
        Stream records from a T24-shaped database table.

        Yields (record_id, ET.Element) tuples — the recordId column paired with
        each <row> element inside that record's XML.

        Read strategy (chosen per table):
        - Tables estimated at/under `buffered_read_max_rows` are read with a
          PLAIN client-side cursor: one round-trip, no server-cursor protocol
          overhead (~2x faster). The result fits in memory by construction.
        - Larger tables (or when the estimate is unknown / the feature is off)
          use a server-side streaming cursor so memory stays constant.

        Identifiers are safely quoted; the connection's read txn is released
        when iteration finishes.
        """
        from psycopg2 import sql

        log_backend_once()
        conn, owns = self._acquire()
        query = sql.SQL("SELECT {id_col}, {col} FROM {schema}.{table}").format(
            id_col=sql.Identifier(self.record_id_column),
            col=sql.Identifier(self.record_column),
            schema=sql.Identifier(self.schema),
            table=sql.Identifier(table),
        )

        use_plain = False
        if self.buffered_read_max_rows and self.buffered_read_max_rows > 0:
            est = self._estimate_rows(conn, table)
            use_plain = 0 <= est <= self.buffered_read_max_rows
            logger.info(
                f"Streaming {self.schema}.{table} (~{est} rows est.): "
                f"{'plain buffered read' if use_plain else 'server-side streaming'}"
            )
        else:
            logger.info(f"Streaming records from database: {self.schema}.{table}")

        record_count = 0
        try:
            if use_plain:
                # Plain client-side cursor: single round-trip, bounded by the
                # size gate above.
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    record_count = yield from self._emit_rows(cursor, table)
            else:
                # Server-side (named) streaming cursor: constant memory.
                with conn.cursor(name="t24_record_stream") as cursor:
                    cursor.itersize = self.batch_size
                    cursor.execute(query)
                    record_count = yield from self._emit_rows(cursor, table)
        finally:
            if owns:
                conn.close()
            else:
                conn.rollback()  # release the read txn on the shared connection

        logger.debug(f"Finished streaming. Total records: {record_count}")

    @staticmethod
    def _parse(xml_value: str):
        """
        Parse one record's XML string into an ET.Element root.

        Encodes to bytes first: a str carrying an `<?xml ... encoding=...?>`
        declaration cannot be passed to ET.fromstring directly (Python raises
        "Unicode strings with encoding declaration are not supported").
        """
        try:
            return fromstring(str(xml_value).encode("utf-8"))
        except XMLParseError as exc:
            logger.error(f"Malformed XML in record column; skipping row: {exc}")
            return None


class T24DatabaseMetadataReader:
    """
    Fetches STANDARD.SELECTION metadata XML for an application from a
    database table that mirrors the record table's layout:

        schema:  t24_adaptor
        table:   STANDARD_SELECTION
        columns: appName (text) | xmlRecord (text)
                                   └─ the full STANDARD.SELECTION <ROW> XML

    Returns the raw XML string for one application; parsing/registration is
    handled by StandardSelectionLoader.load_from_string, so the metadata
    logic is identical whether it comes from a file or the database.
    """

    def __init__(
        self,
        schema: str,
        table: str,
        key_column: str = "appName",
        xml_column: str = "xmlRecord",
        connection=None,
    ):
        self.schema = schema
        self.table = table
        self.key_column = key_column
        self.xml_column = xml_column
        # Optional shared connection (see T24DatabaseDataReader). When set it is
        # reused and never closed here; the owner closes it.
        self._conn = connection

    def fetch_all(self) -> dict:
        """
        Return {key: xml} for EVERY row in the metadata table in a SINGLE query.

        Metadata tables hold one small row per application, so one round-trip
        fetches them all (vs. one query per application). Returns an empty dict
        (and warns) if the table is missing/unreadable, so the caller falls back
        to file-based metadata.
        """
        from psycopg2 import sql

        if self._conn is not None:
            conn, owns = self._conn, False
        else:
            conn, owns = _connect_from_env(), True
        metrics.incr("metadata_roundtrips")
        try:
            with conn.cursor() as cursor:
                query = sql.SQL("SELECT {key}, {xml} FROM {schema}.{table}").format(
                    key=sql.Identifier(self.key_column),
                    xml=sql.Identifier(self.xml_column),
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier(self.table),
                )
                cursor.execute(query)
                rows = cursor.fetchall()
        except Exception as exc:  # e.g. UndefinedTable before migration is run
            logger.warning(
                f"Could not read metadata table "
                f"{self.schema}.{self.table} ({type(exc).__name__}): {exc}"
            )
            if not owns:
                conn.rollback()
            return {}
        finally:
            if owns:
                conn.close()
            else:
                conn.rollback()

        result = {r[0]: r[1] for r in rows if r and r[1]}
        logger.debug(
            f"Prefetched {len(result)} metadata row(s) from "
            f"{self.schema}.{self.table} in one query."
        )
        return result
