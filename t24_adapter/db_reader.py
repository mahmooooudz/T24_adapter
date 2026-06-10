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
import xml.etree.ElementTree as ET
from typing import Iterator

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
        batch_size: int = 1000,
    ):
        self.schema = schema
        self.record_column = record_column
        self.batch_size = batch_size

    # ------------------------------------------------------------------ #
    # Connection
    # ------------------------------------------------------------------ #

    def _connect(self):
        """Open a connection using environment variables (see _connect_from_env)."""
        return _connect_from_env()

    # ------------------------------------------------------------------ #
    # Streaming
    # ------------------------------------------------------------------ #

    def stream_records(self, table: str) -> Iterator[ET.Element]:
        """
        Stream all <row> elements from a T24-shaped database table.

        Parameters
        ----------
        table : table name within the configured schema (e.g. "FBANK_Account")

        Yields
        ------
        ET.Element objects, one per <row> found inside each record's XML.

        Notes
        -----
        - Identifiers are safely quoted, so mixed-case names like
          "FBANK_Account" and "xmlRecord" are handled correctly.
        - A server-side cursor with itersize keeps memory constant.
        - The connection is always closed when iteration finishes.
        """
        from psycopg2 import sql

        logger.info(
            f"Streaming records from database: {self.schema}.{table} "
            f"(column {self.record_column})"
        )

        conn = self._connect()
        record_count = 0
        try:
            # Named cursor => server-side streaming cursor.
            with conn.cursor(name="t24_record_stream") as cursor:
                cursor.itersize = self.batch_size
                query = sql.SQL("SELECT {col} FROM {schema}.{table}").format(
                    col=sql.Identifier(self.record_column),
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier(table),
                )
                cursor.execute(query)

                for (xml_value,) in cursor:
                    if not xml_value or not str(xml_value).strip():
                        logger.warning(
                            f"Empty {self.record_column} encountered in "
                            f"{self.schema}.{table}; skipping row."
                        )
                        continue

                    root = self._parse(xml_value)
                    if root is None:
                        continue

                    for element in root.iter():
                        if XmlUtils.strip_namespace(element.tag).lower() == "row":
                            record_count += 1
                            yield element
        finally:
            conn.close()

        logger.info(f"Finished streaming. Total records: {record_count}")

    @staticmethod
    def _parse(xml_value: str):
        """
        Parse one record's XML string into an ET.Element root.

        Encodes to bytes first: a str carrying an `<?xml ... encoding=...?>`
        declaration cannot be passed to ET.fromstring directly (Python raises
        "Unicode strings with encoding declaration are not supported").
        """
        try:
            return ET.fromstring(str(xml_value).encode("utf-8"))
        except ET.ParseError as exc:
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
    ):
        self.schema = schema
        self.table = table
        self.key_column = key_column
        self.xml_column = xml_column

    def fetch_xml(self, app_name: str):
        """
        Return the STANDARD.SELECTION XML string for app_name, or None if
        there is no matching row. If the metadata table does not exist yet
        (migration not run), logs a warning and returns None so the caller
        can fall back to file-based metadata.
        """
        from psycopg2 import sql

        conn = _connect_from_env()
        try:
            with conn.cursor() as cursor:
                query = sql.SQL(
                    "SELECT {xml} FROM {schema}.{table} WHERE {key} = %s"
                ).format(
                    xml=sql.Identifier(self.xml_column),
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier(self.table),
                    key=sql.Identifier(self.key_column),
                )
                cursor.execute(query, (app_name,))
                row = cursor.fetchone()
        except Exception as exc:  # e.g. UndefinedTable before migration is run
            logger.warning(
                f"Could not read metadata table "
                f"{self.schema}.{self.table} ({type(exc).__name__}): {exc}"
            )
            return None
        finally:
            conn.close()

        if row and row[0]:
            logger.info(
                f"Loaded STANDARD.SELECTION for '{app_name}' from "
                f"{self.schema}.{self.table}."
            )
            return row[0]
        return None
