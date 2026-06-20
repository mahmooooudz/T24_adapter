"""
config.py
---------
T24 Generic Adapter - Configuration Layer

Defines all runtime settings for the pipeline.
No T24 field names or positions are hardcoded here.
All business meaning comes from metadata files.
"""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple


@dataclass
class T24PipelineConfig:
    """
    Central configuration for the T24 Generic Adapter Pipeline.

    Parameters
    ----------
    package_root : Path
        Root folder of the T24 input package.
        Expected subdirectories: data/, metadata/, local_ref/,
        customization/, relationships/, xsd/

    data_dir : str
        Subdirectory name containing T24 XML data files.

    xsd_dir : str
        Subdirectory name containing XSD schema files (optional).

    metadata_dir : str
        Subdirectory name containing STANDARD.SELECTION metadata XML files.

    local_ref_dir : str
        Subdirectory name containing LOCAL.REF metadata XML files.

    customization_dir : str
        Subdirectory name containing bank-specific customization XML files.

    relationships_dir : str
        Subdirectory name containing relationship metadata XML files.

    require_xsd : bool
        If True, pipeline fails when XSD directory is missing.

    require_standard_selection : bool
        If True, pipeline fails when STANDARD.SELECTION metadata is missing.

    require_local_ref_when_declared : bool
        If True, pipeline warns when local ref metadata is absent but data
        contains local reference fields (c64 m-values).

    require_relationships : bool
        If True, pipeline fails when relationship metadata is missing.

    fail_on_unmapped_field : bool
        If True, pipeline raises ValueError on unmapped fields.
        If False (recommended), unmapped fields are emitted as FIELD_<pos>.

    local_ref_base_positions : Tuple[str, ...]
        XML tag positions that act as local reference containers.
        Default: ("64",) meaning c64 m="1" -> position 64.1
        Extend for bank-specific local ref arrays, e.g. ("64", "90", "100")

    output_unknown_fields : bool
        If True, fields not found in metadata are still included in output.
    """

    package_root: Path

    data_dir: str = "data"
    xsd_dir: str = "xsd"
    metadata_dir: str = "metadata"
    local_ref_dir: str = "local_ref"
    customization_dir: str = "customization"
    relationships_dir: str = "relationships"

    require_xsd: bool = False
    require_standard_selection: bool = True
    require_local_ref_when_declared: bool = True
    require_relationships: bool = False
    fail_on_unmapped_field: bool = False

    local_ref_base_positions: Tuple[str, ...] = ("64",)
    output_unknown_fields: bool = True

    # ------------------------------------------------------------------ #
    # Data source selection
    # ------------------------------------------------------------------ #
    # "files"    -> read record data from data/*.xml (default, unchanged)
    # "database" -> read record data from a PostgreSQL table whose record
    #               column holds the T24 XML for each record.
    # Metadata (STANDARD.SELECTION, LOCAL.REF, ...) is ALWAYS loaded from
    # files regardless of this setting.
    source: str = "files"

    # PostgreSQL settings (used only when source == "database").
    # Connection credentials come from environment variables (.env):
    # DB_URL, or DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD.
    db_schema: str = "t24_adaptor"

    # Applications are discovered dynamically: every base table in db_schema
    # (except db_metadata_table) is one application, keyed by its own table
    # name. The table name is also the metadata row key — table and metadata
    # row share the same name, so no app->table mapping is hardcoded.

    # Name of the column holding each record's XML.
    db_record_column: str = "xmlRecord"

    # Optional: load STANDARD.SELECTION metadata from a database table
    # (same schema). One row per application, keyed by the table name:
    #   <db_metadata_key_column> (recordId) | <db_metadata_xml_column> (xmlRecord)
    # When None, metadata is read from files. When set, the DB is tried first
    # and falls back to files (with a warning) if the table/row is absent.
    db_metadata_table: Optional[str] = None
    db_metadata_key_column: str = "recordId"
    db_metadata_xml_column: str = "xmlRecord"

    # Optional: load LOCAL.REF and CUSTOMIZATION metadata from database tables
    # shaped exactly like db_metadata_table — one row per application, keyed by
    # the application/table name, with the metadata XML in the same key/xml
    # columns (db_metadata_key_column / db_metadata_xml_column). When None,
    # these are read from files (the original behaviour). When set, the DB is
    # tried first and falls back to files (with a warning) if the row is absent.
    db_local_ref_table: Optional[str] = None
    db_customization_table: Optional[str] = None

    # Output (wide result) tables are written back into the same schema with
    # this suffix, e.g. ACCOUNT -> "ACCOUNT_wide". Discovery excludes any
    # table ending with this suffix so results are never re-ingested as input.
    db_output_suffix: str = "_wide"

    # ------------------------------------------------------------------ #
    # Record identity
    # ------------------------------------------------------------------ #
    # Which field position holds the primary key, per application. T24's
    # default is field 1, but that is not always the record's own key:
    # for ACCOUNT, field 1 is CUSTOMER (the holder), while the account's
    # key is ACCOUNT.NO at position 2. Map app -> position to override.
    #   e.g. {"ACCOUNT": "2"}  ->  record_id = ACCOUNT.NO
    record_id_positions: Optional[Dict[str, str]] = None
    record_id_default_position: str = "1"

    # In database mode, use the table's own record-id column as record_id
    # (the canonical per-record key, e.g. 1, 2, 3 ...). When True this takes
    # precedence over record_id_positions. Set False to derive record_id from
    # a field position instead.
    record_id_from_db_column: bool = True
    record_id_db_column: str = "recordId"

    # ------------------------------------------------------------------ #
    # Output write behaviour (database sink)
    # ------------------------------------------------------------------ #
    # How rows are written to the result tables. BOTH modes bulk-upsert in
    # chunks of db_batch_size (one network round-trip per chunk) — neither
    # writes a row at a time, which is pathological against a remote DB.
    #   "streaming" -> bounded-memory bulk upsert (only one chunk in flight)
    #   "batching"  -> identical mechanism; explicit "throughput" alias
    # Both UPSERT (INSERT ... ON CONFLICT DO UPDATE) on db_key_column, so
    # re-runs update changed rows in place. No truncation.
    db_write_mode: str = "streaming"
    db_batch_size: int = 1000

    # Single-pass write: read+parse+normalize ONCE, buffering per application,
    # instead of the two-pass discover-then-write. Lossless and byte-identical
    # output. Bounded by db_single_pass_max_rows per app — an app larger than
    # that falls back to two-pass streaming (constant memory preserved).
    # Set db_single_pass=False to force the original two-pass path.
    db_single_pass: bool = True
    db_single_pass_max_rows: int = 200_000

    # Per-application parallelism. db_max_workers=1 keeps today's sequential
    # behaviour byte-for-byte. >1 fans applications out across a thread pool,
    # each worker owning its OWN read+write connections (psycopg2 connections
    # are not safe to share across threads). The wall-clock benefit grows with
    # N applications and per-table size; see THREADING_PLAN.md for projections.
    db_max_workers: int = 1
    # Parallelism only helps once per-table READ work dominates the per-run
    # fixed overhead + write contention. Below this many tables, a run stays
    # sequential even if db_max_workers > 1 (measured: at 2 tables, parallel is
    # not faster and adds remote-DB write contention). Tune per deployment.
    db_parallel_min_tables: int = 3
    # Postgres-side query timeout per worker session, in seconds. The only
    # reliable way to bound a hung query (Python-side thread cancellation
    # while blocked in libpq is not possible). 0 disables the timeout.
    db_statement_timeout_s: int = 600
    # Failure policy when one worker fails:
    #   "independent" (default) — others keep going; UPSERT makes runs re-runnable.
    #   "fail-fast"             — first exception cancels pending workers.
    db_failure_policy: str = "independent"

    # Conflict / sync key column for the result tables (the wide-row key).
    db_key_column: str = "recordId"

    # Full sync: after upserting, delete result rows whose key was NOT seen in
    # this run, so deletions in the source are mirrored. SAFETY-GATED — only
    # runs on a complete, unfiltered, cleanly-finished run (see db_writer).
    db_full_sync: bool = False

    # Set True whenever a run is narrowed (e.g. a date/module filter) so the
    # full-sync delete-sweep is disabled — a partial run must never delete
    # rows that were simply filtered out. Wired into the writer's guard.
    db_filters_active: bool = False

    # ------------------------------------------------------------------ #
    # Application selection
    # ------------------------------------------------------------------ #
    # Optional allowlist of application (table) names to process. When None
    # (default) every discovered application is processed — the original
    # behaviour. When set, discovery is narrowed to just these names (case-
    # insensitive), so a caller (e.g. the web console) can run a chosen
    # subset of tables. Names not present in the source are ignored.
    include_applications: Optional[Tuple[str, ...]] = None

    def resolve(self, relative_dir: str) -> Path:
        """Return absolute path for a subdirectory under package_root."""
        return self.package_root / relative_dir

    def data_path(self) -> Path:
        return self.resolve(self.data_dir)

    def metadata_path(self) -> Path:
        return self.resolve(self.metadata_dir)

    def xsd_path(self) -> Path:
        return self.resolve(self.xsd_dir)

    def local_ref_path(self) -> Path:
        return self.resolve(self.local_ref_dir)

    def customization_path(self) -> Path:
        return self.resolve(self.customization_dir)

    def relationships_path(self) -> Path:
        return self.resolve(self.relationships_dir)
