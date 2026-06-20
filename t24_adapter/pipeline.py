"""
pipeline.py
-----------
T24 Generic Adapter - Master Pipeline Orchestrator

Ties all modules together into a single, end-to-end pipeline.

Pipeline stages (in order):
1. Validate package  (T24PackageValidator)
2. Discover apps     (T24PackageDiscovery)
3. Load metadata     (T24MetadataOrchestrator)
4. Stream records    (T24StreamingDataReader)
5. Normalize fields  (T24Normalizer)
6. Yield output      (Iterator[NormalizedField])

The pipeline is a Python generator: it yields one NormalizedField
at a time, so memory usage is constant regardless of file size.

Usage
-----
from t24_adapter import T24GenericPipeline, WideDatabaseWriter
from settings import build_config

config = build_config()
pipeline = T24GenericPipeline(config)

# pipeline.run() is a generator of NormalizedField; the sink consumes it.
WideDatabaseWriter(schema=config.db_schema).write(pipeline)
"""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Dict, Iterator, List, Optional

from .config import T24PipelineConfig
from .discovery import T24PackageDiscovery
from .metadata_loaders import (
    CustomizationLoader,
    LocalReferenceLoader,
    RelationshipLoader,
    StandardSelectionLoader,
)
from .metadata_registry import T24MetadataRegistry
from .metrics import metrics
from .models import NormalizedField
from .normalizer import T24Normalizer
from .reader import T24StreamingDataReader
from .db_reader import T24DatabaseDataReader, T24DatabaseMetadataReader, _connect_from_env
from .validation import T24PackageValidator

logger = logging.getLogger("t24-adapter.pipeline")


class T24MetadataOrchestrator:
    """
    Loads all metadata sources for a given T24 application.

    Load order (later sources override earlier if same position):
    1. STANDARD.SELECTION (core field definitions)
    2. LOCAL.REF           (local reference field extensions)
    3. CUSTOMIZATION       (bank-specific custom fields)
    4. RELATIONSHIPS       (explicit foreign key relationships)

    The orchestrator also validates that local reference metadata
    is accessible before data extraction begins.
    """

    def __init__(
        self,
        config: T24PipelineConfig,
        discovery: T24PackageDiscovery,
        connection=None,
    ):
        self.config = config
        self.discovery = discovery
        self.registry = T24MetadataRegistry()
        # Optional database-backed STANDARD.SELECTION source.
        self.metadata_reader = self._make_db_reader(config, config.db_metadata_table, connection)
        # Optional database-backed LOCAL.REF and CUSTOMIZATION sources. These
        # tables share STANDARD_SELECTION's layout (one row per app, keyed by
        # the app name, metadata XML in the same key/xml columns).
        self.local_ref_reader = self._make_db_reader(config, config.db_local_ref_table, connection)
        self.customization_reader = self._make_db_reader(config, config.db_customization_table, connection)
        # Bulk-prefetched metadata maps ({app_name: xml}), filled once on first
        # use — one query per metadata table instead of one per (table, app).
        self._prefetched = False
        self._std_all: dict = {}
        self._lref_all: dict = {}
        self._cust_all: dict = {}

    def _ensure_prefetched(self) -> None:
        """Fetch all rows of each configured metadata table once (3 queries
        total), not lazily per application."""
        if self._prefetched:
            return
        if self.metadata_reader is not None:
            self._std_all = self.metadata_reader.fetch_all()
        if self.local_ref_reader is not None:
            self._lref_all = self.local_ref_reader.fetch_all()
        if self.customization_reader is not None:
            self._cust_all = self.customization_reader.fetch_all()
        self._prefetched = True

    @staticmethod
    def _make_db_reader(config: T24PipelineConfig, table, connection=None):
        """Build a metadata reader for `table`, or None if not in DB mode / unset."""
        if config.source == "database" and table:
            return T24DatabaseMetadataReader(
                schema=config.db_schema,
                table=table,
                key_column=config.db_metadata_key_column,
                xml_column=config.db_metadata_xml_column,
                connection=connection,
            )
        return None

    def load_for_application(self, app_name: str) -> T24MetadataRegistry:
        """
        Load all available metadata for one T24 application.

        Parameters
        ----------
        app_name : T24 application name (e.g. "CUSTOMER")

        Returns
        -------
        T24MetadataRegistry populated with all available metadata
        """
        relationship_file = self.discovery.get_relationship_file(app_name)

        # Prefetch all metadata tables once (no-op after the first app).
        self._ensure_prefetched()

        # --- STANDARD.SELECTION (database first if configured, else file) ---
        self._load_standard_selection(app_name)

        # --- LOCAL.REF (database first if configured, else file) ---
        self._load_local_ref(app_name)

        # --- CUSTOMIZATION (database first if configured, else file) ---
        self._load_customization(app_name)

        # --- RELATIONSHIPS (optional or required) ---
        if relationship_file:
            RelationshipLoader().load(app_name, relationship_file, self.registry)
        elif self.config.require_relationships:
            raise FileNotFoundError(
                f"Missing relationship metadata for application: {app_name}"
            )

        # --- Validate local reference accessibility ---
        self._validate_local_ref_coverage(app_name)

        logger.debug(self.registry.summary(app_name))
        return self.registry

    def _load_standard_selection(self, app_name: str) -> None:
        """
        Load STANDARD.SELECTION for one application.

        Order of precedence:
        1. Database metadata table, if configured (source == "database" and
           db_metadata_table set).
        2. File metadata (always the fallback, and the only path in files mode).

        If the database is configured but has no row for this app (or the
        table does not exist yet), a warning is logged and the file is used.
        """
        # 1) Try the database metadata table first (from the prefetched map).
        if self.metadata_reader is not None:
            xml_text = self._std_all.get(app_name)
            if xml_text:
                source = (
                    f"{self.config.db_schema}.{self.config.db_metadata_table}"
                    f"#{app_name}"
                )
                StandardSelectionLoader().load_from_string(
                    app_name, xml_text, self.registry, source=source
                )
                return
            logger.warning(
                f"[{app_name}] No STANDARD.SELECTION row in "
                f"{self.config.db_schema}.{self.config.db_metadata_table}; "
                f"falling back to file metadata. Run the standard-selection "
                f"migration to complete the move to the database."
            )

        # 2) File metadata (fallback, or the only path in files mode).
        standard_file = self.discovery.get_metadata_file(app_name)
        if self.config.require_standard_selection and not standard_file:
            raise FileNotFoundError(
                f"Missing STANDARD.SELECTION metadata for application: {app_name}. "
                f"No database row and no file in: {self.config.metadata_path()}"
            )
        if standard_file:
            StandardSelectionLoader().load(app_name, standard_file, self.registry)

    def _load_local_ref(self, app_name: str) -> None:
        """
        Load LOCAL.REF for one application: database table first (if
        db_local_ref_table is configured), else the local_ref/ file. A missing
        DB row falls back to the file; a missing file is only a warning.
        """
        if self.local_ref_reader is not None:
            xml_text = self._lref_all.get(app_name)
            if xml_text:
                source = (
                    f"{self.config.db_schema}.{self.config.db_local_ref_table}"
                    f"#{app_name}"
                )
                LocalReferenceLoader().load_from_string(
                    app_name, xml_text, self.registry, source=source
                )
                return
            logger.warning(
                f"[{app_name}] No LOCAL.REF row in "
                f"{self.config.db_schema}.{self.config.db_local_ref_table}; "
                f"falling back to file metadata."
            )

        local_ref_file = self.discovery.get_local_ref_file(app_name)
        if local_ref_file:
            LocalReferenceLoader().load(app_name, local_ref_file, self.registry)
        else:
            logger.warning(
                f"No LOCAL.REF metadata for {app_name} (no database row and no "
                f"file). Local reference fields (c64 m-values) may appear as "
                f"FIELD_64.N unless defined in STANDARD.SELECTION."
            )

    def _load_customization(self, app_name: str) -> None:
        """
        Load CUSTOMIZATION for one application: database table first (if
        db_customization_table is configured), else the customization/ file.
        Both sources are optional.
        """
        if self.customization_reader is not None:
            xml_text = self._cust_all.get(app_name)
            if xml_text:
                source = (
                    f"{self.config.db_schema}.{self.config.db_customization_table}"
                    f"#{app_name}"
                )
                CustomizationLoader().load_from_string(
                    app_name, xml_text, self.registry, source=source
                )
                return
            logger.warning(
                f"[{app_name}] No CUSTOMIZATION row in "
                f"{self.config.db_schema}.{self.config.db_customization_table}; "
                f"falling back to file metadata."
            )

        customization_file = self.discovery.get_customization_file(app_name)
        if customization_file:
            CustomizationLoader().load(app_name, customization_file, self.registry)

    def _validate_local_ref_coverage(self, app_name: str) -> None:
        """
        Log a warning if no local reference metadata is registered.
        This prevents silent wrong mapping when data contains c64 m-values.
        """
        local_ref_fields = self.registry.list_local_ref_fields(app_name)

        if not local_ref_fields:
            logger.warning(
                f"[{app_name}] No local reference metadata detected. "
                f"If data contains c64 m-values, they will be emitted as FIELD_64.N. "
                f"Add metadata in local_ref/ or metadata/ to resolve them."
            )
        else:
            logger.debug(
                f"[{app_name}] {len(local_ref_fields)} local reference field(s) registered "
                f"and accessible for lookup."
            )


class T24GenericPipeline:
    """
    End-to-end T24 XML data extraction and normalization pipeline.

    This is the main entry point for all pipeline operations.
    It is a generator-based pipeline: calling run() returns a lazy
    iterator that produces one NormalizedField at a time.

    Parameters
    ----------
    config : T24PipelineConfig - all pipeline settings

    Usage
    -----
    config = T24PipelineConfig(package_root=Path("t24_input_package"))
    pipeline = T24GenericPipeline(config)

    for field in pipeline.run():
        print(field.field_name, field.value)
    """

    def __init__(self, config: T24PipelineConfig):
        self.config = config
        self.validator = T24PackageValidator(config)
        self.discovery = T24PackageDiscovery(config)
        self.reader = T24StreamingDataReader()
        # One shared read connection for the whole run, opened lazily. All DB
        # reads (discovery, metadata, streaming) reuse it instead of opening a
        # new connection per step; closed by close()/the context manager.
        self._read_conn = None
        # Per-application metadata registry cache. The wide writer calls run()
        # twice (schema discovery + write); metadata is static within a run, so
        # the second pass reuses the first pass's registry instead of re-fetching
        # STANDARD.SELECTION/LOCAL.REF/CUSTOMIZATION (3 round-trips per app).
        self._registry_cache: dict = {}
        # Optional preload state: when set, the corresponding stage is skipped.
        # Populated by preload() — used by T24WorkerPool to share validation,
        # discovery and metadata across workers (one fetch for all of them
        # instead of N duplicates).
        self._skip_validation: bool = False
        self._preset_applications: Optional[list] = None
        self.db_reader = (
            T24DatabaseDataReader(
                schema=config.db_schema,
                record_column=config.db_record_column,
                record_id_column=config.record_id_db_column,
                # Tables that fit (single-pass) are read with a plain cursor
                # (one round-trip) instead of a server-side streaming cursor.
                buffered_read_max_rows=(
                    config.db_single_pass_max_rows if config.db_single_pass else 0
                ),
            )
            if config.source == "database"
            else None
        )

    def _read_connection(self):
        """Lazily open (once) and return the shared read connection, or None
        when not reading from a database."""
        if self.config.source != "database":
            return None
        if self._read_conn is None or getattr(self._read_conn, "closed", 0):
            self._read_conn = _connect_from_env()
        return self._read_conn

    def close(self) -> None:
        """Close the shared read connection. Safe to call multiple times."""
        if self._read_conn is not None and not getattr(self._read_conn, "closed", 1):
            self._read_conn.close()
        self._read_conn = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------ #
    # Preload (skip already-done pre-work)
    # ------------------------------------------------------------------ #

    def preload(
        self,
        *,
        applications: Optional[List[str]] = None,
        registries: Optional[Dict[str, T24MetadataRegistry]] = None,
        skip_validation: bool = False,
    ) -> None:
        """Inject pre-computed pipeline state so run() can skip the matching
        stages.

        Used by T24WorkerPool: the pool validates the package, discovers
        applications, and prefetches all metadata ONCE on the orchestrator
        thread, then hands those results to every worker. Without this, each
        of N workers would redo all three independently.

        Parameters
        ----------
        applications     : final list of apps run() should process; bypasses
                           Stage 2 discovery if provided.
        registries       : {app_name: T24MetadataRegistry}; populates
                           _registry_cache so Stage 3 metadata is reused
                           instead of re-fetched.
        skip_validation  : skip Stage 1 entirely. Safe when the pool has
                           already validated the package up-front.
        """
        if skip_validation:
            self._skip_validation = True
        if applications is not None:
            self._preset_applications = list(applications)
        if registries:
            for app, reg in registries.items():
                self._registry_cache[app.upper()] = reg

    def run(self) -> Iterator[NormalizedField]:
        """
        Execute the full pipeline and yield NormalizedField records.

        Stages:
        1. Validate package structure and XML well-formedness
        2. Discover T24 applications from data directory
        3. For each application:
           a. Load all metadata (STANDARD.SELECTION, LOCAL.REF, CUSTOMIZATION, RELATIONSHIPS)
           b. Stream data records from XML
           c. Normalize each record
           d. Yield each NormalizedField

        Raises
        ------
        RuntimeError if package validation fails
        FileNotFoundError if required metadata is missing
        """

        # Open (once) the shared read connection and inject it into the data
        # reader so discovery + streaming reuse it. The metadata orchestrator
        # below receives the same connection. Owned by this pipeline; closed by
        # close()/the context manager, so it persists across re-runs (the
        # writers call run() more than once).
        read_conn = self._read_connection()
        if self.db_reader is not None:
            self.db_reader._conn = read_conn

        # === STAGE 1: Package Validation ===
        if self._skip_validation:
            logger.info("=== STAGE 1: Package Validation (skipped — already validated) ===")
        else:
            logger.info("=== STAGE 1: Package Validation ===")
            with metrics.span("validate"):
                validation_result = self.validator.validate_package()

            if not validation_result.is_valid:
                for error in validation_result.errors:
                    logger.error(error)
                raise RuntimeError(
                    f"T24 package validation failed with {len(validation_result.errors)} error(s). "
                    f"Check logs for details."
                )

            for warning in validation_result.warnings:
                logger.warning(warning)

        # === STAGE 2: Application Discovery ===
        if self._preset_applications is not None:
            logger.info(
                "=== STAGE 2: Application Discovery (preloaded — skipping query) ==="
            )
            applications = list(self._preset_applications)
        else:
            logger.info("=== STAGE 2: Application Discovery ===")
            if self.config.source == "database":
                # Dynamic: every base table in the schema (except the metadata
                # table) is an application, keyed by its own name. No hardcoding.
                with metrics.span("discover_apps"):
                    applications = self.db_reader.discover_tables(
                        exclude={
                            self.config.db_metadata_table,
                            self.config.db_local_ref_table,
                            self.config.db_customization_table,
                        },
                        exclude_suffixes=(self.config.db_output_suffix,),
                    )
                if not applications:
                    raise RuntimeError(
                        f"No data tables found in schema '{self.config.db_schema}' "
                        f"(excluding metadata table '{self.config.db_metadata_table}')."
                    )
            else:
                with metrics.span("discover_apps"):
                    applications = self.discovery.discover_applications()
                if not applications:
                    raise RuntimeError(
                        "No T24 XML data files found in the data directory. "
                        "Ensure data/*.xml files exist."
                    )

        # Optional allowlist: narrow to a caller-chosen subset of applications
        # (e.g. the web console's table selection). Matching is case-insensitive.
        if self.config.include_applications:
            wanted = {a.upper() for a in self.config.include_applications}
            selected = [a for a in applications if a.upper() in wanted]
            missing = wanted - {a.upper() for a in applications}
            if missing:
                logger.warning(
                    f"Requested applications not found in source and skipped: "
                    f"{sorted(missing)}"
                )
            applications = selected
            if not applications:
                raise RuntimeError(
                    "None of the requested applications "
                    f"({sorted(wanted)}) were found in the source."
                )

        logger.info(f"Applications to process: {applications}")

        # === STAGE 3-5: Per-Application Processing ===
        metadata_orchestrator = T24MetadataOrchestrator(
            config=self.config,
            discovery=self.discovery,
            connection=read_conn,
        )

        for app_name in applications:
            app_t0 = perf_counter()
            logger.info(f"━━ {app_name} ━━")

            # Resolve the record source for this application. Both branches
            # produce a stream of (record_id, row_element) pairs. In database
            # mode record_id is the DB recordId column; in file mode it is
            # None (the normalizer then derives it from a field position).
            if self.config.source == "database":
                # The application name IS the table name (and the metadata row
                # name) — they share the same identifier by design.
                record_stream = self.db_reader.stream_records(app_name)
                source_label = f"{self.config.db_schema}.{app_name}"
            else:
                data_file = self.discovery.get_data_file(app_name)
                if not data_file:
                    logger.warning(f"Data file not found for {app_name}, skipping.")
                    continue
                record_stream = (
                    (None, elem) for elem in self.reader.stream_records(data_file)
                )
                source_label = str(data_file)

            use_db_record_id = (
                self.config.source == "database"
                and self.config.record_id_from_db_column
            )

            # === STAGE 3: Load Metadata (cached across passes) ===
            registry = self._registry_cache.get(app_name.upper())
            if registry is None:
                meta_t0 = perf_counter()
                with metrics.span("metadata_fetch"):
                    registry = metadata_orchestrator.load_for_application(app_name)
                self._registry_cache[app_name.upper()] = registry
                n_fields = len(registry.list_fields(app_name))
                n_lref = len(registry.list_local_ref_fields(app_name))
                logger.info(
                    f"   metadata    {n_fields} fields, {n_lref} local-ref"
                    f"   ({perf_counter() - meta_t0:.2f}s)"
                )
            else:
                logger.info("   metadata    reused from cache")

            # === STAGE 4 & 5: Stream + Normalize ===
            normalizer = T24Normalizer(registry=registry, config=self.config)
            read_t0 = perf_counter()
            record_count = 0
            field_count = 0

            for db_record_id, record in record_stream:
                record_count += 1
                normalized_fields = normalizer.normalize_record(
                    app_name=app_name,
                    row=record,
                    source_file=source_label,
                    record_id=db_record_id if use_db_record_id else None,
                )

                for normalized_field in normalized_fields:
                    field_count += 1
                    yield normalized_field

            logger.info(
                f"   read+norm   {record_count:,} records, {field_count:,} fields"
                f"   ({perf_counter() - read_t0:.2f}s)"
            )
            logger.info(f"   ✓ {app_name} processed in {perf_counter() - app_t0:.2f}s")

        logger.info("=== Pipeline Complete ===")
