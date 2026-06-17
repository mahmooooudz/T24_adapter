"""
settings.py
-----------
T24 Generic Adapter - Runtime configuration (single source of truth).

All tunable settings for a run live here. `main.py` just calls build_config().
Database credentials are NOT here — they come from .env (DB_URL, or
DB_HOST/PORT/NAME/USER/PASSWORD), loaded via load_env().
"""

from pathlib import Path

from t24_adapter import T24PipelineConfig


def build_config() -> T24PipelineConfig:
    """Return the fully-populated pipeline configuration for this deployment."""
    return T24PipelineConfig(
        package_root=Path("t24_input_package"),

        # --- Package directory names (relative to package_root) ---
        data_dir="data",
        metadata_dir="metadata",
        local_ref_dir="local_ref",
        customization_dir="customization",
        relationships_dir="relationships",
        xsd_dir="xsd",

        # --- Validation rules ---
        require_xsd=False,
        require_standard_selection=True,
        require_local_ref_when_declared=True,
        require_relationships=False,
        fail_on_unmapped_field=False,

        # --- T24 structure handling ---
        # "64" means c64 m="1" -> position 64.1 (add "90", "100", ... as needed)
        local_ref_base_positions=("64",),
        output_unknown_fields=True,

        # --- Data source: PostgreSQL database ---
        source="database",
        db_schema="t24_adaptor",
        db_record_column="xmlRecord",

        # Metadata (STANDARD.SELECTION) loaded from the DB; table & metadata
        # row share the same name (fully dynamic discovery, no hardcoding).
        db_metadata_table="STANDARD_SELECTION",
        db_metadata_key_column="recordId",
        db_metadata_xml_column="xmlRecord",

        # LOCAL.REF and CUSTOMIZATION metadata, also from the database. Same
        # layout as STANDARD_SELECTION (one row per app, keyed by app name,
        # XML in xmlRecord). Set these to your actual table names; if a table
        # is absent the loader falls back to the file metadata with a warning.
        db_local_ref_table="LOCAL_REFERENCE",
        db_customization_table="CUSTOMIZATION",

        # Record key = the table's own recordId column.
        record_id_from_db_column=True,
        record_id_db_column="recordId",

        # --- Output write behaviour (database sink) ---
        db_output_suffix="_wide",
        db_write_mode="streaming",   # "streaming" (default) | "batching"
        db_batch_size=1000,
        db_single_pass=True,         # read+parse+normalize once (Lever 1)
        db_single_pass_max_rows=200_000,  # per-app buffer cap before two-pass fallback
        db_max_workers=1,            # 1 = sequential; >1 fans apps out across threads
        db_statement_timeout_s=600,
        db_failure_policy="independent",
        db_key_column="recordId",
        db_full_sync=True,           # mirror source deletions (safety-gated)
        db_filters_active=False,     # set True for any narrowed/partial run
    )
