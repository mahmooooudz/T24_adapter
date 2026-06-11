"""
main.py
-------
T24 Generic Adapter - Example Entry Point

Demonstrates full pipeline execution with WIDE-table output:
one row per record, each field_name as a column. One file per application.

Run with:
    python main.py

Adjust config settings below for your environment.
"""

import logging
import sys
from pathlib import Path

# Add parent to path if running as script
sys.path.insert(0, str(Path(__file__).parent))

from t24_adapter import (
    T24GenericPipeline,
    T24PipelineConfig,
    WideDatabaseWriter,
    load_env,
)

# Load database credentials from .env into the environment (no-op if absent).
load_env()

# ============================================================
# Logging Setup
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ]
)

logger = logging.getLogger("t24-main")


def main():
    # ============================================================
    # 1. Configure the Pipeline
    # ============================================================
    config = T24PipelineConfig(
        package_root=Path("t24_input_package"),

        # Directory names (relative to package_root)
        data_dir="data",
        metadata_dir="metadata",
        local_ref_dir="local_ref",
        customization_dir="customization",
        relationships_dir="relationships",
        xsd_dir="xsd",

        # Validation rules
        require_xsd=False,
        require_standard_selection=True,
        require_local_ref_when_declared=True,
        require_relationships=False,
        fail_on_unmapped_field=False,

        # Local reference base positions.
        # "64" means c64 m="1" -> position 64.1
        # Add "90", "100" etc. if your T24 instance uses other local ref columns
        local_ref_base_positions=("64",),

        # Include unmapped fields in output (recommended: True)
        output_unknown_fields=True,

        # ----------------------------------------------------------------
        # Data source: read records from the PostgreSQL database.
        # Metadata (STANDARD.SELECTION etc.) is still loaded from files.
        # Credentials come from .env (DB_URL or DB_HOST/PORT/NAME/USER/PASSWORD).
        # To go back to file-based input, set source="files" (or remove these).
        # ----------------------------------------------------------------
        source="database",
        db_schema="t24_adaptor",
        db_record_column="xmlRecord",

        # Applications are discovered dynamically from the schema: every table
        # except the metadata table is one application, keyed by its own name.
        # The metadata row in STANDARD_SELECTION shares that same name.
        db_metadata_table="STANDARD_SELECTION",
        db_metadata_key_column="recordId",
        db_metadata_xml_column="xmlRecord",

        # record_id = the table's own recordId column (1, 2, 3 ...), so each
        # account is its own record and a customer can own many accounts.
        record_id_from_db_column=True,
        record_id_db_column="recordId",
    )

    # ============================================================
    # 2. Initialize Pipeline
    # ============================================================
    pipeline = T24GenericPipeline(config)

    # ============================================================
    # 3. Write WIDE output back into the database (one table per app).
    #    Result tables are <APP>_wide in the same schema; the "_wide"
    #    suffix is excluded from input discovery so they are never
    #    re-ingested. DDL is managed automatically (create-if-missing,
    #    add new columns), then each table is truncated and reloaded.
    # ============================================================
    writer = WideDatabaseWriter(
        schema=config.db_schema,
        suffix=config.db_output_suffix,
    )

    logger.info("Writing wide output to the database...")
    written = writer.write(pipeline)
    for app_name, (table, rows, cols) in written.items():
        logger.info(f"  {app_name} -> {config.db_schema}.{table}  ({rows} rows x {cols} cols)")

    logger.info("=== Pipeline run complete ===")


if __name__ == "__main__":
    main()
