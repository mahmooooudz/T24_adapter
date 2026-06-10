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
    WidePivotWriter,
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
    # 3. Output directory
    # ============================================================
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    # ============================================================
    # 4. Write WIDE output (one file per application)
    #    Two-pass streaming: constant memory regardless of file size.
    # ============================================================
    writer = WidePivotWriter()

    logger.info("Writing wide CSV output...")
    csv_outputs = writer.write_csv(pipeline, output_dir)
    for app_name, path in csv_outputs.items():
        logger.info(f"  {app_name}: {path}")

    logger.info("Writing wide JSONL output...")
    jsonl_outputs = writer.write_jsonl(pipeline, output_dir)
    for app_name, path in jsonl_outputs.items():
        logger.info(f"  {app_name}: {path}")

    # ============================================================
    # 5. Print preview table (requires pandas)
    # ============================================================
    logger.info("Printing preview table...")
    try:
        import pandas as pd  # noqa: F401
        for app_name in csv_outputs:
            df = writer.to_dataframe(pipeline, app_name)
            print(f"\n=== Wide Output: {app_name} (first 30 rows) ===\n")
            # disable_numparse: T24 values are strings (phones, IDs); keep them
            # verbatim instead of letting the table renderer coerce to floats.
            print(df.head(30).to_markdown(index=False, disable_numparse=True))
            print(f"\nTotal records: {len(df)}")
            print(f"Total columns: {len(df.columns)}")
    except ImportError:
        logger.warning("pandas not installed. Skipping preview table.")

    logger.info("=== Pipeline run complete ===")


if __name__ == "__main__":
    main()
