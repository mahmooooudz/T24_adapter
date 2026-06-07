"""
main.py
-------
T24 Generic Adapter - Example Entry Point

Demonstrates full pipeline execution with CSV, JSONL, and DataFrame output.

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
    NormalizedOutputWriter,
)

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
    )

    # ============================================================
    # 2. Initialize Pipeline
    # ============================================================
    pipeline = T24GenericPipeline(config)

    # ============================================================
    # 3. Output paths
    # ============================================================
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    csv_output = output_dir / "normalized_t24_output.csv"
    jsonl_output = output_dir / "normalized_t24_output.jsonl"

    # ============================================================
    # 4. Write CSV output (streaming - constant memory usage)
    # ============================================================
    logger.info("Writing CSV output...")
    csv_rows = NormalizedOutputWriter.write_csv(
        rows=pipeline.run(),
        output_file=csv_output,
    )
    logger.info(f"CSV written: {csv_output} ({csv_rows} rows)")

    # ============================================================
    # 5. Write JSONL output (re-run pipeline - it's a generator)
    # ============================================================
    logger.info("Writing JSONL output...")
    jsonl_rows = NormalizedOutputWriter.write_jsonl(
        rows=pipeline.run(),
        output_file=jsonl_output,
    )
    logger.info(f"JSONL written: {jsonl_output} ({jsonl_rows} rows)")

    # ============================================================
    # 6. Print preview table (requires pandas)
    # ============================================================
    logger.info("Printing preview table...")
    try:
        import pandas as pd
        df = NormalizedOutputWriter.to_dataframe(pipeline.run())
        print("\n=== Normalized T24 Output (first 30 rows) ===\n")

        display_cols = [
            "app_name", "record_id", "xml_element",
            "resolved_position", "field_name",
            "mv_index", "sv_index", "value", "is_mapped"
        ]
        print(df[display_cols].head(30).to_markdown(index=False))
        print(f"\nTotal rows: {len(df)}")
        print(f"Total unique records: {df['record_id'].nunique()}")
        print(f"Mapped fields: {df['is_mapped'].sum()}")
        print(f"Unmapped fields: {(~df['is_mapped']).sum()}")

    except ImportError:
        logger.warning("pandas not installed. Skipping preview table.")

    logger.info("=== Pipeline run complete ===")


if __name__ == "__main__":
    main()
