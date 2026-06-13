"""
main.py
-------
T24 Generic Adapter - Entry Point

Runs the metadata-driven pipeline and writes the wide (flattened) result
back into PostgreSQL — one relational table per application.

All run settings live in settings.py (build_config). Database credentials
come from .env. Run with:  python main.py
"""

import logging
import sys
from pathlib import Path

# Add parent to path if running as script
sys.path.insert(0, str(Path(__file__).parent))

from t24_adapter import T24GenericPipeline, WideDatabaseWriter, load_env
from settings import build_config

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
    # 1. Configuration (all settings live in settings.py)
    config = build_config()

    # 2. Pipeline
    pipeline = T24GenericPipeline(config)

    # 3. Write the wide output back into the database (one table per app).
    #    Result tables are <APP>_wide in the same schema; the "_wide" suffix
    #    is excluded from input discovery so they are never re-ingested.
    #    Rows are UPSERTed (insert new, update changed) — no truncation.
    #    With full sync on, rows whose key vanished from the source are
    #    deleted too (safety-gated to complete, unfiltered, clean runs).
    writer = WideDatabaseWriter(
        schema=config.db_schema,
        suffix=config.db_output_suffix,
        write_mode=config.db_write_mode,
        batch_size=config.db_batch_size,
        key_column=config.db_key_column,
        full_sync=config.db_full_sync,
        filters_active=config.db_filters_active,
    )

    logger.info(f"Writing wide output to the database (mode={config.db_write_mode}, "
                f"full_sync={config.db_full_sync})...")
    written = writer.write(pipeline)
    for app_name, (table, upserted, deleted, cols) in written.items():
        logger.info(
            f"  {app_name} -> {config.db_schema}.{table}  "
            f"({upserted} upserted, {deleted} deleted, {cols} cols)"
        )

    logger.info("=== Pipeline run complete ===")


if __name__ == "__main__":
    main()
