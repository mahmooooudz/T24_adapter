"""
main.py
-------
T24 Generic Adapter - Entry Point

Runs the metadata-driven pipeline and writes the wide (flattened) result
back into PostgreSQL — one relational table per application.

All run settings live in settings.py (build_config). Database credentials
come from .env. Run with:  python main.py

Concurrency: when config.db_max_workers > 1, applications are processed in
parallel via T24WorkerPool (each worker owns its own connections). With
db_max_workers=1 (default), behavior is identical to the sequential path.
"""

import logging
import sys
from pathlib import Path

# Add parent to path if running as script
sys.path.insert(0, str(Path(__file__).parent))

from t24_adapter import T24GenericPipeline, WideDatabaseWriter, load_env
from t24_adapter.metrics import metrics
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


def _build_writer(cfg) -> WideDatabaseWriter:
    """Construct a WideDatabaseWriter from a config. Used both for the
    sequential path and as the worker factory for the parallel pool."""
    return WideDatabaseWriter(
        schema=cfg.db_schema,
        suffix=cfg.db_output_suffix,
        write_mode=cfg.db_write_mode,
        batch_size=cfg.db_batch_size,
        key_column=cfg.db_key_column,
        full_sync=cfg.db_full_sync,
        filters_active=cfg.db_filters_active,
        single_pass=cfg.db_single_pass,
        single_pass_max_rows=cfg.db_single_pass_max_rows,
    )


def _run_parallel(config):
    """Discover applications once, then fan them out across T24WorkerPool."""
    from t24_adapter.parallel import T24WorkerPool

    # Discover the application list once (the pool ignores per-app discovery
    # inside each worker because we narrow include_applications=(app,)).
    discovery_pipeline = T24GenericPipeline(config)
    try:
        # The pipeline's discovery is part of run(); use it once to get the list.
        # We synthesize the list by reading the DB or the package directly,
        # mirroring what stage 2 of run() would do.
        if config.source == "database":
            apps = discovery_pipeline.db_reader.discover_tables(
                exclude={
                    config.db_metadata_table,
                    config.db_local_ref_table,
                    config.db_customization_table,
                },
                exclude_suffixes=(config.db_output_suffix,),
            )
        else:
            apps = discovery_pipeline.discovery.discover_applications()
    finally:
        discovery_pipeline.close()

    if config.include_applications:
        wanted = {a.upper() for a in config.include_applications}
        apps = [a for a in apps if a.upper() in wanted]

    # Below the threshold, parallelism doesn't pay off — fall back to one worker.
    workers = config.db_max_workers
    if len(apps) < config.db_parallel_min_tables:
        logger.info(
            f"Parallelism requested (workers={workers}) but only {len(apps)} "
            f"table(s); using 1 worker (parallel helps from "
            f"{config.db_parallel_min_tables}+ tables)."
        )
        workers = 1

    pool = T24WorkerPool(config, _build_writer, max_workers=workers)
    return pool.run(apps)


def main():
    # 1. Configuration (all settings live in settings.py)
    config = build_config()

    # Start a fresh latency-measurement run (per-step timings logged at the end).
    metrics.reset()

    sequential = config.db_max_workers <= 1

    try:
        if sequential:
            # Original path — one connection, one pipeline, one writer.
            pipeline = T24GenericPipeline(config)
            writer = _build_writer(config)
            logger.info(
                f"Writing wide output to the database (mode={config.db_write_mode}, "
                f"full_sync={config.db_full_sync})..."
            )
            written = writer.write(pipeline)
            for app_name, (table, upserted, deleted, cols) in written.items():
                logger.info(
                    f"  {app_name} -> {config.db_schema}.{table}  "
                    f"({upserted} upserted, {deleted} deleted, {cols} cols)"
                )
        else:
            logger.info(
                f"Writing wide output in parallel: max_workers={config.db_max_workers}, "
                f"failure_policy={config.db_failure_policy}"
            )
            outcomes = _run_parallel(config)
            failed = [a for a, o in outcomes.items() if o["status"] != "ok"]
            for app_name, o in outcomes.items():
                if o["status"] == "ok" and o["result"]:
                    table, upserted, deleted, cols = o["result"]
                    logger.info(
                        f"  {app_name} -> {config.db_schema}.{table}  "
                        f"({upserted} upserted, {deleted} deleted, {cols} cols)"
                    )
                else:
                    logger.error(f"  {app_name} -> {o['status']}: {o.get('error')}")
            if failed:
                raise RuntimeError(
                    f"{len(failed)} of {len(outcomes)} application(s) failed: {failed}"
                )
    finally:
        # Always emit the latency breakdown, even if the run failed partway.
        metrics.report(logger)

    logger.info("=== Pipeline run complete ===")


if __name__ == "__main__":
    main()
