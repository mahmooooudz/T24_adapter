"""
parallel.py
-----------
T24 Generic Adapter - Bounded worker pool over applications.

Each worker runs ONE application's full pipeline end-to-end (discover →
metadata → single-pass read + buffer → write) in isolation:

- Its own T24GenericPipeline instance with `include_applications=(app,)`,
  so it opens its own read connection and never shares it with another worker.
- Its own WideDatabaseWriter, which opens its own write connection.
- A Postgres-side `statement_timeout` set on each session — the ONLY reliable
  way to bound a hung query, since a Python thread blocked inside libpq
  cannot be cancelled from the outside.

Why not a single shared pipeline with parallel work inside it: psycopg2
connections are NOT safe for concurrent cursors across threads. Cloning the
pipeline per worker sidesteps the question entirely — each worker is
independent, and the only shared state is the (thread-safe) `RunMetrics`.

Failure policy
--------------
- "independent" (default): a worker exception aborts only that app's transaction
  (UPSERT keeps re-runs idempotent). The pool collects errors and returns them.
- "fail-fast": the first exception cancels every not-yet-running worker. Already
  running workers finish (they cannot be cancelled mid-query) but their results
  are still collected.

Scheduling
----------
Applications are dispatched **largest-first** using `pg_class.reltuples`
(zero-scan estimate, already used by the adaptive read). Big tables start at
t=0 and small ones backfill the tail — the standard fix for skewed workloads.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from dataclasses import replace
from typing import Dict, List, Tuple

from .config import T24PipelineConfig
from .db_reader import _connect_from_env
from .metrics import metrics

logger = logging.getLogger("t24-adapter.parallel")


# ---------------------------------------------------------------------------
# Connection registration callback — set by the web console (app.py) so that
# adapter log lines emitted from worker threads route to the correct run's
# SSE stream. Pure-CLI runs ignore this (the default no-op).
# ---------------------------------------------------------------------------

_register_worker_thread = lambda: None     # noqa: E731  (override at runtime)
_deregister_worker_thread = lambda: None   # noqa: E731


def set_worker_thread_hooks(register, deregister) -> None:
    """Install (de)register hooks called inside each worker thread.
    Used by the web console to route worker log lines into its SSE stream."""
    global _register_worker_thread, _deregister_worker_thread
    _register_worker_thread = register or (lambda: None)
    _deregister_worker_thread = deregister or (lambda: None)


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------

class T24WorkerPool:
    """Run N applications in parallel via a bounded ThreadPoolExecutor.

    Each application becomes a self-contained per-worker task: own pipeline,
    own connections, own writer. The outer caller hands in:

    - `config`              : the validated, complete T24PipelineConfig
    - `writer_factory(cfg)` : callable returning a fresh WideDatabaseWriter
                              bound to `cfg` (the caller owns the writer's
                              construction so this module needn't import it)
    """

    def __init__(
        self,
        config: T24PipelineConfig,
        writer_factory,
        *,
        max_workers: int,
    ):
        if max_workers < 1:
            raise ValueError(f"max_workers must be >= 1, got {max_workers}")
        self.config = config
        self.writer_factory = writer_factory
        self.max_workers = max_workers
        self._stop = threading.Event()       # fail-fast trigger
        # Shared prework, computed once on the orchestrator thread (run()) and
        # handed to every worker via pipeline.preload() — avoids each of K
        # workers re-validating, re-discovering, and re-fetching all metadata.
        self._shared_registries: Dict[str, object] = {}

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def run(self, apps: List[str]) -> Dict[str, dict]:
        """Process `apps` in parallel; return a dict per app.

        Return shape: {app_name: {"status": "ok"|"failed",
                                  "result": (table, upserted, deleted, cols) | None,
                                  "error":  "...stringified exception..." | None}}
        """
        if not apps:
            return {}
        ordered = self._largest_first(apps)
        eff_workers = self._cap_workers_to_db_headroom(
            min(self.max_workers, len(ordered))
        )
        # Shared prework: validate + prefetch all metadata ONCE for all workers,
        # not N times (one per worker). Each worker then calls pipeline.preload()
        # to skip those stages. Cuts ~0.5-1s off K=2 wall-clock at this scale.
        self._prepare_shared_state(ordered)
        logger.info(
            f"T24WorkerPool: {len(ordered)} app(s), max_workers={eff_workers}, "
            f"failure_policy={self.config.db_failure_policy}, "
            f"dispatch_order={ordered}"
        )

        results: Dict[str, dict] = {}
        fail_fast = self.config.db_failure_policy == "fail-fast"

        with ThreadPoolExecutor(max_workers=eff_workers, thread_name_prefix="t24-app") as ex:
            futures = {ex.submit(self._run_one, app): app for app in ordered}
            for fut in as_completed(futures):
                app = futures[fut]
                if app in results:
                    # Already marked (e.g. cancelled by fail-fast); don't overwrite.
                    continue
                try:
                    results[app] = {
                        "status": "ok", "result": fut.result(), "error": None,
                    }
                    logger.info(f"[{app}] worker finished OK")
                except CancelledError:
                    results[app] = {
                        "status": "cancelled", "result": None,
                        "error": "cancelled before start",
                    }
                except Exception as exc:
                    results[app] = {
                        "status": "failed", "result": None,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                    logger.error(f"[{app}] worker failed: {exc}")
                    if fail_fast:
                        logger.warning("fail-fast: cancelling pending workers")
                        self._stop.set()
                        for other_fut, other_app in futures.items():
                            if other_fut is fut or other_app in results:
                                continue
                            if other_fut.cancel():
                                results[other_app] = {
                                    "status": "cancelled", "result": None,
                                    "error": "cancelled by fail-fast policy",
                                }
        return results

    # ------------------------------------------------------------------ #
    # One worker = one application
    # ------------------------------------------------------------------ #

    def _run_one(self, app_name: str):
        """Execute one application's pipeline end-to-end in this worker."""
        if self._stop.is_set():
            raise RuntimeError("worker pool stopped before this task started")

        # Hook for the web console: register the worker thread BEFORE any
        # adapter log call so log lines route into the active run's SSE stream.
        _register_worker_thread()
        try:
            # Build a per-worker config narrowed to this single app. The shared
            # config is not mutated; we copy it via dataclasses.replace().
            worker_cfg = replace(
                self.config,
                include_applications=(app_name,),
                # Workers each open their own connections; the shared-conn
                # optimisation is preserved per-worker but not across them.
            )

            # Late imports avoid the parallel module pulling all of these in
            # whenever someone imports the package for read-only purposes.
            from .pipeline import T24GenericPipeline

            with metrics.span("pass_single", app=app_name):
                pipeline = T24GenericPipeline(worker_cfg)
                # Inject the pool's shared prework so this worker skips the
                # already-done validation + discovery + metadata fetch.
                reg = self._shared_registries.get(app_name.upper())
                pipeline.preload(
                    applications=[app_name],
                    registries={app_name.upper(): reg} if reg is not None else None,
                    skip_validation=True,
                )
                # Apply Postgres-side query timeout on the read connection's
                # session, so a hung SELECT is killed by the server.
                self._apply_statement_timeout(pipeline._read_connection())
                writer = self.writer_factory(worker_cfg)
                try:
                    written = writer.write(pipeline)
                except Exception:
                    # writer.write() closes the pipeline on success; on error
                    # release the read connection explicitly so we don't leak.
                    pipeline.close()
                    raise

            # writer.write() returns {app: (table, upserted, deleted, cols)};
            # with include_applications=(app,) only one entry will be present.
            return next(iter(written.values())) if written else None
        finally:
            _deregister_worker_thread()

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _prepare_shared_state(self, apps: List[str]) -> None:
        """One-shot validation + metadata prefetch on the orchestrator thread.

        Builds a registry for each app from the BULK metadata maps (one
        fetch_all per metadata table — not per-app) and stashes them so each
        worker can pipeline.preload() instead of re-doing the work. Also runs
        package validation once.

        On any failure the registries dict is left empty; workers will then
        fall back to the per-worker prefetch (correct, just not optimised).
        """
        import time
        # Late imports for the same reason as elsewhere in this module.
        from .pipeline import T24GenericPipeline, T24MetadataOrchestrator
        from .validation import T24PackageValidator
        from .discovery import T24PackageDiscovery

        try:
            t0 = time.perf_counter()
            # 1) Validate the package once (cheap; safe to call here).
            res = T24PackageValidator(self.config).validate_package()
            if not res.is_valid:
                # Don't pre-do anything else; let the workers surface this.
                logger.warning(
                    f"shared-state: package validation failed "
                    f"({len(res.errors)} error(s)); workers will re-validate."
                )
                return

            # 2) Build ONE primer pipeline so we get a single shared read
            # connection for the orchestrator's bulk metadata prefetch.
            primer = T24GenericPipeline(self.config)
            try:
                conn = primer._read_connection()
                if primer.db_reader is not None:
                    primer.db_reader._conn = conn

                orch = T24MetadataOrchestrator(
                    self.config, T24PackageDiscovery(self.config), connection=conn,
                )
                # 3) One bulk SELECT per metadata table (3 round-trips total),
                # regardless of how many apps we have.
                orch._ensure_prefetched()

                # 4) Per-app registry, all from the prefetched maps.
                for app in apps:
                    reg = orch.load_for_application(app)
                    self._shared_registries[app.upper()] = reg
            finally:
                primer.close()
            logger.info(
                f"shared-state: pre-validated + pre-loaded metadata for "
                f"{len(self._shared_registries)} app(s) in "
                f"{time.perf_counter() - t0:.3f}s "
                f"(workers will skip those stages)"
            )
        except Exception as exc:
            logger.warning(
                f"shared-state prep failed ({type(exc).__name__}: {exc}); "
                f"workers will fall back to per-worker prefetch."
            )
            self._shared_registries = {}

    def _cap_workers_to_db_headroom(self, desired: int) -> int:
        """Safety cap: never let the pool consume more than 25% of the DB's
        connection headroom (2 connections per worker: read + write).

        The shared remote server also serves other consumers — an unchecked
        K can hit `FATAL: too many connections` and break unrelated workloads.
        We lower K silently here only if the math demands it; logs say why.
        """
        if desired <= 1:
            return desired
        try:
            conn = _connect_from_env()
            try:
                with conn.cursor() as cur:
                    cur.execute("SHOW max_connections")
                    max_conns = int(cur.fetchone()[0])
                    cur.execute("SELECT count(*) FROM pg_stat_activity")
                    in_use = int(cur.fetchone()[0])
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(
                f"Could not read DB connection headroom ({type(exc).__name__}): "
                f"{exc}; keeping desired K={desired}."
            )
            return desired

        headroom = max(1, max_conns - in_use)
        # 2 connections per worker; cap at 25% of headroom so we leave plenty
        # for the server's other consumers and any spikes.
        budget = max(1, int(0.25 * headroom) // 2)
        capped = min(desired, budget)
        if capped < desired:
            logger.warning(
                f"DB connection headroom is tight (max_connections={max_conns}, "
                f"in_use={in_use}, headroom={headroom}). Lowering max_workers "
                f"{desired} -> {capped} to stay under 25% of headroom."
            )
        return capped

    def _apply_statement_timeout(self, conn) -> None:
        """SET LOCAL statement_timeout on the given connection's current txn,
        so a hung query is killed by Postgres instead of pinning a thread."""
        if not self.config.db_statement_timeout_s or conn is None:
            return
        ms = int(self.config.db_statement_timeout_s) * 1000
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {ms}")  # int literal: safe

    def _largest_first(self, apps: List[str]) -> List[str]:
        """Sort `apps` by estimated row count (pg_class.reltuples), descending.
        Unknown estimates sort last so they don't accidentally dominate. The
        helper is reused from db_reader (zero new SQL paths)."""
        from .db_reader import T24DatabaseDataReader

        # One short-lived connection for the estimate query is fine — it's a
        # one-time, sub-second cost amortised over the whole pool run.
        conn = _connect_from_env()
        try:
            reader = T24DatabaseDataReader(
                schema=self.config.db_schema, connection=conn,
            )
            sizes: List[Tuple[str, int]] = [
                (app, reader._estimate_rows(conn, app)) for app in apps
            ]
        finally:
            conn.close()

        def sort_key(item):
            app, est = item
            # Unknown (-1) → sort to the END (huge negative -> last).
            return (-est if est >= 0 else float("inf"), app)

        ordered = sorted(sizes, key=sort_key)
        logger.info(
            "Largest-first dispatch order: "
            + ", ".join(f"{a}(~{e})" for a, e in ordered)
        )
        return [a for a, _ in ordered]
