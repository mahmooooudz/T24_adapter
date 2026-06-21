"""
app.py
------
T24 Generic Adapter - Web Console backend (Flask).

Serves the single-page flattening console (t24_flattening_console.html) and
exposes a small JSON/SSE API that drives the REAL metadata-driven pipeline
defined in t24_adapter/. Nothing here simulates data: table lists, progress,
record counts and result stats all come from the actual pipeline run.

Endpoints
---------
GET  /                         -> serve the console UI
POST /api/test-connection      -> real connectivity check (PostgreSQL / files)
POST /api/tables               -> discover applications + real record counts
POST /api/run                  -> start a flattening run (background thread)
GET  /api/run/<id>/stream      -> Server-Sent Events: live log + progress
GET  /api/run/<id>             -> run status / result summary (polling fallback)
POST /api/run/<id>/stop        -> request a running job to stop
GET  /api/download/<id>/<app>  -> download a produced output file

Data source
-----------
The console's "source" can be:
  - "database": PostgreSQL. Connection comes from the Step-1 form fields, which
    this server injects into the environment the adapter reads (DB_HOST/PORT/...).
    Only PostgreSQL is supported by the adapter today; Oracle / SQL Server are
    rejected with a clear message (stubbed, not faked).
  - "files": the local t24_input_package/data/*.xml package. Lets the whole
    flow run end-to-end without a live database.

Features the UI shows that the backend does NOT yet support are reported
honestly (HTTP 501 / a clear message) rather than mocked:
  - Excel (xlsx) output
  - date / module range filters, data preview
  - per-toggle multi-value / sub-value / local-ref control (always on)
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from flask import (
    Flask,
    Response,
    jsonify,
    request,
    send_file,
    send_from_directory,
    stream_with_context,
)

from t24_adapter import (
    T24GenericPipeline,
    T24PipelineConfig,
    T24StreamingDataReader,
    WideDatabaseWriter,
    WidePivotWriter,
    load_env,
)

# Load any DB_* defaults from .env (real env still wins).
load_env()

BASE_DIR = Path(__file__).parent
PACKAGE_ROOT = BASE_DIR / "t24_input_package"
OUTPUT_ROOT = BASE_DIR / "output" / "web_runs"
CONSOLE_HTML = "t24_flattening_console.html"

app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path="")
logger = logging.getLogger("t24-web")


# ====================================================================== #
# Saved connections — persisted to a local file
# ====================================================================== #
# Saved connections used to live only in the browser's localStorage, which is
# scoped per origin (including the port). The console's free-port fallback can
# bind a different port each launch, so that store appeared to vanish on every
# restart. Persisting them server-side to a local JSON file makes "Save
# connection" durable across restarts, independent of browser/port.
#
# A password is written ONLY when the connection opted into "remember"; without
# it the password is never stored on disk. The file is git-ignored.

CONNECTIONS_FILE = BASE_DIR / "connections.json"
_CONN_LOCK = threading.Lock()


def _load_connections() -> List[dict]:
    """Return the saved connections list (empty if the file is absent/corrupt)."""
    try:
        with CONNECTIONS_FILE.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, list) else []
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_connections(conns: List[dict]) -> None:
    """Write the connections list atomically (temp file + replace)."""
    tmp = CONNECTIONS_FILE.with_name(CONNECTIONS_FILE.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(conns, fh, indent=2)
    tmp.replace(CONNECTIONS_FILE)


@app.route("/api/connections", methods=["GET"])
def list_connections():
    """Return all saved connections."""
    with _CONN_LOCK:
        return jsonify(ok=True, connections=_load_connections())


@app.route("/api/connections", methods=["POST"])
def upsert_connection():
    """Create or update one saved connection (keyed by its id)."""
    conn = request.get_json(force=True, silent=True) or {}
    name = str(conn.get("name") or "").strip()
    conn_id = str(conn.get("id") or "").strip()
    if not name:
        return jsonify(ok=False, message="Connection name is required."), 400
    if not conn_id:
        return jsonify(ok=False, message="Connection id is required."), 400
    conn["name"] = name
    conn["id"] = conn_id
    # Never persist a password unless the connection explicitly opted in.
    if not conn.get("remember"):
        conn.pop("password", None)
    with _CONN_LOCK:
        conns = _load_connections()
        idx = next((i for i, c in enumerate(conns) if c.get("id") == conn_id), None)
        if idx is None:
            conns.append(conn)
        else:
            conns[idx] = conn
        _save_connections(conns)
    return jsonify(ok=True, connections=conns)


@app.route("/api/connections/<conn_id>", methods=["DELETE"])
def delete_connection(conn_id):
    """Delete one saved connection by id."""
    with _CONN_LOCK:
        conns = [c for c in _load_connections() if c.get("id") != conn_id]
        _save_connections(conns)
    return jsonify(ok=True, connections=conns)


# ====================================================================== #
# Run registry + live log routing
# ====================================================================== #

class RunState:
    """Mutable state for one flattening run, shared with the SSE stream."""

    def __init__(self, run_id: str):
        self.id = run_id
        self.status = "queued"          # queued|running|done|error|stopped
        self.error: Optional[str] = None
        self.events: "queue.Queue[dict]" = queue.Queue()
        self.stop = threading.Event()
        # Per-app live state: {app: {total, processed, status}}
        self.apps: Dict[str, dict] = {}
        # Per-app result stats, filled as the run completes.
        self.results: Dict[str, dict] = {}
        self.output_dir = OUTPUT_ROOT / run_id

    def emit(self, kind: str, **data) -> None:
        """Push one event onto the SSE queue."""
        self.events.put({"kind": kind, **data})

    def log(self, level: str, message: str) -> None:
        self.emit("log", level=level, message=message)


RUNS: Dict[str, RunState] = {}
# Maps a worker thread id -> run id, so the logging handler can route the
# adapter's own log lines to the correct run's SSE stream.
_THREAD_RUN: Dict[int, str] = {}
_LOCK = threading.Lock()

# ---------------------------------------------------------------------------
# Connection / metadata warm cache.
# When the user reaches the table-selection step, we kick off a background
# warm-up for that connection: discover apps, prefetch + build all metadata
# registries, and pre-count rows. By the time they hit "Run", the run can skip
# validation/discovery/metadata entirely (via pipeline.preload) and reuse the
# counts — overlapping the slow I/O with the human's configuration time.
# Keyed by a non-secret connection signature; entries expire after WARM_TTL.
# NOTE: assumes the typical single-user local console; the warm worker reapplies
# its captured DB_* env before connecting to avoid cross-request bleed.
WARM: Dict[str, dict] = {}
_WARM_LOCK = threading.Lock()
WARM_TTL = 300  # seconds
_DB_ENV_KEYS = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_URL")


def _warm_signature(cfg) -> str:
    """Non-secret key identifying a connection+schema (no password)."""
    return "|".join(str(x) for x in (
        os.environ.get("DB_HOST"), os.environ.get("DB_PORT"),
        os.environ.get("DB_NAME"), os.environ.get("DB_USER"),
        cfg.db_schema, cfg.db_metadata_table,
        cfg.db_local_ref_table, cfg.db_customization_table,
    ))


def _get_warm(sig: str):
    """Return the ready warm entry for sig if fresh, else None."""
    with _WARM_LOCK:
        e = WARM.get(sig)
    if e and e.get("status") == "ready" and (time.time() - e["ts"]) < WARM_TTL:
        return e
    return None


def _warm_async(cfg, sig: str, env_snapshot: dict) -> None:
    """Background warm-up: build per-app metadata registries + row counts so a
    later run can skip discovery/metadata. Best-effort; failures are recorded
    and simply mean the run falls back to cold setup."""
    from psycopg2 import sql
    from t24_adapter.pipeline import T24GenericPipeline, T24MetadataOrchestrator
    from t24_adapter.discovery import T24PackageDiscovery

    # Reapply the captured connection env in case other requests changed it.
    for k, v in env_snapshot.items():
        if v is not None:
            os.environ[k] = v
    try:
        pipe = T24GenericPipeline(cfg)
        try:
            conn = pipe._read_connection()
            pipe.db_reader._conn = conn
            apps = pipe.db_reader.discover_tables(
                exclude={cfg.db_metadata_table, cfg.db_local_ref_table,
                         cfg.db_customization_table},
                exclude_suffixes=(cfg.db_output_suffix,),
            )
            orch = T24MetadataOrchestrator(cfg, T24PackageDiscovery(cfg), connection=conn)
            orch._ensure_prefetched()
            registries = {a.upper(): orch.load_for_application(a) for a in apps}
            counts = {}
            with conn.cursor() as cur:
                for a in apps:
                    cur.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(
                        sql.Identifier(cfg.db_schema), sql.Identifier(a)))
                    counts[a.upper()] = int(cur.fetchone()[0])
                conn.rollback()
        finally:
            pipe.close()
        with _WARM_LOCK:
            WARM[sig] = {"status": "ready", "registries": registries,
                         "counts": counts, "apps": apps, "ts": time.time()}
        logger.info(f"warm ready: {len(apps)} app(s), metadata + counts cached")
    except Exception as exc:
        with _WARM_LOCK:
            WARM[sig] = {"status": "error", "error": str(exc), "ts": time.time()}
        logger.warning(f"warm-up failed (run will set up cold): {exc}")


def _trigger_warm(cfg) -> None:
    """Start a background warm-up for cfg's connection unless one is already
    fresh or in flight. Non-blocking."""
    sig = _warm_signature(cfg)
    with _WARM_LOCK:
        e = WARM.get(sig)
        fresh = e and e.get("status") in ("ready", "warming") and \
            (time.time() - e["ts"]) < WARM_TTL
        if fresh:
            return
        WARM[sig] = {"status": "warming", "ts": time.time()}
    env_snapshot = {k: os.environ.get(k) for k in _DB_ENV_KEYS}
    threading.Thread(target=_warm_async, args=(cfg, sig, env_snapshot),
                     daemon=True).start()


# ---------------------------------------------------------------------------
# Speculative data prefetch.
# Once the user has picked tables (and is configuring format/filters), read +
# normalize + buffer those tables in the background. At run time the data is
# already in memory, so the run skips straight to the DB write — moving the
# ~read time off the user's critical path entirely. Keyed by connection
# signature + the selected table set, so changing the selection starts a fresh
# prefetch and the old one is ignored.
PREFETCH: Dict[str, dict] = {}
_PREFETCH_LOCK = threading.Lock()
PREFETCH_TTL = 300  # seconds


def _prefetch_key(cfg, tables) -> str:
    return _warm_signature(cfg) + "||" + ",".join(sorted(t.upper() for t in tables))


def _stats_accumulator(rows, stats):
    """Passthrough generator that tallies per-app records/fields/mapped/warnings
    (so a prefetched run still reports mappedPct + warnings without re-reading)."""
    last = {"app": None, "rid": object()}
    for f in rows:
        a = f.app_name.upper()
        s = stats.setdefault(a, {"records": 0, "fields": 0, "mapped": 0, "warnings": 0})
        if a != last["app"] or f.record_id != last["rid"]:
            s["records"] += 1
            last["app"] = a
            last["rid"] = f.record_id
        s["fields"] += 1
        if f.is_mapped:
            s["mapped"] += 1
        if f.warnings:
            s["warnings"] += len(f.warnings)
        yield f


def _get_prefetch(key: str, wait_timeout: float = 0):
    """Return the ready+fresh prefetch entry for key, or None.
    If it's still reading and wait_timeout>0, JOIN the in-flight prefetch
    (wait for it) rather than starting a duplicate read."""
    with _PREFETCH_LOCK:
        e = PREFETCH.get(key)
    if not e:
        return None
    if e.get("status") == "reading" and wait_timeout > 0:
        e["event"].wait(wait_timeout)
        with _PREFETCH_LOCK:
            e = PREFETCH.get(key)
    if e and e.get("status") == "ready" and (time.time() - e["ts"]) < PREFETCH_TTL:
        return e
    return None


def _prefetch_async(cfg, key: str, tables, env_snapshot: dict) -> None:
    """Background read+normalize+buffer of `tables`. Stores schemas+buffers+stats
    for the run to write directly. Best-effort: any failure (incl. a table too
    big to buffer) just means the run reads normally."""
    for k, v in env_snapshot.items():
        if v is not None:
            os.environ[k] = v
    stats = {t: {"records": 0, "fields": 0, "mapped": 0, "warnings": 0} for t in tables}
    try:
        from t24_adapter import T24GenericPipeline, WidePivotWriter
        pipe = T24GenericPipeline(cfg)
        warm = _get_warm(_warm_signature(cfg))
        regs = ({t: warm["registries"][t] for t in tables if t in warm.get("registries", {})}
                if warm else {})
        pipe.preload(applications=list(tables), registries=regs, skip_validation=True)
        pivot = WidePivotWriter()
        schemas, buffers, overflow = pivot.stream_buffer_and_schema(
            _stats_accumulator(pipe.run(), stats),
            max_buffer_rows=cfg.db_single_pass_max_rows,
        )
        pipe.close()
        if overflow:
            raise RuntimeError(f"tables too large to buffer: {sorted(overflow)}")
        with _PREFETCH_LOCK:
            ev = PREFETCH.get(key, {}).get("event") or threading.Event()
            PREFETCH[key] = {"status": "ready", "schemas": schemas, "buffers": buffers,
                             "stats": stats, "ts": time.time(), "event": ev}
            ev.set()
        rows = sum(len(b) for b in buffers.values())
        logger.info(f"prefetch ready: {rows:,} rows buffered for {sorted(tables)}")
    except Exception as exc:
        with _PREFETCH_LOCK:
            ev = PREFETCH.get(key, {}).get("event") or threading.Event()
            PREFETCH[key] = {"status": "error", "error": str(exc),
                             "ts": time.time(), "event": ev}
            ev.set()
        logger.warning(f"prefetch failed (run will read normally): {exc}")


def _trigger_prefetch(cfg, tables) -> None:
    """Kick off a background prefetch for the selected tables unless a fresh one
    (ready or in flight) already exists for the same connection+selection."""
    if not tables:
        return
    key = _prefetch_key(cfg, tables)
    with _PREFETCH_LOCK:
        e = PREFETCH.get(key)
        if e and e.get("status") in ("reading", "ready") and \
                (time.time() - e["ts"]) < PREFETCH_TTL:
            return
        PREFETCH[key] = {"status": "reading", "ts": time.time(), "event": threading.Event()}
    env_snapshot = {k: os.environ.get(k) for k in _DB_ENV_KEYS}
    threading.Thread(target=_prefetch_async, args=(cfg, key, list(tables), env_snapshot),
                     daemon=True).start()


class _SSELogHandler(logging.Handler):
    """Routes t24-adapter log records to the SSE stream of the active run."""

    _LEVEL_MAP = {
        logging.INFO: "info",
        logging.WARNING: "warn",
        logging.ERROR: "err",
        logging.CRITICAL: "err",
    }

    def emit(self, record: logging.LogRecord) -> None:
        run_id = _THREAD_RUN.get(threading.get_ident())
        if not run_id:
            return
        run = RUNS.get(run_id)
        if not run:
            return
        level = self._LEVEL_MAP.get(record.levelno, "dim")
        try:
            run.log(level, record.getMessage())
        except Exception:
            pass


# Attach the SSE handler to the adapter's logger tree once.
_adapter_logger = logging.getLogger("t24-adapter")
_adapter_logger.setLevel(logging.INFO)
_adapter_logger.addHandler(_SSELogHandler())


# ====================================================================== #
# Config building
# ====================================================================== #

def _apply_db_env(conn: dict) -> None:
    """Inject Step-1 connection form values into the environment the adapter
    reads. Only PostgreSQL is supported by the adapter today."""
    if conn.get("host"):
        os.environ["DB_HOST"] = str(conn["host"])
    if conn.get("port"):
        os.environ["DB_PORT"] = str(conn["port"])
    if conn.get("database"):
        os.environ["DB_NAME"] = str(conn["database"])
    if conn.get("username"):
        os.environ["DB_USER"] = str(conn["username"])
    if conn.get("password") is not None:
        os.environ["DB_PASSWORD"] = str(conn["password"])
    # A form-driven connection overrides any DB_URL from .env.
    os.environ.pop("DB_URL", None)


def _build_config(payload: dict) -> T24PipelineConfig:
    """Translate the console payload into a real T24PipelineConfig.

    Honest mapping notes:
      - multi-value / sub-value / local-ref / resolve-positions are ALWAYS on
        in the engine; those UI toggles are informational and not wired.
      - 'include unmapped fields'  -> output_unknown_fields
      - 'degrade unknowns to FIELD_N' off -> fail_on_unmapped_field=True
    """
    source = payload.get("source", "files")
    options = payload.get("options", {})
    tables = payload.get("tables") or []

    cfg = T24PipelineConfig(
        package_root=PACKAGE_ROOT,
        source="database" if source == "database" else "files",
        output_unknown_fields=bool(options.get("includeUnmapped", True)),
        fail_on_unmapped_field=not bool(options.get("degradeUnknown", True)),
        include_applications=tuple(tables) if tables else None,
    )

    if cfg.source == "database":
        cfg.db_schema = payload.get("schema") or cfg.db_schema
        # Metadata also from the DB by default (matches settings.py deployment).
        # If a table is absent the reader falls back to file metadata gracefully.
        cfg.db_metadata_table = payload.get("metadataTable", "STANDARD_SELECTION")
        cfg.db_local_ref_table = payload.get("localRefTable", "LOCAL_REFERENCE")
        cfg.db_customization_table = payload.get("customizationTable", "CUSTOMIZATION")
        # A narrowed (subset) run must never trigger the full-sync delete sweep.
        cfg.db_filters_active = bool(tables)
        # Parallelism from the UI dropdown (default 1 = sequential).
        try:
            mw = int(options.get("maxWorkers", 1))
            cfg.db_max_workers = mw if mw >= 1 else 1
        except (TypeError, ValueError):
            cfg.db_max_workers = 1
    return cfg


# ====================================================================== #
# Connectivity + discovery
# ====================================================================== #

@app.route("/api/test-connection", methods=["POST"])
def test_connection():
    payload = request.get_json(force=True) or {}
    source = payload.get("source", "files")

    if source == "files":
        data_dir = PACKAGE_ROOT / "data"
        if not data_dir.exists():
            return jsonify(ok=False, message=f"No data directory at {data_dir}"), 200
        n = len(list(data_dir.glob("*.xml")))
        return jsonify(ok=True, message=f"Local package OK · {n} data file(s)"), 200

    dbtype = (payload.get("dbType") or "postgresql").lower()
    if "postgre" not in dbtype:
        return jsonify(
            ok=False,
            message=f"{payload.get('dbType')} is not supported yet — "
                    f"the adapter currently connects to PostgreSQL only.",
        ), 200

    _apply_db_env(payload.get("conn", {}))
    try:
        import psycopg2
    except ImportError:
        return jsonify(ok=False, message="psycopg2 not installed on the server."), 200

    t0 = time.time()
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT"),
            dbname=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
    except Exception as exc:
        return jsonify(ok=False, message=f"{type(exc).__name__}: {exc}"), 200

    ms = int((time.time() - t0) * 1000)
    return jsonify(ok=True, message=f"Connection successful · {ms}ms"), 200


@app.route("/api/tables", methods=["POST"])
def list_tables():
    """Discover applications and their REAL record counts for the chosen source."""
    payload = request.get_json(force=True) or {}
    source = payload.get("source", "files")

    try:
        if source == "files":
            reader = T24StreamingDataReader()
            data_dir = PACKAGE_ROOT / "data"
            tables = []
            for xml in sorted(data_dir.glob("*.xml")):
                app_name = xml.stem.upper()
                tables.append({"name": app_name, "records": reader.count_records(xml)})
            return jsonify(ok=True, source="files", tables=tables), 200

        dbtype = (payload.get("dbType") or "postgresql").lower()
        if "postgre" not in dbtype:
            return jsonify(ok=False, message="Only PostgreSQL is supported."), 200

        _apply_db_env(payload.get("conn", {}))
        cfg = _build_config(payload)
        from t24_adapter import T24DatabaseDataReader
        import psycopg2
        from psycopg2 import sql

        reader = T24DatabaseDataReader(schema=cfg.db_schema)
        names = reader.discover_tables(
            exclude={cfg.db_metadata_table, cfg.db_local_ref_table, cfg.db_customization_table},
            exclude_suffixes=(cfg.db_output_suffix,),
        )
        tables = []
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"), port=os.environ.get("DB_PORT"),
            dbname=os.environ.get("DB_NAME"), user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
        )
        try:
            for name in names:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("SELECT count(*) FROM {}.{}").format(
                            sql.Identifier(cfg.db_schema), sql.Identifier(name)
                        )
                    )
                    tables.append({"name": name, "records": cur.fetchone()[0]})
        finally:
            conn.close()
        # Warm the connection + metadata in the background while the user picks
        # tables / format, so the eventual run can skip cold setup.
        _trigger_warm(cfg)
        return jsonify(ok=True, source="database", tables=tables), 200

    except Exception as exc:
        logger.exception("table discovery failed")
        return jsonify(ok=False, message=f"{type(exc).__name__}: {exc}"), 200


@app.route("/api/prefetch", methods=["POST"])
def prefetch():
    """Start a background read+buffer of the selected tables (database mode).
    Called when the user advances past table selection, so the eventual run
    can skip the read. Returns immediately; idempotent per selection."""
    payload = request.get_json(force=True) or {}
    if payload.get("source") != "database":
        return jsonify(ok=True, prefetch=False, reason="files mode"), 200
    tables = [t.upper() for t in (payload.get("tables") or [])]
    if not tables:
        return jsonify(ok=True, prefetch=False, reason="no tables"), 200
    try:
        _apply_db_env(payload.get("conn", {}))
        cfg = _build_config(payload)
        _trigger_prefetch(cfg, tables)
        return jsonify(ok=True, prefetch=True, tables=tables), 200
    except Exception as exc:
        logger.warning(f"prefetch trigger failed: {exc}")
        return jsonify(ok=True, prefetch=False, reason=str(exc)), 200


# ====================================================================== #
# Run execution
# ====================================================================== #

# Formats the engine can actually produce.
_FILE_FORMATS = {"csv", "jsonl"}


@app.route("/api/run", methods=["POST"])
def start_run():
    payload = request.get_json(force=True) or {}
    fmt = payload.get("format", "csv")

    if fmt == "xlsx":
        return jsonify(ok=False, message="Excel (xlsx) output is not implemented yet."), 501
    if fmt not in _FILE_FORMATS and fmt != "db":
        return jsonify(ok=False, message=f"Unknown output format: {fmt}"), 400
    if not payload.get("tables"):
        return jsonify(ok=False, message="Select at least one table to flatten."), 400

    run_id = uuid.uuid4().hex[:12]
    run = RunState(run_id)
    RUNS[run_id] = run

    thread = threading.Thread(target=_run_worker, args=(run, payload), daemon=True)
    thread.start()
    return jsonify(ok=True, runId=run_id), 200


def _run_worker(run: RunState, payload: dict) -> None:
    """Execute the pipeline for one run, emitting real progress + stats."""
    _THREAD_RUN[threading.get_ident()] = run.id
    run.status = "running"
    fmt = payload.get("format", "csv")
    tables = [t.upper() for t in payload["tables"]]
    for t in tables:
        run.apps[t] = {"total": 0, "processed": 0, "status": "pending"}

    pipeline = None
    try:
        if (payload.get("source") == "database"
                and "postgre" not in (payload.get("dbType") or "postgresql").lower()):
            raise RuntimeError("Only PostgreSQL is supported for database runs.")
        if payload.get("source") == "database":
            _apply_db_env(payload.get("conn", {}))

        cfg = _build_config(payload)
        pipeline = T24GenericPipeline(cfg)

        run.log("ok", f"Starting pipeline · {len(tables)} table(s) · format={fmt}")

        # Reuse background-warmed connection state (metadata + counts) if a
        # fresh warm-up exists for this connection. Removes validation +
        # discovery + metadata fetch from the run's critical path.
        warm = _get_warm(_warm_signature(cfg)) if payload.get("source") == "database" else None
        if warm:
            regs = {t: warm["registries"][t] for t in tables if t in warm.get("registries", {})}
            pipeline.preload(applications=tables, registries=regs, skip_validation=True)
            run.log("ok", "✓ using warmed connection · metadata + row counts ready")

        # Stats accumulator — populated DURING the single-pass work by the
        # progress wrapper, so neither path needs a separate discovery scan.
        stats = {t: {"records": 0, "fields": 0, "mapped": 0, "warnings": 0} for t in tables}

        # Pre-compute totals (DB: COUNT(*); files: count <row> per XML) so the
        # progress bar has a denominator before any streaming starts. Warm runs
        # reuse the pre-counted totals; otherwise count now.
        if warm and warm.get("counts"):
            totals = {t: warm["counts"].get(t, 0) for t in tables}
        else:
            totals = _resolve_totals(cfg, tables, payload.get("source"))
        for t in tables:
            run.apps[t]["total"] = totals.get(t, 0)
            run.apps[t]["processed"] = 0
            run.apps[t]["status"] = "pending"
        run.emit("schema", apps={t: run.apps[t]["total"] for t in tables})

        # Speculative prefetch: if the data was read+buffered in the background
        # while the user configured, write directly — the read is already done.
        # The prefetch is keyed by the INPUT source + tables (format-agnostic),
        # so it applies to BOTH database and file output. Joins an in-flight
        # prefetch rather than starting a duplicate read.
        prefetched = (_get_prefetch(_prefetch_key(cfg, tables), wait_timeout=180)
                      if payload.get("source") == "database" else None)

        if prefetched:
            run.log("ok", "✓ data prefetched during setup · skipping read, writing only")
            for a, s in prefetched.get("stats", {}).items():
                stats[a] = s
            pipeline.close()  # the prefetch already read; this pipeline is unused
            if fmt == "db":
                _write_database_prefetched(run, cfg, tables, totals, stats, prefetched)
            else:
                _write_files_prefetched(run, cfg, tables, totals, stats, prefetched, fmt)
        elif fmt == "db":
            _write_database_streaming(run, cfg, pipeline, tables, totals, stats)
        else:
            _write_files_streaming(run, cfg, pipeline, tables, totals, stats, fmt)

        run.status = "done"
        total_records = sum(stats.get(t, {}).get("records", 0) for t in tables)
        run.log("ok", f"Pipeline complete · {total_records:,} records · 0 errors")
        run.emit("done", results=run.results)

    except _Stopped:
        run.status = "stopped"
        run.log("warn", "Run stopped by user.")
        run.emit("stopped")
    except Exception as exc:
        logger.exception("run failed")
        run.status = "error"
        run.error = f"{type(exc).__name__}: {exc}"
        run.log("err", run.error)
        run.emit("error", message=run.error)
    finally:
        if pipeline is not None:
            pipeline.close()          # release the shared read connection
        _THREAD_RUN.pop(threading.get_ident(), None)


def _resolve_totals(cfg, tables, source):
    """Pre-compute per-app record totals for the progress-bar denominator.

    - DB source: one COUNT(*) per table (exact, ~70 ms each on the remote DB).
    - Files source: count <row> elements per XML file (fast local scan).

    Returned dict is {app_name: int}. Missing entries (e.g. a file that
    doesn't exist) default to 0; the progress wrapper handles a 0 denominator.
    """
    totals = {}
    if source == "database":
        from t24_adapter.db_reader import _connect_from_env
        from psycopg2 import sql
        conn = _connect_from_env()
        try:
            with conn.cursor() as cur:
                for t in tables:
                    cur.execute(
                        sql.SQL("SELECT count(*) FROM {}.{}").format(
                            sql.Identifier(cfg.db_schema), sql.Identifier(t),
                        )
                    )
                    totals[t] = int(cur.fetchone()[0])
        finally:
            conn.close()
    else:
        reader = T24StreamingDataReader()
        for t in tables:
            f = PACKAGE_ROOT / "data" / f"{t}.xml"
            totals[t] = reader.count_records(f) if f.exists() else 0
    return totals


def _streaming_progress_wrapper(run, totals, stats):
    """Wraps the pipeline.run() field stream for the single-pass DB write.

    For every NormalizedField passing through:
    - accumulates stats[app] (records / fields / mapped / warnings)
    - on a record boundary, bumps the processed counter and (throttled to
      ~1% of total) emits an SSE 'progress' event so the UI fills the bar
      live, not at the end
    - honours run.stop.is_set() so the user's "stop" button works
    """
    last_key = {"app": None, "rid": object()}
    processed = {t: 0 for t in totals}
    # Track the last emit's processed-count per app so we only push when
    # the % bar has moved at least one unit.
    next_emit = {t: 1 for t in totals}

    def step_for(app):
        # Throttle progress events: ~one per 1% of the table, min 50 rows,
        # so very small tables still tick visibly and big ones don't flood SSE.
        total = totals.get(app, 0)
        return max(50, total // 100) if total else 50

    def wrap(rows):
        # processed / next_emit are mutated in place (dict items), not rebound,
        # so no `nonlocal` is needed.
        for f in rows:
            if run.stop.is_set():
                raise _Stopped()
            a = f.app_name.upper()
            s = stats.setdefault(
                a, {"records": 0, "fields": 0, "mapped": 0, "warnings": 0})
            # Detect record boundary (app or record_id changed).
            if a != last_key["app"] or f.record_id != last_key["rid"]:
                s["records"] += 1
                processed[a] = processed.get(a, 0) + 1
                last_key["app"] = a
                last_key["rid"] = f.record_id
                # Mark this app's row as running on its first observed record.
                if run.apps.get(a, {}).get("status") != "running":
                    run.apps.setdefault(a, {}).update(status="running")
                    run.emit("progress", app=a, processed=0,
                             total=totals.get(a, 0), status="running", phase="read")
                # Throttled progress emit.
                if processed[a] >= next_emit.get(a, 1):
                    run.apps[a]["processed"] = processed[a]
                    run.emit("progress", app=a, processed=processed[a],
                             total=totals.get(a, 0), status="running", phase="read")
                    next_emit[a] = processed[a] + step_for(a)
            s["fields"] += 1
            if f.is_mapped:
                s["mapped"] += 1
            if f.warnings:
                s["warnings"] += len(f.warnings)
            yield f

        # Final flush: each touched app shows its true final READ count (the
        # read half of the bar is now full; the write half fills next).
        for a, n in processed.items():
            if n and run.apps.get(a, {}).get("processed", 0) != n:
                run.apps[a]["processed"] = n
                run.emit("progress", app=a, processed=n,
                         total=totals.get(a, 0), status="running", phase="read")
    return wrap


def _write_progress_emitter(run, totals):
    """Returns a callback (app_name, rows_written) -> None for the writer to
    fire after each batch flush, emitting a 'saving' progress event so the UI
    fills the WRITE half of the bar live instead of jumping to done."""
    def on_write_progress(app_name, written):
        a = app_name.upper()
        run.apps.setdefault(a, {}).update(processed=written, status="saving")
        run.emit("progress", app=a, processed=written,
                 total=totals.get(a, 0), status="saving", phase="write")
    return on_write_progress


def _make_db_writer(cfg):
    """Construct a WideDatabaseWriter from a config. Shared by the sequential
    DB write and the per-worker factory used by T24WorkerPool."""
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


def _record_db_result(run, cfg, app_name, table, upserted, deleted, cols, stats):
    """Update RunState + emit the final 'done' progress for one app."""
    a = app_name.upper()
    st = stats.get(a, {})
    mapped_pct = round(100 * st.get("mapped", 0) / st["fields"]) if st.get("fields") else 100
    run.apps.setdefault(a, {"total": upserted})
    run.apps[a].update(processed=upserted, status="done")
    run.results[a] = {
        "file": f"{cfg.db_schema}.{table}",
        "records": upserted,
        "deleted": deleted,
        "mappedPct": mapped_pct,
        "warnings": st.get("warnings", 0),
        "columns": cols,
    }
    run.log("ok", f"{app_name} → {cfg.db_schema}.{table} "
                  f"({upserted} upserted, {deleted} deleted, {cols} cols)")
    run.emit("progress", app=a, processed=upserted,
             total=run.apps[a].get("total", upserted), status="done", phase="done")


def _write_database_prefetched(run, cfg, tables, totals, stats, pf):
    """Write tables whose data was already read+buffered by a background
    prefetch. No source read — the read half of the bar is already complete,
    so we mark it full and then stream the WRITE half live."""
    # Read was done in the background: show the read half complete instantly.
    for t in tables:
        run.apps[t].update(processed=totals.get(t, 0), status="running")
        run.emit("progress", app=t, processed=totals.get(t, 0),
                 total=totals.get(t, 0), status="running", phase="read")
    db_writer = _make_db_writer(cfg)
    on_write = _write_progress_emitter(run, totals)
    written = db_writer.write_prebuilt(pf["schemas"], pf["buffers"], on_write_progress=on_write)
    for app_name, (table, upserted, deleted, cols) in written.items():
        _record_db_result(run, cfg, app_name, table, upserted, deleted, cols, stats)


def _should_parallelize(max_workers: int, n_tables: int, min_tables: int) -> bool:
    """Parallelize only when more than one worker is requested AND there are
    enough tables for read-overlap to outweigh per-run overhead + remote-DB
    write contention. At a couple of tables, parallel is not faster."""
    return max_workers > 1 and n_tables >= min_tables


def _write_database_streaming(run, cfg, pipeline, tables, totals, stats):
    """DB sink, single source read, progress events flowing live as records
    pass through the normalize→buffer phase.

    Dispatch:
    - cfg.db_max_workers <= 1 — one pipeline + writer, sequential (today's path).
    - cfg.db_max_workers >  1 — T24WorkerPool fans applications out across
      worker threads (each with its own connections); the SSE log handler
      already routes per-thread via _THREAD_RUN, so each worker registers
      itself on enter via set_worker_thread_hooks.
    """
    if _should_parallelize(cfg.db_max_workers, len(tables), cfg.db_parallel_min_tables):
        return _write_database_parallel(run, cfg, tables, totals, stats)
    if cfg.db_max_workers > 1:
        # Requested but below the threshold where parallelism pays off — at a
        # few tables the remote-DB write dominates and concurrent writers only
        # add contention/variance. Run sequentially and say why.
        run.log("info",
                f"Parallelism requested (workers={cfg.db_max_workers}) but only "
                f"{len(tables)} table(s); running sequentially "
                f"(parallel helps from {cfg.db_parallel_min_tables}+ tables).")

    # Sequential path — same code shape as before, simplified.
    db_writer = _make_db_writer(cfg)
    wrap = _streaming_progress_wrapper(run, totals, stats)
    on_write = _write_progress_emitter(run, totals)
    written = db_writer.write(pipeline, rows_wrapper=wrap, on_write_progress=on_write)
    for app_name, (table, upserted, deleted, cols) in written.items():
        _record_db_result(run, cfg, app_name, table, upserted, deleted, cols, stats)


def _write_database_parallel(run, cfg, tables, totals, stats):
    """Parallel DB sink via T24WorkerPool. Each worker:
      - registers its thread in _THREAD_RUN so its adapter log lines flow
        into THIS run's SSE stream (otherwise they'd silently disappear);
      - gets its OWN _streaming_progress_wrapper instance (the wrapper's
        boundary-detection state — last_key / processed / next_emit — is
        per-stream; sharing one wrapper across threads breaks boundary
        detection when their fields interleave and inflates record counts);
      - returns its app's (table, upserted, deleted, cols) tuple, which we
        feed back through the same _record_db_result path as sequential.
    """
    from t24_adapter.parallel import T24WorkerPool, set_worker_thread_hooks

    run_id = run.id

    def _register():
        # Route this worker thread's adapter log lines to the active run.
        _THREAD_RUN[threading.get_ident()] = run_id

    def _deregister():
        _THREAD_RUN.pop(threading.get_ident(), None)

    set_worker_thread_hooks(_register, _deregister)

    # One write-progress emitter is safe to share across workers: it only does
    # run.apps[app] / run.emit keyed by app, and each worker handles a distinct
    # app, so there's no cross-worker state collision (unlike the read wrapper).
    on_write = _write_progress_emitter(run, totals)

    def factory(worker_cfg):
        w = _make_db_writer(worker_cfg)
        # Each worker gets its OWN read wrapper closure — sharing breaks because
        # the wrapper's last_key flickers across apps when threads interleave.
        wrap = _streaming_progress_wrapper(run, totals, stats)
        # Monkey-attach the wrappers so each worker's write() uses them. Equivalent
        # to passing them at every call site without changing the pool API.
        original_write = w.write
        def write_with_wrapper(pipe, schemas=None):
            return original_write(pipe, schemas=schemas,
                                  rows_wrapper=wrap, on_write_progress=on_write)
        w.write = write_with_wrapper
        return w

    pool = T24WorkerPool(cfg, factory, max_workers=cfg.db_max_workers)
    try:
        outcomes = pool.run(tables)
    finally:
        # Detach hooks so a later sequential run isn't surprised by them.
        set_worker_thread_hooks(None, None)

    failed = []
    for app_name, o in outcomes.items():
        if o["status"] == "ok" and o["result"]:
            table, upserted, deleted, cols = o["result"]
            _record_db_result(run, cfg, app_name, table, upserted, deleted, cols, stats)
        else:
            failed.append((app_name, o["status"], o.get("error")))
            run.log("err", f"{app_name} → {o['status']}: {o.get('error')}")
    if failed:
        raise RuntimeError(f"{len(failed)} of {len(outcomes)} application(s) failed: {failed}")


def _write_files_streaming(run, cfg, pipeline, tables, totals, stats, fmt):
    """Files sink, single-pass: stream + normalize + buffer ONCE (progress
    emitted live), then write each app's buffered rows to its own CSV/JSONL."""
    pivot = WidePivotWriter()
    wrap = _streaming_progress_wrapper(run, totals, stats)
    schemas, buffers, overflow = pivot.stream_buffer_and_schema(
        wrap(pipeline.run()), max_buffer_rows=cfg.db_single_pass_max_rows,
    )
    pipeline.close()
    _write_buffers_to_files(run, tables, totals, stats, pivot,
                            schemas, buffers, overflow, fmt)


def _write_files_prefetched(run, cfg, tables, totals, stats, pf, fmt):
    """Files sink using data already read+buffered by a background prefetch —
    no source read. Mirrors _write_database_prefetched but writes CSV/JSONL."""
    # Read happened in the background: show the read half complete instantly.
    for t in tables:
        run.apps[t].update(processed=totals.get(t, 0), status="running")
        run.emit("progress", app=t, processed=totals.get(t, 0),
                 total=totals.get(t, 0), status="running", phase="read")
    _write_buffers_to_files(run, tables, totals, stats, WidePivotWriter(),
                            pf["schemas"], pf["buffers"], set(), fmt)


def _write_buffers_to_files(run, tables, totals, stats, pivot,
                            schemas, buffers, overflow, fmt):
    """Shared sink: write per-app buffered wide rows to CSV/JSONL, emitting
    live write-phase progress. Used by both the read-then-write path and the
    prefetched path."""
    import csv
    import json

    run.output_dir.mkdir(parents=True, exist_ok=True)
    ext = "csv" if fmt == "csv" else "jsonl"

    for app_name in tables:
        schema = schemas.get(app_name)
        if schema is None or app_name in overflow:
            run.apps[app_name]["status"] = "done"
            note = "no records found" if schema is None else (
                "table exceeded single-pass buffer cap; re-run with a larger "
                "db_single_pass_max_rows or smaller table"
            )
            run.log("warn", f"{app_name}: {note}")
            run.emit("progress", app=app_name, processed=0, total=0, status="done")
            continue

        out_path = run.output_dir / f"{app_name}_wide.{ext}"
        app_total = totals.get(app_name, 0)
        written = 0
        write_step = max(50, app_total // 100) if app_total else 50
        next_write_emit = write_step
        with out_path.open("w", newline="", encoding="utf-8") as fh:
            if fmt == "csv":
                w = csv.DictWriter(fh, fieldnames=schema.columns)
                w.writeheader()
            for rid, values in buffers.get(app_name, []):
                if run.stop.is_set():
                    raise _Stopped()
                row = pivot.wide_row_from_buffer(rid, values, schema, app_name)
                if fmt == "csv":
                    w.writerow(row)
                else:
                    rec = {k: (v if v != "" else None) for k, v in row.items()}
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                written += 1
                # Live write-phase progress (fills the WRITE half of the bar).
                if written >= next_write_emit:
                    run.apps[app_name].update(processed=written, status="saving")
                    run.emit("progress", app=app_name, processed=written,
                             total=app_total, status="saving", phase="write")
                    next_write_emit = written + write_step

        processed = len(buffers.get(app_name, []))
        st = stats.get(app_name, {})
        mapped_pct = round(100 * st.get("mapped", 0) / st["fields"]) if st.get("fields") else 100
        run.apps[app_name].update(processed=processed, status="done")
        run.results[app_name] = {
            "file": out_path.name,
            "records": processed,
            "mappedPct": mapped_pct,
            "warnings": st.get("warnings", 0),
            "columns": len(schema.columns),
        }
        run.log("ok", f"{app_name} complete → {out_path.name} "
                      f"({processed:,} rows, {mapped_pct}% mapped)")
        run.emit("progress", app=app_name, processed=processed,
                 total=run.apps[app_name]["total"], status="done", phase="done")




class _Stopped(Exception):
    """Raised internally to unwind a run that the user asked to stop."""


@app.route("/api/run/<run_id>/stop", methods=["POST"])
def stop_run(run_id):
    run = RUNS.get(run_id)
    if not run:
        return jsonify(ok=False, message="Unknown run."), 404
    run.stop.set()
    return jsonify(ok=True), 200


@app.route("/api/run/<run_id>")
def run_status(run_id):
    run = RUNS.get(run_id)
    if not run:
        return jsonify(ok=False, message="Unknown run."), 404
    return jsonify(ok=True, status=run.status, error=run.error,
                   apps=run.apps, results=run.results), 200


@app.route("/api/run/<run_id>/stream")
def run_stream(run_id):
    run = RUNS.get(run_id)
    if not run:
        return jsonify(ok=False, message="Unknown run."), 404

    @stream_with_context
    def gen():
        import json
        yield f"data: {json.dumps({'kind': 'hello', 'status': run.status})}\n\n"
        while True:
            try:
                ev = run.events.get(timeout=15)
            except queue.Empty:
                yield ": keep-alive\n\n"
                if run.status in ("done", "error", "stopped") and run.events.empty():
                    break
                continue
            yield f"data: {json.dumps(ev)}\n\n"
            if ev["kind"] in ("done", "error", "stopped"):
                break

    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/download/<run_id>/<app_name>")
def download(run_id, app_name):
    run = RUNS.get(run_id)
    if not run or app_name.upper() not in run.results:
        return jsonify(ok=False, message="Not found."), 404
    fname = run.results[app_name.upper()]["file"]
    fpath = run.output_dir / fname
    if not fpath.exists():
        return jsonify(ok=False, message="File no longer available "
                                          "(database output is not a file)."), 404
    return send_file(fpath, as_attachment=True, download_name=fname)


# ====================================================================== #
# Static UI
# ====================================================================== #

@app.route("/")
def index():
    return send_from_directory(str(BASE_DIR), CONSOLE_HTML)


def _pick_port(preferred: int) -> int:
    """Return `preferred` if it's free, otherwise an OS-assigned free port.

    macOS often occupies port 5000 (AirPlay Receiver / ControlCenter), which
    would otherwise crash the server with 'Address already in use'. This keeps
    'just press Run' working without any manual port juggling.
    """
    import socket

    def is_free(p: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", p))
                return True
            except OSError:
                return False

    if is_free(preferred):
        return preferred
    # Deterministic fallback: scan a fixed range upward from the preferred port
    # and take the first free one. This keeps the origin (host:port) stable
    # across restarts — important because saved-connection state and bookmarks
    # are origin-scoped. (An OS-assigned random port would change every launch.)
    for p in range(preferred + 1, preferred + 51):
        if is_free(p):
            return p
    # Last resort (50 consecutive ports busy): let the OS assign one.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


if __name__ == "__main__":
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    requested = int(os.environ.get("PORT", "5000"))
    port = _pick_port(requested)
    if port != requested:
        print(f"[t24] Port {requested} is busy; using free port {port} instead.")
    print(f"[t24] T24 Flattening Console →  http://127.0.0.1:{port}")
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
