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
        return jsonify(ok=True, source="database", tables=tables), 200

    except Exception as exc:
        logger.exception("table discovery failed")
        return jsonify(ok=False, message=f"{type(exc).__name__}: {exc}"), 200


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

    try:
        if (payload.get("source") == "database"
                and "postgre" not in (payload.get("dbType") or "postgresql").lower()):
            raise RuntimeError("Only PostgreSQL is supported for database runs.")
        if payload.get("source") == "database":
            _apply_db_env(payload.get("conn", {}))

        cfg = _build_config(payload)
        pipeline = T24GenericPipeline(cfg)
        writer = WidePivotWriter()

        run.log("ok", f"Starting pipeline · {len(tables)} table(s) · format={fmt}")

        # --- Pass 1: discover schema while counting REAL totals + stats. ---
        stats = {t: {"records": 0, "fields": 0, "mapped": 0, "warnings": 0} for t in tables}
        last_key = {"app": None, "rid": object()}

        def counting(rows):
            for f in rows:
                if run.stop.is_set():
                    raise _Stopped()
                a = f.app_name.upper()
                s = stats.setdefault(
                    a, {"records": 0, "fields": 0, "mapped": 0, "warnings": 0})
                if a != last_key["app"] or f.record_id != last_key["rid"]:
                    s["records"] += 1
                    last_key["app"] = a
                    last_key["rid"] = f.record_id
                s["fields"] += 1
                if f.is_mapped:
                    s["mapped"] += 1
                if f.warnings:
                    s["warnings"] += len(f.warnings)
                yield f

        schemas = writer.discover_schema(counting(pipeline.run()))
        for t in tables:
            total = stats.get(t, {}).get("records", 0)
            run.apps.setdefault(t, {})["total"] = total
            run.apps[t]["processed"] = 0
            run.apps[t]["status"] = "pending"
        run.emit("schema", apps={t: run.apps[t]["total"] for t in tables})

        # --- Pass 2: write output, emitting per-row progress. ---
        if fmt == "db":
            _write_database(run, cfg, pipeline, tables, stats)
        else:
            _write_files(run, writer, pipeline, schemas, tables, fmt, stats)

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
        _THREAD_RUN.pop(threading.get_ident(), None)


def _write_files(run, writer, pipeline, schemas, tables, fmt, stats):
    import csv
    import json

    run.output_dir.mkdir(parents=True, exist_ok=True)
    for app_name in tables:
        schema = schemas.get(app_name)
        run.apps[app_name]["status"] = "running"
        run.emit("progress", app=app_name, processed=0,
                 total=run.apps[app_name]["total"], status="running")
        if schema is None:
            run.apps[app_name]["status"] = "done"
            run.log("warn", f"{app_name}: no records found.")
            run.emit("progress", app=app_name, processed=0, total=0, status="done")
            continue

        ext = "csv" if fmt == "csv" else "jsonl"
        out_path = run.output_dir / f"{app_name}_wide.{ext}"
        processed = 0
        total = run.apps[app_name]["total"] or 1

        with out_path.open("w", newline="", encoding="utf-8") as fh:
            if fmt == "csv":
                w = csv.DictWriter(fh, fieldnames=schema.columns)
                w.writeheader()
            for row in writer.iter_wide_rows(pipeline.run(), schema, app_name):
                if run.stop.is_set():
                    raise _Stopped()
                if fmt == "csv":
                    w.writerow(row)
                else:
                    rec = {k: (v if v != "" else None) for k, v in row.items()}
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                processed += 1
                # Emit progress every ~1% (or every row for small sets).
                if processed % max(1, total // 100) == 0 or processed == total:
                    run.apps[app_name]["processed"] = processed
                    run.emit("progress", app=app_name, processed=processed,
                             total=run.apps[app_name]["total"], status="running")

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
                 total=run.apps[app_name]["total"], status="done")


def _write_database(run, cfg, pipeline, tables, stats):
    db_writer = WideDatabaseWriter(
        schema=cfg.db_schema,
        suffix=cfg.db_output_suffix,
        write_mode=cfg.db_write_mode,
        batch_size=cfg.db_batch_size,
        key_column=cfg.db_key_column,
        full_sync=cfg.db_full_sync,
        filters_active=cfg.db_filters_active,
    )
    for t in tables:
        run.apps[t]["status"] = "running"
        run.emit("progress", app=t, processed=0, total=run.apps[t]["total"], status="running")

    written = db_writer.write(pipeline)
    for app_name, (table, upserted, deleted, cols) in written.items():
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
                 total=run.apps[a]["total"], status="done")


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
