"""
metrics.py
----------
T24 Generic Adapter - Run-scoped latency instrumentation.

A tiny, dependency-free timing registry used to answer one question:
"where did the wall-clock go in this run?"

Two kinds of measurement:

- phases   : top-level, (mostly) sequential spans that sum to ~the whole run
             (e.g. "pass1_discover_schema", "pass2_write"). These are what you
             read first.
- components: diagnostic sub-spans that may overlap a phase (e.g. "db_upsert"
             happens *inside* pass 2). These tell you which part of a phase is
             expensive. They are NOT expected to sum to the total.

Plus simple counters (e.g. number of DB round-trips), so a slow run caused by
network latency is immediately obvious.

Parallel execution
------------------
Under a thread pool the SAME phase (e.g. "pass_single") runs concurrently for
N apps. Summing those would yield ~N * wall (meaningless). Use the `app=` kwarg
to tag a span with its application name. The reporter then aggregates parallel
phases as **MAX** across apps (the longest worker gates wall-clock) and
counters as **SUM** (round-trips really do add up).

Usage
-----
    from t24_adapter.metrics import metrics

    metrics.reset()
    with metrics.span("pass2_write"):
        ...                                # sequential
    with metrics.span("pass_single", app="CUSTOMER"):
        ...                                # one worker among many
    metrics.add("db_upsert", dt)           # pre-measured duration
    metrics.incr("db_roundtrips")          # bump a counter
    metrics.report(logger)                 # log the summary table

The module exposes a single shared `metrics` instance so any module in the
pipeline can record into the same run without threading an object through
every constructor. `reset()` starts a fresh run.

Thread safety
-------------
All mutators (`add`, `incr`, `span` finalizers) take a Lock — concurrent
increments and dict insertions are safe. `reset()` and `report()` are not meant
to be called while workers are mutating (caller's responsibility).
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from time import perf_counter
from typing import Dict, List, Optional, Tuple

# Phases are the true top-level, sequential spans: the two source passes. They
# sum to ~the whole run. Everything else (validate/discover/metadata/write)
# executes *inside* one of these passes, so it is reported as a component.
# (label, key) — keys are recorded via span()/add(). Declared order = report order.
_PHASES: List[Tuple[str, str]] = [
    ("Single pass — stream + normalize + buffer", "pass_single"),
    ("Pass 1 — discover wide schema (stream + normalize)", "pass1_discover_schema"),
    ("Pass 2 — pivot + write to database", "pass2_write"),
]

# Components are diagnostic; each overlaps one of the phases above.
_COMPONENTS: List[Tuple[str, str, str]] = [
    # (label, time-key, round-trip-counter-key or "")
    ("validate package (both passes)", "validate", ""),
    ("discover applications (both passes)", "discover_apps", ""),
    ("metadata fetch — STD.SEL/LOCAL.REF/CUSTOMIZATION (both passes)", "metadata_fetch", "metadata_roundtrips"),
    ("DB upsert flush (bulk INSERT … ON CONFLICT)", "db_upsert", "db_roundtrips"),
    ("DDL (ensure table / add columns / unique)", "ddl", ""),
    ("full-sync delete sweep", "db_sweep", ""),
]

# Separator between a phase key and its app tag in the internal store
# (e.g. "pass_single::CUSTOMER"). External callers never see this — they pass
# `app=` and the storage shape is an implementation detail.
_APP_SEP = "::"


class RunMetrics:
    """Accumulates named timing spans and counters for one pipeline run.

    Sequential callers see the original API: `span("name")`, `add`, `incr`.
    Parallel callers tag their work with `span("name", app="X")`; the reporter
    aggregates those tags as MAX (because they overlap each other and gate
    wall-clock together, not in sequence).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        """Begin a fresh run: clear all spans/counters and start the wall clock."""
        # Lock isn't strictly required here (no one should be writing yet) but
        # keep it consistent with the rest of the mutators.
        with self._lock:
            self._spans: Dict[str, float] = {}
            self._counts: Dict[str, int] = {}
            self._wall_start = perf_counter()

    @contextmanager
    def span(self, name: str, app: Optional[str] = None):
        """Time a block and accumulate it.

        If `app` is given, the time goes under `<name>::<APP>` and the reporter
        treats `<name>` as a parallel phase (aggregates apps as MAX). Otherwise
        the legacy behavior applies (accumulate into `name`).
        """
        start = perf_counter()
        try:
            yield
        finally:
            key = f"{name}{_APP_SEP}{app.upper()}" if app else name
            dt = perf_counter() - start
            with self._lock:
                self._spans[key] = self._spans.get(key, 0.0) + dt

    def add(self, name: str, seconds: float, app: Optional[str] = None) -> None:
        """Add a pre-measured duration. Optional `app` tag, same rules as span()."""
        key = f"{name}{_APP_SEP}{app.upper()}" if app else name
        with self._lock:
            self._spans[key] = self._spans.get(key, 0.0) + seconds

    def incr(self, name: str, n: int = 1) -> None:
        """Bump a named counter (e.g. DB round-trips) — atomic across threads."""
        with self._lock:
            self._counts[name] = self._counts.get(name, 0) + n

    def get(self, name: str, app: Optional[str] = None) -> float:
        """Return the recorded time for `name` (or `name::APP` if app given)."""
        key = f"{name}{_APP_SEP}{app.upper()}" if app else name
        with self._lock:
            return self._spans.get(key, 0.0)

    def count(self, name: str) -> int:
        with self._lock:
            return self._counts.get(name, 0)

    # ------------------------------------------------------------------ #
    # Internal aggregation helpers (lock NOT held; callers snapshot first)
    # ------------------------------------------------------------------ #

    def _phase_time(self, spans: Dict[str, float], phase_key: str) -> Tuple[float, Dict[str, float]]:
        """Return (aggregated_time_in_seconds, per_app_breakdown).

        - If the phase has only the legacy un-tagged key, return that value.
        - If it has `phase::APP` tags, aggregate as **MAX** over apps (because
          they ran in parallel and the longest worker gated the wall-clock).
        - If both exist, take MAX(legacy, parallel_max).
        """
        legacy = spans.get(phase_key, 0.0)
        prefix = f"{phase_key}{_APP_SEP}"
        per_app = {k[len(prefix):]: v for k, v in spans.items() if k.startswith(prefix)}
        parallel_max = max(per_app.values()) if per_app else 0.0
        return max(legacy, parallel_max), per_app

    def _component_time(self, spans: Dict[str, float], comp_key: str) -> float:
        """Components may also be tagged per-app; aggregate as SUM since they
        are diagnostic totals (e.g. DDL across all tables) not phase gates."""
        legacy = spans.get(comp_key, 0.0)
        prefix = f"{comp_key}{_APP_SEP}"
        return legacy + sum(v for k, v in spans.items() if k.startswith(prefix))

    # ------------------------------------------------------------------ #
    # Reporting
    # ------------------------------------------------------------------ #

    def report(self, logger: logging.Logger) -> None:
        """Log a human-readable latency breakdown for the run.

        Under parallel execution, phases tagged with `app=` are aggregated as
        MAX across apps (so the sum reads against wall-clock cleanly), and a
        "Parallel detail" sub-section lists the per-app timings.
        """
        wall = perf_counter() - self._wall_start
        wall = wall if wall > 0 else 1e-9

        with self._lock:
            spans = dict(self._spans)
            counts = dict(self._counts)

        lines: List[str] = []
        lines.append("=" * 64)
        lines.append("RUN LATENCY SUMMARY")
        lines.append("-" * 64)
        lines.append(f"{'Phase':<46}{'Time':>9}{'%':>8}")
        lines.append("-" * 64)
        parallel_details: List[Tuple[str, Dict[str, float]]] = []
        for label, key in _PHASES:
            t, per_app = self._phase_time(spans, key)
            if t <= 0:
                continue
            lines.append(f"{label:<46}{t:>8.3f}s{100 * t / wall:>7.0f}%")
            if per_app:
                parallel_details.append((label, per_app))
        lines.append("-" * 64)
        lines.append(f"{'TOTAL (wall clock)':<46}{wall:>8.3f}s{100:>7.0f}%")

        # Components only if any were recorded.
        comp_lines: List[str] = []
        for label, tkey, ckey in _COMPONENTS:
            t = self._component_time(spans, tkey)
            c = counts.get(ckey, 0) if ckey else 0
            if t <= 0 and c <= 0:
                continue
            suffix = f"  ({c} round-trips)" if ckey and c else ""
            comp_lines.append(f"  {label:<44}{t:>8.3f}s{suffix}")
        if comp_lines:
            lines.append("-" * 64)
            lines.append("Component breakdown (diagnostic; overlaps phases above):")
            lines.extend(comp_lines)

        # Parallel detail: per-app phase timings, so you can see worker spread.
        if parallel_details:
            lines.append("-" * 64)
            lines.append("Parallel detail (per-app phase time — MAX gates wall-clock):")
            for label, per_app in parallel_details:
                lines.append(f"  {label}")
                for app in sorted(per_app):
                    lines.append(f"    [{app}] {per_app[app]:.3f}s")

        # Surface network cost explicitly — the usual culprit on a remote DB.
        total_rt = counts.get("db_roundtrips", 0) + counts.get("metadata_roundtrips", 0)
        if total_rt:
            lines.append("-" * 64)
            lines.append(f"DB round-trips this run: {total_rt}")
        lines.append("=" * 64)

        logger.info("Run latency breakdown:\n" + "\n".join(lines))


# Shared, module-level instance for the whole run.
metrics = RunMetrics()
