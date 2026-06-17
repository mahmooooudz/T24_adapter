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

Usage
-----
    from t24_adapter.metrics import metrics

    metrics.reset()
    with metrics.span("pass2_write"):
        ...                       # times the block
    metrics.add("db_upsert", dt)  # add a pre-measured duration
    metrics.incr("db_roundtrips") # bump a counter
    metrics.report(logger)        # log the summary table

The module exposes a single shared `metrics` instance so any module in the
pipeline can record into the same run without threading an object through
every constructor. `reset()` starts a fresh run.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from time import perf_counter
from typing import Dict, List, Tuple

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


class RunMetrics:
    """Accumulates named timing spans and counters for one pipeline run."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        """Begin a fresh run: clear all spans/counters and start the wall clock."""
        self._spans: Dict[str, float] = {}
        self._counts: Dict[str, int] = {}
        self._wall_start = perf_counter()

    @contextmanager
    def span(self, name: str):
        """Time a block and accumulate it under `name` (re-entrant: adds up)."""
        start = perf_counter()
        try:
            yield
        finally:
            self._spans[name] = self._spans.get(name, 0.0) + (perf_counter() - start)

    def add(self, name: str, seconds: float) -> None:
        """Add a pre-measured duration (for spans timed manually)."""
        self._spans[name] = self._spans.get(name, 0.0) + seconds

    def incr(self, name: str, n: int = 1) -> None:
        """Bump a named counter (e.g. DB round-trips)."""
        self._counts[name] = self._counts.get(name, 0) + n

    def get(self, name: str) -> float:
        return self._spans.get(name, 0.0)

    def count(self, name: str) -> int:
        return self._counts.get(name, 0)

    # ------------------------------------------------------------------ #
    # Reporting
    # ------------------------------------------------------------------ #

    def report(self, logger: logging.Logger) -> None:
        """Log a human-readable latency breakdown for the run."""
        wall = perf_counter() - self._wall_start
        wall = wall if wall > 0 else 1e-9

        lines: List[str] = []
        lines.append("=" * 64)
        lines.append("RUN LATENCY SUMMARY")
        lines.append("-" * 64)
        lines.append(f"{'Phase':<46}{'Time':>9}{'%':>8}")
        lines.append("-" * 64)
        phase_total = 0.0
        for label, key in _PHASES:
            t = self._spans.get(key, 0.0)
            if t <= 0:
                continue
            phase_total += t
            lines.append(f"{label:<46}{t:>8.3f}s{100 * t / wall:>7.0f}%")
        lines.append("-" * 64)
        lines.append(f"{'TOTAL (wall clock)':<46}{wall:>8.3f}s{100:>7.0f}%")

        # Components only if any were recorded.
        comp_lines: List[str] = []
        for label, tkey, ckey in _COMPONENTS:
            t = self._spans.get(tkey, 0.0)
            c = self._counts.get(ckey, 0) if ckey else 0
            if t <= 0 and c <= 0:
                continue
            suffix = f"  ({c} round-trips)" if ckey and c else ""
            comp_lines.append(f"  {label:<44}{t:>8.3f}s{suffix}")
        if comp_lines:
            lines.append("-" * 64)
            lines.append("Component breakdown (diagnostic; overlaps phases above):")
            lines.extend(comp_lines)

        # Surface network cost explicitly — the usual culprit on a remote DB.
        total_rt = self._counts.get("db_roundtrips", 0) + self._counts.get("metadata_roundtrips", 0)
        if total_rt:
            lines.append("-" * 64)
            lines.append(f"DB round-trips this run: {total_rt}")
        lines.append("=" * 64)

        logger.info("Run latency breakdown:\n" + "\n".join(lines))


# Shared, module-level instance for the whole run.
metrics = RunMetrics()
