"""
wide_writer.py
--------------
T24 Generic Adapter - Wide-Table Output Layer

Pivots the long (vertical) NormalizedField stream into a wide table:
one row per record, with each field_name becoming a column header.

Long format (one row per value):
    record_id  field_name   value
    JD1009     MNEMONIC     JD1009
    JD1009     SHORT.NAME   John Doe
    JD1009     TAX.ID       TX-998822
    JD1009     TAX.ID       TX-445511

Wide format (one row per record):
    record_id  MNEMONIC  SHORT.NAME  TAX.ID_1    TAX.ID_2
    JD1009     JD1009    John Doe    TX-998822   TX-445511

Indexing rule
-------------
A field that never occurs more than once within a single record stays a
single plain column (SHORT.NAME). A field that occurs multiple times in
any record (multi-value and/or sub-value) expands into indexed columns
(TAX.ID_1, TAX.ID_2, ...). Records with fewer values leave the extra
indexed columns blank.

Memory model
------------
Two-pass streaming, constant memory:

    Pass 1 (discover_schema): stream once to learn, per application, the
            set of fields, their max occurrence count within any single
            record, and column ordering. Holds only the schema, never data.

    Pass 2 (write_*): stream again. Because all fields of one record arrive
            contiguously, accumulate the current record and flush one wide
            row when the record boundary is reached. Holds one record at a
            time.

Both passes call pipeline.run() (the pipeline is a re-runnable generator),
so the source XML is parsed twice — but both passes reuse the pipeline's single
read connection, so the cost is I/O, not connection setup. The discovery pass
is what makes the exact bare-vs-indexed shape possible WITHOUT ever losing a
value: a field becomes NAME only if it never repeats, and NAME_1..N sized to
its true maximum. (A single-pass scheme cannot know that maximum up front, and
deriving width from metadata cardinality risks dropping values when a field is
mislabelled SINGLE — so the two-pass discovery is the correct, lossless choice.)

Why not pandas
--------------
The engine is deliberately pure-Python and streaming (one record in flight),
so memory is constant regardless of row count and it scales to very large
extracts. pandas would load the data into an in-memory frame and defeat that,
so it is reserved strictly for the optional, bounded `to_dataframe` preview.
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from .models import NormalizedField

logger = logging.getLogger("t24-adapter.wide")


# Leading columns present in every wide row, before the field columns.
LEADING_COLUMNS = ["recordId", "app_name"]

# Sentinel marking "no record started yet" — distinct from a real record_id
# of None, so a stream of None-keyed records still produces rows.
_NO_RECORD = object()


class _FieldSpec:
    """
    Schema for one logical field within one application.

    Attributes
    ----------
    position    : resolved_position (unique key, e.g. "2", "64.3")
    field_name  : display name used to build the column header(s)
    max_count   : max number of occurrences seen within a single record
    sort_key    : numeric tuple for column ordering (e.g. (64, 3))
    """

    __slots__ = ("position", "field_name", "max_count", "sort_key")

    def __init__(self, position: str, field_name: str, sort_key: Tuple[int, ...]):
        self.position = position
        self.field_name = field_name
        self.max_count = 1
        self.sort_key = sort_key


class _AppSchema:
    """
    Discovered wide-table schema for a single application.

    Built during pass 1. Knows every field, how many indexed columns each
    field needs, and the final ordered list of column headers.
    """

    def __init__(self, app_name: str):
        self.app_name = app_name
        # position -> _FieldSpec
        self._fields: Dict[str, _FieldSpec] = {}
        # Cached after finalize()
        self._columns: Optional[List[str]] = None
        # position -> ordered list of column headers for that field
        self._headers_by_position: Dict[str, List[str]] = {}

    def observe_record(self, counts: Dict[str, int], names: Dict[str, str]) -> None:
        """
        Update the schema from one record's per-field occurrence counts.

        Parameters
        ----------
        counts : position -> number of values seen in this record
        names  : position -> field_name seen for this position
        """
        for position, count in counts.items():
            spec = self._fields.get(position)
            if spec is None:
                spec = _FieldSpec(
                    position=position,
                    field_name=names[position],
                    sort_key=_position_sort_key(position),
                )
                self._fields[position] = spec
            if count > spec.max_count:
                spec.max_count = count

    def finalize(self) -> None:
        """
        Freeze the schema: compute the ordered column list and the
        per-position header lists. Resolves header-name collisions
        (two positions sharing one field_name) by appending the position.
        """
        ordered_specs = sorted(self._fields.values(), key=lambda s: s.sort_key)

        # Detect field_name collisions across different positions.
        name_counts: Dict[str, int] = {}
        for spec in ordered_specs:
            name_counts[spec.field_name] = name_counts.get(spec.field_name, 0) + 1

        columns: List[str] = list(LEADING_COLUMNS)
        for spec in ordered_specs:
            base = spec.field_name
            if name_counts[base] > 1:
                # Collision: disambiguate with the position, e.g. TAX.ID(64.3)
                base = f"{base}({spec.position})"

            if spec.max_count == 1:
                headers = [base]
            else:
                headers = [f"{base}_{i}" for i in range(1, spec.max_count + 1)]

            self._headers_by_position[spec.position] = headers
            columns.extend(headers)

        self._columns = columns

    @property
    def columns(self) -> List[str]:
        if self._columns is None:
            raise RuntimeError("Schema not finalized. Call finalize() first.")
        return self._columns

    def headers_for(self, position: str) -> List[str]:
        """Ordered column headers for a given position (empty if unknown)."""
        return self._headers_by_position.get(position, [])


def _position_sort_key(position: str) -> Tuple[int, ...]:
    """
    Convert a resolved_position into a numeric tuple for stable ordering.

        "2"    -> (2, 0)
        "64.3" -> (64, 3)
        "176"  -> (176, 0)

    Non-numeric or malformed positions sort after numeric ones, ordered by
    their raw string so the result stays deterministic across runs.
    """
    parts = position.split(".")
    try:
        nums = tuple(int(p) for p in parts)
    except ValueError:
        # Malformed position: sort after all numeric ones, then by the raw
        # string so ordering stays deterministic across runs.
        return (10**9, position)
    # Pad to length 2 so "2" and "64.3" compare consistently.
    if len(nums) == 1:
        return (nums[0], 0)
    return nums


class WidePivotWriter:
    """
    Pivots a long NormalizedField stream into per-application wide tables.

    Usage
    -----
    writer = WidePivotWriter()
    written = writer.write_csv(pipeline, output_dir=Path("output"))
    # written -> {"CUSTOMER": Path("output/CUSTOMER_wide.csv"), ...}
    """

    # ------------------------------------------------------------------ #
    # Pass 1: Schema discovery
    # ------------------------------------------------------------------ #

    def discover_schema(self, rows: Iterator[NormalizedField]) -> Dict[str, _AppSchema]:
        """
        Stream the long output once and build a wide-table schema per app.

        Returns
        -------
        Dict[app_name, _AppSchema] with each schema finalized.
        """
        schemas: Dict[str, _AppSchema] = {}

        # Per-record accumulators, reset at each record boundary.
        cur_app: object = _NO_RECORD
        cur_record_id: object = _NO_RECORD
        cur_counts: Dict[str, int] = {}
        cur_names: Dict[str, str] = {}

        def flush() -> None:
            if cur_app is _NO_RECORD:
                return
            schema = schemas.setdefault(cur_app, _AppSchema(cur_app))
            schema.observe_record(cur_counts, cur_names)

        for row in rows:
            boundary = (row.app_name != cur_app) or (row.record_id != cur_record_id)
            if boundary:
                flush()
                cur_app = row.app_name
                cur_record_id = row.record_id
                cur_counts = {}
                cur_names = {}

            pos = row.resolved_position
            cur_counts[pos] = cur_counts.get(pos, 0) + 1
            # First field_name seen for this position wins (they are identical
            # across occurrences of the same position by construction).
            cur_names.setdefault(pos, row.field_name)

        flush()

        for schema in schemas.values():
            schema.finalize()
            logger.info(
                f"[{schema.app_name}] wide schema: {len(schema.columns)} columns "
                f"({len(schema.columns) - len(LEADING_COLUMNS)} field columns)"
            )

        return schemas

    # ------------------------------------------------------------------ #
    # Pass 2: Row assembly
    # ------------------------------------------------------------------ #

    def _iter_wide_rows(
        self,
        rows: Iterator[NormalizedField],
        schema: _AppSchema,
        app_name: str,
    ) -> Iterator[Dict[str, str]]:
        """
        Stream the long output and yield one wide row dict per record of
        the given application. Records of other applications are skipped.

        A record boundary is detected when record_id changes. A non-adjacent
        duplicate primary key therefore yields its own wide row (two records
        are never silently merged). We deliberately do not track all seen ids
        to flag duplicates, as that would grow with the record count and break
        the pipeline's constant-memory guarantee.
        """
        cur_record_id: object = _NO_RECORD
        cur_row: Optional[Dict[str, str]] = None
        # Per-record, per-position running occurrence index (for _n ordering).
        cur_slot: Dict[str, int] = {}

        def new_row(record_id: Optional[str]) -> Dict[str, str]:
            row_dict = {col: "" for col in schema.columns}
            row_dict["recordId"] = record_id if record_id is not None else ""
            row_dict["app_name"] = app_name
            return row_dict

        for field in rows:
            if field.app_name != app_name:
                continue

            if field.record_id != cur_record_id:
                if cur_row is not None:
                    yield cur_row
                cur_record_id = field.record_id
                cur_row = new_row(field.record_id)
                cur_slot = {}

            headers = schema.headers_for(field.resolved_position)
            if not headers:
                # Field not present in schema (should not happen, both passes
                # see the same stream); skip defensively.
                continue

            slot = cur_slot.get(field.resolved_position, 0)
            if slot < len(headers):
                cur_row[headers[slot]] = field.value if field.value is not None else ""
            else:
                # More occurrences than pass 1 saw: impossible for the same
                # source, but guard rather than crash.
                logger.warning(
                    f"[{app_name}] record '{cur_record_id}' position "
                    f"{field.resolved_position} exceeded discovered max "
                    f"({len(headers)}); extra value dropped."
                )
            cur_slot[field.resolved_position] = slot + 1

        if cur_row is not None:
            yield cur_row

    def iter_wide_rows(self, rows, schema, app_name):
        """
        Public access to the wide-row generator: yields one wide row dict per
        record of `app_name`. Used by output sinks (CSV, JSONL, database).
        """
        return self._iter_wide_rows(rows, schema, app_name)

    def iter_all_wide_rows(self, rows, schemas: Dict[str, "_AppSchema"]):
        """
        Single pass over the FULL long stream, yielding (app_name, wide_row)
        for every record across all applications — so the source is read once
        for all apps instead of once per app.

        The pipeline emits records app-by-app (all of one application's records
        before the next), so rows for an app arrive contiguously; a new wide
        row starts whenever the application or record_id changes.
        """
        cur_app: object = _NO_RECORD
        cur_record_id: object = _NO_RECORD
        cur_schema: Optional[_AppSchema] = None
        cur_row: Optional[Dict[str, str]] = None
        cur_slot: Dict[str, int] = {}

        for field in rows:
            schema = schemas.get(field.app_name)
            if schema is None:
                # No schema discovered for this app (shouldn't happen — both
                # passes see the same stream); skip defensively.
                continue

            if field.app_name != cur_app or field.record_id != cur_record_id:
                if cur_row is not None:
                    yield (cur_app, cur_row)
                cur_app = field.app_name
                cur_record_id = field.record_id
                cur_schema = schema
                cur_row = {col: "" for col in schema.columns}
                cur_row["recordId"] = field.record_id if field.record_id is not None else ""
                cur_row["app_name"] = field.app_name
                cur_slot = {}

            headers = cur_schema.headers_for(field.resolved_position)
            if not headers:
                continue
            slot = cur_slot.get(field.resolved_position, 0)
            if slot < len(headers):
                cur_row[headers[slot]] = field.value if field.value is not None else ""
            cur_slot[field.resolved_position] = slot + 1

        if cur_row is not None:
            yield (cur_app, cur_row)

    # ------------------------------------------------------------------ #
    # Public writers
    # ------------------------------------------------------------------ #

    def write_csv(self, pipeline, output_dir: Path) -> Dict[str, Path]:
        """
        Write one wide CSV per application into output_dir.

        Parameters
        ----------
        pipeline   : T24GenericPipeline (re-runnable; run() called twice)
        output_dir : directory for the *_wide.csv files

        Returns
        -------
        Dict[app_name, output_path]
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        schemas = self.discover_schema(pipeline.run())

        if not schemas:
            logger.warning("No records discovered; no wide CSV written.")
            return {}

        outputs: Dict[str, Path] = {}
        for app_name, schema in schemas.items():
            out_path = output_dir / f"{app_name}_wide.csv"
            row_count = 0
            with out_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=schema.columns)
                writer.writeheader()
                for wide_row in self._iter_wide_rows(pipeline.run(), schema, app_name):
                    writer.writerow(wide_row)
                    row_count += 1
            outputs[app_name] = out_path
            logger.info(
                f"Wide CSV written: {out_path} "
                f"({row_count} rows x {len(schema.columns)} columns)"
            )
        return outputs

    def write_jsonl(self, pipeline, output_dir: Path) -> Dict[str, Path]:
        """
        Write one wide JSON Lines file per application into output_dir.
        Each line is one record as a flat JSON object keyed by column name.
        Empty cells are emitted as null.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        schemas = self.discover_schema(pipeline.run())

        if not schemas:
            logger.warning("No records discovered; no wide JSONL written.")
            return {}

        outputs: Dict[str, Path] = {}
        for app_name, schema in schemas.items():
            out_path = output_dir / f"{app_name}_wide.jsonl"
            row_count = 0
            with out_path.open("w", encoding="utf-8") as fh:
                for wide_row in self._iter_wide_rows(pipeline.run(), schema, app_name):
                    record = {k: (v if v != "" else None) for k, v in wide_row.items()}
                    fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                    row_count += 1
            outputs[app_name] = out_path
            logger.info(f"Wide JSONL written: {out_path} ({row_count} rows)")
        return outputs

    def to_dataframe(self, pipeline, app_name: str):
        """
        Build a wide pandas DataFrame for a single application.
        Requires pandas. Use for interactive analysis.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for to_dataframe(). Install with: pip install pandas"
            )

        schemas = self.discover_schema(pipeline.run())
        schema = schemas.get(app_name.upper())
        if schema is None:
            raise ValueError(
                f"No data found for application '{app_name}'. "
                f"Available: {sorted(schemas.keys())}"
            )

        data = list(self._iter_wide_rows(pipeline.run(), schema, app_name.upper()))
        df = pd.DataFrame(data, columns=schema.columns)
        # Blank cells -> proper NaN for analytics.
        df = df.replace("", None)
        logger.info(
            f"[{app_name.upper()}] wide DataFrame: {len(df)} rows x {len(df.columns)} columns"
        )
        return df
