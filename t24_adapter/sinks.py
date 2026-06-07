"""
sinks.py
--------
T24 Generic Adapter - Output Serialization Layer

Converts NormalizedField records into various output formats:
- CSV file
- JSON Lines file (one JSON object per line, streaming-friendly)
- Python dict (for in-memory use / DataFrame)
- Pandas DataFrame (if pandas is available)

All writers accept an Iterator[NormalizedField] so they work
directly with the streaming pipeline without buffering all records.
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from .models import NormalizedField

logger = logging.getLogger("t24-adapter.sinks")

# Standard output column order
OUTPUT_COLUMNS = [
    "app_name",
    "record_id",
    "source_file",
    "xml_element",
    "raw_position",
    "resolved_position",
    "field_name",
    "system_type",
    "data_type",
    "format_rule",
    "cardinality",
    "mv_index",
    "sv_index",
    "value",
    "relationship_target",
    "is_local_ref",
    "is_custom_field",
    "is_mapped",
    "warnings",
]


class NormalizedOutputWriter:
    """
    Serializes NormalizedField records to various output formats.

    All methods are class methods or static methods and can be
    called without instantiation.
    """

    @staticmethod
    def to_dict(row: NormalizedField) -> Dict[str, Any]:
        """
        Convert a NormalizedField to a plain Python dictionary.
        This is the base representation used by all other serializers.
        """
        return {
            "app_name": row.app_name,
            "record_id": row.record_id,
            "source_file": row.source_file,
            "xml_element": row.xml_element,
            "raw_position": row.raw_position,
            "resolved_position": row.resolved_position,
            "field_name": row.field_name,
            "system_type": row.system_type,
            "data_type": row.data_type,
            "format_rule": row.format_rule,
            "cardinality": row.cardinality,
            "mv_index": row.mv_index,
            "sv_index": row.sv_index,
            "value": row.value,
            "relationship_target": row.relationship_target,
            "is_local_ref": row.is_local_ref,
            "is_custom_field": row.is_custom_field,
            "is_mapped": row.is_mapped,
            "warnings": ",".join(row.warnings) if row.warnings else None,
        }

    @classmethod
    def write_csv(
        cls,
        rows: Iterator[NormalizedField],
        output_file: Path
    ) -> int:
        """
        Write NormalizedField records to a CSV file.

        Parameters
        ----------
        rows        : Iterator of NormalizedField records
        output_file : Output file path

        Returns
        -------
        int : Number of rows written
        """
        output_file.parent.mkdir(parents=True, exist_ok=True)
        count = 0

        with output_file.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=OUTPUT_COLUMNS)
            writer.writeheader()
            for row in rows:
                writer.writerow(cls.to_dict(row))
                count += 1

        logger.info(f"CSV written: {output_file} ({count} rows)")
        return count

    @classmethod
    def write_jsonl(
        cls,
        rows: Iterator[NormalizedField],
        output_file: Path
    ) -> int:
        """
        Write NormalizedField records to a JSON Lines file.
        Each line is a complete JSON object (streaming-friendly format).

        Parameters
        ----------
        rows        : Iterator of NormalizedField records
        output_file : Output file path

        Returns
        -------
        int : Number of rows written
        """
        output_file.parent.mkdir(parents=True, exist_ok=True)
        count = 0

        with output_file.open("w", encoding="utf-8") as file:
            for row in rows:
                file.write(json.dumps(cls.to_dict(row), ensure_ascii=False) + "\n")
                count += 1

        logger.info(f"JSONL written: {output_file} ({count} rows)")
        return count

    @classmethod
    def to_list(cls, rows: Iterator[NormalizedField]) -> List[Dict[str, Any]]:
        """
        Collect all records into a Python list of dicts.
        Use for small datasets or in-memory processing.
        For large datasets, prefer write_csv or write_jsonl.
        """
        return [cls.to_dict(row) for row in rows]

    @classmethod
    def to_dataframe(cls, rows: Iterator[NormalizedField]):
        """
        Convert records to a Pandas DataFrame.

        Requires pandas to be installed.
        Use for interactive analysis or display.

        Returns
        -------
        pandas.DataFrame
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for to_dataframe(). "
                "Install it with: pip install pandas"
            )

        data = cls.to_list(rows)
        df = pd.DataFrame(data, columns=OUTPUT_COLUMNS)
        logger.info(f"DataFrame created: {len(df)} rows x {len(df.columns)} columns")
        return df

    @classmethod
    def print_table(cls, rows: Iterator[NormalizedField], max_rows: int = 50) -> None:
        """
        Print a subset of records as a formatted table to stdout.
        Requires pandas and tabulate.
        Limited to max_rows for readability.

        Parameters
        ----------
        rows     : Iterator of NormalizedField records
        max_rows : Maximum number of rows to print (default 50)
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for print_table()")

        data = cls.to_list(rows)
        if not data:
            print("No records to display.")
            return

        df = pd.DataFrame(data[:max_rows], columns=OUTPUT_COLUMNS)

        # Select key display columns for readability
        display_cols = [
            "app_name", "record_id", "xml_element",
            "resolved_position", "field_name",
            "mv_index", "sv_index", "value",
            "is_mapped", "warnings"
        ]
        display_cols = [c for c in display_cols if c in df.columns]

        try:
            print(df[display_cols].to_markdown(index=False))
        except Exception:
            print(df[display_cols].to_string(index=False))

        if len(data) > max_rows:
            print(f"\n... {len(data) - max_rows} more rows not shown ...")
