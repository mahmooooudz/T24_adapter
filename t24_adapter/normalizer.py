"""
normalizer.py
-------------
T24 Generic Adapter - Normalization Engine

The core field-resolution and flattening engine.

Converts a T24 XML <row> element into a flat list of NormalizedField records.

Handles:
- Standard single-value fields (c1, c2, c30)
- Multi-value fields (c2 m="1", c2 m="2")
- Sub-value fields (c64 m="3" s="1", c64 m="3" s="2")
- Local reference arrays (c64 m="1" -> position 64.1)
- Unknown / unmapped fields (emitted as FIELD_<position>)
- Configurable local reference base positions
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Optional

from .config import T24PipelineConfig
from .metadata_registry import T24MetadataRegistry
from .models import NormalizedField
from .xml_utils import XmlUtils

logger = logging.getLogger("t24-adapter.normalizer")


class T24Normalizer:
    """
    Normalizes a single T24 XML <row> element into a list of NormalizedField records.

    Position Resolution Logic
    -------------------------
    Normal fields:
        <c1>  -> raw_position = "1",  resolved_position = "1"
        <c2>  -> raw_position = "2",  resolved_position = "2"
        <c30> -> raw_position = "30", resolved_position = "30"

    Multi-value fields (same position, multiple occurrences):
        <c2 m="1"> -> mv_index = "1"
        <c2 m="2"> -> mv_index = "2"
        Position stays as "2" for both.

    Local reference arrays (c64 is special):
        <c64 m="1"> -> raw_position = "64", resolved_position = "64.1"
        <c64 m="2"> -> raw_position = "64", resolved_position = "64.2"
        <c64 m="3"> -> raw_position = "64", resolved_position = "64.3"

    Sub-values within local references:
        <c64 m="3" s="1"> -> resolved = "64.3", sv_index = "1"
        <c64 m="3" s="2"> -> resolved = "64.3", sv_index = "2"

    Both produce the same resolved_position (64.3) but different sv_index values.
    They resolve to the same field name (e.g. TAX.ID) from the registry.

    Configuring additional local reference base positions:
        local_ref_base_positions=("64", "90", "100")
        This makes c90 m="1" -> resolved 90.1 as well.

    Usage
    -----
    normalizer = T24Normalizer(registry=registry, config=config)
    fields = normalizer.normalize_record(
        app_name="CUSTOMER",
        row=row_element,
        source_file="data/CUSTOMER.xml"
    )
    """

    def __init__(
        self,
        registry: T24MetadataRegistry,
        config: T24PipelineConfig
    ):
        self.registry = registry
        self.config = config

    def normalize_record(
        self,
        app_name: str,
        row: ET.Element,
        source_file: str,
        record_id: Optional[str] = None,
    ) -> List[NormalizedField]:
        """
        Normalize all XML elements in a <row> into NormalizedField records.

        Parameters
        ----------
        app_name    : T24 application name (e.g. "CUSTOMER")
        row         : ET.Element representing the <row>
        source_file : Source XML file path string (for traceability)
        record_id   : Explicit primary key (e.g. the database recordId column).
                      When None, it is derived from a field position.

        Returns
        -------
        List[NormalizedField] - one entry per XML element in the row
        """
        if record_id is None:
            record_id = self._extract_record_id(row, app_name)
        output: List[NormalizedField] = []

        for child in row:
            xml_element = XmlUtils.strip_namespace(child.tag).lower()
            raw_position = XmlUtils.extract_numeric_position_from_tag(xml_element)

            if not raw_position:
                # Not a field element (e.g. comments, unknown structural tags)
                continue

            mv_index = child.attrib.get("m")
            sv_index = child.attrib.get("s")
            value = XmlUtils.text(child)

            resolved_position = self._resolve_position(raw_position, mv_index)
            field_meta = self.registry.resolve_field(app_name, resolved_position)
            relationship_meta = self.registry.resolve_relationship(app_name, resolved_position)

            warnings = self._build_warnings(
                raw_position=raw_position,
                resolved_position=resolved_position,
                mv_index=mv_index,
                sv_index=sv_index,
                field_meta=field_meta
            )

            if not field_meta and self.config.fail_on_unmapped_field:
                raise ValueError(
                    f"Unmapped field: app={app_name}, "
                    f"position={resolved_position}, element={xml_element}"
                )

            normalized = NormalizedField(
                app_name=app_name.upper(),
                record_id=record_id,
                source_file=source_file,
                xml_element=xml_element,
                raw_position=raw_position,
                resolved_position=resolved_position,
                field_name=(
                    field_meta.field_name
                    if field_meta
                    else f"FIELD_{resolved_position}"
                ),
                system_type=field_meta.system_type if field_meta else None,
                data_type=field_meta.data_type if field_meta else None,
                format_rule=field_meta.format_rule if field_meta else None,
                cardinality=field_meta.cardinality if field_meta else None,
                mv_index=mv_index,
                sv_index=sv_index,
                value=value,
                relationship_target=(
                    relationship_meta.target_application
                    if relationship_meta
                    else (field_meta.relationship_target if field_meta else None)
                ),
                is_local_ref=(
                    field_meta.is_local_ref
                    if field_meta
                    else self._is_local_ref(raw_position, mv_index)
                ),
                is_custom_field=field_meta.is_custom_field if field_meta else False,
                is_mapped=field_meta is not None,
                warnings=warnings
            )

            output.append(normalized)

        return output

    # ------------------------------------------------------------------ #
    # Private Helpers
    # ------------------------------------------------------------------ #

    def _resolve_position(
        self,
        raw_position: str,
        mv_index: Optional[str]
    ) -> str:
        """
        Compute the metadata lookup key from raw XML position and mv_index.

        For positions in local_ref_base_positions (default: "64"):
            raw="64", mv="3" -> "64.3"

        For all other positions:
            raw="2", mv="1" -> "2"  (mv_index does not change the position key)
        """
        if raw_position in self.config.local_ref_base_positions and mv_index:
            return f"{raw_position}.{mv_index}"
        return raw_position

    def _is_local_ref(self, raw_position: str, mv_index: Optional[str]) -> bool:
        """
        Determine if a field is a local reference based on position alone.
        Used when field_meta is not available.
        """
        return raw_position in self.config.local_ref_base_positions and bool(mv_index)

    def _build_warnings(
        self,
        raw_position: str,
        resolved_position: str,
        mv_index: Optional[str],
        sv_index: Optional[str],
        field_meta
    ) -> List[str]:
        """
        Build a list of warning codes for a field.
        Warnings do not stop processing; they are attached to the output row.
        """
        warnings = []

        if field_meta is None:
            warnings.append("UNMAPPED_FIELD")

        if (
            raw_position in self.config.local_ref_base_positions
            and mv_index
            and field_meta is None
        ):
            warnings.append("LOCAL_REF_WITHOUT_METADATA")

        if sv_index and not mv_index:
            warnings.append("SUB_VALUE_WITHOUT_MULTI_VALUE")

        return warnings

    def _extract_record_id(self, row: ET.Element, app_name: str) -> Optional[str]:
        """
        Extract the primary record identifier for one record.

        The key field position is configurable per application via
        config.record_id_positions (e.g. {"ACCOUNT": "2"} to key ACCOUNT on
        ACCOUNT.NO). Applications not listed use record_id_default_position
        ("1"), which is the T24 default (the first field, e.g. CUSTOMER's
        MNEMONIC).

        This avoids the wrong assumption that field 1 is always the key: in
        ACCOUNT, field 1 is CUSTOMER (the holder), not the account itself.
        """
        positions = self.config.record_id_positions or {}
        key_position = positions.get(
            app_name.upper(), self.config.record_id_default_position
        )
        for child in row:
            pos = XmlUtils.extract_numeric_position_from_tag(
                XmlUtils.strip_namespace(child.tag)
            )
            if pos == key_position:
                return XmlUtils.text(child)
        return None
