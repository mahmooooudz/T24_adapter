"""
metadata_loaders.py
-------------------
T24 Generic Adapter - Metadata Loaders

Four loader classes, one per metadata source type:

1. StandardSelectionLoader  - Reads STANDARD.SELECTION XML (repeated C1/C3 pattern)
2. LocalReferenceLoader     - Reads LOCAL.REF XML (supports two XML shapes)
3. CustomizationLoader      - Reads bank custom field XML
4. RelationshipLoader       - Reads explicit relationship XML

Each loader is tolerant and writes into the T24MetadataRegistry.
Loaders never throw on missing optional attributes; they log and continue.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional

from .metadata_registry import T24MetadataRegistry
from .models import FieldMetadata, RelationshipMetadata
from .xml_utils import XmlUtils

logger = logging.getLogger("t24-adapter.loaders")


# ================================================================== #
# 1. Standard Selection Loader
# ================================================================== #

class StandardSelectionLoader:
    """
    Loads STANDARD.SELECTION-style metadata XML into the registry.

    Expected XML structure
    ----------------------
    <ROW>
        <C1>MNEMONIC</C1>      <!-- Field name, repeated once per field -->
        <C1>SHORT.NAME</C1>
        <C1>NAME.1</C1>
        ...
        <C2>SYS</C2>            <!-- System type: SYS or USR, repeated -->
        ...
        <C3>1</C3>              <!-- Position key, repeated once per field -->
        <C3>2</C3>
        <C3>3</C3>
        <C3>64.1</C3>           <!-- Local ref positions use dot notation -->
        ...
        <C4>A</C4>              <!-- Data type: A/R/D/N, repeated -->
        <C5>IN2A</C5>           <!-- Validation rule, repeated -->
        <C6>35.1</C6>           <!-- Format rule, repeated -->
        <C7>MULTI</C7>          <!-- Cardinality: SINGLE/MULTI, repeated -->
        <C8>COUNTRY</C8>        <!-- Foreign key / relationship target, repeated -->
        <C9>L</C9>              <!-- Alignment: L/R, repeated -->
        <C10>INPUT</C10>        <!-- Input status, repeated -->
        <C11>Short name...</C11> <!-- Description, repeated -->
        <C12>2</C12>            <!-- MV flag count, repeated -->
    </ROW>

    Minimum required: C1 (field names) and C3 (positions).
    All other columns are optional but recommended.
    """

    def load(
        self,
        app_name: str,
        file_path: Path,
        registry: T24MetadataRegistry
    ) -> None:
        logger.info(f"Loading STANDARD.SELECTION for '{app_name}' from: {file_path.name}")

        tree = ET.parse(file_path)
        root = tree.getroot()
        row = XmlUtils.find_first_row(root)

        if row is None:
            raise ValueError(
                f"No <ROW> element found in STANDARD.SELECTION file: {file_path}"
            )

        columns = XmlUtils.extract_repeated_columns(row)
        field_names = columns.get("C1", [])
        positions = columns.get("C3", [])

        if not field_names or not positions:
            raise ValueError(
                f"STANDARD.SELECTION must contain repeated <C1> (field names) "
                f"and <C3> (positions): {file_path}"
            )

        count = min(len(field_names), len(positions))
        logger.info(f"  Found {count} field definitions")

        registered = 0
        for index in range(count):
            field_name = self._get(columns, "C1", index)
            position = self._get(columns, "C3", index)

            if not field_name or not position:
                continue

            system_type = self._get(columns, "C2", index)
            relationship_target = self._get(columns, "C8", index)

            meta = FieldMetadata(
                app_name=app_name.upper(),
                field_name=field_name,
                position=position,
                system_type=system_type,
                data_type=self._get(columns, "C4", index),
                validation_rule=self._get(columns, "C5", index),
                format_rule=self._get(columns, "C6", index),
                cardinality=self._get(columns, "C7", index),
                relationship_target=relationship_target,
                alignment=self._get(columns, "C9", index),
                input_status=self._get(columns, "C10", index),
                description=self._get(columns, "C11", index),
                is_local_ref=self._is_local_ref_position(position),
                is_custom_field=(system_type or "").upper() in {"USR", "LOCAL", "CUSTOM"},
                source=str(file_path)
            )

            registry.register_field(meta)
            registered += 1

            # Auto-register relationship if C8 is populated
            if relationship_target and relationship_target.strip():
                registry.register_relationship(
                    RelationshipMetadata(
                        app_name=app_name.upper(),
                        field_name=field_name,
                        position=position,
                        target_application=relationship_target,
                        relationship_type="STANDARD_SELECTION_C8",
                        source=str(file_path)
                    )
                )

        logger.info(f"  Registered {registered} fields from STANDARD.SELECTION")

    @staticmethod
    def _get(columns: Dict[str, List[str]], col: str, index: int) -> Optional[str]:
        values = columns.get(col, [])
        if index < len(values):
            value = values[index].strip()
            return value if value else None
        return None

    @staticmethod
    def _is_local_ref_position(position: str) -> bool:
        return "." in position


# ================================================================== #
# 2. Local Reference Loader
# ================================================================== #

class LocalReferenceLoader:
    """
    Loads LOCAL.REF metadata into the registry.

    Supports two XML shapes:

    Shape 1 - Repeated column style (same as STANDARD.SELECTION):
    <ROW>
        <C1>CUST.SEGMENT</C1>
        <C3>64.1</C3>
        ...
    </ROW>

    Shape 2 - Explicit field node style:
    <LOCAL.REF>
        <FIELD>
            <NAME>CUST.SEGMENT</NAME>
            <POSITION>64.1</POSITION>
            <TYPE>A</TYPE>
            <FORMAT>15.1</FORMAT>
        </FIELD>
    </LOCAL.REF>

    The loader auto-detects which shape is present.
    """

    def load(
        self,
        app_name: str,
        file_path: Path,
        registry: T24MetadataRegistry
    ) -> None:
        logger.info(f"Loading LOCAL.REF for '{app_name}' from: {file_path.name}")

        tree = ET.parse(file_path)
        root = tree.getroot()

        if self._looks_like_repeated_c_row(root):
            logger.info(f"  Detected repeated-column style LOCAL.REF -> delegating to StandardSelectionLoader")
            StandardSelectionLoader().load(app_name, file_path, registry)
            return

        registered = 0
        for field_node in root.iter():
            tag = XmlUtils.strip_namespace(field_node.tag).upper()
            if tag not in {"FIELD", "LOCAL.FIELD", "LOCAL_REF_FIELD", "REF.FIELD"}:
                continue

            name = self._child_text(field_node, {"NAME", "FIELD.NAME", "C1"})
            position = self._child_text(field_node, {"POSITION", "LOCATION", "POS", "C3"})

            if not name or not position:
                continue

            meta = FieldMetadata(
                app_name=app_name.upper(),
                field_name=name,
                position=position,
                system_type="USR",
                data_type=self._child_text(field_node, {"TYPE", "DATA.TYPE", "C4"}),
                validation_rule=self._child_text(field_node, {"VALIDATION", "VALIDATION.RULE", "C5"}),
                format_rule=self._child_text(field_node, {"FORMAT", "LENGTH", "C6"}),
                cardinality=self._child_text(field_node, {"CARDINALITY", "MULTI.VALUE", "C7"}),
                relationship_target=self._child_text(field_node, {"RELATIONSHIP", "FOREIGN.KEY", "C8"}),
                description=self._child_text(field_node, {"DESCRIPTION", "DESC", "C11"}),
                is_local_ref=True,
                is_custom_field=True,
                source=str(file_path)
            )

            registry.register_field(meta)
            registered += 1

        logger.info(f"  Registered {registered} local reference fields")

    @staticmethod
    def _looks_like_repeated_c_row(root: ET.Element) -> bool:
        tags = {XmlUtils.strip_namespace(x.tag).upper() for x in root.iter()}
        return "C1" in tags and "C3" in tags

    @staticmethod
    def _child_text(node: ET.Element, names: set) -> Optional[str]:
        names_upper = {x.upper() for x in names}
        for child in node:
            tag = XmlUtils.strip_namespace(child.tag).upper()
            if tag in names_upper:
                value = XmlUtils.text(child)
                return value if value else None
        return None


# ================================================================== #
# 3. Customization Loader
# ================================================================== #

class CustomizationLoader:
    """
    Loads bank-specific custom field definitions into the registry.

    Looks for nodes tagged FIELD, CUSTOM.FIELD, CUSTOM_FIELD, or LOCAL.FIELD
    and extracts name/position/type/format/relationship.

    Custom fields are flagged as is_custom_field=True in the output.
    """

    def load(
        self,
        app_name: str,
        file_path: Path,
        registry: T24MetadataRegistry
    ) -> None:
        logger.info(f"Loading CUSTOMIZATION for '{app_name}' from: {file_path.name}")

        tree = ET.parse(file_path)
        root = tree.getroot()

        registered = 0
        for node in root.iter():
            tag = XmlUtils.strip_namespace(node.tag).upper()
            if tag not in {"FIELD", "CUSTOM.FIELD", "CUSTOM_FIELD", "LOCAL.FIELD"}:
                continue

            name = self._get(node, ["NAME", "FIELD.NAME", "FIELD_NAME"])
            position = self._get(node, ["POSITION", "LOCATION", "FIELD.NO", "POS"])

            if not name or not position:
                continue

            meta = FieldMetadata(
                app_name=app_name.upper(),
                field_name=name,
                position=position,
                system_type="CUSTOM",
                data_type=self._get(node, ["TYPE", "DATA.TYPE", "DATA_TYPE"]),
                validation_rule=self._get(node, ["VALIDATION", "VALIDATION.RULE"]),
                format_rule=self._get(node, ["FORMAT", "LENGTH"]),
                cardinality=self._get(node, ["CARDINALITY", "SINGLE.MULTI", "MULTI.VALUE"]),
                relationship_target=self._get(node, ["RELATIONSHIP", "TARGET.APPLICATION", "FOREIGN.KEY"]),
                description=self._get(node, ["DESCRIPTION", "DESC"]),
                is_local_ref="." in position,
                is_custom_field=True,
                source=str(file_path)
            )

            registry.register_field(meta)
            registered += 1

        logger.info(f"  Registered {registered} custom fields")

    @staticmethod
    def _get(node: ET.Element, names: List[str]) -> Optional[str]:
        names_upper = {x.upper() for x in names}
        for child in node:
            tag = XmlUtils.strip_namespace(child.tag).upper()
            if tag in names_upper:
                value = XmlUtils.text(child)
                return value if value else None
        return None


# ================================================================== #
# 4. Relationship Loader
# ================================================================== #

class RelationshipLoader:
    """
    Loads explicit relationship metadata from a dedicated relationship XML file.

    These augment relationships already detected from STANDARD.SELECTION C8.

    Expected XML structure:
    <RELATIONSHIPS>
        <RELATIONSHIP>
            <FIELD>ACCOUNT.OFFICER</FIELD>
            <POSITION>13</POSITION>
            <TARGET>DEPT.ACCT.OFFICER</TARGET>
            <TYPE>FOREIGN_KEY</TYPE>
        </RELATIONSHIP>
    </RELATIONSHIPS>
    """

    def load(
        self,
        app_name: str,
        file_path: Path,
        registry: T24MetadataRegistry
    ) -> None:
        logger.info(f"Loading RELATIONSHIPS for '{app_name}' from: {file_path.name}")

        tree = ET.parse(file_path)
        root = tree.getroot()

        registered = 0
        for node in root.iter():
            tag = XmlUtils.strip_namespace(node.tag).upper()
            if tag not in {"RELATIONSHIP", "RELATION", "LINK"}:
                continue

            field_name = self._child_text(node, ["FIELD", "FIELD.NAME", "SOURCE.FIELD"])
            position = self._child_text(node, ["POSITION", "LOCATION", "SOURCE.POSITION"])
            target = self._child_text(
                node, ["TARGET", "TARGET.APPLICATION", "APPLICATION", "FOREIGN.APPLICATION"]
            )

            if not position or not target:
                continue

            registry.register_relationship(
                RelationshipMetadata(
                    app_name=app_name.upper(),
                    field_name=field_name or f"FIELD_{position}",
                    position=position,
                    target_application=target,
                    relationship_type=self._child_text(node, ["TYPE", "RELATIONSHIP.TYPE"]),
                    source=str(file_path)
                )
            )
            registered += 1

        logger.info(f"  Registered {registered} explicit relationships")

    @staticmethod
    def _child_text(node: ET.Element, names: List[str]) -> Optional[str]:
        names_upper = {x.upper() for x in names}
        for child in node:
            tag = XmlUtils.strip_namespace(child.tag).upper()
            if tag in names_upper:
                value = XmlUtils.text(child)
                return value if value else None
        return None
