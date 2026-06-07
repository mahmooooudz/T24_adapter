"""
metadata_registry.py
--------------------
T24 Generic Adapter - Metadata Registry

Central in-memory store for all T24 field and relationship metadata.
All metadata is indexed by (app_name, position) for O(1) lookup.

This is the engine that makes the adapter metadata-driven.
No field names are hardcoded. All business meaning comes from
the STANDARD.SELECTION, LOCAL.REF, and CUSTOMIZATION loaders
that populate this registry.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, List, Optional

from .models import FieldMetadata, RelationshipMetadata

logger = logging.getLogger("t24-adapter.registry")


class T24MetadataRegistry:
    """
    In-memory metadata store for T24 field schemas and relationships.

    Storage structure:
        fields_by_position[app_name][position] -> FieldMetadata
        relationships_by_position[app_name][position] -> RelationshipMetadata

    Position keys follow T24 addressing conventions:
        "1"     -> first field (c1)
        "2"     -> second field (c2)
        "64.1"  -> first local reference (c64 m="1")
        "64.3"  -> third local reference (c64 m="3")

    Usage
    -----
    registry = T24MetadataRegistry()
    registry.register_field(field_meta)
    meta = registry.resolve_field("CUSTOMER", "64.3")
    """

    def __init__(self):
        # Primary field store: app -> position -> FieldMetadata
        self.fields_by_position: Dict[str, Dict[str, FieldMetadata]] = defaultdict(dict)

        # Relationship store: app -> position -> RelationshipMetadata
        self.relationships_by_position: Dict[str, Dict[str, RelationshipMetadata]] = defaultdict(dict)

    # ------------------------------------------------------------------ #
    # Registration
    # ------------------------------------------------------------------ #

    def register_field(self, field_meta: FieldMetadata) -> None:
        """
        Register a field metadata record.
        If a record for the same (app_name, position) already exists,
        it is overwritten (later loaders take priority).
        """
        app = field_meta.app_name.upper()
        pos = field_meta.position
        self.fields_by_position[app][pos] = field_meta
        logger.debug(f"  Registered field: {app} pos={pos} name={field_meta.field_name}")

    def register_relationship(self, relationship: RelationshipMetadata) -> None:
        """
        Register a relationship metadata record.
        """
        app = relationship.app_name.upper()
        pos = relationship.position
        self.relationships_by_position[app][pos] = relationship
        logger.debug(
            f"  Registered relationship: {app} pos={pos} -> {relationship.target_application}"
        )

    # ------------------------------------------------------------------ #
    # Lookup
    # ------------------------------------------------------------------ #

    def resolve_field(self, app_name: str, position: str) -> Optional[FieldMetadata]:
        """
        Resolve field metadata by application name and position key.

        Parameters
        ----------
        app_name : T24 application name (case-insensitive)
        position : Field position key, e.g. "2", "64.3"

        Returns None if no metadata is registered for this position.
        """
        return self.fields_by_position.get(app_name.upper(), {}).get(position)

    def resolve_relationship(self, app_name: str, position: str) -> Optional[RelationshipMetadata]:
        """
        Resolve relationship metadata by application name and position.
        Returns None if no relationship is registered for this position.
        """
        return self.relationships_by_position.get(app_name.upper(), {}).get(position)

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #

    def has_app_metadata(self, app_name: str) -> bool:
        """Return True if any fields are registered for this application."""
        return bool(self.fields_by_position.get(app_name.upper()))

    def list_fields(self, app_name: str) -> List[FieldMetadata]:
        """Return all registered field metadata for an application."""
        return list(self.fields_by_position.get(app_name.upper(), {}).values())

    def list_local_ref_fields(self, app_name: str) -> List[FieldMetadata]:
        """Return only local reference fields (position contains a dot)."""
        return [f for f in self.list_fields(app_name) if "." in f.position]

    def summary(self, app_name: str) -> str:
        """Return a summary string for logging."""
        fields = self.list_fields(app_name)
        local_refs = self.list_local_ref_fields(app_name)
        return (
            f"Registry summary for {app_name.upper()}: "
            f"{len(fields)} fields, {len(local_refs)} local-ref fields"
        )
