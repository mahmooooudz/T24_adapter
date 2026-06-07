"""
config.py
---------
T24 Generic Adapter - Configuration Layer

Defines all runtime settings for the pipeline.
No T24 field names or positions are hardcoded here.
All business meaning comes from metadata files.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Tuple


@dataclass
class T24PipelineConfig:
    """
    Central configuration for the T24 Generic Adapter Pipeline.

    Parameters
    ----------
    package_root : Path
        Root folder of the T24 input package.
        Expected subdirectories: data/, metadata/, local_ref/,
        customization/, relationships/, xsd/

    data_dir : str
        Subdirectory name containing T24 XML data files.

    xsd_dir : str
        Subdirectory name containing XSD schema files (optional).

    metadata_dir : str
        Subdirectory name containing STANDARD.SELECTION metadata XML files.

    local_ref_dir : str
        Subdirectory name containing LOCAL.REF metadata XML files.

    customization_dir : str
        Subdirectory name containing bank-specific customization XML files.

    relationships_dir : str
        Subdirectory name containing relationship metadata XML files.

    require_xsd : bool
        If True, pipeline fails when XSD directory is missing.

    require_standard_selection : bool
        If True, pipeline fails when STANDARD.SELECTION metadata is missing.

    require_local_ref_when_declared : bool
        If True, pipeline warns when local ref metadata is absent but data
        contains local reference fields (c64 m-values).

    require_relationships : bool
        If True, pipeline fails when relationship metadata is missing.

    fail_on_unmapped_field : bool
        If True, pipeline raises ValueError on unmapped fields.
        If False (recommended), unmapped fields are emitted as FIELD_<pos>.

    local_ref_base_positions : Tuple[str, ...]
        XML tag positions that act as local reference containers.
        Default: ("64",) meaning c64 m="1" -> position 64.1
        Extend for bank-specific local ref arrays, e.g. ("64", "90", "100")

    output_unknown_fields : bool
        If True, fields not found in metadata are still included in output.
    """

    package_root: Path

    data_dir: str = "data"
    xsd_dir: str = "xsd"
    metadata_dir: str = "metadata"
    local_ref_dir: str = "local_ref"
    customization_dir: str = "customization"
    relationships_dir: str = "relationships"

    require_xsd: bool = False
    require_standard_selection: bool = True
    require_local_ref_when_declared: bool = True
    require_relationships: bool = False
    fail_on_unmapped_field: bool = False

    local_ref_base_positions: Tuple[str, ...] = ("64",)
    output_unknown_fields: bool = True

    def resolve(self, relative_dir: str) -> Path:
        """Return absolute path for a subdirectory under package_root."""
        return self.package_root / relative_dir

    def data_path(self) -> Path:
        return self.resolve(self.data_dir)

    def metadata_path(self) -> Path:
        return self.resolve(self.metadata_dir)

    def xsd_path(self) -> Path:
        return self.resolve(self.xsd_dir)

    def local_ref_path(self) -> Path:
        return self.resolve(self.local_ref_dir)

    def customization_path(self) -> Path:
        return self.resolve(self.customization_dir)

    def relationships_path(self) -> Path:
        return self.resolve(self.relationships_dir)
