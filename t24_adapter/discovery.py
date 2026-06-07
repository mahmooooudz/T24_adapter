"""
discovery.py
------------
T24 Generic Adapter - Package Discovery

Discovers T24 applications from the data directory and locates
associated metadata, local_ref, customization, and relationship files.

No application names are hardcoded. The pipeline discovers them
dynamically by scanning the data folder for XML files.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

from .config import T24PipelineConfig

logger = logging.getLogger("t24-adapter.discovery")


class T24PackageDiscovery:
    """
    Discovers T24 application names and locates their support files.

    How it works
    ------------
    1. Scans data/ folder for *.xml files.
    2. Uses the filename stem (without extension) as the application name.
       Example: data/CUSTOMER.xml -> application = "CUSTOMER"
    3. For each application, searches multiple naming conventions
       for metadata, local_ref, customization, and relationship files.

    Usage
    -----
    discovery = T24PackageDiscovery(config)
    apps = discovery.discover_applications()  # ["ACCOUNT", "CUSTOMER", ...]
    data_file = discovery.get_data_file("CUSTOMER")
    meta_file = discovery.get_metadata_file("CUSTOMER")
    """

    def __init__(self, config: T24PipelineConfig):
        self.config = config

    def discover_applications(self) -> List[str]:
        """
        Discover all T24 application names from the data directory.

        Returns a sorted, deduplicated list of application names.
        Each name is uppercase (e.g. ["ACCOUNT", "CUSTOMER"]).
        """
        data_path = self.config.data_path()
        if not data_path.exists():
            logger.warning(f"Data directory not found: {data_path}")
            return []

        apps = []
        for file in data_path.glob("*.xml"):
            app_name = file.stem.upper()
            apps.append(app_name)
            logger.info(f"  Discovered application: {app_name} -> {file.name}")

        return sorted(set(apps))

    def get_data_file(self, app_name: str) -> Optional[Path]:
        """
        Return the data XML file path for a given application.
        Returns None if the file does not exist.
        """
        path = self.config.data_path() / f"{app_name.upper()}.xml"
        return path if path.exists() else None

    def get_metadata_file(self, app_name: str) -> Optional[Path]:
        """
        Locate the STANDARD.SELECTION metadata file for an application.

        Tries the following filename patterns in order:
        1. STANDARD_SELECTION_{APP}.xml
        2. {APP}_STANDARD_SELECTION.xml
        3. {APP}.xml
        """
        metadata_path = self.config.metadata_path()
        if not metadata_path.exists():
            return None

        candidates = [
            metadata_path / f"STANDARD_SELECTION_{app_name.upper()}.xml",
            metadata_path / f"{app_name.upper()}_STANDARD_SELECTION.xml",
            metadata_path / f"{app_name.upper()}.xml",
        ]

        for file in candidates:
            if file.exists():
                logger.info(f"  Metadata file found: {file.name}")
                return file

        logger.warning(
            f"No metadata file found for {app_name}. "
            f"Searched in: {[str(c) for c in candidates]}"
        )
        return None

    def get_local_ref_file(self, app_name: str) -> Optional[Path]:
        """
        Locate the LOCAL.REF metadata file for an application.

        Tries the following filename patterns in order:
        1. LOCAL_REF_{APP}.xml
        2. {APP}_LOCAL_REF.xml
        3. {APP}.xml  (in local_ref directory)
        """
        local_ref_path = self.config.local_ref_path()
        if not local_ref_path.exists():
            return None

        candidates = [
            local_ref_path / f"LOCAL_REF_{app_name.upper()}.xml",
            local_ref_path / f"{app_name.upper()}_LOCAL_REF.xml",
            local_ref_path / f"{app_name.upper()}.xml",
        ]

        for file in candidates:
            if file.exists():
                logger.info(f"  Local ref file found: {file.name}")
                return file

        return None

    def get_customization_file(self, app_name: str) -> Optional[Path]:
        """
        Locate the bank-specific customization metadata file.

        Tries the following filename patterns in order:
        1. CUSTOMIZATION_{APP}.xml
        2. {APP}_CUSTOMIZATION.xml
        3. {APP}.xml  (in customization directory)
        """
        customization_path = self.config.customization_path()
        if not customization_path.exists():
            return None

        candidates = [
            customization_path / f"CUSTOMIZATION_{app_name.upper()}.xml",
            customization_path / f"{app_name.upper()}_CUSTOMIZATION.xml",
            customization_path / f"{app_name.upper()}.xml",
        ]

        for file in candidates:
            if file.exists():
                logger.info(f"  Customization file found: {file.name}")
                return file

        return None

    def get_relationship_file(self, app_name: str) -> Optional[Path]:
        """
        Locate the relationship metadata file for an application.

        Tries the following filename patterns in order:
        1. RELATIONSHIPS_{APP}.xml
        2. {APP}_RELATIONSHIPS.xml
        3. {APP}.xml  (in relationships directory)
        """
        relationship_path = self.config.relationships_path()
        if not relationship_path.exists():
            return None

        candidates = [
            relationship_path / f"RELATIONSHIPS_{app_name.upper()}.xml",
            relationship_path / f"{app_name.upper()}_RELATIONSHIPS.xml",
            relationship_path / f"{app_name.upper()}.xml",
        ]

        for file in candidates:
            if file.exists():
                logger.info(f"  Relationship file found: {file.name}")
                return file

        return None
