"""
validation.py
-------------
T24 Generic Adapter - Package Pre-Validation Engine

Validates the T24 input package before any extraction begins.
Checks:
- Package root exists
- Required directories exist
- XML files are present
- XML files are well-formed
- Metadata availability

This is the first stage of the pipeline. If validation fails,
the pipeline stops and reports all errors before touching data.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from .config import T24PipelineConfig
from .models import FileValidationResult
from .xml_utils import XmlUtils

logger = logging.getLogger("t24-adapter.validation")


class T24PackageValidator:
    """
    Validates a T24 input package directory structure and XML file integrity.

    Usage
    -----
    validator = T24PackageValidator(config)
    result = validator.validate_package()

    if not result.is_valid:
        for error in result.errors:
            print(error)
    """

    def __init__(self, config: T24PipelineConfig):
        self.config = config

    def validate_package(self) -> FileValidationResult:
        """
        Run all validation checks on the package.

        Returns a FileValidationResult with:
        - is_valid: True if all required checks pass
        - errors:   blocking issues (pipeline cannot continue)
        - warnings: non-blocking issues (pipeline can continue)
        """
        result = FileValidationResult(is_valid=True)

        self._check_root_exists(result)
        if not result.is_valid:
            return result

        # In database mode the record data comes from PostgreSQL, not from
        # data/*.xml, so the data-file checks are skipped. Metadata is still
        # loaded from files and therefore still validated.
        if self.config.source != "database":
            self._check_data_directory(result)
        self._check_metadata_directory(result)
        self._check_xsd_directory(result)
        if self.config.source != "database":
            self._check_data_xml_files(result)
        self._check_metadata_xml_files(result)
        self._check_optional_directories(result)

        return result

    # ------------------------------------------------------------------ #
    # Private Validation Steps
    # ------------------------------------------------------------------ #

    def _check_root_exists(self, result: FileValidationResult) -> None:
        root = self.config.package_root
        if not root.exists():
            result.is_valid = False
            result.errors.append(
                f"[CRITICAL] Package root does not exist: {root}"
            )
        elif not root.is_dir():
            result.is_valid = False
            result.errors.append(
                f"[CRITICAL] Package root is not a directory: {root}"
            )
        else:
            logger.info(f"Package root validated: {root}")

    def _check_data_directory(self, result: FileValidationResult) -> None:
        data_path = self.config.data_path()
        if not data_path.exists():
            result.is_valid = False
            result.errors.append(
                f"[CRITICAL] Missing required data directory: {data_path}"
            )
        else:
            logger.info(f"Data directory found: {data_path}")

    def _check_metadata_directory(self, result: FileValidationResult) -> None:
        if not self.config.require_standard_selection:
            return
        metadata_path = self.config.metadata_path()
        if not metadata_path.exists():
            result.is_valid = False
            result.errors.append(
                f"[CRITICAL] Missing required metadata directory: {metadata_path}"
            )
        else:
            logger.info(f"Metadata directory found: {metadata_path}")

    def _check_xsd_directory(self, result: FileValidationResult) -> None:
        if not self.config.require_xsd:
            return
        xsd_path = self.config.xsd_path()
        if not xsd_path.exists():
            result.is_valid = False
            result.errors.append(
                f"[CRITICAL] Missing required XSD directory: {xsd_path}"
            )

    def _check_data_xml_files(self, result: FileValidationResult) -> None:
        data_path = self.config.data_path()
        if not data_path.exists():
            return

        xml_files = list(data_path.glob("*.xml"))
        if not xml_files:
            result.is_valid = False
            result.errors.append(
                f"[CRITICAL] No XML files found in data directory: {data_path}"
            )
            return

        logger.info(f"Found {len(xml_files)} XML data file(s)")

        for xml_file in xml_files:
            is_valid, error = XmlUtils.is_xml_well_formed(xml_file)
            if not is_valid:
                result.is_valid = False
                result.errors.append(
                    f"[CRITICAL] Malformed XML data file '{xml_file.name}': {error}"
                )
            else:
                logger.info(f"  Data file validated: {xml_file.name}")

    def _check_metadata_xml_files(self, result: FileValidationResult) -> None:
        metadata_path = self.config.metadata_path()
        if not metadata_path.exists():
            return

        for xml_file in metadata_path.glob("*.xml"):
            is_valid, error = XmlUtils.is_xml_well_formed(xml_file)
            if not is_valid:
                result.is_valid = False
                result.errors.append(
                    f"[CRITICAL] Malformed metadata XML '{xml_file.name}': {error}"
                )
            else:
                logger.info(f"  Metadata file validated: {xml_file.name}")

    def _check_optional_directories(self, result: FileValidationResult) -> None:
        for dir_name, attr in [
            ("local_ref", self.config.local_ref_path()),
            ("customization", self.config.customization_path()),
            ("relationships", self.config.relationships_path()),
        ]:
            if attr.exists():
                logger.info(f"Optional directory found: {attr}")
            else:
                result.warnings.append(
                    f"[INFO] Optional directory not found: {attr}"
                )
