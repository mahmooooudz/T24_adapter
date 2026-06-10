"""
t24_adapter
-----------
Generic T24 XML Data Adapter Pipeline

A metadata-driven, streaming pipeline for extracting and normalizing
T24 core banking XML data into a flat, relational format.

Quick Start
-----------
from pathlib import Path
from t24_adapter import T24GenericPipeline, T24PipelineConfig, NormalizedOutputWriter

config = T24PipelineConfig(package_root=Path("t24_input_package"))
pipeline = T24GenericPipeline(config)
NormalizedOutputWriter.write_csv(rows=pipeline.run(), output_file=Path("output/result.csv"))
"""

from .config import T24PipelineConfig
from .models import FieldMetadata, NormalizedField, RelationshipMetadata, FileValidationResult
from .pipeline import T24GenericPipeline, T24MetadataOrchestrator
from .sinks import NormalizedOutputWriter
from .wide_writer import WidePivotWriter
from .metadata_registry import T24MetadataRegistry
from .metadata_loaders import (
    StandardSelectionLoader,
    LocalReferenceLoader,
    CustomizationLoader,
    RelationshipLoader,
)
from .normalizer import T24Normalizer
from .reader import T24StreamingDataReader
from .db_reader import T24DatabaseDataReader, T24DatabaseMetadataReader
from .env_loader import load_env
from .validation import T24PackageValidator
from .discovery import T24PackageDiscovery
from .xml_utils import XmlUtils

__all__ = [
    "T24PipelineConfig",
    "T24GenericPipeline",
    "T24MetadataOrchestrator",
    "T24MetadataRegistry",
    "T24Normalizer",
    "T24StreamingDataReader",
    "T24DatabaseDataReader",
    "T24DatabaseMetadataReader",
    "load_env",
    "T24PackageValidator",
    "T24PackageDiscovery",
    "NormalizedOutputWriter",
    "WidePivotWriter",
    "StandardSelectionLoader",
    "LocalReferenceLoader",
    "CustomizationLoader",
    "RelationshipLoader",
    "XmlUtils",
    "FieldMetadata",
    "NormalizedField",
    "RelationshipMetadata",
    "FileValidationResult",
]
