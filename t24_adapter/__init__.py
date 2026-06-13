"""
t24_adapter
-----------
Generic T24 XML Data Adapter Pipeline

A metadata-driven, streaming pipeline for extracting and normalizing
T24 core banking XML data into a flat, relational format.

Quick Start
-----------
from t24_adapter import T24GenericPipeline, WideDatabaseWriter, load_env
from settings import build_config

load_env()                                   # DB credentials from .env
config = build_config()                      # source=database, output=database
pipeline = T24GenericPipeline(config)

# Flatten and UPSERT the wide result back into PostgreSQL (one table per app).
WideDatabaseWriter(
    schema=config.db_schema, suffix=config.db_output_suffix,
    write_mode=config.db_write_mode, full_sync=config.db_full_sync,
).write(pipeline)
"""

from .config import T24PipelineConfig
from .models import FieldMetadata, NormalizedField, RelationshipMetadata, FileValidationResult
from .pipeline import T24GenericPipeline, T24MetadataOrchestrator
from .wide_writer import WidePivotWriter
from .db_writer import WideDatabaseWriter
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
    "WidePivotWriter",
    "WideDatabaseWriter",
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
