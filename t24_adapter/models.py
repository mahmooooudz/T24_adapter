"""
models.py
---------
T24 Generic Adapter - Data Models

Pure data classes representing:
- FieldMetadata    : schema definition of one T24 field
- RelationshipMetadata : foreign-key / link between T24 applications
- NormalizedField  : one output record after flattening a data row
- FileValidationResult : result of package pre-validation
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass(frozen=True)
class FieldMetadata:
    """
    Represents the schema definition of a single T24 field.
    Populated from STANDARD.SELECTION, LOCAL.REF, or CUSTOMIZATION metadata.

    Attributes
    ----------
    app_name          : T24 application name (e.g. CUSTOMER, ACCOUNT)
    field_name        : Business field name  (e.g. SHORT.NAME, TAX.ID)
    position          : Metadata position key (e.g. "2", "64.3")
    system_type       : SYS (core), USR (custom), LOCAL, CUSTOM
    data_type         : A=Alpha, R=Reference, D=Date, N=Numeric
    validation_rule   : T24 validation routine (e.g. IN2A, IN2GGG)
    format_rule       : Display length and type (e.g. "35.1")
    cardinality       : SINGLE or MULTI
    relationship_target : Linked T24 application (e.g. COUNTRY, SECTOR)
    alignment         : L=Left, R=Right
    input_status      : INPUT, NO.INPUT, etc.
    description       : Human-readable field description
    is_local_ref      : True if position contains a dot (e.g. 64.1)
    is_custom_field   : True if field is bank-specific customization
    source            : Source file path for traceability
    """
    app_name: str
    field_name: str
    position: str
    system_type: Optional[str] = None
    data_type: Optional[str] = None
    validation_rule: Optional[str] = None
    format_rule: Optional[str] = None
    cardinality: Optional[str] = None
    relationship_target: Optional[str] = None
    alignment: Optional[str] = None
    input_status: Optional[str] = None
    description: Optional[str] = None
    is_local_ref: bool = False
    is_custom_field: bool = False
    source: Optional[str] = None


@dataclass(frozen=True)
class RelationshipMetadata:
    """
    Represents a foreign-key relationship between T24 applications.
    Example: COUNTRY field in CUSTOMER links to the COUNTRY application.

    Attributes
    ----------
    app_name           : Source T24 application
    field_name         : Source field that holds the foreign key
    position           : Field position in source application
    target_application : T24 application being referenced
    relationship_type  : How the relationship was detected
    source             : Source file path for traceability
    """
    app_name: str
    field_name: str
    position: str
    target_application: str
    relationship_type: Optional[str] = None
    source: Optional[str] = None


@dataclass(slots=True)
class NormalizedField:
    """
    One normalized output record, representing a single field value
    from a T24 XML data row after metadata resolution.

    Uses __slots__: this is the hot, high-volume object (one per field per
    record — tens of thousands per run), so slots cut allocation time and
    memory versus a __dict__-backed dataclass.

    Each XML element in a <row> becomes one NormalizedField.
    Multi-values and sub-values each become separate NormalizedField records.

    Attributes
    ----------
    app_name           : T24 application name
    record_id          : Primary key of the record (value of c1)
    source_file        : XML file path this record came from
    xml_element        : Original XML tag name (e.g. c64)
    raw_position       : Numeric position from XML tag (e.g. "64")
    resolved_position  : Metadata lookup key (e.g. "64.3" for c64 m="3")
    field_name         : Resolved business name, or FIELD_<pos> if unmapped
    system_type        : SYS / USR / CUSTOM / None
    data_type          : A / R / D / N / None
    format_rule        : Format rule string or None
    cardinality        : SINGLE / MULTI / None
    mv_index           : Multi-value index (m attribute) or None
    sv_index           : Sub-value index (s attribute) or None
    value              : Actual field value
    relationship_target: Linked application name or None
    is_local_ref       : True if field is from a local reference array
    is_custom_field    : True if field is a bank customization
    is_mapped          : True if metadata was found for this field
    warnings           : List of warning codes for this field
    """
    app_name: str
    record_id: Optional[str]
    source_file: str
    xml_element: str
    raw_position: str
    resolved_position: str
    field_name: str
    system_type: Optional[str]
    data_type: Optional[str]
    format_rule: Optional[str]
    cardinality: Optional[str]
    mv_index: Optional[str]
    sv_index: Optional[str]
    value: Optional[str]
    relationship_target: Optional[str]
    is_local_ref: bool
    is_custom_field: bool
    is_mapped: bool
    warnings: List[str] = field(default_factory=list)


@dataclass
class FileValidationResult:
    """
    Result of the pre-extraction package validation.

    Attributes
    ----------
    is_valid : True if all required files and formats are correct
    errors   : List of blocking error messages
    warnings : List of non-blocking warning messages
    """
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
