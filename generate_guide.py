"""
generate_guide.py
-----------------
Generates a professional PDF guide for the T24 Generic Adapter Pipeline.
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm, mm
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, KeepTogether
)
from reportlab.platypus.flowables import Flowable
from reportlab.lib.colors import HexColor
import os

# ============================================================
# Color Palette
# ============================================================
C_DARK_BLUE   = HexColor("#0D2137")
C_MID_BLUE    = HexColor("#1A4A7A")
C_ACCENT      = HexColor("#2E86DE")
C_LIGHT_BLUE  = HexColor("#D6E8F9")
C_VERY_LIGHT  = HexColor("#F0F6FC")
C_GREEN       = HexColor("#1A7A3C")
C_GREEN_LIGHT = HexColor("#D4EDDA")
C_ORANGE      = HexColor("#B5530A")
C_ORANGE_LIGHT= HexColor("#FDE8D5")
C_GREY_DARK   = HexColor("#343A40")
C_GREY_MID    = HexColor("#6C757D")
C_GREY_LIGHT  = HexColor("#E9ECEF")
C_WHITE       = HexColor("#FFFFFF")
C_CODE_BG     = HexColor("#1E1E2E")
C_CODE_FG     = HexColor("#CDD6F4")
C_CODE_ACCENT = HexColor("#89B4FA")

PAGE_W, PAGE_H = A4
MARGIN = 2 * cm

# ============================================================
# Style Factory
# ============================================================
def build_styles():
    base = getSampleStyleSheet()

    styles = {}

    styles["cover_title"] = ParagraphStyle(
        "cover_title",
        fontName="Helvetica-Bold",
        fontSize=30,
        textColor=C_WHITE,
        alignment=TA_CENTER,
        spaceAfter=8,
        leading=36,
    )
    styles["cover_sub"] = ParagraphStyle(
        "cover_sub",
        fontName="Helvetica",
        fontSize=14,
        textColor=HexColor("#A8C8F0"),
        alignment=TA_CENTER,
        spaceAfter=4,
        leading=18,
    )
    styles["cover_version"] = ParagraphStyle(
        "cover_version",
        fontName="Helvetica",
        fontSize=10,
        textColor=HexColor("#7FB3D3"),
        alignment=TA_CENTER,
    )
    styles["h1"] = ParagraphStyle(
        "h1",
        fontName="Helvetica-Bold",
        fontSize=18,
        textColor=C_DARK_BLUE,
        spaceBefore=16,
        spaceAfter=8,
        leading=22,
    )
    styles["h2"] = ParagraphStyle(
        "h2",
        fontName="Helvetica-Bold",
        fontSize=13,
        textColor=C_MID_BLUE,
        spaceBefore=12,
        spaceAfter=5,
        leading=18,
    )
    styles["h3"] = ParagraphStyle(
        "h3",
        fontName="Helvetica-Bold",
        fontSize=11,
        textColor=C_GREY_DARK,
        spaceBefore=8,
        spaceAfter=4,
        leading=14,
    )
    styles["body"] = ParagraphStyle(
        "body",
        fontName="Helvetica",
        fontSize=10,
        textColor=C_GREY_DARK,
        leading=15,
        spaceAfter=4,
        alignment=TA_JUSTIFY,
    )
    styles["body_plain"] = ParagraphStyle(
        "body_plain",
        fontName="Helvetica",
        fontSize=10,
        textColor=C_GREY_DARK,
        leading=15,
        spaceAfter=4,
    )
    styles["code"] = ParagraphStyle(
        "code",
        fontName="Courier",
        fontSize=8.5,
        textColor=C_CODE_FG,
        leading=13,
        leftIndent=0,
        spaceAfter=2,
    )
    styles["code_label"] = ParagraphStyle(
        "code_label",
        fontName="Courier-Bold",
        fontSize=8,
        textColor=C_CODE_ACCENT,
        leading=11,
        spaceAfter=1,
    )
    styles["bullet"] = ParagraphStyle(
        "bullet",
        fontName="Helvetica",
        fontSize=10,
        textColor=C_GREY_DARK,
        leading=14,
        leftIndent=16,
        spaceAfter=3,
        bulletIndent=4,
    )
    styles["note"] = ParagraphStyle(
        "note",
        fontName="Helvetica-Oblique",
        fontSize=9,
        textColor=HexColor("#5A7FA0"),
        leading=13,
        spaceAfter=4,
    )
    styles["table_header"] = ParagraphStyle(
        "table_header",
        fontName="Helvetica-Bold",
        fontSize=9,
        textColor=C_WHITE,
        alignment=TA_CENTER,
    )
    styles["table_cell"] = ParagraphStyle(
        "table_cell",
        fontName="Helvetica",
        fontSize=9,
        textColor=C_GREY_DARK,
        leading=12,
    )
    styles["table_code"] = ParagraphStyle(
        "table_code",
        fontName="Courier",
        fontSize=8,
        textColor=C_MID_BLUE,
    )
    styles["toc_entry"] = ParagraphStyle(
        "toc_entry",
        fontName="Helvetica",
        fontSize=10,
        textColor=C_GREY_DARK,
        leading=16,
        leftIndent=0,
    )
    styles["toc_sub"] = ParagraphStyle(
        "toc_sub",
        fontName="Helvetica",
        fontSize=9,
        textColor=C_GREY_MID,
        leading=14,
        leftIndent=16,
    )
    return styles


# ============================================================
# Helper Components
# ============================================================

def hr(color=C_ACCENT, thickness=1):
    return HRFlowable(width="100%", thickness=thickness, color=color, spaceAfter=6, spaceBefore=6)

def section_break():
    return [Spacer(1, 0.3*cm), hr(), Spacer(1, 0.1*cm)]

def code_block(lines, styles, label=None):
    """Render a code block with dark background."""
    items = []
    if label:
        label_para = Paragraph(f"  {label}", styles["code_label"])
        items.append(
            Table([[label_para]], colWidths=[PAGE_W - 2*MARGIN],
                  style=TableStyle([
                      ("BACKGROUND", (0,0), (-1,-1), HexColor("#2A2A3E")),
                      ("TOPPADDING", (0,0), (-1,-1), 3),
                      ("BOTTOMPADDING", (0,0), (-1,-1), 2),
                      ("LEFTPADDING", (0,0), (-1,-1), 8),
                  ]))
        )

    code_paras = []
    for line in lines:
        safe_line = line.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        code_paras.append(Paragraph(safe_line if safe_line.strip() else " ", styles["code"]))

    block = Table(
        [[para] for para in code_paras],
        colWidths=[PAGE_W - 2*MARGIN],
        style=TableStyle([
            ("BACKGROUND", (0,0), (-1,-1), C_CODE_BG),
            ("TOPPADDING", (0,0), (-1,-1), 2),
            ("BOTTOMPADDING", (0,0), (-1,-1), 2),
            ("LEFTPADDING", (0,0), (-1,-1), 12),
            ("RIGHTPADDING", (0,0), (-1,-1), 8),
        ])
    )
    items.append(block)
    items.append(Spacer(1, 6))
    return items

def info_box(text, styles, bg=C_LIGHT_BLUE, border=C_ACCENT):
    para = Paragraph(text, styles["body_plain"])
    t = Table([[para]], colWidths=[PAGE_W - 2*MARGIN - 0.4*cm],
              style=TableStyle([
                  ("BACKGROUND", (0,0), (-1,-1), bg),
                  ("LEFTPADDING", (0,0), (-1,-1), 10),
                  ("RIGHTPADDING", (0,0), (-1,-1), 10),
                  ("TOPPADDING", (0,0), (-1,-1), 7),
                  ("BOTTOMPADDING", (0,0), (-1,-1), 7),
                  ("LINEAFTER", (0,0), (0,-1), 0, colors.white),
              ]))
    return [t, Spacer(1, 5)]

def warning_box(text, styles):
    return info_box(text, styles, bg=C_ORANGE_LIGHT, border=C_ORANGE)

def success_box(text, styles):
    return info_box(text, styles, bg=C_GREEN_LIGHT, border=C_GREEN)

def module_card(name, filename, purpose, styles):
    header = Paragraph(f"{name}  <font color='#7FB3D3' size='9'>({filename})</font>", styles["h3"])
    body = Paragraph(purpose, styles["body"])
    t = Table([[header], [body]],
              colWidths=[PAGE_W - 2*MARGIN],
              style=TableStyle([
                  ("BACKGROUND", (0,0), (-1,0), C_VERY_LIGHT),
                  ("BACKGROUND", (0,1), (-1,-1), C_WHITE),
                  ("LINEABOVE", (0,0), (-1,0), 2, C_ACCENT),
                  ("LINEBELOW", (0,-1), (-1,-1), 0.5, C_GREY_LIGHT),
                  ("LEFTPADDING", (0,0), (-1,-1), 10),
                  ("RIGHTPADDING", (0,0), (-1,-1), 10),
                  ("TOPPADDING", (0,0), (-1,0), 7),
                  ("BOTTOMPADDING", (0,0), (-1,0), 4),
                  ("TOPPADDING", (0,1), (-1,-1), 5),
                  ("BOTTOMPADDING", (0,1), (-1,-1), 8),
              ]))
    return [t, Spacer(1, 6)]


# ============================================================
# Cover Page
# ============================================================
def build_cover(styles):
    story = []

    # Full-width banner using a table
    banner_content = [
        [Paragraph("T24 Generic Adapter", styles["cover_title"])],
        [Paragraph("Pipeline Developer Guide", styles["cover_sub"])],
        [Spacer(1, 0.3*cm)],
        [Paragraph("Metadata-Driven XML Extraction &amp; Normalization", styles["cover_sub"])],
        [Spacer(1, 0.5*cm)],
        [Paragraph("Version 1.0  |  June 2026  |  Production-Ready", styles["cover_version"])],
    ]
    banner = Table(banner_content,
                   colWidths=[PAGE_W - 2*MARGIN],
                   style=TableStyle([
                       ("BACKGROUND", (0,0), (-1,-1), C_DARK_BLUE),
                       ("TOPPADDING", (0,0), (-1,-1), 12),
                       ("BOTTOMPADDING", (0,0), (-1,-1), 12),
                       ("LEFTPADDING", (0,0), (-1,-1), 20),
                       ("RIGHTPADDING", (0,0), (-1,-1), 20),
                       ("LINEABOVE", (0,0), (-1,0), 4, C_ACCENT),
                       ("LINEBELOW", (0,-1), (-1,-1), 4, C_ACCENT),
                   ]))
    story.append(banner)
    story.append(Spacer(1, 1*cm))

    # What this document covers
    story.append(Paragraph("What This Document Covers", styles["h2"]))
    story.append(hr(C_GREY_LIGHT, 0.5))

    bullets = [
        "Complete module-by-module code walkthrough",
        "Step-by-step pipeline execution guide",
        "Input folder structure and file naming conventions",
        "How to handle multi-values, sub-values, and local references",
        "How to add metadata for custom bank fields",
        "Output format reference and column definitions",
        "Configuration options and extension points",
    ]
    for b in bullets:
        story.append(Paragraph(f"&#8226;  {b}", styles["bullet"]))

    story.append(Spacer(1, 0.8*cm))

    # Quick facts table
    facts = [
        [Paragraph("Language", styles["table_header"]),
         Paragraph("Python 3.9+", styles["table_cell"])],
        [Paragraph("Dependencies", styles["table_header"]),
         Paragraph("Standard library + pandas (optional)", styles["table_cell"])],
        [Paragraph("Memory model", styles["table_header"]),
         Paragraph("Streaming — constant memory regardless of file size", styles["table_cell"])],
        [Paragraph("Hardcoded fields", styles["table_header"]),
         Paragraph("None — 100% metadata-driven", styles["table_cell"])],
        [Paragraph("Applications supported", styles["table_header"]),
         Paragraph("Any T24 application (CUSTOMER, ACCOUNT, FUNDS.TRANSFER, etc.)", styles["table_cell"])],
    ]
    fact_table = Table(facts, colWidths=[4*cm, PAGE_W - 2*MARGIN - 4*cm],
                       style=TableStyle([
                           ("BACKGROUND", (0,0), (0,-1), C_MID_BLUE),
                           ("BACKGROUND", (1,0), (1,-1), C_WHITE),
                           ("ROWBACKGROUNDS", (1,0), (1,-1), [C_VERY_LIGHT, C_WHITE]),
                           ("GRID", (0,0), (-1,-1), 0.5, C_GREY_LIGHT),
                           ("LEFTPADDING", (0,0), (-1,-1), 8),
                           ("RIGHTPADDING", (0,0), (-1,-1), 8),
                           ("TOPPADDING", (0,0), (-1,-1), 6),
                           ("BOTTOMPADDING", (0,0), (-1,-1), 6),
                           ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
                       ]))
    story.append(fact_table)

    story.append(PageBreak())
    return story


# ============================================================
# Table of Contents
# ============================================================
def build_toc(styles):
    story = []
    story.append(Paragraph("Table of Contents", styles["h1"]))
    story.append(hr())

    toc_items = [
        ("1.", "Architecture Overview", False),
        ("2.", "Project Structure", False),
        ("   2.1", "Module Files", True),
        ("   2.2", "Input Package Layout", True),
        ("3.", "Module Reference", False),
        ("   3.1", "config.py — Configuration", True),
        ("   3.2", "models.py — Data Models", True),
        ("   3.3", "xml_utils.py — XML Utilities", True),
        ("   3.4", "validation.py — Package Validator", True),
        ("   3.5", "discovery.py — Application Discovery", True),
        ("   3.6", "metadata_registry.py — Registry", True),
        ("   3.7", "metadata_loaders.py — Four Loaders", True),
        ("   3.8", "reader.py — Streaming Reader", True),
        ("   3.9", "normalizer.py — Normalization Engine", True),
        ("   3.10", "sinks.py — Output Writers", True),
        ("   3.11", "pipeline.py — Master Orchestrator", True),
        ("4.", "Step-by-Step Execution Guide", False),
        ("   4.1", "Step 1: Prepare the Package Folder", True),
        ("   4.2", "Step 2: Prepare Metadata Files", True),
        ("   4.3", "Step 3: Configure the Pipeline", True),
        ("   4.4", "Step 4: Run the Pipeline", True),
        ("   4.5", "Step 5: Inspect the Output", True),
        ("5.", "T24 Data Concepts Explained", False),
        ("   5.1", "Multi-Values and Sub-Values", True),
        ("   5.2", "Local Reference Fields (c64)", True),
        ("   5.3", "Field Position Resolution", True),
        ("6.", "Output Column Reference", False),
        ("7.", "Extending the Pipeline", False),
        ("8.", "Troubleshooting", False),
    ]

    for num, title, is_sub in toc_items:
        style = styles["toc_sub"] if is_sub else styles["toc_entry"]
        prefix = "" if is_sub else "<b>"
        suffix = "" if is_sub else "</b>"
        story.append(Paragraph(f"{prefix}{num}  {title}{suffix}", style))

    story.append(PageBreak())
    return story


# ============================================================
# Section 1: Architecture
# ============================================================
def build_architecture(styles):
    story = []
    story.append(Paragraph("1. Architecture Overview", styles["h1"]))
    story.append(hr())

    story.append(Paragraph(
        "The T24 Generic Adapter is a metadata-driven, streaming pipeline. No T24 field names "
        "are hardcoded in the code. The business meaning of every field comes entirely from "
        "the STANDARD.SELECTION, LOCAL.REF, and CUSTOMIZATION metadata files you supply. "
        "This means the same codebase works unchanged for any T24 application.",
        styles["body"]
    ))
    story.append(Spacer(1, 5))

    story.append(Paragraph("The five-stage pipeline:", styles["h3"]))

    pipeline_stages = [
        ["Stage", "Module", "Responsibility"],
        ["1 — Validate", "validation.py", "Check package structure, file existence, XML well-formedness"],
        ["2 — Discover", "discovery.py", "Find T24 application names from data/*.xml filenames"],
        ["3 — Load Metadata", "metadata_loaders.py +\nmetadata_registry.py", "Load STANDARD.SELECTION, LOCAL.REF, CUSTOMIZATION, RELATIONSHIPS into memory"],
        ["4 — Stream", "reader.py", "Stream <row> records one-by-one using ET.iterparse() — no full file load"],
        ["5 — Normalize", "normalizer.py", "Resolve field names, handle m-values, s-values, local refs; emit NormalizedField records"],
        ["Output", "wide_writer.py / db_writer.py", "Pivot long records into wide tables and UPSERT them into PostgreSQL (also wide CSV / JSONL / DataFrame)"],
    ]

    col_w = [(PAGE_W - 2*MARGIN) / 3] * 3
    col_w = [3.5*cm, 5*cm, PAGE_W - 2*MARGIN - 8.5*cm]

    def stage_cell(text, header=False):
        style = styles["table_header"] if header else styles["table_cell"]
        return Paragraph(text, style)

    table_data = []
    for i, row in enumerate(pipeline_stages):
        table_data.append([stage_cell(c, header=(i==0)) for c in row])

    stage_table = Table(table_data, colWidths=col_w,
                        style=TableStyle([
                            ("BACKGROUND", (0,0), (-1,0), C_DARK_BLUE),
                            ("ROWBACKGROUNDS", (0,1), (-1,-1), [C_VERY_LIGHT, C_WHITE]),
                            ("BACKGROUND", (0,1), (0,1), C_LIGHT_BLUE),
                            ("BACKGROUND", (0,2), (0,2), HexColor("#D0E8FF")),
                            ("GRID", (0,0), (-1,-1), 0.5, C_GREY_LIGHT),
                            ("LEFTPADDING", (0,0), (-1,-1), 8),
                            ("RIGHTPADDING", (0,0), (-1,-1), 8),
                            ("TOPPADDING", (0,0), (-1,-1), 5),
                            ("BOTTOMPADDING", (0,0), (-1,-1), 5),
                            ("VALIGN", (0,0), (-1,-1), "TOP"),
                            ("FONTNAME", (0,1), (0,-1), "Courier-Bold"),
                            ("FONTSIZE", (0,1), (0,-1), 9),
                        ]))
    story.append(stage_table)
    story.append(Spacer(1, 6))

    story += info_box(
        "<b>Key Design Principle:</b> The data XML never defines the business meaning of a field. "
        "STANDARD.SELECTION and LOCAL.REF metadata define it. "
        "The pipeline reads metadata first, then data.",
        styles
    )

    story.append(PageBreak())
    return story


# ============================================================
# Section 2: Project Structure
# ============================================================
def build_structure(styles):
    story = []
    story.append(Paragraph("2. Project Structure", styles["h1"]))
    story.append(hr())

    story.append(Paragraph("2.1  Module Files", styles["h2"]))
    story += code_block([
        "t24_adapter/",
        "    __init__.py            # Package entry point and public API",
        "    config.py              # T24PipelineConfig dataclass",
        "    models.py              # FieldMetadata, NormalizedField, RelationshipMetadata",
        "    xml_utils.py           # XML parsing helpers",
        "    validation.py          # T24PackageValidator",
        "    discovery.py           # T24PackageDiscovery",
        "    metadata_registry.py   # T24MetadataRegistry (in-memory store)",
        "    metadata_loaders.py    # Four loader classes",
        "    reader.py              # T24StreamingDataReader",
        "    normalizer.py          # T24Normalizer",
        "    wide_writer.py         # WidePivotWriter (long -> wide pivot)",
        "    db_writer.py           # WideDatabaseWriter (UPSERT + full sync)",
        "    pipeline.py            # T24GenericPipeline (master orchestrator)",
        "",
        "main.py                    # Example execution entry point",
    ], styles, label="t24_adapter/ — Module Layout")

    story.append(Paragraph("2.2  Input Package Layout", styles["h2"]))
    story.append(Paragraph(
        "Arrange your T24 export files under a root folder. The pipeline discovers applications "
        "automatically by scanning data/*.xml. All other directories are optional but recommended.",
        styles["body"]
    ))

    story += code_block([
        "t24_input_package/",
        "    data/",
        "        CUSTOMER.xml                         # T24 data records",
        "        ACCOUNT.xml",
        "        FUNDS.TRANSFER.xml",
        "    metadata/",
        "        STANDARD_SELECTION_CUSTOMER.xml      # Core field schema",
        "        STANDARD_SELECTION_ACCOUNT.xml",
        "    local_ref/",
        "        LOCAL_REF_CUSTOMER.xml               # Local reference fields",
        "    customization/",
        "        CUSTOMIZATION_CUSTOMER.xml           # Bank-specific custom fields",
        "    relationships/",
        "        RELATIONSHIPS_CUSTOMER.xml           # Explicit FK relationships",
        "    xsd/                                     # Optional XSD schemas",
        "        CUSTOMER.xsd",
    ], styles, label="t24_input_package/ — Required Input Structure")

    story += info_box(
        "<b>Naming Convention:</b> For each application <i>APP</i>, the pipeline looks for "
        "STANDARD_SELECTION_APP.xml, APP_STANDARD_SELECTION.xml, or just APP.xml in each folder. "
        "You only need to match one of these patterns.",
        styles
    )

    story.append(PageBreak())
    return story


# ============================================================
# Section 3: Module Reference
# ============================================================
def build_modules(styles):
    story = []
    story.append(Paragraph("3. Module Reference", styles["h1"]))
    story.append(hr())

    # 3.1 config.py
    story.append(Paragraph("3.1  config.py — Configuration", styles["h2"]))
    story += module_card(
        "T24PipelineConfig", "config.py",
        "Holds all runtime settings. The pipeline reads configuration from this dataclass. "
        "No T24 field names or positions are stored here — only structural settings "
        "like directory names, flags, and the local_ref_base_positions tuple.",
        styles
    )
    story += code_block([
        "from pathlib import Path",
        "from t24_adapter.config import T24PipelineConfig",
        "",
        "config = T24PipelineConfig(",
        "    package_root=Path('t24_input_package'),",
        "    require_standard_selection=True,    # Fail if no metadata found",
        "    fail_on_unmapped_field=False,        # Emit FIELD_N for unknowns",
        "    local_ref_base_positions=('64',),    # c64 m=N -> position 64.N",
        "    output_unknown_fields=True,          # Include unmapped fields",
        ")",
    ], styles, label="config.py — Usage Example")

    # 3.2 models.py
    story.append(Paragraph("3.2  models.py — Data Models", styles["h2"]))
    story += module_card(
        "FieldMetadata", "models.py",
        "Frozen dataclass representing one field schema entry from STANDARD.SELECTION, "
        "LOCAL.REF, or CUSTOMIZATION. Contains: field_name, position, system_type, "
        "data_type, validation_rule, format_rule, cardinality, relationship_target, "
        "is_local_ref, is_custom_field, source.",
        styles
    )
    story += module_card(
        "NormalizedField", "models.py",
        "One output record per XML element. Each <row> in the data file produces a list "
        "of NormalizedField objects — one per XML tag. Contains: app_name, record_id, "
        "xml_element, resolved_position, field_name, mv_index, sv_index, value, "
        "is_mapped, warnings, relationship_target.",
        styles
    )
    story += module_card(
        "RelationshipMetadata", "models.py",
        "A foreign-key relationship between two T24 applications. "
        "Example: CUSTOMER.COUNTRY -> COUNTRY application. "
        "Populated automatically from STANDARD.SELECTION C8 column, "
        "or from a dedicated relationships XML file.",
        styles
    )

    # 3.3 xml_utils.py
    story.append(Paragraph("3.3  xml_utils.py — XML Utilities", styles["h2"]))
    story += module_card(
        "XmlUtils", "xml_utils.py",
        "Static helper methods used throughout the pipeline. "
        "Handles: namespace stripping, comment removal, multi-root fragment wrapping, "
        "XML well-formedness checking, and extraction of numeric position from T24 tag names "
        "(c1 -> '1', c64 -> '64').",
        styles
    )

    # 3.4 validation.py
    story.append(Paragraph("3.4  validation.py — Package Validator", styles["h2"]))
    story += module_card(
        "T24PackageValidator", "validation.py",
        "First stage of the pipeline. Validates the package structure before any "
        "extraction begins. Checks: package root exists, data directory exists, "
        "XML files are present, XML files are well-formed, metadata directory "
        "exists when required. Returns a FileValidationResult with errors and warnings.",
        styles
    )
    story += code_block([
        "validator = T24PackageValidator(config)",
        "result = validator.validate_package()",
        "",
        "if not result.is_valid:",
        "    for error in result.errors:",
        "        print(error)          # Blocking errors",
        "for warning in result.warnings:",
        "    print(warning)            # Non-blocking warnings",
    ], styles, label="validation.py — Usage")

    # 3.5 discovery.py
    story.append(Paragraph("3.5  discovery.py — Application Discovery", styles["h2"]))
    story += module_card(
        "T24PackageDiscovery", "discovery.py",
        "Discovers T24 application names by scanning data/*.xml. "
        "The filename stem becomes the application name (CUSTOMER.xml -> CUSTOMER). "
        "For each application, discovers metadata, local_ref, customization, "
        "and relationship file paths using multiple filename convention candidates.",
        styles
    )
    story += code_block([
        "discovery = T24PackageDiscovery(config)",
        "apps = discovery.discover_applications()",
        "# Returns: ['ACCOUNT', 'CUSTOMER', 'FUNDS.TRANSFER']",
        "",
        "meta_file = discovery.get_metadata_file('CUSTOMER')",
        "# Searches for: STANDARD_SELECTION_CUSTOMER.xml",
        "#               CUSTOMER_STANDARD_SELECTION.xml",
        "#               CUSTOMER.xml  (in metadata/)",
    ], styles, label="discovery.py — Usage")

    story.append(PageBreak())

    # 3.6 metadata_registry.py
    story.append(Paragraph("3.6  metadata_registry.py — Registry", styles["h2"]))
    story += module_card(
        "T24MetadataRegistry", "metadata_registry.py",
        "The in-memory metadata store. Indexed as dict[app_name][position] -> FieldMetadata. "
        "O(1) lookup at normalization time. Populated by the four metadata loaders. "
        "Also stores RelationshipMetadata indexed the same way.",
        styles
    )
    story += code_block([
        "# Lookup a field by app and position",
        "meta = registry.resolve_field('CUSTOMER', '64.3')",
        "# Returns: FieldMetadata(field_name='TAX.ID', data_type='A', ...)",
        "",
        "# Check if metadata exists",
        "if registry.has_app_metadata('CUSTOMER'):",
        "    fields = registry.list_fields('CUSTOMER')",
        "    local_refs = registry.list_local_ref_fields('CUSTOMER')",
    ], styles, label="metadata_registry.py — Usage")

    # 3.7 metadata_loaders.py
    story.append(Paragraph("3.7  metadata_loaders.py — Four Loaders", styles["h2"]))
    story += module_card(
        "StandardSelectionLoader", "metadata_loaders.py",
        "Reads STANDARD.SELECTION-style XML into the registry. Expects repeated <C1> elements "
        "(field names) and <C3> elements (positions). Also reads C2 (system type), C4 (data type), "
        "C5 (validation), C6 (format), C7 (cardinality), C8 (relationship), C9-C11 (alignment, "
        "status, description). Minimum required: C1 and C3.",
        styles
    )
    story += module_card(
        "LocalReferenceLoader", "metadata_loaders.py",
        "Loads LOCAL.REF metadata. Supports two XML shapes automatically: "
        "(1) Repeated-column style identical to STANDARD.SELECTION — delegates to that loader. "
        "(2) Explicit field-node style with NAME, POSITION, TYPE, FORMAT child elements. "
        "All local ref fields get is_local_ref=True and is_custom_field=True.",
        styles
    )
    story += module_card(
        "CustomizationLoader", "metadata_loaders.py",
        "Loads bank-specific custom field definitions. Looks for FIELD, CUSTOM.FIELD, "
        "CUSTOM_FIELD, or LOCAL.FIELD nodes. Tolerant of missing optional attributes. "
        "All custom fields get system_type='CUSTOM' and is_custom_field=True.",
        styles
    )
    story += module_card(
        "RelationshipLoader", "metadata_loaders.py",
        "Loads explicit relationship metadata from a dedicated file. Augments relationships "
        "already detected from STANDARD.SELECTION C8. Looks for RELATIONSHIP, RELATION, "
        "or LINK nodes with FIELD, POSITION, TARGET, TYPE child elements.",
        styles
    )

    # 3.8 reader.py
    story.append(Paragraph("3.8  reader.py — Streaming Reader", styles["h2"]))
    story += module_card(
        "T24StreamingDataReader", "reader.py",
        "Streams T24 XML data records one at a time using ET.iterparse(). "
        "Each <row> element is yielded, then immediately cleared from memory. "
        "This means a 10 GB file uses no more memory than processing a single record. "
        "Tag matching is case-insensitive (handles both <row> and <ROW>).",
        styles
    )
    story += code_block([
        "reader = T24StreamingDataReader()",
        "",
        "# Stream records one by one (constant memory)",
        "for row_element in reader.stream_records(Path('data/CUSTOMER.xml')):",
        "    # row_element is an ET.Element with all child nodes",
        "    pass",
        "",
        "# Count records (useful for progress bars)",
        "total = reader.count_records(Path('data/CUSTOMER.xml'))",
    ], styles, label="reader.py — Usage")

    # 3.9 normalizer.py
    story.append(Paragraph("3.9  normalizer.py — Normalization Engine", styles["h2"]))
    story += module_card(
        "T24Normalizer", "normalizer.py",
        "Converts a <row> ET.Element into a list of NormalizedField records. "
        "Handles: standard fields, multi-value fields (m attribute), sub-value fields "
        "(s attribute), local reference arrays (c64 m=N -> position 64.N), unmapped "
        "fields (emitted as FIELD_pos), and warning codes per field.",
        styles
    )

    # 3.10 output writers
    story.append(Paragraph("3.10  wide_writer.py / db_writer.py — Output Writers", styles["h2"]))
    story += module_card(
        "WidePivotWriter", "wide_writer.py",
        "Pivots the long NormalizedField stream into WIDE rows: one row per record, "
        "each field a column, multi/sub-values expanded into indexed columns "
        "(e.g. TAX.ID_1, TAX.ID_2). Can emit wide CSV, JSONL, or a Pandas DataFrame.",
        styles
    )
    story += module_card(
        "WideDatabaseWriter", "db_writer.py",
        "Writes the wide result back into PostgreSQL, one table per application "
        "(<APP>_wide). Manages DDL automatically (create-if-missing, add new columns, "
        "ensure a UNIQUE key) and UPSERTs rows (INSERT ... ON CONFLICT DO UPDATE). "
        "Optional, safety-gated full sync also deletes rows whose key vanished from "
        "the source. Streaming (per-row) or batching write modes.",
        styles
    )

    # 3.11 pipeline.py
    story.append(Paragraph("3.11  pipeline.py — Master Orchestrator", styles["h2"]))
    story += module_card(
        "T24GenericPipeline", "pipeline.py",
        "The main entry point. Ties all modules together. run() is a Python generator "
        "that yields NormalizedField records from all applications in the package. "
        "Internally runs: validate -> discover -> load metadata -> stream -> normalize "
        "for each application in alphabetical order.",
        styles
    )
    story += module_card(
        "T24MetadataOrchestrator", "pipeline.py",
        "Loads all metadata sources for one application in order: STANDARD.SELECTION, "
        "then LOCAL.REF, then CUSTOMIZATION, then RELATIONSHIPS. Later sources override "
        "earlier ones for the same position. Also validates local reference coverage "
        "before data extraction starts.",
        styles
    )

    story.append(PageBreak())
    return story


# ============================================================
# Section 4: Step-by-Step Execution Guide
# ============================================================
def build_execution_guide(styles):
    story = []
    story.append(Paragraph("4. Step-by-Step Execution Guide", styles["h1"]))
    story.append(hr())

    # Step 1
    story.append(Paragraph("4.1  Step 1: Prepare the Package Folder", styles["h2"]))
    story.append(Paragraph(
        "Create the folder structure. Only data/ is required; all others are optional "
        "but highly recommended for correct field name resolution.",
        styles["body"]
    ))
    story += code_block([
        "mkdir -p t24_input_package/data",
        "mkdir -p t24_input_package/metadata",
        "mkdir -p t24_input_package/local_ref",
        "mkdir -p t24_input_package/customization",
        "mkdir -p t24_input_package/relationships",
        "",
        "# Place your T24 XML exports here:",
        "cp CUSTOMER_export.xml t24_input_package/data/CUSTOMER.xml",
        "cp ACCOUNT_export.xml  t24_input_package/data/ACCOUNT.xml",
    ], styles, label="Shell — Create Package Structure")

    # Step 2
    story.append(Paragraph("4.2  Step 2: Prepare Metadata Files", styles["h2"]))
    story.append(Paragraph(
        "Export STANDARD.SELECTION for each application from T24 and place in metadata/. "
        "The file must contain repeated <C1> (field names) and <C3> (positions) elements.",
        styles["body"]
    ))
    story += code_block([
        "<!-- metadata/STANDARD_SELECTION_CUSTOMER.xml -->",
        "<ROW>",
        "    <C1>MNEMONIC</C1>      <!-- Repeated: one per field -->",
        "    <C1>SHORT.NAME</C1>",
        "    <C1>NAME.1</C1>",
        "    ...",
        "    <C3>1</C3>             <!-- Repeated: position for each field -->",
        "    <C3>2</C3>",
        "    <C3>3</C3>",
        "    <C3>64.1</C3>          <!-- Local ref: c64 m=1 maps to this -->",
        "    ...",
        "    <C4>A</C4>             <!-- Optional: data types -->",
        "    <C7>MULTI</C7>         <!-- Optional: SINGLE or MULTI -->",
        "    <C8>COUNTRY</C8>       <!-- Optional: relationship target -->",
        "    <C11>Short name</C11>  <!-- Optional: description -->",
        "</ROW>",
    ], styles, label="metadata/STANDARD_SELECTION_CUSTOMER.xml — Minimum Required Structure")

    # Step 3
    story.append(Paragraph("4.3  Step 3: Configure the Pipeline", styles["h2"]))
    story += code_block([
        "from pathlib import Path",
        "from t24_adapter import T24GenericPipeline, T24PipelineConfig",
        "",
        "config = T24PipelineConfig(",
        "    package_root=Path('t24_input_package'),",
        "",
        "    # Set to True to fail if STANDARD.SELECTION is missing",
        "    require_standard_selection=True,",
        "",
        "    # False = unknown fields become FIELD_<pos> (recommended)",
        "    # True  = unknown fields raise ValueError (strict mode)",
        "    fail_on_unmapped_field=False,",
        "",
        "    # List all XML column numbers that are local reference arrays.",
        "    # Default: (64,) meaning c64 m=N -> position 64.N",
        "    # Add more if your T24 uses other columns for local refs:",
        "    local_ref_base_positions=('64',),",
        ")",
        "",
        "pipeline = T24GenericPipeline(config)",
    ], styles, label="main.py — Pipeline Configuration")

    # Step 4
    story.append(Paragraph("4.4  Step 4: Run the Pipeline", styles["h2"]))
    story += code_block([
        "from t24_adapter import WideDatabaseWriter, WidePivotWriter",
        "from pathlib import Path",
        "",
        "# --- Default: UPSERT the wide result into PostgreSQL (one table/app) ---",
        "WideDatabaseWriter(",
        "    schema=config.db_schema, suffix=config.db_output_suffix,",
        "    write_mode=config.db_write_mode,   # 'streaming' | 'batching'",
        "    full_sync=config.db_full_sync,     # mirror source deletions (gated)",
        ").write(pipeline)",
        "",
        "# --- Or emit the same wide rows to files / a DataFrame ---",
        "wp = WidePivotWriter()",
        "wp.write_csv(pipeline, Path('output'))     # one <APP>_wide.csv per app",
        "wp.write_jsonl(pipeline, Path('output'))",
        "df = wp.to_dataframe(pipeline, 'ACCOUNT')",
    ], styles, label="main.py — Output Options")

    story += warning_box(
        "<b>Important:</b> pipeline.run() is a Python generator. The DB sink reads the "
        "source exactly twice (discover the wide schema, then write); it never buffers "
        "the whole dataset in memory.",
        styles
    )

    # Step 5
    story.append(Paragraph("4.5  Step 5: Inspect the Output", styles["h2"]))
    story += code_block([
        "import pandas as pd",
        "",
        "df = pd.read_csv('output/result.csv')",
        "",
        "# Filter to one record",
        "record = df[df['record_id'] == 'JD1009']",
        "",
        "# Show only mapped fields",
        "mapped = df[df['is_mapped'] == True]",
        "",
        "# Show local reference fields",
        "local_refs = df[df['is_local_ref'] == True]",
        "",
        "# Show fields with warnings",
        "warnings = df[df['warnings'].notna()]",
        "",
        "# Note: the wide pivot is built in — WidePivotWriter / WideDatabaseWriter",
        "# already emit one row per record with one column per field, so no manual",
        "# df.pivot_table() step is needed.",
    ], styles, label="Inspecting Output with Pandas")

    story.append(PageBreak())
    return story


# ============================================================
# Section 5: T24 Concepts
# ============================================================
def build_concepts(styles):
    story = []
    story.append(Paragraph("5. T24 Data Concepts Explained", styles["h1"]))
    story.append(hr())

    story.append(Paragraph("5.1  Multi-Values and Sub-Values", styles["h2"]))
    story.append(Paragraph(
        "T24 fields can hold multiple values using the m (multi-value) attribute, "
        "and each multi-value can hold further sub-values using the s attribute. "
        "Each value becomes a separate NormalizedField record.",
        styles["body"]
    ))

    mv_data = [
        [Paragraph("XML Input", styles["table_header"]),
         Paragraph("resolved_position", styles["table_header"]),
         Paragraph("mv_index", styles["table_header"]),
         Paragraph("sv_index", styles["table_header"]),
         Paragraph("value", styles["table_header"])],
        [Paragraph("<c2 m='1'>John Doe</c2>", styles["table_code"]),
         Paragraph("2", styles["table_cell"]),
         Paragraph("1", styles["table_cell"]),
         Paragraph("—", styles["table_cell"]),
         Paragraph("John Doe", styles["table_cell"])],
        [Paragraph("<c2 m='2'>J. Doe</c2>", styles["table_code"]),
         Paragraph("2", styles["table_cell"]),
         Paragraph("2", styles["table_cell"]),
         Paragraph("—", styles["table_cell"]),
         Paragraph("J. Doe", styles["table_cell"])],
        [Paragraph("<c64 m='3' s='1'>TX-998</c64>", styles["table_code"]),
         Paragraph("64.3", styles["table_cell"]),
         Paragraph("3", styles["table_cell"]),
         Paragraph("1", styles["table_cell"]),
         Paragraph("TX-998", styles["table_cell"])],
        [Paragraph("<c64 m='3' s='2'>TX-445</c64>", styles["table_code"]),
         Paragraph("64.3", styles["table_cell"]),
         Paragraph("3", styles["table_cell"]),
         Paragraph("2", styles["table_cell"]),
         Paragraph("TX-445", styles["table_cell"])],
    ]

    col_w_mv = [5.5*cm, 3.5*cm, 2.5*cm, 2.5*cm, 3*cm]
    mv_table = Table(mv_data, colWidths=col_w_mv,
                     style=TableStyle([
                         ("BACKGROUND", (0,0), (-1,0), C_DARK_BLUE),
                         ("ROWBACKGROUNDS", (0,1), (-1,-1), [C_VERY_LIGHT, C_WHITE]),
                         ("GRID", (0,0), (-1,-1), 0.5, C_GREY_LIGHT),
                         ("LEFTPADDING", (0,0), (-1,-1), 6),
                         ("RIGHTPADDING", (0,0), (-1,-1), 6),
                         ("TOPPADDING", (0,0), (-1,-1), 5),
                         ("BOTTOMPADDING", (0,0), (-1,-1), 5),
                     ]))
    story.append(mv_table)
    story.append(Spacer(1, 8))

    story.append(Paragraph("5.2  Local Reference Fields (c64)", styles["h2"]))
    story.append(Paragraph(
        "c64 is special: it is a local reference container. Its m attribute does not mean "
        "multi-value of the same field — it means a different field for each m value. "
        "So c64 m=1, c64 m=2, c64 m=3 are three completely different fields.",
        styles["body"]
    ))
    story += code_block([
        "# c64 m='1' -> resolved_position = '64.1' -> CUST.SEGMENT",
        "# c64 m='2' -> resolved_position = '64.2' -> CUST.TIER",
        "# c64 m='3' -> resolved_position = '64.3' -> TAX.ID",
        "# c64 m='4' -> resolved_position = '64.4' -> KYC.REVIEW.DATE",
        "",
        "# In metadata (STANDARD.SELECTION or LOCAL.REF):",
        "# <C1>CUST.SEGMENT</C1>  <C3>64.1</C3>",
        "# <C1>CUST.TIER</C1>     <C3>64.2</C3>",
        "# <C1>TAX.ID</C1>        <C3>64.3</C3>",
        "# <C1>KYC.REVIEW.DATE</C1> <C3>64.4</C3>",
        "",
        "# To add c90 as another local reference column:",
        "config = T24PipelineConfig(",
        "    local_ref_base_positions=('64', '90'),",
        ")",
        "# Now c90 m='1' -> '90.1', c90 m='2' -> '90.2', etc.",
    ], styles, label="Local Reference Position Resolution")

    story.append(Paragraph("5.3  Field Position Resolution", styles["h2"]))
    pos_data = [
        [Paragraph("XML Tag", styles["table_header"]),
         Paragraph("m attr", styles["table_header"]),
         Paragraph("raw_position", styles["table_header"]),
         Paragraph("resolved_position", styles["table_header"]),
         Paragraph("Registry Lookup Key", styles["table_header"])],
        [Paragraph("<c1>", styles["table_code"]), Paragraph("—", styles["table_cell"]),
         Paragraph("1", styles["table_cell"]), Paragraph("1", styles["table_cell"]),
         Paragraph("1", styles["table_cell"])],
        [Paragraph("<c2 m='1'>", styles["table_code"]), Paragraph("1", styles["table_cell"]),
         Paragraph("2", styles["table_cell"]), Paragraph("2", styles["table_cell"]),
         Paragraph("2", styles["table_cell"])],
        [Paragraph("<c64 m='1'>", styles["table_code"]), Paragraph("1", styles["table_cell"]),
         Paragraph("64", styles["table_cell"]), Paragraph("64.1", styles["table_cell"]),
         Paragraph("64.1", styles["table_cell"])],
        [Paragraph("<c64 m='3' s='2'>", styles["table_code"]), Paragraph("3", styles["table_cell"]),
         Paragraph("64", styles["table_cell"]), Paragraph("64.3", styles["table_cell"]),
         Paragraph("64.3", styles["table_cell"])],
        [Paragraph("<c176 m='1'>", styles["table_code"]), Paragraph("1", styles["table_cell"]),
         Paragraph("176", styles["table_cell"]), Paragraph("176", styles["table_cell"]),
         Paragraph("176 -> FIELD_176", styles["table_cell"])],
    ]
    col_w_pos = [3.5*cm, 1.8*cm, 2.8*cm, 3.5*cm, 4.5*cm]
    pos_table = Table(pos_data, colWidths=col_w_pos,
                      style=TableStyle([
                          ("BACKGROUND", (0,0), (-1,0), C_DARK_BLUE),
                          ("ROWBACKGROUNDS", (0,1), (-1,-1), [C_VERY_LIGHT, C_WHITE]),
                          ("GRID", (0,0), (-1,-1), 0.5, C_GREY_LIGHT),
                          ("LEFTPADDING", (0,0), (-1,-1), 6),
                          ("RIGHTPADDING", (0,0), (-1,-1), 6),
                          ("TOPPADDING", (0,0), (-1,-1), 5),
                          ("BOTTOMPADDING", (0,0), (-1,-1), 5),
                      ]))
    story.append(pos_table)

    story.append(PageBreak())
    return story


# ============================================================
# Section 6: Output Reference
# ============================================================
def build_output_reference(styles):
    story = []
    story.append(Paragraph("6. Output Column Reference", styles["h1"]))
    story.append(hr())

    story.append(Paragraph(
        "Each XML field in a <row> becomes one row in the output table. "
        "The following columns are produced for every record.",
        styles["body"]
    ))
    story.append(Spacer(1, 5))

    col_data = [
        [Paragraph("Column", styles["table_header"]),
         Paragraph("Type", styles["table_header"]),
         Paragraph("Description", styles["table_header"])],
        [Paragraph("app_name", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("T24 application name (e.g. CUSTOMER)", styles["table_cell"])],
        [Paragraph("record_id", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Primary key — value of the first c1 field in the row", styles["table_cell"])],
        [Paragraph("source_file", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Path to the XML file this record came from", styles["table_cell"])],
        [Paragraph("xml_element", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Original XML tag name (e.g. c64)", styles["table_cell"])],
        [Paragraph("raw_position", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Numeric part of tag (c64 -> '64')", styles["table_cell"])],
        [Paragraph("resolved_position", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Metadata lookup key ('64.3' for local ref, '2' for normal)", styles["table_cell"])],
        [Paragraph("field_name", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Resolved business name from metadata, or FIELD_<pos> if unmapped", styles["table_cell"])],
        [Paragraph("system_type", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("SYS (core), USR (custom), CUSTOM (bank-specific), or None", styles["table_cell"])],
        [Paragraph("data_type", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("A=Alpha, R=Reference, D=Date, N=Numeric, or None", styles["table_cell"])],
        [Paragraph("format_rule", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Length and format constraint from C6 (e.g. '35.1')", styles["table_cell"])],
        [Paragraph("cardinality", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("SINGLE or MULTI from C7", styles["table_cell"])],
        [Paragraph("mv_index", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Multi-value index (m attribute value), or None", styles["table_cell"])],
        [Paragraph("sv_index", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Sub-value index (s attribute value), or None", styles["table_cell"])],
        [Paragraph("value", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Actual field value from the XML element", styles["table_cell"])],
        [Paragraph("relationship_target", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Linked T24 application name (e.g. COUNTRY), or None", styles["table_cell"])],
        [Paragraph("is_local_ref", styles["table_code"]), Paragraph("bool", styles["table_cell"]),
         Paragraph("True if field came from a local reference array (c64)", styles["table_cell"])],
        [Paragraph("is_custom_field", styles["table_code"]), Paragraph("bool", styles["table_cell"]),
         Paragraph("True if field is bank-specific customization (USR/CUSTOM type)", styles["table_cell"])],
        [Paragraph("is_mapped", styles["table_code"]), Paragraph("bool", styles["table_cell"]),
         Paragraph("True if metadata was found; False if FIELD_N fallback used", styles["table_cell"])],
        [Paragraph("warnings", styles["table_code"]), Paragraph("str", styles["table_cell"]),
         Paragraph("Comma-separated warning codes: UNMAPPED_FIELD, LOCAL_REF_WITHOUT_METADATA", styles["table_cell"])],
    ]

    col_w_out = [3.8*cm, 1.8*cm, PAGE_W - 2*MARGIN - 5.6*cm]
    out_table = Table(col_data, colWidths=col_w_out,
                      style=TableStyle([
                          ("BACKGROUND", (0,0), (-1,0), C_DARK_BLUE),
                          ("ROWBACKGROUNDS", (0,1), (-1,-1), [C_VERY_LIGHT, C_WHITE]),
                          ("GRID", (0,0), (-1,-1), 0.5, C_GREY_LIGHT),
                          ("LEFTPADDING", (0,0), (-1,-1), 6),
                          ("RIGHTPADDING", (0,0), (-1,-1), 6),
                          ("TOPPADDING", (0,0), (-1,-1), 5),
                          ("BOTTOMPADDING", (0,0), (-1,-1), 5),
                          ("VALIGN", (0,0), (-1,-1), "TOP"),
                      ]))
    story.append(out_table)

    story.append(PageBreak())
    return story


# ============================================================
# Section 7: Extending
# ============================================================
def build_extending(styles):
    story = []
    story.append(Paragraph("7. Extending the Pipeline", styles["h1"]))
    story.append(hr())

    story.append(Paragraph("Adding a New T24 Application", styles["h2"]))
    story.append(Paragraph(
        "No code changes required. Simply drop the files in place:",
        styles["body"]
    ))
    story += code_block([
        "# 1. Add data file",
        "t24_input_package/data/ACCOUNT.xml",
        "",
        "# 2. Add metadata",
        "t24_input_package/metadata/STANDARD_SELECTION_ACCOUNT.xml",
        "",
        "# 3. Run pipeline — ACCOUNT is auto-discovered",
        "python main.py",
    ], styles, label="Adding ACCOUNT Application — No Code Changes")

    story.append(Paragraph("Adding a New Local Reference Column", styles["h2"]))
    story += code_block([
        "# If your T24 uses c90 as a second local reference array:",
        "config = T24PipelineConfig(",
        "    package_root=Path('t24_input_package'),",
        "    local_ref_base_positions=('64', '90'),  # Add '90'",
        ")",
        "# Now <c90 m='1'> -> resolved_position = '90.1'",
        "# Add 90.1, 90.2 etc. to your metadata C3 column",
    ], styles, label="Extending Local Reference Base Positions")

    story.append(Paragraph("Adding a Custom Output Sink", styles["h2"]))
    story += code_block([
        "from t24_adapter import NormalizedField",
        "from typing import Iterator",
        "",
        "def write_to_database(rows: Iterator[NormalizedField], conn):",
        "    cursor = conn.cursor()",
        "    for row in rows:",
        "        cursor.execute(",
        "            'INSERT INTO t24_normalized VALUES (?,?,?,?,?,?)',",
        "            (row.app_name, row.record_id, row.field_name,",
        "             row.mv_index, row.sv_index, row.value)",
        "        )",
        "    conn.commit()",
        "",
        "write_to_database(pipeline.run(), db_conn)",
    ], styles, label="Custom Database Sink")

    story.append(PageBreak())
    return story


# ============================================================
# Section 8: Troubleshooting
# ============================================================
def build_troubleshooting(styles):
    story = []
    story.append(Paragraph("8. Troubleshooting", styles["h1"]))
    story.append(hr())

    issues = [
        (
            "All c64 fields appear as FIELD_64.1, FIELD_64.2, etc.",
            "Your STANDARD.SELECTION metadata does not have positions 64.1, 64.2 etc. in <C3>. "
            "Add them: <C3>64.1</C3><C3>64.2</C3> with matching <C1>FIELD.NAME</C1>. "
            "Or add a LOCAL.REF file.",
        ),
        (
            "Pipeline raises FileNotFoundError: Missing STANDARD.SELECTION",
            "Set require_standard_selection=False in config, or place a metadata file in "
            "t24_input_package/metadata/STANDARD_SELECTION_APP.xml.",
        ),
        (
            "XML validation fails: ParseError",
            "T24 XML exports sometimes contain XML comments that break parsers. "
            "XmlUtils.strip_comments() handles this automatically. "
            "If you still get ParseError, check for unescaped < or & characters in field values.",
        ),
        (
            "is_mapped=False for known fields",
            "The position in your metadata C3 column does not match the field tag. "
            "For c13, metadata must have <C3>13</C3>. "
            "For c64 m='2', metadata must have <C3>64.2</C3>.",
        ),
        (
            "Memory usage is high with large files",
            "Ensure you are using the pipeline as a generator (for field in pipeline.run()) "
            "not collecting all rows first (list(pipeline.run())). "
            "The reader uses ET.iterparse() and clears elements after each row.",
        ),
    ]

    for problem, solution in issues:
        story.append(Paragraph(f"Problem: {problem}", styles["h3"]))
        story += info_box(f"<b>Solution:</b> {solution}", styles)
        story.append(Spacer(1, 4))

    story += success_box(
        "<b>Tip:</b> Run with DEBUG logging to see every field registration and lookup: "
        "logging.basicConfig(level=logging.DEBUG)",
        styles
    )

    return story


# ============================================================
# Page Template
# ============================================================
def add_page_decorations(canvas, doc):
    canvas.saveState()
    w, h = A4

    # Header bar
    canvas.setFillColor(C_DARK_BLUE)
    canvas.rect(0, h - 1.2*cm, w, 1.2*cm, fill=1, stroke=0)
    canvas.setFillColor(C_WHITE)
    canvas.setFont("Helvetica-Bold", 9)
    canvas.drawString(MARGIN, h - 0.75*cm, "T24 Generic Adapter — Developer Guide")
    canvas.setFont("Helvetica", 9)
    canvas.drawRightString(w - MARGIN, h - 0.75*cm, "v1.0 | June 2026")

    # Footer bar
    canvas.setFillColor(C_GREY_LIGHT)
    canvas.rect(0, 0, w, 1.0*cm, fill=1, stroke=0)
    canvas.setFillColor(C_GREY_MID)
    canvas.setFont("Helvetica", 8)
    canvas.drawCentredString(w / 2, 0.35*cm, f"Page {doc.page}")

    # Accent line under header
    canvas.setFillColor(C_ACCENT)
    canvas.rect(0, h - 1.2*cm - 2, w, 2, fill=1, stroke=0)

    canvas.restoreState()


# ============================================================
# Build PDF
# ============================================================
def build_pdf(output_path: str):
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=1.8*cm,
        bottomMargin=1.6*cm,
    )

    styles = build_styles()
    story = []

    story += build_cover(styles)
    story += build_toc(styles)
    story += build_architecture(styles)
    story += build_structure(styles)
    story += build_modules(styles)
    story += build_execution_guide(styles)
    story += build_concepts(styles)
    story += build_output_reference(styles)
    story += build_extending(styles)
    story += build_troubleshooting(styles)

    doc.build(story, onFirstPage=add_page_decorations, onLaterPages=add_page_decorations)
    print(f"PDF generated: {output_path}")


if __name__ == "__main__":
    build_pdf("/mnt/user-data/outputs/T24_Generic_Adapter_Guide.pdf")
