"""
Tests for T24Normalizer — record_id resolution and field flattening.
Network-free; synthetic apps/positions prove the logic is config-driven.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

from t24_adapter.config import T24PipelineConfig
from t24_adapter.metadata_registry import T24MetadataRegistry
from t24_adapter.normalizer import T24Normalizer


def _normalizer(**cfg):
    base = dict(package_root=Path("."), local_ref_base_positions=("64",))
    base.update(cfg)
    return T24Normalizer(registry=T24MetadataRegistry(), config=T24PipelineConfig(**base))


ROW = '<row><c1>KEY1</c1><c2>ACC9</c2><c64 m="1">A</c64><c64 m="2">B</c64></row>'


def test_record_id_defaults_to_position_1():
    fields = _normalizer().normalize_record("APPX", ET.fromstring(ROW), "src")
    assert {f.record_id for f in fields} == {"KEY1"}


def test_record_id_position_override_per_app():
    nz = _normalizer(record_id_positions={"APPX": "2"})
    fields = nz.normalize_record("APPX", ET.fromstring(ROW), "src")
    assert {f.record_id for f in fields} == {"ACC9"}   # c2 value, not c1


def test_explicit_record_id_wins():
    fields = _normalizer().normalize_record("APPX", ET.fromstring(ROW), "src", record_id="OVERRIDE")
    assert {f.record_id for f in fields} == {"OVERRIDE"}


def test_local_ref_positions_resolve_to_dotted_keys():
    fields = _normalizer().normalize_record("APPX", ET.fromstring(ROW), "src")
    by_pos = {f.resolved_position: f for f in fields}
    assert "64.1" in by_pos and "64.2" in by_pos          # c64 m=N -> 64.N
    assert by_pos["64.1"].is_local_ref is True
    # No metadata registered -> unmapped, flagged
    assert by_pos["64.1"].is_mapped is False
    assert "LOCAL_REF_WITHOUT_METADATA" in by_pos["64.1"].warnings


def test_unmapped_field_falls_back_to_field_position():
    fields = _normalizer().normalize_record("APPX", ET.fromstring(ROW), "src")
    f2 = next(f for f in fields if f.resolved_position == "2")
    assert f2.field_name == "FIELD_2" and f2.is_mapped is False


def test_load_from_string_matches_file_loader():
    """DB-loaded STANDARD.SELECTION must register identically to file-loaded."""
    from t24_adapter.metadata_loaders import StandardSelectionLoader
    f = Path("t24_input_package/metadata/STANDARD_SELECTION_ACCOUNT.xml")
    if not f.exists():
        import pytest
        pytest.skip("sample metadata file not present")
    r_file = T24MetadataRegistry()
    StandardSelectionLoader().load("ACCOUNT", f, r_file)
    r_str = T24MetadataRegistry()
    StandardSelectionLoader().load_from_string("ACCOUNT", f.read_text(encoding="utf-8"), r_str, "db#ACCOUNT")
    assert ({p: m.field_name for p, m in r_file.fields_by_position["ACCOUNT"].items()}
            == {p: m.field_name for p, m in r_str.fields_by_position["ACCOUNT"].items()})
