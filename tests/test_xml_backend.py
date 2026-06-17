"""
Lever 3 — pluggable XML backend (lxml when present, else ElementTree).

Backend-agnostic: these run under whichever parser is active and prove the
Element API the pipeline relies on (.tag/.attrib/.text/.iter) behaves the same,
and that malformed XML raises the unified XMLParseError.
"""

import pytest

from t24_adapter._xml_backend import XMLParseError, backend_name, fromstring
from t24_adapter.xml_utils import XmlUtils

SAMPLE = (
    b'<?xml version="1.0" encoding="UTF-8"?>'
    b'<DATA><row><c1>JD1009</c1><c2 m="1">John</c2>'
    b'<c64 m="3" s="2">2.75</c64></row></DATA>'
)


def test_backend_name_is_known():
    assert backend_name() in ("lxml", "ElementTree")


def test_parse_yields_expected_elements():
    root = fromstring(SAMPLE)
    rows = [e for e in root.iter() if XmlUtils.strip_namespace(e.tag).lower() == "row"]
    assert len(rows) == 1
    children = list(rows[0])
    tags = [XmlUtils.strip_namespace(c.tag).lower() for c in children]
    assert tags == ["c1", "c2", "c64"]
    assert XmlUtils.text(children[0]) == "JD1009"
    assert children[1].attrib.get("m") == "1"
    assert children[2].attrib.get("m") == "3" and children[2].attrib.get("s") == "2"
    assert XmlUtils.text(children[2]) == "2.75"


def test_malformed_raises_unified_error():
    with pytest.raises(XMLParseError):
        fromstring(b"<DATA><row><c1>oops</row></DATA>")  # unclosed c1
