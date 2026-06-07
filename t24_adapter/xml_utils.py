"""
xml_utils.py
------------
T24 Generic Adapter - XML Utility Helpers

Low-level XML parsing utilities used across all modules.
Handles:
- XML namespace stripping
- XML comment removal
- Multi-root fragment wrapping
- Well-formedness checking
- Numeric position extraction from T24 XML tags (c1, c64, etc.)
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Tuple


class XmlUtils:
    """
    Static utility methods for T24 XML processing.
    All methods are stateless and can be called without instantiation.
    """

    # ------------------------------------------------------------------ #
    # Namespace Handling
    # ------------------------------------------------------------------ #

    @staticmethod
    def strip_namespace(tag: str) -> str:
        """
        Remove XML namespace prefix from a tag.

        Example:
            {http://example.com/schema}ROW -> ROW
            ROW                            -> ROW
        """
        if "}" in tag:
            return tag.split("}", 1)[1]
        return tag

    # ------------------------------------------------------------------ #
    # XML Comment Handling
    # ------------------------------------------------------------------ #

    @staticmethod
    def strip_comments(xml_text: str) -> str:
        """
        Remove all XML comments from a string.
        T24 XML extracts often contain inline comments that can
        break standard parsers.

        Example:
            <!-- LOCAL.REF Array --> -> (removed)
        """
        return re.sub(r"<!--.*?-->", "", xml_text, flags=re.DOTALL)

    # ------------------------------------------------------------------ #
    # Parsing
    # ------------------------------------------------------------------ #

    @staticmethod
    def parse_xml_file(path: Path) -> ET.ElementTree:
        """
        Parse a well-formed XML file into an ElementTree.
        Does not handle comments or multi-root fragments.
        Use parse_xml_fragment for those cases.
        """
        return ET.parse(path)

    @staticmethod
    def parse_xml_fragment(xml_text: str) -> ET.Element:
        """
        Parse an XML string that may:
        - Contain XML comments
        - Have multiple root-level elements (invalid XML)
        - Have leading/trailing whitespace

        Strategy:
        1. Strip comments
        2. Try direct parse
        3. If ParseError, wrap in <ROOT>...</ROOT> and retry

        Returns an ET.Element (root node).
        """
        cleaned = XmlUtils.strip_comments(xml_text).strip()
        try:
            return ET.fromstring(cleaned)
        except ET.ParseError:
            wrapped = f"<ROOT>{cleaned}</ROOT>"
            return ET.fromstring(wrapped)

    @staticmethod
    def is_xml_well_formed(path: Path) -> Tuple[bool, Optional[str]]:
        """
        Check whether a file is well-formed XML.

        Returns
        -------
        (True, None)       if valid
        (False, error_msg) if invalid
        """
        try:
            ET.parse(path)
            return True, None
        except ET.ParseError as exc:
            return False, str(exc)

    # ------------------------------------------------------------------ #
    # T24-Specific Tag Parsing
    # ------------------------------------------------------------------ #

    @staticmethod
    def extract_numeric_position_from_tag(tag: str) -> Optional[str]:
        """
        Extract the numeric field position from a T24 XML element tag.

        T24 XML tags always follow the pattern cN where N is the field number.
        This method strips the leading 'c' or 'C' and returns the digit(s).

        Examples:
            c1   -> "1"
            c64  -> "64"
            c176 -> "176"
            ROW  -> None  (not a field tag)
            root -> None
        """
        clean_tag = XmlUtils.strip_namespace(tag)
        match = re.fullmatch(r"[cC](\d+)", clean_tag)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def text(node: ET.Element) -> str:
        """
        Safely extract and strip text content from an XML element.
        Returns empty string if text is None.
        """
        return (node.text or "").strip()

    @staticmethod
    def find_first_row(root: ET.Element) -> Optional[ET.Element]:
        """
        Find the first <ROW> or <row> element in an XML tree.
        Searches root first, then iterates children.
        Used when metadata and data XML have different wrapper structures.
        """
        if XmlUtils.strip_namespace(root.tag).upper() == "ROW":
            return root
        for child in root.iter():
            if XmlUtils.strip_namespace(child.tag).upper() == "ROW":
                return child
        return None

    @staticmethod
    def extract_repeated_columns(row: ET.Element) -> dict:
        """
        Extract all repeated column elements from a STANDARD.SELECTION
        style <ROW> into a dictionary of lists.

        T24 metadata XML repeats elements like:
            <C1>MNEMONIC</C1>
            <C1>SHORT.NAME</C1>
            <C1>NAME.1</C1>
            <C3>1</C3>
            <C3>2</C3>
            <C3>3</C3>

        Returns:
            {
                "C1": ["MNEMONIC", "SHORT.NAME", "NAME.1"],
                "C3": ["1", "2", "3"],
                ...
            }
        """
        from collections import defaultdict
        columns = defaultdict(list)
        for child in row:
            tag = XmlUtils.strip_namespace(child.tag).upper()
            if re.fullmatch(r"C\d+", tag):
                columns[tag].append(XmlUtils.text(child))
        return columns
