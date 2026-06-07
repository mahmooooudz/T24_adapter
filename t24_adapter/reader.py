"""
reader.py
---------
T24 Generic Adapter - Streaming XML Data Reader

Streams T24 XML data records row-by-row without loading the
entire file into memory. This is critical for large T24 extracts
which can contain hundreds of thousands of records.

Uses ET.iterparse() for memory-efficient streaming.
Each <row> element is yielded, then cleared from memory.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator

from .xml_utils import XmlUtils

logger = logging.getLogger("t24-adapter.reader")


class T24StreamingDataReader:
    """
    Streams T24 XML data records as individual ET.Element objects.

    How it works
    ------------
    T24 XML data files contain one or more <row> elements.
    Each <row> contains the field values for one T24 record.

    Example input file structure:
        <DATA>
            <row>
                <c1>JD1009</c1>
                <c2 m="1">John Doe</c2>
                ...
            </row>
            <row>
                <c1>JD1010</c1>
                ...
            </row>
        </DATA>

    The reader uses iterparse to process one <row> at a time,
    then calls elem.clear() to free memory before the next record.

    This means a 10GB file uses no more memory than a single record.

    Usage
    -----
    reader = T24StreamingDataReader()
    for row_element in reader.stream_records(Path("data/CUSTOMER.xml")):
        # process row_element
        pass
    """

    def stream_records(self, file_path: Path) -> Iterator[ET.Element]:
        """
        Stream all <row> elements from a T24 XML data file.

        Parameters
        ----------
        file_path : Path to the XML data file

        Yields
        ------
        ET.Element objects, one per <row> in the file.
        Each element is complete with all child nodes populated.

        Notes
        -----
        - Tag matching is case-insensitive (handles both <row> and <ROW>)
        - Elements are cleared after yielding to free memory
        - The file is processed as a stream, not loaded into memory
        """
        logger.info(f"Streaming records from: {file_path}")
        record_count = 0

        context = ET.iterparse(file_path, events=("end",))

        for event, elem in context:
            # Normalize tag for matching (strip namespace + lowercase)
            tag = XmlUtils.strip_namespace(elem.tag).lower()

            if tag == "row":
                record_count += 1
                logger.debug(f"  Yielding record #{record_count}")
                yield elem
                elem.clear()  # Free memory immediately after processing

        logger.info(f"Finished streaming. Total records: {record_count}")

    def count_records(self, file_path: Path) -> int:
        """
        Count the number of <row> elements in a file without normalizing.
        Useful for progress reporting before streaming.
        """
        count = 0
        context = ET.iterparse(file_path, events=("end",))
        for event, elem in context:
            tag = XmlUtils.strip_namespace(elem.tag).lower()
            if tag == "row":
                count += 1
                elem.clear()
        return count
