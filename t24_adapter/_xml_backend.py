"""
_xml_backend.py
---------------
Selects the fastest available XML parser at import time:

- `lxml` (C-based, typically 3-5x faster) when installed, else
- the standard library `xml.etree.ElementTree`.

Both expose a compatible Element API for the parts the pipeline uses
(`.tag`, `.attrib`, `.text`, `.iter()`), so the normalizer and XmlUtils are
unchanged regardless of backend. There is NO hard dependency on lxml — it is a
drop-in accelerator.

Exports
-------
fromstring(data)  : parse bytes/str into a root Element
XMLParseError     : the exception type raised on malformed XML (unified)
backend_name()    : "lxml" or "ElementTree"
log_backend_once(): log the active backend exactly once
"""

from __future__ import annotations

import logging

logger = logging.getLogger("t24-adapter.xml")

try:
    from lxml import etree as _ET  # type: ignore

    _BACKEND = "lxml"
    XMLParseError = _ET.XMLSyntaxError

    def fromstring(data):
        # lxml parses bytes (incl. an <?xml encoding=...?> declaration) directly.
        return _ET.fromstring(data)

except ImportError:  # pragma: no cover - depends on the environment
    import xml.etree.ElementTree as _ET  # type: ignore

    _BACKEND = "ElementTree"
    XMLParseError = _ET.ParseError

    def fromstring(data):
        return _ET.fromstring(data)


def backend_name() -> str:
    return _BACKEND


_logged = False


def log_backend_once() -> None:
    """Log which XML backend is active, once per process."""
    global _logged
    if not _logged:
        logger.info(f"XML parser backend: {_BACKEND}")
        _logged = True
