"""
Shared pytest fixtures/helpers for the T24 adapter tests.

The tests are network-free by default: they exercise the real (dynamic) code
paths with synthetic in-memory data — no database, no files required — which
also proves nothing is hardcoded to specific app/table/field names.
"""

import os
import sys

# Make the repo root importable (t24_adapter, settings) regardless of cwd.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from t24_adapter.models import NormalizedField


def nf(app, rid, pos, name, value, mv=None, sv=None, mapped=True, local=False):
    """Build a NormalizedField the way the normalizer would, for pivot tests."""
    return NormalizedField(
        app_name=app, record_id=rid, source_file="test",
        xml_element="c" + pos.split(".")[0], raw_position=pos.split(".")[0],
        resolved_position=pos, field_name=name,
        system_type=None, data_type=None, format_rule=None, cardinality=None,
        mv_index=mv, sv_index=sv, value=value, relationship_target=None,
        is_local_ref=local, is_custom_field=False, is_mapped=mapped, warnings=[],
    )


class FakePipeline:
    """A re-runnable stand-in for T24GenericPipeline: run() yields fixed fields."""

    def __init__(self, fields):
        self._fields = list(fields)

    def run(self):
        return iter(list(self._fields))
