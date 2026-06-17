"""
Tests for the wide pivot (WidePivotWriter) — the flattening core.

Uses synthetic app/field names to prove the pivot is fully dynamic (no app or
column names are hardcoded anywhere in the logic).
"""

from conftest import nf, FakePipeline
from t24_adapter.wide_writer import WidePivotWriter


def _schema(fields, app):
    return WidePivotWriter().discover_schema(FakePipeline(fields).run())[app]


def test_single_field_stays_one_column():
    fields = [nf("APPX", "r1", "1", "KEY", "r1"), nf("APPX", "r1", "2", "NAME", "Alice")]
    cols = _schema(fields, "APPX").columns
    assert "NAME" in cols and "NAME_1" not in cols


def test_repeating_field_expands_to_indexed_columns():
    # One record with three occurrences of position 9 -> THREE indexed columns.
    fields = [
        nf("APPX", "r1", "1", "KEY", "r1"),
        nf("APPX", "r1", "9", "OFFICER", "a", mv="1"),
        nf("APPX", "r1", "9", "OFFICER", "b", mv="2"),
        nf("APPX", "r1", "9", "OFFICER", "c", mv="3"),
    ]
    cols = _schema(fields, "APPX").columns
    assert {"OFFICER_1", "OFFICER_2", "OFFICER_3"} <= set(cols)
    assert "OFFICER" not in cols


def test_schema_growth_uses_max_occurrence_and_pads():
    # r1 has 1 occurrence, r2 has 3 -> schema sizes to 3; r1 pads blanks.
    fields = [
        nf("APPX", "r1", "1", "KEY", "r1"), nf("APPX", "r1", "9", "OFFICER", "x", mv="1"),
        nf("APPX", "r2", "1", "KEY", "r2"),
        nf("APPX", "r2", "9", "OFFICER", "a", mv="1"),
        nf("APPX", "r2", "9", "OFFICER", "b", mv="2"),
        nf("APPX", "r2", "9", "OFFICER", "c", mv="3"),
    ]
    wp = WidePivotWriter()
    schema = wp.discover_schema(FakePipeline(fields).run())["APPX"]
    rows = {r["recordId"]: r for r in wp.iter_wide_rows(FakePipeline(fields).run(), schema, "APPX")}
    assert {"OFFICER_1", "OFFICER_2", "OFFICER_3"} <= set(schema.columns)
    assert rows["r1"]["OFFICER_1"] == "x"
    assert rows["r1"]["OFFICER_2"] == "" and rows["r1"]["OFFICER_3"] == ""   # padded
    assert [rows["r2"]["OFFICER_1"], rows["r2"]["OFFICER_2"], rows["r2"]["OFFICER_3"]] == ["a", "b", "c"]


def test_recordid_and_appname_are_leading_columns():
    fields = [nf("APPX", "r1", "1", "KEY", "r1")]
    schema = _schema(fields, "APPX")
    assert schema.columns[:2] == ["recordId", "app_name"]
    wp = WidePivotWriter()
    row = next(iter(wp.iter_wide_rows(FakePipeline(fields).run(), schema, "APPX")))
    assert row["recordId"] == "r1" and row["app_name"] == "APPX"


def test_special_characters_preserved_verbatim():
    val = 'Smith "Jr" | Holdings, Café'
    fields = [nf("APPX", "r1", "1", "KEY", "r1"), nf("APPX", "r1", "2", "TITLE", val)]
    wp = WidePivotWriter()
    schema = wp.discover_schema(FakePipeline(fields).run())["APPX"]
    row = next(iter(wp.iter_wide_rows(FakePipeline(fields).run(), schema, "APPX")))
    assert row["TITLE"] == val   # not split, escaped, or mangled


def test_iter_all_wide_rows_matches_per_app_single_pass():
    # Two apps in one stream (app-grouped). The single-pass iter_all_wide_rows
    # must yield exactly what per-app iter_wide_rows produces — this guards the
    # N+1 -> 2 refactor.
    fields = [
        nf("A", "a1", "1", "KEY", "a1"), nf("A", "a1", "2", "X", "1"),
        nf("A", "a2", "1", "KEY", "a2"), nf("A", "a2", "2", "X", "2"),
        nf("B", "b1", "1", "KEY", "b1"), nf("B", "b1", "2", "Y", "9"),
    ]
    wp = WidePivotWriter()
    schemas = wp.discover_schema(FakePipeline(fields).run())

    per_app = {}
    for app in schemas:
        per_app[app] = list(wp.iter_wide_rows(FakePipeline(fields).run(), schemas[app], app))

    all_rows = {}
    for app, row in wp.iter_all_wide_rows(FakePipeline(fields).run(), schemas):
        all_rows.setdefault(app, []).append(row)

    assert all_rows == per_app
    assert [r["recordId"] for r in all_rows["A"]] == ["a1", "a2"]
    assert [r["recordId"] for r in all_rows["B"]] == ["b1"]


# ---------------------------------------------------------------------------
# Lever 1 — single-pass buffered pivot must equal the two-pass output exactly.
# ---------------------------------------------------------------------------

def _single_pass_rows(fields, max_buffer_rows=None):
    """Run the single-pass buffered pivot and return {app: [wide_row, ...]}."""
    wp = WidePivotWriter()
    schemas, buffers, overflow = wp.stream_buffer_and_schema(
        FakePipeline(fields).run(), max_buffer_rows=max_buffer_rows
    )
    out = {}
    for app, schema in schemas.items():
        if app in overflow:
            continue
        out[app] = [
            wp.wide_row_from_buffer(rid, vals, schema, app)
            for rid, vals in buffers.get(app, [])
        ]
    return schemas, out, overflow


def test_single_pass_equals_two_pass_multi_app():
    # Mixed: repeating field, schema growth (pad), multiple apps, special chars.
    fields = [
        nf("A", "a1", "1", "KEY", "a1"), nf("A", "a1", "9", "OFFICER", "x", mv="1"),
        nf("A", "a2", "1", "KEY", "a2"),
        nf("A", "a2", "9", "OFFICER", "p", mv="1"),
        nf("A", "a2", "9", "OFFICER", "q", mv="2"),
        nf("A", "a2", "9", "OFFICER", "r", mv="3"),
        nf("B", "b1", "1", "KEY", "b1"), nf("B", "b1", "2", "TITLE", 'Café, "Jr" | x'),
    ]
    wp = WidePivotWriter()
    schemas_2p = wp.discover_schema(FakePipeline(fields).run())
    two_pass = {}
    for app, row in wp.iter_all_wide_rows(FakePipeline(fields).run(), schemas_2p):
        two_pass.setdefault(app, []).append(row)

    schemas_1p, single_pass, overflow = _single_pass_rows(fields)

    assert not overflow
    # Same columns per app...
    for app in two_pass:
        assert schemas_1p[app].columns == schemas_2p[app].columns
    # ...and byte-identical wide rows.
    assert single_pass == two_pass


def test_single_pass_none_value_becomes_blank():
    fields = [nf("A", "a1", "1", "KEY", "a1"), nf("A", "a1", "2", "OPT", None)]
    _, rows, _ = _single_pass_rows(fields)
    assert rows["A"][0]["OPT"] == ""


def test_single_pass_overflow_keeps_schema_drops_buffer():
    # Cap of 1 row: app A has 2 records -> overflow. Schema must still be complete
    # (so the caller can re-stream), but A's data buffer is emptied (bounded mem).
    fields = [
        nf("A", "a1", "1", "KEY", "a1"), nf("A", "a1", "9", "OFFICER", "x", mv="1"),
        nf("A", "a2", "1", "KEY", "a2"),
        nf("A", "a2", "9", "OFFICER", "p", mv="1"), nf("A", "a2", "9", "OFFICER", "q", mv="2"),
    ]
    wp = WidePivotWriter()
    schemas, buffers, overflow = wp.stream_buffer_and_schema(
        FakePipeline(fields).run(), max_buffer_rows=1
    )
    assert "A" in overflow
    assert buffers.get("A") == []                       # data dropped
    assert {"OFFICER_1", "OFFICER_2"} <= set(schemas["A"].columns)  # schema intact
