"""
Integration test: K=1 (sequential) vs K=2 (parallel) end-to-end equivalence.

Opt-in via `pytest -m integration`. Requires a live PostgreSQL connection
(reads credentials from .env) and pre-seeded data; SKIPs cleanly when the
environment isn't available so default `pytest` runs stay network-free.

Guarantees the parallel pool's output is byte-identical to sequential — the
contract the threading work was scoped against.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.integration


def _read_env(env_path: Path) -> dict:
    """Parse a simple KEY=VALUE .env file. Lines starting with # are ignored."""
    out = {}
    if not env_path.exists():
        return out
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k] = v
    return out


def _connect():
    """Open a psycopg2 connection from .env vars, or skip the test if absent."""
    psycopg2 = pytest.importorskip("psycopg2")
    env = _read_env(REPO_ROOT / ".env")
    if not all(env.get(k) for k in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")):
        pytest.skip("DB_* not configured in .env — integration test skipped")
    try:
        return psycopg2.connect(
            host=env["DB_HOST"], port=env["DB_PORT"], dbname=env["DB_NAME"],
            user=env["DB_USER"], password=env["DB_PASSWORD"], connect_timeout=5,
        )
    except Exception as exc:
        pytest.skip(f"DB unreachable: {exc}")


# Columns the CURRENT writer populates, in writer-determined order. Hashing
# these (rather than `SELECT *`) is robust to legacy columns lingering in the
# physical table from old runs that no current code writes to.
LIVE_COLS = {
    "ACCOUNT_wide": [
        "recordId", "app_name",
        "CUSTOMER", "ACCOUNT.NO", "CATEGORY", "ACCOUNT.TITLE.1", "CURRENCY",
        "OPENING.DATE", "WORKING.BALANCE", "ONLINE.ACT.BAL",
        "ACCOUNT.OFFICER_1", "ACCOUNT.OFFICER_2", "ACCOUNT.OFFICER_3",
        "RELATIONSHIP.CODE", "RISK.RATING",
        "INTEREST.RATE_1", "INTEREST.RATE_2", "INTEREST.RATE_3",
        "REVIEW.DATE", "AUDIT.NOTE",
    ],
    "CUSTOMER_wide": [
        "recordId", "app_name",
        "MNEMONIC", "SHORT.NAME", "NAME.1", "STREET", "TOWN.COUNTRY", "COUNTRY",
        "SECTOR", "NATIONALITY", "RESIDENCE", "EMAIL.1",
        "MARITAL.STATUS", "GENDER", "DATE.OF.BIRTH", "LEGAL.ID",
        "CUST.SEGMENT", "CUST.TIER", "TAX.ID_1", "TAX.ID_2", "KYC.REVIEW.DATE",
    ],
}


def _hash_live(conn) -> dict:
    """SHA-256 each `_wide` table's live (writer-populated) columns, ordered
    by recordId. Stable input -> stable hash regardless of physical col order."""
    h = {}
    with conn.cursor() as cur:
        for table, cols in LIVE_COLS.items():
            quoted = ", ".join(f'"{c}"' for c in cols)
            cur.execute(f'SELECT {quoted} FROM t24_adaptor."{table}" ORDER BY "recordId"::int')
            rows = cur.fetchall()
            blob = "|".join(
                "".join("" if v is None else str(v) for v in row) for row in rows
            ).encode()
            h[table] = hashlib.sha256(blob).hexdigest()
    return h


def _run_main(workers: int) -> int:
    """Run `python main.py` with db_max_workers temporarily set to `workers`.
    Restores settings.py afterwards. Returns the process exit code."""
    settings = REPO_ROOT / "settings.py"
    src = settings.read_text()
    patched = _set_max_workers(src, workers)
    settings.write_text(patched)
    try:
        proc = subprocess.run(
            [sys.executable, "main.py"], cwd=REPO_ROOT,
            capture_output=True, text=True, timeout=300,
        )
        if proc.returncode != 0:
            print(proc.stdout)
            print(proc.stderr, file=sys.stderr)
        return proc.returncode
    finally:
        settings.write_text(src)


def _set_max_workers(src: str, n: int) -> str:
    """Rewrite `db_max_workers=<x>` to `db_max_workers=n` in settings.py text.
    Asserts the line exists so a settings refactor doesn't silently break this."""
    import re
    new, count = re.subn(r"db_max_workers=\d+", f"db_max_workers={n}", src)
    assert count >= 1, "settings.py: db_max_workers=<n> line not found"
    return new


def test_k1_and_k2_produce_byte_identical_wide_tables():
    """Run main.py twice (K=1, then K=2) and assert the live columns of both
    `_wide` tables match SHA-256 after each run.

    UPSERT semantics mean the second run overwrites the first; we hash AFTER
    each so any divergence shows up immediately. Equivalence here is the load-
    bearing safety contract for the parallel pool work."""
    conn = _connect()
    try:
        # K=1 baseline
        assert _run_main(1) == 0, "K=1 run failed"
        h_k1 = _hash_live(conn)

        # K=2 should produce the SAME output
        assert _run_main(2) == 0, "K=2 run failed"
        h_k2 = _hash_live(conn)

        assert h_k1 == h_k2, (
            f"Parallel output diverged from sequential!\n"
            f"  K=1: {h_k1}\n  K=2: {h_k2}"
        )
    finally:
        conn.close()
