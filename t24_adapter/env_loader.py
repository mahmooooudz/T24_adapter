"""
env_loader.py
-------------
T24 Generic Adapter - Minimal .env loader (stdlib only)

Loads KEY=VALUE pairs from a .env file into os.environ so database
credentials never have to be hardcoded or committed. Existing environment
variables are NOT overridden (real env wins over the file).

This avoids a python-dotenv dependency, keeping the project on the
standard library (+ optional pandas / psycopg2).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger("t24-adapter.env")


def load_env(path: str | Path = ".env") -> bool:
    """
    Load environment variables from a .env file into os.environ.

    Parameters
    ----------
    path : path to the .env file (default: ".env" in the current directory)

    Returns
    -------
    bool : True if a file was found and read, False otherwise.

    Notes
    -----
    - Lines that are blank or start with '#' are ignored.
    - Surrounding single/double quotes around values are stripped.
    - Variables already set in the environment are left untouched.
    """
    env_path = Path(path)
    if not env_path.exists():
        logger.info(f"No .env file found at {env_path}; relying on existing environment.")
        return False

    loaded = 0
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        # Real environment variables take priority over the file.
        if key not in os.environ:
            os.environ[key] = value
            loaded += 1

    logger.info(f"Loaded {loaded} variable(s) from {env_path}.")
    return True
