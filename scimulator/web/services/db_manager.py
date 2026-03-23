"""
DuckDB connection management for the web server.
"""

from pathlib import Path
from typing import List, Dict

import duckdb


def get_connection(db_path: str, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection. Callers must close it when done."""
    return duckdb.connect(db_path, read_only=read_only)


def list_databases(data_dir: Path) -> List[Dict]:
    """List all .duckdb files in the data directory."""
    files = sorted(data_dir.glob("*.duckdb"))
    result = []
    for f in files:
        size_mb = f.stat().st_size / (1024 * 1024)
        result.append({
            "name": f.name,
            "size_mb": round(size_mb, 2),
            "path": str(f),
        })
    return result
