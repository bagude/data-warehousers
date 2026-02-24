"""Project-wide configuration — paths, DuckDB settings, and directory helpers."""

from __future__ import annotations

from pathlib import Path

# -- Project root (two levels up from this file: src/utils/config.py) ---------
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# -- Data directories (medallion layers) --------------------------------------
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
BRONZE_TX_DIR = BRONZE_DIR / "tx"
BRONZE_NM_DIR = BRONZE_DIR / "nm"
BRONZE_OK_DIR = BRONZE_DIR / "ok"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# -- DuckDB warehouse database ------------------------------------------------
DUCKDB_PATH = DATA_DIR / "warehouse.duckdb"

# -- Paper & embedding paths --------------------------------------------------
PAPER_DATA_DIR = DATA_DIR / "paper_data"
EMBEDDINGS_DB_PATH = DATA_DIR / "embeddings.duckdb"
PAPERS_DIR = PROJECT_ROOT / "docs" / "papers"


def ensure_data_dirs() -> None:
    """Create all medallion-layer data directories if they do not exist."""
    for d in (BRONZE_TX_DIR, BRONZE_NM_DIR, BRONZE_OK_DIR, SILVER_DIR, GOLD_DIR):
        d.mkdir(parents=True, exist_ok=True)
