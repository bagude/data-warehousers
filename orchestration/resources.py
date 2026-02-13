"""Shared resources and configuration for the Dagster pipeline.

Exposes project paths and DuckDB settings from ``src.utils.config`` as a
simple Dagster resource so that assets can access them via the context.
"""

from __future__ import annotations

from dagster import ConfigurableResource

from src.utils.config import (
    DATA_DIR,
    BRONZE_DIR,
    BRONZE_TX_DIR,
    BRONZE_NM_DIR,
    SILVER_DIR,
    GOLD_DIR,
    DUCKDB_PATH,
    ensure_data_dirs,
)


class WarehouseConfig(ConfigurableResource):
    """Project paths and DuckDB configuration exposed as a Dagster resource.

    All values default to the paths defined in ``src.utils.config``.
    Override via Dagster's run config or environment variables when needed.
    """

    data_dir: str = str(DATA_DIR)
    bronze_dir: str = str(BRONZE_DIR)
    bronze_tx_dir: str = str(BRONZE_TX_DIR)
    bronze_nm_dir: str = str(BRONZE_NM_DIR)
    silver_dir: str = str(SILVER_DIR)
    gold_dir: str = str(GOLD_DIR)
    duckdb_path: str = str(DUCKDB_PATH)

    def ensure_dirs(self) -> None:
        """Create all medallion-layer data directories if they do not exist."""
        ensure_data_dirs()
