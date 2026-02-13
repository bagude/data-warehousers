"""Dimensional model builder -- orchestrates DDL, seeds, ETL, views, validation.

Replaces the flat _gold_builder.py with the full digital twin dimensional model.
Called by both the production gold_models Dagster asset and E2E test assets.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from src.gold.ddl import create_all_tables, seed_reference_dimensions, populate_time_dimensions
from src.gold.etl import load_silver_to_dimensional_model
from src.gold.views import create_canonical_views
from src.gold.validation import run_all_validations
from src.gold.seed_constants import validate_seeds


def build_dimensional_model(
    *,
    duckdb_path: str | Path,
    silver_parquet_glob: str,
    log: Any = None,
    start_year: int = 2000,
    end_year: int = 2026,
) -> dict[str, Any]:
    """Build the full dimensional model from silver Parquet.

    Parameters
    ----------
    duckdb_path : path to DuckDB file
    silver_parquet_glob : glob for silver Parquet files
    log : optional logger with .info() and .error()
    start_year, end_year : range for time dimension generation

    Returns
    -------
    dict with table counts, validation results.

    Raises
    ------
    RuntimeError if seed validation or post-load validation fails.
    """

    def _log_info(msg, *a):
        if log:
            log.info(msg, *a)

    def _log_error(msg, *a):
        if log:
            log.error(msg, *a)

    con = duckdb.connect(str(duckdb_path))
    try:
        # 1. Schema
        _log_info("Creating dimensional model schema...")
        create_all_tables(con)

        # 2. Seeds
        _log_info("Seeding reference dimensions...")
        seed_reference_dimensions(con)

        # 3. Validate seeds match constants
        seed_errors = validate_seeds(con)
        if seed_errors:
            for e in seed_errors:
                _log_error("SEED ERROR: %s", e)
            raise RuntimeError(f"Seed validation failed: {seed_errors}")

        # 4. Time dimensions
        _log_info("Generating time dimensions (%d-%d)...", start_year, end_year)
        populate_time_dimensions(con, start_year=start_year, end_year=end_year)

        # 5. ETL
        _log_info("Loading silver to dimensional model...")
        counts = load_silver_to_dimensional_model(con, silver_parquet_glob, log=log)

        # 6. Views
        _log_info("Creating canonical views...")
        create_canonical_views(con)

        # 7. Validation
        _log_info("Running post-load validations...")
        validation_errors = run_all_validations(con)
        for e in validation_errors:
            _log_error("VALIDATION ERROR: %s", e)

        if validation_errors:
            raise RuntimeError(f"{len(validation_errors)} validation error(s): " + "; ".join(validation_errors))

        _log_info("Dimensional model build complete.")

    finally:
        con.close()

    return {
        "tables_built": len(counts),
        "validation_errors": validation_errors,
        **counts,
    }
