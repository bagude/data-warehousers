"""Gold-layer assets -- build dimensional model via DuckDB Python API.

Depends on both silver assets.  Builds the full dimensional model
(22 tables + 3 canonical views) into DuckDB, then runs validation.

Dependency graph::

    silver_tx  \
                --> gold_models --> (DuckDB dimensional model)
    silver_nm  /
"""

import os
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.utils.config import PROJECT_ROOT

from assets._dimensional_builder import build_dimensional_model

PROD_DUCKDB = Path(os.environ.get(
    "DUCKDB_PATH",
    str(PROJECT_ROOT / "data" / "warehouse.duckdb"),
))
PROD_SILVER_GLOB = str(
    PROJECT_ROOT / "data" / "silver" / "production" / "**" / "*.parquet"
).replace("\\", "/")


@asset(
    group_name="gold",
    deps=["silver_tx", "silver_nm"],
    description=(
        "Gold-layer dimensional model. Builds reference dimensions, time "
        "dimensions, core dimensions (lease, well, wellbore, completion, "
        "prod_unit), fact tables (event, bridge, production_detail), and "
        "canonical views. Runs all post-load validations."
    ),
)
def gold_models(context: AssetExecutionContext) -> MaterializeResult:
    """Build all gold-layer dimensional model tables and run validations."""
    PROD_DUCKDB.parent.mkdir(parents=True, exist_ok=True)
    context.log.info("Building dimensional model in %s", PROD_DUCKDB)

    result = build_dimensional_model(
        duckdb_path=PROD_DUCKDB,
        silver_parquet_glob=PROD_SILVER_GLOB,
        log=context.log,
    )

    return MaterializeResult(
        metadata={
            "duckdb_path": MetadataValue.path(str(PROD_DUCKDB)),
            "tables_built": MetadataValue.int(result["tables_built"]),
            "dim_lease": MetadataValue.int(result.get("dim_lease", 0)),
            "dim_well": MetadataValue.int(result.get("dim_well", 0)),
            "fact_event": MetadataValue.int(result.get("fact_event", 0)),
            "fact_production_detail": MetadataValue.int(result.get("fact_production_detail", 0)),
            "validation_errors": MetadataValue.int(len(result["validation_errors"])),
        },
    )
