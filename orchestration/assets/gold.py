"""Gold-layer assets -- build flat gold tables via DuckDB Python API.

Depends on all silver assets.  Builds staging view + gold tables into DuckDB,
then runs data quality tests.

Dependency graph::

    silver_tx  \
                \
    silver_nm  ---> gold_models --> (DuckDB gold tables)
                /
    silver_ok  /
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

from assets._gold_builder import build_gold

PROD_DUCKDB = Path(os.environ.get(
    "DUCKDB_PATH",
    str(PROJECT_ROOT / "data" / "warehouse.duckdb"),
))
PROD_SILVER_GLOB = str(
    PROJECT_ROOT / "data" / "silver" / "production" / "**" / "*.parquet"
).replace("\\", "/")


@asset(
    group_name="gold",
    deps=["silver_tx", "silver_nm", "silver_ok"],
    description=(
        "Gold-layer tables. Builds stg_production view over silver Parquet, "
        "then creates production_monthly, decline_curve_inputs, and "
        "schema_registry tables. Runs all post-build data quality tests."
    ),
)
def gold_models(context: AssetExecutionContext) -> MaterializeResult:
    """Build all gold-layer tables and run data quality tests."""
    PROD_DUCKDB.parent.mkdir(parents=True, exist_ok=True)
    context.log.info("Building gold tables in %s", PROD_DUCKDB)

    result = build_gold(
        duckdb_path=PROD_DUCKDB,
        silver_parquet_glob=PROD_SILVER_GLOB,
        log=context.log,
    )

    return MaterializeResult(
        metadata={
            "duckdb_path": MetadataValue.path(str(PROD_DUCKDB)),
            "stg_rows": MetadataValue.int(result["stg_rows"]),
            "models_built": MetadataValue.int(len(result["models"])),
            "tests_passed": MetadataValue.int(result["tests_passed"]),
            **{
                f"table_{name}": MetadataValue.int(count)
                for name, count in result["models"].items()
            },
        },
    )
