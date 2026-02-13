"""Silver-layer assets -- bronze-to-silver transforms.

Each asset depends on its corresponding bronze asset and runs the
state-specific parser to produce Parquet files in
``data/silver/production/``.

Dependency graph::

    bronze_tx --> silver_tx
    bronze_nm --> silver_nm
"""

from pathlib import Path

import pyarrow.parquet as pq
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.transforms.tx_parser import TxParser
from src.transforms.nm_parser import NmParser

daily_partitions = DailyPartitionsDefinition(start_date="2026-01-01")


def _parquet_row_count(path: Path) -> int:
    """Read row count from Parquet metadata without loading data."""
    if not path.exists():
        return 0
    return pq.read_metadata(path).num_rows


@asset(
    group_name="silver",
    deps=["bronze_tx"],
    partitions_def=daily_partitions,
    description=(
        "Silver-layer TX production Parquet. Joins OG_LEASE_CYCLE with "
        "OG_WELL_COMPLETION and OG_OPERATOR_DW, maps to canonical schema, "
        "validates, and deduplicates."
    ),
)
def silver_tx(context: AssetExecutionContext) -> MaterializeResult:
    """Parse TX RRC bronze CSVs into silver Parquet."""
    context.log.info("Starting TX silver transform")

    parser = TxParser(dagster_log=context.log)
    output_path = parser.run()

    row_count = _parquet_row_count(output_path)
    context.log.info("TX silver output: %s (%d rows)", output_path, row_count)

    return MaterializeResult(
        metadata={
            "output_path": MetadataValue.path(str(output_path)),
            "row_count": MetadataValue.int(row_count),
        },
    )


@asset(
    group_name="silver",
    deps=["bronze_nm"],
    partitions_def=daily_partitions,
    description=(
        "Silver-layer NM production Parquet. Joins ArcGIS well master with "
        "GO-TECH county production CSVs, maps to canonical schema, "
        "validates, and deduplicates."
    ),
)
def silver_nm(context: AssetExecutionContext) -> MaterializeResult:
    """Parse NM OCD bronze data into silver Parquet."""
    pull_date = context.partition_key
    context.log.info("Starting NM silver transform for pull_date=%s", pull_date)

    parser = NmParser(pull_date=pull_date, dagster_log=context.log)
    df = parser.parse()

    output_path = parser.output_dir / "nm_production.parquet"
    context.log.info("NM silver output: %s (%d rows)", output_path, len(df))

    return MaterializeResult(
        metadata={
            "pull_date": MetadataValue.text(pull_date),
            "output_path": MetadataValue.path(str(output_path)),
            "row_count": MetadataValue.int(len(df)),
        },
    )
