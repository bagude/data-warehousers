"""Bronze-layer assets -- raw data ingestion from TX RRC and NM OCD.

Each asset runs the corresponding ingester and materialises raw files
into ``data/bronze/{state}/{pull_date}/``.

Dependency graph::

    (none) --> bronze_tx
    (none) --> bronze_nm
"""

import datetime

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.ingestion.tx_rrc import TxRrcIngester
from src.ingestion.nm_ocd import NmOcdIngester
from src.utils.config import ensure_data_dirs

daily_partitions = DailyPartitionsDefinition(start_date="2026-01-01")


@asset(
    group_name="bronze",
    partitions_def=daily_partitions,
    description=(
        "Raw TX RRC PDQ CSV dump files. Downloads OG_LEASE_CYCLE, "
        "OG_WELL_COMPLETION, and OG_OPERATOR_DW from the RRC MFT server."
    ),
)
def bronze_tx(context: AssetExecutionContext) -> MaterializeResult:
    """Ingest Texas RRC production data for a given pull date."""
    ensure_data_dirs()

    pull_date = datetime.date.fromisoformat(context.partition_key)
    context.log.info("Starting TX RRC ingestion for %s", pull_date.isoformat())

    with TxRrcIngester(pull_date=pull_date, dagster_log=context.log) as ingester:
        output_dir = ingester.ingest()

    dsv_files = list(output_dir.glob("*.dsv"))
    context.log.info(
        "TX RRC bronze complete: %d DSV files in %s", len(dsv_files), output_dir
    )

    return MaterializeResult(
        metadata={
            "pull_date": MetadataValue.text(pull_date.isoformat()),
            "output_dir": MetadataValue.path(str(output_dir)),
            "file_count": MetadataValue.int(len(dsv_files)),
        },
    )


@asset(
    group_name="bronze",
    partitions_def=daily_partitions,
    description=(
        "Raw NM OCD data. Phase 1: ArcGIS well master JSON batches. "
        "Phase 2: GO-TECH county production ZIP downloads."
    ),
)
def bronze_nm(context: AssetExecutionContext) -> MaterializeResult:
    """Ingest New Mexico OCD production data for a given pull date."""
    ensure_data_dirs()

    pull_date = context.partition_key
    context.log.info("Starting NM OCD ingestion for %s", pull_date)

    with NmOcdIngester(pull_date=pull_date, dagster_log=context.log) as ingester:
        output_dir = ingester.ingest()

    context.log.info("NM OCD bronze complete: %s", output_dir)

    return MaterializeResult(
        metadata={
            "pull_date": MetadataValue.text(pull_date),
            "output_dir": MetadataValue.path(str(output_dir)),
        },
    )
