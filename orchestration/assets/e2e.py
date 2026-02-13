"""E2E test assets -- minimal seed data through the full pipeline.

Mirrors the production bronze -> silver -> gold flow but:
- Bronze: copies local fixture files (no network calls)
- Silver: reuses existing parser logic with overridden paths
- Gold: builds full dimensional model via DuckDB Python API

All artifacts go to data/e2e/ to avoid polluting production data.
"""

import shutil
from pathlib import Path

import pyarrow.parquet as pq
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.transforms.base import BaseParser
from src.transforms.tx_parser import TxParser
from src.transforms.nm_parser import NmParser
from src.utils.config import PROJECT_ROOT

from assets._dimensional_builder import build_dimensional_model

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures" / "e2e"
E2E_DATA_DIR = PROJECT_ROOT / "data" / "e2e"
E2E_BRONZE_TX = E2E_DATA_DIR / "bronze" / "tx" / "seed"
E2E_BRONZE_NM = E2E_DATA_DIR / "bronze" / "nm"
E2E_SILVER_DIR = E2E_DATA_DIR / "silver" / "production"
E2E_DUCKDB = E2E_DATA_DIR / "warehouse.duckdb"


# ---------------------------------------------------------------------------
# E2E parser subclasses (override paths, reuse all parsing logic)
# ---------------------------------------------------------------------------

class _E2ETxParser(TxParser):
    """TxParser with overridden input/output paths for E2E testing."""

    def __init__(self, *, pull_dir: Path, output_dir: Path, **kwargs) -> None:
        BaseParser.__init__(
            self,
            input_dir=pull_dir.parent,
            output_dir=output_dir,
            **kwargs,
        )
        self._pull_dir = pull_dir


class _E2ENmParser(NmParser):
    """NmParser with overridden input/output paths for E2E testing."""

    def __init__(
        self, *, pull_date: str, input_dir: Path, output_dir: Path, **kwargs
    ) -> None:
        BaseParser.__init__(
            self,
            input_dir=input_dir,
            output_dir=output_dir,
            **kwargs,
        )
        self.pull_date = pull_date
        self.run_dir = self.input_dir / self.pull_date


# ---------------------------------------------------------------------------
# Bronze assets -- copy fixtures into E2E data directory
# ---------------------------------------------------------------------------

@asset(
    group_name="test_bronze",
    description="Copy TX seed fixtures into E2E bronze directory.",
)
def test_bronze_tx(context: AssetExecutionContext) -> MaterializeResult:
    src = FIXTURES_DIR / "tx"

    E2E_BRONZE_TX.parent.mkdir(parents=True, exist_ok=True)
    if E2E_BRONZE_TX.exists():
        shutil.rmtree(E2E_BRONZE_TX)
    shutil.copytree(src, E2E_BRONZE_TX)

    files = list(E2E_BRONZE_TX.glob("*"))
    context.log.info("Copied %d TX fixture files to %s", len(files), E2E_BRONZE_TX)

    return MaterializeResult(
        metadata={
            "output_dir": MetadataValue.path(str(E2E_BRONZE_TX)),
            "file_count": MetadataValue.int(len(files)),
        },
    )


@asset(
    group_name="test_bronze",
    description="Copy NM seed fixtures into E2E bronze directory.",
)
def test_bronze_nm(context: AssetExecutionContext) -> MaterializeResult:
    src = FIXTURES_DIR / "nm"
    dest = E2E_BRONZE_NM / "seed"

    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest)

    context.log.info("Copied NM fixtures to %s", dest)

    return MaterializeResult(
        metadata={
            "output_dir": MetadataValue.path(str(dest)),
        },
    )


# ---------------------------------------------------------------------------
# Silver assets -- parse from E2E bronze to E2E silver
# ---------------------------------------------------------------------------

@asset(
    group_name="test_silver",
    deps=["test_bronze_tx"],
    description="Parse TX seed data from E2E bronze to silver Parquet.",
)
def test_silver_tx(context: AssetExecutionContext) -> MaterializeResult:
    parser = _E2ETxParser(
        pull_dir=E2E_BRONZE_TX,
        output_dir=E2E_SILVER_DIR,
        dagster_log=context.log,
    )
    output_path = parser.run()

    row_count = pq.read_metadata(output_path).num_rows if output_path.exists() else 0
    context.log.info("TX E2E silver: %s (%d rows)", output_path, row_count)

    return MaterializeResult(
        metadata={
            "output_path": MetadataValue.path(str(output_path)),
            "row_count": MetadataValue.int(row_count),
        },
    )


@asset(
    group_name="test_silver",
    deps=["test_bronze_nm"],
    description="Parse NM seed data from E2E bronze to silver Parquet.",
)
def test_silver_nm(context: AssetExecutionContext) -> MaterializeResult:
    parser = _E2ENmParser(
        pull_date="seed",
        input_dir=E2E_BRONZE_NM,
        output_dir=E2E_SILVER_DIR / "state=NM",
        dagster_log=context.log,
    )
    df = parser.parse()

    output_path = parser.output_dir / "nm_production.parquet"
    context.log.info("NM E2E silver: %s (%d rows)", output_path, len(df))

    return MaterializeResult(
        metadata={
            "output_path": MetadataValue.path(str(output_path)),
            "row_count": MetadataValue.int(len(df)),
        },
    )


# ---------------------------------------------------------------------------
# Gold asset -- build dimensional model via shared _dimensional_builder
# ---------------------------------------------------------------------------

@asset(
    group_name="test_gold",
    deps=["test_silver_tx", "test_silver_nm"],
    description=(
        "Build dimensional model and run validations via DuckDB Python API "
        "against E2E silver data. Isolated DuckDB at data/e2e/warehouse.duckdb."
    ),
)
def test_gold_models(context: AssetExecutionContext) -> MaterializeResult:
    # Clean previous DuckDB state for repeatable runs.
    for f in [E2E_DUCKDB, E2E_DUCKDB.with_suffix(".duckdb.wal")]:
        if f.exists():
            f.unlink()
    E2E_DUCKDB.parent.mkdir(parents=True, exist_ok=True)

    silver_glob = str(E2E_SILVER_DIR / "**" / "*.parquet").replace("\\", "/")

    result = build_dimensional_model(
        duckdb_path=E2E_DUCKDB,
        silver_parquet_glob=silver_glob,
        log=context.log,
    )

    return MaterializeResult(
        metadata={
            "duckdb_path": MetadataValue.path(str(E2E_DUCKDB)),
            "tables_built": MetadataValue.int(result["tables_built"]),
            "dim_lease": MetadataValue.int(result.get("dim_lease", 0)),
            "dim_well": MetadataValue.int(result.get("dim_well", 0)),
            "fact_event": MetadataValue.int(result.get("fact_event", 0)),
            "fact_production_detail": MetadataValue.int(result.get("fact_production_detail", 0)),
            "validation_errors": MetadataValue.int(len(result["validation_errors"])),
        },
    )
