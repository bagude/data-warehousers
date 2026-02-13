"""Job definitions for the og-data-warehouse pipeline.

NOTE: This module is imported by definitions.py which adds the dagster/
directory to sys.path before importing.

Two jobs:
- **full_refresh_job**: Materialises all assets (bronze -> silver -> gold).
- **incremental_job**: Materialises only bronze + silver for the latest
  partition, then gold.
"""

from __future__ import annotations

from dagster import AssetSelection, define_asset_job

# -- Asset group selections ----------------------------------------------------

bronze_selection = AssetSelection.groups("bronze")
silver_selection = AssetSelection.groups("silver")
gold_selection = AssetSelection.groups("gold")

all_assets = bronze_selection | silver_selection | gold_selection

# -- Jobs ----------------------------------------------------------------------

full_refresh_job = define_asset_job(
    name="full_refresh",
    selection=all_assets,
    description=(
        "Full pipeline refresh: ingest raw data (bronze), transform to "
        "silver Parquet, and rebuild all gold dbt models."
    ),
)

incremental_job = define_asset_job(
    name="incremental",
    selection=all_assets,
    description=(
        "Incremental pipeline run: ingest latest data (bronze), "
        "transform to silver, and rebuild gold dbt models."
    ),
)

# -- E2E test asset group selections -------------------------------------------

test_bronze_selection = AssetSelection.groups("test_bronze")
test_silver_selection = AssetSelection.groups("test_silver")
test_gold_selection = AssetSelection.groups("test_gold")

test_all_assets = test_bronze_selection | test_silver_selection | test_gold_selection

# -- E2E test jobs -------------------------------------------------------------

test_full_pipeline = define_asset_job(
    name="test_full_pipeline",
    selection=test_all_assets,
    description=(
        "E2E test: full pipeline with minimal seed data. "
        "Copies TX and NM fixtures to bronze, parses silver, "
        "builds and tests all gold dbt models."
    ),
)

test_incremental_pipeline = define_asset_job(
    name="test_incremental_pipeline",
    selection=test_silver_selection | test_gold_selection,
    description=(
        "E2E test: incremental rebuild. Re-parses silver from "
        "existing bronze fixtures and rebuilds all gold models. "
        "Validates pipeline idempotency."
    ),
)
