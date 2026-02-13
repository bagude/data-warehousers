"""Integration test for the dimensional builder."""

import pytest
from pathlib import Path

from src.gold.seed_constants import validate_seeds


def test_build_dimensional_model(tmp_path, small_silver_df):
    """Full integration: silver Parquet -> dimensional model -> validations pass."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from src.schemas.production import PRODUCTION_SCHEMA

    # Write silver Parquet
    for state in ["TX", "NM"]:
        state_df = small_silver_df[small_silver_df["state"] == state]
        if state_df.empty:
            continue
        out_dir = tmp_path / "silver" / "production" / f"state={state}"
        out_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(state_df, schema=PRODUCTION_SCHEMA)
        pq.write_table(table, out_dir / "data.parquet")

    glob_path = str(tmp_path / "silver" / "production" / "**" / "*.parquet").replace("\\", "/")
    db_path = tmp_path / "warehouse.duckdb"

    from orchestration.assets._dimensional_builder import build_dimensional_model

    result = build_dimensional_model(
        duckdb_path=db_path,
        silver_parquet_glob=glob_path,
    )

    assert result["tables_built"] > 0
    assert result["validation_errors"] == []
    assert result["dim_lease"] > 0
    assert result["dim_well"] > 0
    assert result["fact_event"] > 0
    assert result["fact_production_detail"] > 0
