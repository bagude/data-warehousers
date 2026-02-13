"""Tests for dimensional model validation jobs."""

import duckdb
import pytest

from src.gold.ddl import create_all_tables, seed_reference_dimensions, populate_time_dimensions
from src.gold.etl import load_silver_to_dimensional_model
from src.gold.views import create_canonical_views
from src.gold.validation import run_all_validations


@pytest.fixture
def loaded_db(tmp_path, small_silver_df):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from src.schemas.production import PRODUCTION_SCHEMA

    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    create_all_tables(con)
    seed_reference_dimensions(con)
    populate_time_dimensions(con, start_year=2024, end_year=2025)

    for state in ["TX", "NM"]:
        state_df = small_silver_df[small_silver_df["state"] == state]
        if state_df.empty:
            continue
        out_dir = tmp_path / "silver" / "production" / f"state={state}"
        out_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(state_df, schema=PRODUCTION_SCHEMA)
        pq.write_table(table, out_dir / "data.parquet")

    glob_path = str(tmp_path / "silver" / "production" / "**" / "*.parquet").replace("\\", "/")
    load_silver_to_dimensional_model(con, glob_path)
    create_canonical_views(con)
    yield con
    con.close()


def test_all_validations_pass_on_clean_load(loaded_db):
    errors = run_all_validations(loaded_db)
    assert errors == [], f"Validation errors: {errors}"
