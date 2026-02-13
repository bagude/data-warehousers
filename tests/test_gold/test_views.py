"""Tests for canonical reportable views."""

import duckdb
import pytest

from src.gold.ddl import create_all_tables, seed_reference_dimensions, populate_time_dimensions
from src.gold.etl import load_silver_to_dimensional_model
from src.gold.views import create_canonical_views


@pytest.fixture
def loaded_db(tmp_path, small_silver_df):
    """DuckDB with full dimensional model loaded from test data."""
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


def test_vw_fact_event_current_excludes_superseded(loaded_db):
    count = loaded_db.execute(
        "SELECT COUNT(*) FROM vw_fact_event_current"
    ).fetchone()[0]
    total = loaded_db.execute(
        "SELECT COUNT(*) FROM fact_event"
    ).fetchone()[0]
    assert count == total  # Initial load: all are current


def test_vw_fact_event_reportable_excludes_retracted(loaded_db):
    count = loaded_db.execute(
        "SELECT COUNT(*) FROM vw_fact_event_reportable"
    ).fetchone()[0]
    assert count > 0


def test_vw_production_current_has_measures(loaded_db):
    row = loaded_db.execute(
        "SELECT COUNT(*), SUM(total_oil_bbl) FROM vw_production_current"
    ).fetchone()
    assert row[0] > 0
    assert row[1] is not None and row[1] > 0
