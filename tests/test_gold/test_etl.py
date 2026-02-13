"""Tests for silver-to-dimensional-model ETL."""

import duckdb
import pytest
from pathlib import Path

from src.gold.ddl import create_all_tables, seed_reference_dimensions, populate_time_dimensions
from src.gold.etl import load_silver_to_dimensional_model


@pytest.fixture
def dimensional_db(tmp_path):
    """In-memory DuckDB with schema + seeds + time dims."""
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    create_all_tables(con)
    seed_reference_dimensions(con)
    populate_time_dimensions(con, start_year=2024, end_year=2025)
    yield con
    con.close()


@pytest.fixture
def silver_parquet(tmp_path, small_silver_df):
    """Write small_silver_df fixture to Parquet for DuckDB to read."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from src.schemas.production import PRODUCTION_SCHEMA

    # Write hive-partitioned by state
    for state in ["TX", "NM"]:
        state_df = small_silver_df[small_silver_df["state"] == state]
        if state_df.empty:
            continue
        out_dir = tmp_path / "silver" / "production" / f"state={state}"
        out_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(state_df, schema=PRODUCTION_SCHEMA)
        pq.write_table(table, out_dir / "data.parquet")

    glob_path = str(tmp_path / "silver" / "production" / "**" / "*.parquet").replace("\\", "/")
    return glob_path


def test_etl_populates_dim_lease(dimensional_db, silver_parquet):
    load_silver_to_dimensional_model(dimensional_db, silver_parquet)
    count = dimensional_db.execute("SELECT COUNT(*) FROM dim_lease").fetchone()[0]
    assert count > 0, "DIM_LEASE should have rows after ETL"


def test_etl_populates_dim_well(dimensional_db, silver_parquet):
    load_silver_to_dimensional_model(dimensional_db, silver_parquet)
    count = dimensional_db.execute(
        "SELECT COUNT(*) FROM dim_well WHERE is_current = TRUE"
    ).fetchone()[0]
    assert count > 0, "DIM_WELL should have current rows after ETL"


def test_etl_populates_fact_event(dimensional_db, silver_parquet):
    load_silver_to_dimensional_model(dimensional_db, silver_parquet)
    count = dimensional_db.execute(
        "SELECT COUNT(*) FROM fact_event WHERE is_current_version = TRUE"
    ).fetchone()[0]
    assert count > 0, "FACT_EVENT should have current rows"


def test_etl_populates_production_detail(dimensional_db, silver_parquet):
    load_silver_to_dimensional_model(dimensional_db, silver_parquet)
    count = dimensional_db.execute(
        "SELECT COUNT(*) FROM fact_production_detail"
    ).fetchone()[0]
    assert count > 0, "FACT_PRODUCTION_DETAIL should have rows"


def test_etl_bridge_has_primary_per_event(dimensional_db, silver_parquet):
    load_silver_to_dimensional_model(dimensional_db, silver_parquet)
    # Every event should have exactly one PRIMARY bridge row
    violations = dimensional_db.execute("""
        SELECT fe.event_id, COUNT(*) AS primary_count
        FROM fact_event fe
        LEFT JOIN fact_event_object_bridge b
            ON fe.event_id = b.event_id AND b.object_role_key = 1
        WHERE fe.is_current_version = TRUE
        GROUP BY fe.event_id
        HAVING COUNT(*) != 1
    """).fetchall()
    assert len(violations) == 0, f"Events without exactly 1 PRIMARY: {violations}"


def test_etl_synthetic_leases_for_nm(dimensional_db, silver_parquet):
    load_silver_to_dimensional_model(dimensional_db, silver_parquet)
    nm_leases = dimensional_db.execute(
        "SELECT COUNT(*) FROM dim_lease WHERE state = 'NM' AND is_synthetic = TRUE"
    ).fetchone()[0]
    assert nm_leases > 0, "NM wells should have synthetic leases"


def test_etl_no_orphan_events(dimensional_db, silver_parquet):
    load_silver_to_dimensional_model(dimensional_db, silver_parquet)
    # Every production detail should have a matching event
    orphans = dimensional_db.execute("""
        SELECT pd.event_id FROM fact_production_detail pd
        LEFT JOIN fact_event fe ON pd.event_id = fe.event_id
        WHERE fe.event_id IS NULL
    """).fetchall()
    assert len(orphans) == 0
