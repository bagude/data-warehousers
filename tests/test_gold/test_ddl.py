"""Tests for DDL execution and seed population."""

import duckdb
import pytest

from src.gold.ddl import create_all_tables, seed_reference_dimensions
from src.gold.seed_constants import validate_seeds


@pytest.fixture
def fresh_db():
    """In-memory DuckDB for testing."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


def test_create_all_tables_creates_expected_tables(fresh_db):
    create_all_tables(fresh_db)
    tables = {
        row[0]
        for row in fresh_db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    expected = {
        # Reference dims
        "dim_object_role", "dim_object_type", "dim_well_type",
        "dim_well_status", "dim_completion_status", "dim_event_status",
        # Time dims
        "dim_date", "dim_month",
        # Analytics dims
        "dim_event_type", "dim_source_system",
        # Config
        "event_type_object_rules",
        # Core dims
        "dim_lease", "dim_well", "dim_wellbore", "dim_completion", "dim_prod_unit",
        # Facts
        "fact_event", "fact_event_object_bridge",
        "fact_production_detail", "fact_drilling_detail", "fact_completion_detail",
        # Audit
        "_mapping_exceptions",
    }
    assert expected.issubset(tables), f"Missing tables: {expected - tables}"


def test_seed_reference_dimensions_populates_all_seeds(fresh_db):
    create_all_tables(fresh_db)
    seed_reference_dimensions(fresh_db)

    # Check row counts for key reference tables
    assert fresh_db.execute("SELECT COUNT(*) FROM dim_object_role").fetchone()[0] == 4
    assert fresh_db.execute("SELECT COUNT(*) FROM dim_object_type").fetchone()[0] == 5
    assert fresh_db.execute("SELECT COUNT(*) FROM dim_event_type").fetchone()[0] >= 4
    assert fresh_db.execute("SELECT COUNT(*) FROM dim_event_status").fetchone()[0] >= 4
    assert fresh_db.execute("SELECT COUNT(*) FROM dim_well_type").fetchone()[0] >= 8
    assert fresh_db.execute("SELECT COUNT(*) FROM dim_well_status").fetchone()[0] >= 8
    assert fresh_db.execute("SELECT COUNT(*) FROM dim_completion_status").fetchone()[0] >= 8
    assert fresh_db.execute("SELECT COUNT(*) FROM dim_source_system").fetchone()[0] >= 4


def test_seed_constants_validate_after_seeding(fresh_db):
    create_all_tables(fresh_db)
    seed_reference_dimensions(fresh_db)
    errors = validate_seeds(fresh_db)
    assert errors == [], f"Seed validation errors: {errors}"


def test_dim_date_generates_range(fresh_db):
    create_all_tables(fresh_db)
    from src.gold.ddl import populate_time_dimensions
    populate_time_dimensions(fresh_db, start_year=2020, end_year=2026)
    count = fresh_db.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
    # ~2557 days from 2020-01-01 to 2026-12-31
    assert count > 2500


def test_dim_month_generates_range(fresh_db):
    create_all_tables(fresh_db)
    from src.gold.ddl import populate_time_dimensions
    populate_time_dimensions(fresh_db, start_year=2020, end_year=2026)
    count = fresh_db.execute("SELECT COUNT(*) FROM dim_month").fetchone()[0]
    # 84 months from 2020-01 to 2026-12
    assert count == 84
