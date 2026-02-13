# Gold Dimensional Model Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the current flat gold aggregation tables with a digital twin dimensional model (22 tables + 3 canonical views) populated from existing silver Parquet data.

**Architecture:** New `src/gold/` package containing seed constants, DDL definitions, ETL mapping logic, and validation queries. The existing `_gold_builder.py` is replaced by a new `dimensional_builder.py` that builds the full dimensional model. The existing gold tables are preserved as backward-compatible views sourced from the new model. DuckDB remains the warehouse engine.

**Tech Stack:** Python 3.11+, DuckDB, PyArrow/Pandas (silver reads), pytest, Dagster (orchestration)

---

## Implementation Sequence

The build is ordered by dependency:

1. Seed constants + reference dimensions (no dependencies)
2. Time dimensions (no dependencies)
3. Core object graph dimensions (depend on reference dims)
4. DIM_PROD_UNIT (depends on core dims)
5. Analytics layer: FACT_EVENT + bridge (depends on everything above)
6. Detail facts: FACT_PRODUCTION_DETAIL (depends on FACT_EVENT)
7. Canonical views (depend on facts)
8. Validation jobs (depend on everything)
9. Backward-compatible legacy views (depend on canonical views)
10. Dagster asset + E2E integration
11. Reconciliation updates

---

### Task 1: Create src/gold/ Package with Seed Constants

**Files:**
- Create: `src/gold/__init__.py`
- Create: `src/gold/seed_constants.py`
- Test: `tests/test_gold/__init__.py`
- Test: `tests/test_gold/test_seed_constants.py`

**Step 1: Write the failing test**

```python
# tests/test_gold/__init__.py
# (empty)

# tests/test_gold/test_seed_constants.py
"""Tests for seed constants and bootstrap validation."""

from src.gold.seed_constants import SEED_CONSTANTS, validate_seeds

import duckdb
import pytest


def test_seed_constants_has_all_required_keys():
    required = [
        "PRIMARY_ROLE_KEY", "ASSOCIATED_ROLE_KEY", "PARENT_ROLE_KEY",
        "PROD_UNIT_TYPE_KEY", "LEASE_TYPE_KEY", "WELL_TYPE_KEY",
        "WELLBORE_TYPE_KEY", "COMPLETION_TYPE_KEY",
        "PRODUCTION_EVENT_KEY", "STATUS_CHANGE_EVENT_KEY",
        "REPORTED_STATUS_KEY", "AMENDED_STATUS_KEY", "RETRACTED_STATUS_KEY",
    ]
    for key in required:
        assert key in SEED_CONSTANTS, f"Missing seed constant: {key}"


def test_seed_constants_values_are_positive_ints():
    for key, val in SEED_CONSTANTS.items():
        assert isinstance(val, int) and val > 0, f"{key} must be positive int, got {val}"


def test_seed_constants_no_duplicate_values_within_dimension():
    """Keys sharing a dimension prefix must have unique values."""
    from collections import defaultdict
    dims = defaultdict(list)
    for key, val in SEED_CONSTANTS.items():
        # Group by prefix: e.g., PRIMARY_ROLE_KEY -> ROLE
        parts = key.rsplit("_", 2)
        if len(parts) >= 2:
            dim_suffix = parts[-2]  # ROLE, TYPE, EVENT, STATUS
            dims[dim_suffix].append((key, val))
    for dim, entries in dims.items():
        vals = [v for _, v in entries]
        assert len(vals) == len(set(vals)), f"Duplicate values in {dim}: {entries}"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_gold/test_seed_constants.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.gold'`

**Step 3: Write minimal implementation**

```python
# src/gold/__init__.py
"""Gold dimensional model package."""

# src/gold/seed_constants.py
"""Seed constants for reference dimension surrogate keys.

These values are baked into DDL (filtered unique indexes) and validation
queries.  They MUST match the surrogate keys assigned during seed inserts.
Bootstrap validation (validate_seeds) asserts correctness at runtime.

IMPORTANT: If you reorder or renumber seed inserts, update this file and
re-run bootstrap validation.  These are versioned, not discovered.
"""

from __future__ import annotations

# -- DIM_OBJECT_ROLE --
PRIMARY_ROLE_KEY = 1
ASSOCIATED_ROLE_KEY = 2
PARENT_ROLE_KEY = 3
CHILD_ROLE_KEY = 4

# -- DIM_OBJECT_TYPE --
PROD_UNIT_TYPE_KEY = 1
LEASE_TYPE_KEY = 2
WELL_TYPE_KEY = 3
WELLBORE_TYPE_KEY = 4
COMPLETION_TYPE_KEY = 5

# -- DIM_EVENT_TYPE --
PRODUCTION_EVENT_KEY = 1
STATUS_CHANGE_EVENT_KEY = 2
DRILLING_EVENT_KEY = 3
COMPLETION_JOB_EVENT_KEY = 4

# -- DIM_EVENT_STATUS --
REPORTED_STATUS_KEY = 1
AMENDED_STATUS_KEY = 2
RETRACTED_STATUS_KEY = 3
PROVISIONAL_STATUS_KEY = 4

# -- Collected dict for iteration/validation --
SEED_CONSTANTS: dict[str, int] = {
    "PRIMARY_ROLE_KEY": PRIMARY_ROLE_KEY,
    "ASSOCIATED_ROLE_KEY": ASSOCIATED_ROLE_KEY,
    "PARENT_ROLE_KEY": PARENT_ROLE_KEY,
    "CHILD_ROLE_KEY": CHILD_ROLE_KEY,
    "PROD_UNIT_TYPE_KEY": PROD_UNIT_TYPE_KEY,
    "LEASE_TYPE_KEY": LEASE_TYPE_KEY,
    "WELL_TYPE_KEY": WELL_TYPE_KEY,
    "WELLBORE_TYPE_KEY": WELLBORE_TYPE_KEY,
    "COMPLETION_TYPE_KEY": COMPLETION_TYPE_KEY,
    "PRODUCTION_EVENT_KEY": PRODUCTION_EVENT_KEY,
    "STATUS_CHANGE_EVENT_KEY": STATUS_CHANGE_EVENT_KEY,
    "DRILLING_EVENT_KEY": DRILLING_EVENT_KEY,
    "COMPLETION_JOB_EVENT_KEY": COMPLETION_JOB_EVENT_KEY,
    "REPORTED_STATUS_KEY": REPORTED_STATUS_KEY,
    "AMENDED_STATUS_KEY": AMENDED_STATUS_KEY,
    "RETRACTED_STATUS_KEY": RETRACTED_STATUS_KEY,
    "PROVISIONAL_STATUS_KEY": PROVISIONAL_STATUS_KEY,
}

# -- Mapping: constant name -> (table, code_column, code_value) --
_SEED_VALIDATION_MAP: dict[str, tuple[str, str, str]] = {
    "PRIMARY_ROLE_KEY": ("dim_object_role", "object_role_code", "PRIMARY"),
    "ASSOCIATED_ROLE_KEY": ("dim_object_role", "object_role_code", "ASSOCIATED"),
    "PARENT_ROLE_KEY": ("dim_object_role", "object_role_code", "PARENT"),
    "CHILD_ROLE_KEY": ("dim_object_role", "object_role_code", "CHILD"),
    "PROD_UNIT_TYPE_KEY": ("dim_object_type", "object_type_code", "PROD_UNIT"),
    "LEASE_TYPE_KEY": ("dim_object_type", "object_type_code", "LEASE"),
    "WELL_TYPE_KEY": ("dim_object_type", "object_type_code", "WELL"),
    "WELLBORE_TYPE_KEY": ("dim_object_type", "object_type_code", "WELLBORE"),
    "COMPLETION_TYPE_KEY": ("dim_object_type", "object_type_code", "COMPLETION"),
    "PRODUCTION_EVENT_KEY": ("dim_event_type", "event_type_code", "PRODUCTION"),
    "STATUS_CHANGE_EVENT_KEY": ("dim_event_type", "event_type_code", "STATUS_CHANGE"),
    "DRILLING_EVENT_KEY": ("dim_event_type", "event_type_code", "DRILLING"),
    "COMPLETION_JOB_EVENT_KEY": ("dim_event_type", "event_type_code", "COMPLETION_JOB"),
    "REPORTED_STATUS_KEY": ("dim_event_status", "event_status_code", "REPORTED"),
    "AMENDED_STATUS_KEY": ("dim_event_status", "event_status_code", "AMENDED"),
    "RETRACTED_STATUS_KEY": ("dim_event_status", "event_status_code", "RETRACTED"),
    "PROVISIONAL_STATUS_KEY": ("dim_event_status", "event_status_code", "PROVISIONAL"),
}


def validate_seeds(con) -> list[str]:
    """Assert that seeded surrogate keys match SEED_CONSTANTS.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection with seed tables populated.

    Returns
    -------
    List of error messages.  Empty list = all valid.
    """
    errors: list[str] = []
    for const_name, (table, code_col, code_val) in _SEED_VALIDATION_MAP.items():
        expected_key = SEED_CONSTANTS[const_name]
        pk_col = table.replace("dim_", "") + "_key"  # e.g., dim_object_role -> object_role_key
        try:
            row = con.execute(
                f"SELECT {pk_col} FROM {table} WHERE {code_col} = ?",
                [code_val],
            ).fetchone()
        except Exception as exc:
            errors.append(f"{const_name}: query failed on {table}: {exc}")
            continue

        if row is None:
            errors.append(f"{const_name}: {code_val} not found in {table}")
        elif row[0] != expected_key:
            errors.append(
                f"{const_name}: expected key {expected_key}, "
                f"got {row[0]} for {code_val} in {table}"
            )
    return errors
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_gold/test_seed_constants.py -v`
Expected: PASS (3 tests)

**Step 5: Commit**

```bash
git add src/gold/__init__.py src/gold/seed_constants.py tests/test_gold/__init__.py tests/test_gold/test_seed_constants.py
git commit -m "feat(gold): add seed constants and bootstrap validation for dimensional model"
```

---

### Task 2: Create DDL Definitions — Reference Dimensions + Time Dimensions

**Files:**
- Create: `src/gold/ddl.py`
- Test: `tests/test_gold/test_ddl.py`

**Step 1: Write the failing test**

```python
# tests/test_gold/test_ddl.py
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_gold/test_ddl.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.gold.ddl'`

**Step 3: Write implementation**

```python
# src/gold/ddl.py
"""DDL definitions and seed data for the gold dimensional model.

All tables are created in DuckDB via CREATE TABLE/VIEW statements.
Reference dimensions are seeded with controlled vocabularies.
Time dimensions are generated from a date range.

The surrogate key assignments for reference dimensions MUST match
the values in seed_constants.py.  Bootstrap validation (validate_seeds)
checks this at runtime.
"""

from __future__ import annotations

from typing import Any


# ===================================================================
# DDL: CREATE TABLE statements (dependency-ordered)
# ===================================================================

_DDL_STATEMENTS: list[str] = [
    # -- Reference dimensions --
    """
    CREATE TABLE IF NOT EXISTS dim_object_role (
        object_role_key INTEGER PRIMARY KEY,
        object_role_code VARCHAR NOT NULL UNIQUE,
        object_role_name VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_object_type (
        object_type_key INTEGER PRIMARY KEY,
        object_type_code VARCHAR NOT NULL UNIQUE,
        object_type_name VARCHAR NOT NULL,
        dimension_table VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_well_type (
        well_type_key INTEGER PRIMARY KEY,
        well_type_code VARCHAR NOT NULL UNIQUE,
        well_type_name VARCHAR NOT NULL,
        is_producing BOOLEAN NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_well_status (
        well_status_key INTEGER PRIMARY KEY,
        well_status_code VARCHAR NOT NULL UNIQUE,
        well_status_name VARCHAR NOT NULL,
        is_active BOOLEAN NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_completion_status (
        completion_status_key INTEGER PRIMARY KEY,
        completion_status_code VARCHAR NOT NULL UNIQUE,
        completion_status_name VARCHAR NOT NULL,
        is_producing BOOLEAN NOT NULL,
        lifecycle_stage VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_event_status (
        event_status_key INTEGER PRIMARY KEY,
        event_status_code VARCHAR NOT NULL UNIQUE,
        event_status_name VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_event_type (
        event_type_key INTEGER PRIMARY KEY,
        event_type_code VARCHAR NOT NULL UNIQUE,
        event_type_name VARCHAR NOT NULL,
        event_category VARCHAR NOT NULL,
        is_active BOOLEAN NOT NULL,
        rules_complete BOOLEAN NOT NULL DEFAULT FALSE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_source_system (
        source_system_key INTEGER PRIMARY KEY,
        source_code VARCHAR NOT NULL UNIQUE,
        source_name VARCHAR NOT NULL,
        source_url VARCHAR,
        refresh_cadence VARCHAR
    )
    """,
    # -- Time dimensions --
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INTEGER PRIMARY KEY,
        full_date DATE NOT NULL UNIQUE,
        year INTEGER NOT NULL,
        quarter INTEGER NOT NULL,
        month INTEGER NOT NULL,
        month_name VARCHAR NOT NULL,
        day_of_month INTEGER NOT NULL,
        is_month_start BOOLEAN NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_month (
        month_key INTEGER PRIMARY KEY,
        month_start_date DATE NOT NULL UNIQUE,
        month_end_date DATE NOT NULL,
        year INTEGER NOT NULL,
        quarter INTEGER NOT NULL,
        month INTEGER NOT NULL,
        month_name VARCHAR NOT NULL
    )
    """,
    # -- Config table --
    """
    CREATE TABLE IF NOT EXISTS event_type_object_rules (
        rule_id INTEGER PRIMARY KEY,
        event_type_key INTEGER NOT NULL REFERENCES dim_event_type(event_type_key),
        rule_variant VARCHAR,
        object_type_key INTEGER NOT NULL REFERENCES dim_object_type(object_type_key),
        is_primary BOOLEAN NOT NULL,
        is_required BOOLEAN NOT NULL,
        cardinality VARCHAR NOT NULL,
        CHECK (rule_variant IS NULL OR LENGTH(rule_variant) > 0),
        CHECK (cardinality IN ('EXACTLY_ONE', 'ZERO_OR_ONE', 'ZERO_OR_MANY', 'NOT_APPLICABLE'))
    )
    """,
    # -- Core dimensions --
    """
    CREATE TABLE IF NOT EXISTS dim_lease (
        lease_entity_key INTEGER PRIMARY KEY,
        lease_id VARCHAR NOT NULL UNIQUE,
        state VARCHAR(2) NOT NULL,
        lease_number VARCHAR,
        district VARCHAR,
        lease_name VARCHAR,
        county VARCHAR,
        basin VARCHAR,
        is_synthetic BOOLEAN NOT NULL,
        lease_grain_type VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CHECK (lease_grain_type IN ('REAL_LEASE', 'SYNTHETIC_CONTAINER'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_well (
        well_version_key INTEGER PRIMARY KEY,
        well_entity_key INTEGER NOT NULL,
        well_id VARCHAR NOT NULL,
        lease_entity_key INTEGER NOT NULL REFERENCES dim_lease(lease_entity_key),
        api_number VARCHAR NOT NULL,
        state VARCHAR(2) NOT NULL,
        well_name VARCHAR,
        well_number VARCHAR,
        operator VARCHAR,
        operator_id VARCHAR,
        county VARCHAR,
        field_name VARCHAR,
        basin VARCHAR,
        well_type_key INTEGER REFERENCES dim_well_type(well_type_key),
        well_status_key INTEGER REFERENCES dim_well_status(well_status_key),
        latitude DOUBLE,
        longitude DOUBLE,
        spud_date DATE,
        completion_date DATE,
        total_depth_ft DOUBLE,
        valid_from TIMESTAMP NOT NULL,
        valid_to TIMESTAMP,
        is_current BOOLEAN NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_wellbore (
        wellbore_entity_key INTEGER PRIMARY KEY,
        well_entity_key INTEGER NOT NULL,
        wellbore_id VARCHAR NOT NULL UNIQUE,
        wellbore_type VARCHAR,
        kickoff_depth_ft DOUBLE,
        total_depth_ft DOUBLE,
        true_vertical_depth_ft DOUBLE,
        azimuth_deg DOUBLE,
        inclination_deg DOUBLE,
        target_formation VARCHAR,
        is_horizontal BOOLEAN,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_completion (
        completion_version_key INTEGER PRIMARY KEY,
        completion_entity_key INTEGER NOT NULL,
        wellbore_entity_key INTEGER NOT NULL REFERENCES dim_wellbore(wellbore_entity_key),
        completion_id VARCHAR NOT NULL,
        completion_type VARCHAR,
        producing_formation VARCHAR,
        completion_status_key INTEGER REFERENCES dim_completion_status(completion_status_key),
        valid_from DATE NOT NULL,
        valid_to DATE,
        is_current BOOLEAN NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_prod_unit (
        prod_unit_key INTEGER PRIMARY KEY,
        prod_unit_id VARCHAR NOT NULL UNIQUE,
        prod_unit_type VARCHAR NOT NULL,
        completion_entity_key INTEGER,
        lease_entity_key INTEGER REFERENCES dim_lease(lease_entity_key),
        state VARCHAR(2) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CHECK (prod_unit_type IN ('COMPLETION', 'LEASE_COMBINED'))
    )
    """,
    # -- Fact tables --
    """
    CREATE TABLE IF NOT EXISTS fact_event (
        event_id BIGINT PRIMARY KEY,
        natural_event_id VARCHAR NOT NULL,
        event_version INTEGER NOT NULL DEFAULT 1,
        event_hash VARCHAR(64) NOT NULL,
        event_type_key INTEGER NOT NULL REFERENCES dim_event_type(event_type_key),
        event_date_key INTEGER REFERENCES dim_date(date_key),
        month_key INTEGER REFERENCES dim_month(month_key),
        event_ts TIMESTAMP NOT NULL,
        source_system_key INTEGER NOT NULL REFERENCES dim_source_system(source_system_key),
        event_status_key INTEGER NOT NULL REFERENCES dim_event_status(event_status_key),
        is_current_version BOOLEAN NOT NULL DEFAULT TRUE,
        superseded_at TIMESTAMP,
        source_file VARCHAR,
        ingested_at TIMESTAMP NOT NULL,
        CHECK ((month_key IS NULL) != (event_date_key IS NULL))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_event_object_bridge (
        event_id BIGINT NOT NULL REFERENCES fact_event(event_id),
        object_type_key INTEGER NOT NULL REFERENCES dim_object_type(object_type_key),
        object_key INTEGER NOT NULL,
        object_role_key INTEGER NOT NULL REFERENCES dim_object_role(object_role_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_production_detail (
        event_id BIGINT PRIMARY KEY REFERENCES fact_event(event_id),
        oil_bbl DOUBLE,
        gas_mcf DOUBLE,
        condensate_bbl DOUBLE,
        casinghead_gas_mcf DOUBLE,
        water_bbl DOUBLE,
        total_oil_bbl DOUBLE,
        total_gas_mcf DOUBLE,
        days_produced INTEGER,
        oil_rate_bbl_per_day DOUBLE,
        gas_rate_mcf_per_day DOUBLE,
        gor DOUBLE,
        water_cut_pct DOUBLE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_drilling_detail (
        event_id BIGINT PRIMARY KEY REFERENCES fact_event(event_id),
        rig_key INTEGER,
        permit_number VARCHAR,
        spud_date DATE,
        td_date DATE,
        md_start_ft DOUBLE,
        md_end_ft DOUBLE,
        tvd_ft DOUBLE,
        lateral_length_ft DOUBLE,
        rop_ft_per_hr DOUBLE,
        drill_cost_usd DOUBLE,
        days_to_td INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_completion_detail (
        event_id BIGINT PRIMARY KEY REFERENCES fact_event(event_id),
        completion_method VARCHAR,
        stimulation_type VARCHAR,
        stage_count INTEGER,
        perf_top_ft DOUBLE,
        perf_bottom_ft DOUBLE,
        fluid_type VARCHAR,
        fluid_volume_gal DOUBLE,
        proppant_type VARCHAR,
        proppant_mass_lbs DOUBLE,
        max_treating_pressure_psi DOUBLE,
        completion_cost_usd DOUBLE,
        ip_oil_bbl_per_day DOUBLE,
        ip_gas_mcf_per_day DOUBLE
    )
    """,
    # -- Audit table --
    """
    CREATE TABLE IF NOT EXISTS _mapping_exceptions (
        exception_id INTEGER PRIMARY KEY,
        dimension_table VARCHAR NOT NULL,
        source_code VARCHAR NOT NULL,
        source_system_key INTEGER NOT NULL,
        mapped_to_key INTEGER NOT NULL,
        record_count INTEGER NOT NULL DEFAULT 1,
        first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
]

# Indexes created after tables (including filtered uniques)
_INDEX_STATEMENTS: list[str] = [
    # Unique: exactly one current version per natural event
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_event_current
    ON fact_event (natural_event_id)
    WHERE is_current_version = TRUE
    """,
    # Unique: exactly one PRIMARY per event in bridge
    # PRIMARY_ROLE_KEY = 1 (from seed_constants)
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_bridge_one_primary
    ON fact_event_object_bridge (event_id)
    WHERE object_role_key = 1
    """,
    # Unique: one rule per (event_type, variant, object_type)
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_rule_natural_key
    ON event_type_object_rules (event_type_key, COALESCE(rule_variant, '__DEFAULT__'), object_type_key)
    """,
    # Unique: one current well version per entity
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_well_current
    ON dim_well (well_entity_key)
    WHERE is_current = TRUE
    """,
    # Unique: one current completion version per entity
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_completion_current
    ON dim_completion (completion_entity_key)
    WHERE is_current = TRUE
    """,
    # Natural event id + version uniqueness
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_event_natural_version
    ON fact_event (natural_event_id, event_version)
    """,
    # Bridge composite unique
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_bridge_composite
    ON fact_event_object_bridge (event_id, object_type_key, object_key, object_role_key)
    """,
]


def create_all_tables(con) -> None:
    """Execute all DDL statements to create the dimensional model schema."""
    for sql in _DDL_STATEMENTS:
        con.execute(sql)
    for sql in _INDEX_STATEMENTS:
        con.execute(sql)


# ===================================================================
# Seed data
# ===================================================================

def seed_reference_dimensions(con) -> None:
    """Insert controlled vocabulary rows into all reference dimensions.

    Surrogate keys are explicit (not auto-increment) to match seed_constants.
    """
    _seed_object_roles(con)
    _seed_object_types(con)
    _seed_well_types(con)
    _seed_well_statuses(con)
    _seed_completion_statuses(con)
    _seed_event_statuses(con)
    _seed_event_types(con)
    _seed_source_systems(con)
    _seed_object_rules(con)


def _seed_object_roles(con) -> None:
    con.executemany(
        "INSERT OR IGNORE INTO dim_object_role VALUES (?, ?, ?)",
        [
            (1, "PRIMARY", "Primary object for this event"),
            (2, "ASSOCIATED", "Associated object (e.g., lease for a well event)"),
            (3, "PARENT", "Parent in the object hierarchy"),
            (4, "CHILD", "Child in the object hierarchy"),
        ],
    )


def _seed_object_types(con) -> None:
    con.executemany(
        "INSERT OR IGNORE INTO dim_object_type VALUES (?, ?, ?, ?)",
        [
            (1, "PROD_UNIT", "Production Unit", "dim_prod_unit"),
            (2, "LEASE", "Lease", "dim_lease"),
            (3, "WELL", "Well", "dim_well"),
            (4, "WELLBORE", "Wellbore", "dim_wellbore"),
            (5, "COMPLETION", "Completion", "dim_completion"),
        ],
    )


def _seed_well_types(con) -> None:
    con.executemany(
        "INSERT OR IGNORE INTO dim_well_type VALUES (?, ?, ?, ?)",
        [
            (1, "OIL", "Oil Well", True),
            (2, "GAS", "Gas Well", True),
            (3, "GAS_CONDENSATE", "Gas Condensate Well", True),
            (4, "INJ", "Injection Well", False),
            (5, "SALT_WATER_DISPOSAL", "Salt Water Disposal Well", False),
            (6, "STORAGE", "Storage Well", False),
            (7, "DISPOSAL", "Disposal Well", False),
            (8, "WATER", "Water Well", False),
            (9, "OTHER", "Other Well Type", False),
            (10, "UNKNOWN", "Unknown Well Type", False),
        ],
    )


def _seed_well_statuses(con) -> None:
    con.executemany(
        "INSERT OR IGNORE INTO dim_well_status VALUES (?, ?, ?, ?)",
        [
            (1, "ACTIVE", "Active", True),
            (2, "INACTIVE", "Inactive", False),
            (3, "SHUT-IN", "Shut In", False),
            (4, "P&A", "Plugged and Abandoned", False),
            (5, "NEW", "New", True),
            (6, "LEASED", "Leased", False),
            (7, "CANCELLED", "Cancelled", False),
            (8, "UNKNOWN", "Unknown Status", False),
        ],
    )


def _seed_completion_statuses(con) -> None:
    con.executemany(
        "INSERT OR IGNORE INTO dim_completion_status VALUES (?, ?, ?, ?, ?)",
        [
            (1, "ACTIVE", "Active", True, "ACTIVE"),
            (2, "INACTIVE", "Inactive", False, "SUSPENDED"),
            (3, "SHUT-IN", "Shut In", False, "SUSPENDED"),
            (4, "P&A", "Plugged and Abandoned", False, "ABANDONED"),
            (5, "NEW", "New", False, "PRE_DRILL"),
            (6, "CANCELLED", "Cancelled", False, "ABANDONED"),
            (7, "DRY", "Dry Hole", False, "ABANDONED"),
            (8, "NEVER_DRILLED", "Never Drilled", False, "PRE_DRILL"),
            (9, "TEMPORARY_ABANDONMENT", "Temporary Abandonment", False, "SUSPENDED"),
            (10, "PLUGGED", "Plugged", False, "ABANDONED"),
            (11, "ZONES_PLUGGED", "Zones Plugged", False, "ABANDONED"),
            (12, "UNKNOWN", "Unknown Status", False, "UNKNOWN"),
        ],
    )


def _seed_event_statuses(con) -> None:
    con.executemany(
        "INSERT OR IGNORE INTO dim_event_status VALUES (?, ?, ?)",
        [
            (1, "REPORTED", "Reported"),
            (2, "AMENDED", "Amended (superseded by newer version)"),
            (3, "RETRACTED", "Retracted (removed from reporting)"),
            (4, "PROVISIONAL", "Provisional (subject to revision)"),
        ],
    )


def _seed_event_types(con) -> None:
    con.executemany(
        "INSERT OR IGNORE INTO dim_event_type VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "PRODUCTION", "Monthly Production", "OPERATIONAL", True, True),
            (2, "STATUS_CHANGE", "Status Change", "OPERATIONAL", True, True),
            (3, "DRILLING", "Drilling Event", "OPERATIONAL", False, False),
            (4, "COMPLETION_JOB", "Completion Job", "OPERATIONAL", False, False),
        ],
    )


def _seed_source_systems(con) -> None:
    con.executemany(
        "INSERT OR IGNORE INTO dim_source_system VALUES (?, ?, ?, ?, ?)",
        [
            (1, "TX_RRC_PDQ", "Texas RRC Public Data Query",
             "https://www.rrc.texas.gov/resource-center/research/data-sets-available-for-download/",
             "MONTHLY"),
            (2, "TX_RRC_ARCGIS", "Texas RRC ArcGIS Well Locations",
             "https://gis.rrc.texas.gov/server/rest/services/", "MONTHLY"),
            (3, "NM_OCD_FTP", "New Mexico OCD FTP Production Data",
             "ftp://164.64.106.6/Public/OCD/", "WEEKLY"),
            (4, "NM_OCD_ARCGIS", "New Mexico OCD ArcGIS Well Master",
             "https://gis.emnrd.nm.gov/arcgis/rest/services/", "WEEKLY"),
        ],
    )


def _seed_object_rules(con) -> None:
    """Seed EVENT_TYPE_OBJECT_RULES with production rules (both variants)."""
    rules = [
        # PRODUCTION + COMPLETION variant
        (1, 1, "COMPLETION", 1, True, True, "EXACTLY_ONE"),     # PROD_UNIT = PRIMARY
        (2, 1, "COMPLETION", 5, False, True, "EXACTLY_ONE"),    # COMPLETION = required
        (3, 1, "COMPLETION", 4, False, True, "EXACTLY_ONE"),    # WELLBORE = required
        (4, 1, "COMPLETION", 3, False, True, "EXACTLY_ONE"),    # WELL = required
        (5, 1, "COMPLETION", 2, False, True, "EXACTLY_ONE"),    # LEASE = required
        # PRODUCTION + LEASE_COMBINED variant
        (6, 1, "LEASE_COMBINED", 1, True, True, "EXACTLY_ONE"),   # PROD_UNIT = PRIMARY
        (7, 1, "LEASE_COMBINED", 5, False, False, "NOT_APPLICABLE"),  # COMPLETION N/A
        (8, 1, "LEASE_COMBINED", 4, False, False, "NOT_APPLICABLE"),  # WELLBORE N/A
        (9, 1, "LEASE_COMBINED", 3, False, False, "ZERO_OR_MANY"),    # WELL optional
        (10, 1, "LEASE_COMBINED", 2, False, True, "EXACTLY_ONE"),     # LEASE = required (PARENT)
        # STATUS_CHANGE (no variant)
        (11, 2, None, 5, True, True, "EXACTLY_ONE"),    # COMPLETION = PRIMARY
        (12, 2, None, 3, False, True, "EXACTLY_ONE"),    # WELL = required
    ]
    con.executemany(
        "INSERT OR IGNORE INTO event_type_object_rules VALUES (?, ?, ?, ?, ?, ?, ?)",
        rules,
    )


def populate_time_dimensions(con, *, start_year: int = 2000, end_year: int = 2026) -> None:
    """Generate DIM_DATE and DIM_MONTH rows for the given year range."""
    # DIM_DATE: one row per day
    con.execute(f"""
        INSERT OR IGNORE INTO dim_date
        SELECT
            CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
            d AS full_date,
            EXTRACT(YEAR FROM d)::INT AS year,
            EXTRACT(QUARTER FROM d)::INT AS quarter,
            EXTRACT(MONTH FROM d)::INT AS month,
            strftime(d, '%B') AS month_name,
            EXTRACT(DAY FROM d)::INT AS day_of_month,
            EXTRACT(DAY FROM d) = 1 AS is_month_start
        FROM generate_series(
            DATE '{start_year}-01-01',
            DATE '{end_year}-12-31',
            INTERVAL 1 DAY
        ) AS t(d)
    """)

    # DIM_MONTH: one row per month
    con.execute(f"""
        INSERT OR IGNORE INTO dim_month
        SELECT
            CAST(strftime(d, '%Y%m') AS INTEGER) AS month_key,
            d AS month_start_date,
            (d + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE AS month_end_date,
            EXTRACT(YEAR FROM d)::INT AS year,
            EXTRACT(QUARTER FROM d)::INT AS quarter,
            EXTRACT(MONTH FROM d)::INT AS month,
            strftime(d, '%B') AS month_name
        FROM generate_series(
            DATE '{start_year}-01-01',
            DATE '{end_year}-12-01',
            INTERVAL 1 MONTH
        ) AS t(d)
    """)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_gold/test_ddl.py -v`
Expected: PASS (5 tests)

**Step 5: Commit**

```bash
git add src/gold/ddl.py tests/test_gold/test_ddl.py
git commit -m "feat(gold): add DDL definitions, seed data, and time dimension generators"
```

---

### Task 3: Create ETL Mapping Logic — Silver to Dimensional Model

**Files:**
- Create: `src/gold/etl.py`
- Create: `src/gold/hash_utils.py`
- Test: `tests/test_gold/test_hash_utils.py`
- Test: `tests/test_gold/test_etl.py`

This is the core mapping engine. It reads silver Parquet via DuckDB's `read_parquet()` and populates every dimension and fact table.

**Step 1: Write the failing test for hash_utils**

```python
# tests/test_gold/test_hash_utils.py
"""Tests for canonical event hashing."""

from src.gold.hash_utils import compute_production_hash


def test_hash_deterministic():
    h1 = compute_production_hash(1000.0, 500.0, 50.0, 25.0, None, None)
    h2 = compute_production_hash(1000.0, 500.0, 50.0, 25.0, None, None)
    assert h1 == h2


def test_hash_null_vs_zero_differ():
    h_null = compute_production_hash(1000.0, 500.0, None, None, None, None)
    h_zero = compute_production_hash(1000.0, 500.0, 0.0, 0.0, 0.0, 0)
    assert h_null != h_zero


def test_hash_rounding():
    h1 = compute_production_hash(1000.0001, 500.0, None, None, None, None)
    h2 = compute_production_hash(1000.0004, 500.0, None, None, None, None)
    assert h1 == h2  # Both round to 1000.000


def test_hash_format_is_64_char_hex():
    h = compute_production_hash(100.0, 200.0, 0.0, 0.0, 0.0, 30)
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_gold/test_hash_utils.py -v`
Expected: FAIL

**Step 3: Write hash_utils**

```python
# src/gold/hash_utils.py
"""Canonical hashing for event amendment detection.

Spec:
- Scope: FACT_PRODUCTION_DETAIL measure columns only
- Column order: oil_bbl, gas_mcf, condensate_bbl, casinghead_gas_mcf, water_bbl, days_produced
- Floats: ROUND to 3 decimals, format as "{:.3f}"
- Integers: format as str, no decimals
- NULL: empty string ""
- Separator: pipe "|"
- Hash: SHA-256, lowercase hex, 64 chars
"""

from __future__ import annotations

import hashlib


def _fmt_float(val) -> str:
    if val is None:
        return ""
    return f"{round(float(val), 3):.3f}"


def _fmt_int(val) -> str:
    if val is None:
        return ""
    return str(int(val))


def compute_production_hash(
    oil_bbl, gas_mcf, condensate_bbl, casinghead_gas_mcf, water_bbl, days_produced,
) -> str:
    """Compute canonical SHA-256 hash for production event measures."""
    canonical = "|".join([
        _fmt_float(oil_bbl),
        _fmt_float(gas_mcf),
        _fmt_float(condensate_bbl),
        _fmt_float(casinghead_gas_mcf),
        _fmt_float(water_bbl),
        _fmt_int(days_produced),
    ])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
```

**Step 4: Run hash_utils tests**

Run: `pytest tests/test_gold/test_hash_utils.py -v`
Expected: PASS (4 tests)

**Step 5: Write the failing ETL test**

```python
# tests/test_gold/test_etl.py
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
```

**Step 6: Run ETL test to verify it fails**

Run: `pytest tests/test_gold/test_etl.py -v`
Expected: FAIL with `ImportError: cannot import name 'load_silver_to_dimensional_model'`

**Step 7: Write ETL implementation**

This is the largest single file. The ETL reads silver Parquet into DuckDB staging, then populates each table in dependency order using SQL.

```python
# src/gold/etl.py
"""Silver-to-dimensional-model ETL.

Reads silver Parquet via DuckDB read_parquet(), then populates all
dimension and fact tables in dependency order.

Mapping rules:
- DIM_LEASE: TX leases from (district, lease_number). NM synthetic from api_number.
- DIM_WELL: SCD2 on (operator, field_name, well_status). Keyed by api_number.
- DIM_WELLBORE: 1:1 with well, default WB01 suffix.
- DIM_COMPLETION: 1:1 with wellbore, default C01 suffix. SCD2 on status.
- DIM_PROD_UNIT: COMPLETION type for wells, LEASE_COMBINED for TX multi-well leases.
- FACT_EVENT: One per prod_unit-month. natural_event_id is source-stable.
- FACT_EVENT_OBJECT_BRIDGE: Per-event hierarchy walk.
- FACT_PRODUCTION_DETAIL: Measures from silver.
"""

from __future__ import annotations

from typing import Any

from src.gold.seed_constants import (
    PRIMARY_ROLE_KEY, ASSOCIATED_ROLE_KEY, PARENT_ROLE_KEY,
    PROD_UNIT_TYPE_KEY, LEASE_TYPE_KEY, WELL_TYPE_KEY,
    WELLBORE_TYPE_KEY, COMPLETION_TYPE_KEY,
    PRODUCTION_EVENT_KEY, REPORTED_STATUS_KEY,
)


def load_silver_to_dimensional_model(
    con,
    silver_parquet_glob: str,
    *,
    log: Any = None,
) -> dict[str, int]:
    """Load silver Parquet into the dimensional model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
    silver_parquet_glob : str
        Glob pattern for silver Parquet files.
    log : optional
        Logger with .info() and .error() methods.

    Returns
    -------
    dict with row counts per table.
    """
    def _log(msg, *args):
        if log:
            log.info(msg, *args)

    counts: dict[str, int] = {}

    # Step 0: Create staging view over silver Parquet
    con.execute(f"""
        CREATE OR REPLACE VIEW _stg_silver AS
        SELECT *,
            CASE
                WHEN entity_type = 'well' THEN api_number
                WHEN entity_type = 'lease' THEN state || '-' || district || '-' || lease_number
            END AS entity_id
        FROM read_parquet('{silver_parquet_glob}', hive_partitioning=true)
    """)
    stg_count = con.execute("SELECT COUNT(*) FROM _stg_silver").fetchone()[0]
    _log("Staging: %d silver rows", stg_count)

    # Step 1: Resolve source_system_key from source_file patterns
    con.execute("""
        CREATE OR REPLACE VIEW _stg_with_source AS
        SELECT s.*,
            CASE
                WHEN s.state = 'TX' THEN 1  -- TX_RRC_PDQ
                WHEN s.state = 'NM' THEN 3  -- NM_OCD_FTP
            END AS source_system_key
        FROM _stg_silver s
    """)

    # Step 2: DIM_LEASE
    con.execute("""
        INSERT OR IGNORE INTO dim_lease (
            lease_entity_key, lease_id, state, lease_number, district,
            lease_name, county, basin, is_synthetic, lease_grain_type
        )
        -- TX real leases
        SELECT
            ROW_NUMBER() OVER () AS lease_entity_key,
            lease_id, state, lease_number, district,
            lease_name, county, basin, is_synthetic, lease_grain_type
        FROM (
            SELECT DISTINCT
                state || '-' || district || '-' || lease_number AS lease_id,
                state, lease_number, district,
                FIRST_VALUE(well_name) OVER (
                    PARTITION BY state, district, lease_number
                    ORDER BY production_date DESC
                ) AS lease_name,
                FIRST_VALUE(county) OVER (
                    PARTITION BY state, district, lease_number
                    ORDER BY production_date DESC
                ) AS county,
                FIRST_VALUE(basin) OVER (
                    PARTITION BY state, district, lease_number
                    ORDER BY production_date DESC
                ) AS basin,
                FALSE AS is_synthetic,
                'REAL_LEASE' AS lease_grain_type
            FROM _stg_silver
            WHERE entity_type = 'lease'
              AND lease_number IS NOT NULL
              AND district IS NOT NULL
        )
        UNION ALL
        -- NM synthetic leases (1:1 with well)
        SELECT
            ROW_NUMBER() OVER () + (SELECT COALESCE(MAX(lease_entity_key), 0) FROM dim_lease) AS lease_entity_key,
            'NM-' || api_number AS lease_id,
            'NM' AS state,
            NULL AS lease_number,
            NULL AS district,
            FIRST_VALUE(well_name) OVER (
                PARTITION BY api_number ORDER BY production_date DESC
            ) AS lease_name,
            FIRST_VALUE(county) OVER (
                PARTITION BY api_number ORDER BY production_date DESC
            ) AS county,
            FIRST_VALUE(basin) OVER (
                PARTITION BY api_number ORDER BY production_date DESC
            ) AS basin,
            TRUE AS is_synthetic,
            'SYNTHETIC_CONTAINER' AS lease_grain_type
        FROM _stg_silver
        WHERE state = 'NM' AND api_number IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY api_number ORDER BY production_date DESC) = 1
        -- TX well-level entities also get synthetic leases if not already covered
        UNION ALL
        SELECT
            ROW_NUMBER() OVER () + (SELECT COALESCE(MAX(lease_entity_key), 0) FROM dim_lease) AS lease_entity_key,
            'TX-WELL-' || api_number AS lease_id,
            'TX' AS state,
            NULL AS lease_number,
            NULL AS district,
            FIRST_VALUE(well_name) OVER (
                PARTITION BY api_number ORDER BY production_date DESC
            ) AS lease_name,
            FIRST_VALUE(county) OVER (
                PARTITION BY api_number ORDER BY production_date DESC
            ) AS county,
            FIRST_VALUE(basin) OVER (
                PARTITION BY api_number ORDER BY production_date DESC
            ) AS basin,
            FALSE AS is_synthetic,
            'REAL_LEASE' AS lease_grain_type
        FROM _stg_silver
        WHERE state = 'TX' AND entity_type = 'well' AND api_number IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY api_number ORDER BY production_date DESC) = 1
    """)
    counts["dim_lease"] = con.execute("SELECT COUNT(*) FROM dim_lease").fetchone()[0]
    _log("DIM_LEASE: %d rows", counts["dim_lease"])

    # Step 3: DIM_WELL (SCD2 - initial load = all current)
    # Resolve well_type_key and well_status_key from reference dims
    con.execute("""
        INSERT INTO dim_well (
            well_version_key, well_entity_key, well_id,
            lease_entity_key, api_number, state,
            well_name, well_number, operator, operator_id,
            county, field_name, basin,
            well_type_key, well_status_key,
            latitude, longitude,
            spud_date, completion_date, total_depth_ft,
            valid_from, valid_to, is_current
        )
        SELECT
            ROW_NUMBER() OVER () AS well_version_key,
            ROW_NUMBER() OVER () AS well_entity_key,
            w.api_number AS well_id,
            l.lease_entity_key,
            w.api_number, w.state,
            CASE WHEN w.state = 'TX' THEN NULL ELSE w.well_name END AS well_name,
            NULL AS well_number,
            w.operator, NULL AS operator_id,
            w.county, w.field_name, w.basin,
            wt.well_type_key,
            ws.well_status_key,
            w.latitude, w.longitude,
            NULL AS spud_date, NULL AS completion_date, NULL AS total_depth_ft,
            TIMESTAMP '2000-01-01' AS valid_from,
            NULL AS valid_to,
            TRUE AS is_current
        FROM (
            SELECT DISTINCT ON (api_number, state)
                api_number, state, well_name, operator, county,
                field_name, basin, well_type, well_status,
                latitude, longitude
            FROM _stg_silver
            WHERE api_number IS NOT NULL
            ORDER BY api_number, state, production_date DESC
        ) w
        LEFT JOIN dim_lease l ON (
            CASE
                WHEN w.state = 'NM' THEN 'NM-' || w.api_number
                WHEN w.state = 'TX' THEN 'TX-WELL-' || w.api_number
            END = l.lease_id
        )
        LEFT JOIN dim_well_type wt ON wt.well_type_code = COALESCE(w.well_type, 'UNKNOWN')
        LEFT JOIN dim_well_status ws ON ws.well_status_code = COALESCE(w.well_status, 'UNKNOWN')
    """)
    counts["dim_well"] = con.execute("SELECT COUNT(*) FROM dim_well").fetchone()[0]
    _log("DIM_WELL: %d rows", counts["dim_well"])

    # Step 4: DIM_WELLBORE (1:1 with well)
    con.execute("""
        INSERT INTO dim_wellbore (
            wellbore_entity_key, well_entity_key, wellbore_id
        )
        SELECT
            well_entity_key AS wellbore_entity_key,
            well_entity_key,
            api_number || '-WB01' AS wellbore_id
        FROM dim_well
        WHERE is_current = TRUE
    """)
    counts["dim_wellbore"] = con.execute("SELECT COUNT(*) FROM dim_wellbore").fetchone()[0]
    _log("DIM_WELLBORE: %d rows", counts["dim_wellbore"])

    # Step 5: DIM_COMPLETION (1:1 with wellbore, initial load)
    con.execute("""
        INSERT INTO dim_completion (
            completion_version_key, completion_entity_key,
            wellbore_entity_key, completion_id,
            completion_type, producing_formation,
            completion_status_key,
            valid_from, valid_to, is_current
        )
        SELECT
            wb.wellbore_entity_key AS completion_version_key,
            wb.wellbore_entity_key AS completion_entity_key,
            wb.wellbore_entity_key,
            wb.wellbore_id || '-C01' AS completion_id,
            NULL AS completion_type,
            NULL AS producing_formation,
            w.well_status_key AS completion_status_key,
            TIMESTAMP '2000-01-01' AS valid_from,
            NULL AS valid_to,
            TRUE AS is_current
        FROM dim_wellbore wb
        JOIN dim_well w ON wb.well_entity_key = w.well_entity_key AND w.is_current = TRUE
    """)
    counts["dim_completion"] = con.execute("SELECT COUNT(*) FROM dim_completion").fetchone()[0]
    _log("DIM_COMPLETION: %d rows", counts["dim_completion"])

    # Step 6: DIM_PROD_UNIT
    con.execute("""
        INSERT INTO dim_prod_unit (
            prod_unit_key, prod_unit_id, prod_unit_type,
            completion_entity_key, lease_entity_key, state
        )
        -- COMPLETION-type prod units (well-level production)
        SELECT
            ROW_NUMBER() OVER () AS prod_unit_key,
            c.completion_id AS prod_unit_id,
            'COMPLETION' AS prod_unit_type,
            c.completion_entity_key,
            NULL AS lease_entity_key,
            w.state
        FROM dim_completion c
        JOIN dim_wellbore wb ON c.wellbore_entity_key = wb.wellbore_entity_key
        JOIN dim_well w ON wb.well_entity_key = w.well_entity_key AND w.is_current = TRUE
        WHERE c.is_current = TRUE
        UNION ALL
        -- LEASE_COMBINED prod units (TX multi-well oil leases)
        SELECT
            ROW_NUMBER() OVER () + (SELECT COALESCE(MAX(prod_unit_key), 0) FROM dim_prod_unit),
            l.lease_id || '-COMBINED' AS prod_unit_id,
            'LEASE_COMBINED' AS prod_unit_type,
            NULL AS completion_entity_key,
            l.lease_entity_key,
            l.state
        FROM dim_lease l
        WHERE l.lease_grain_type = 'REAL_LEASE'
          AND l.state = 'TX'
          AND l.lease_number IS NOT NULL
    """)
    counts["dim_prod_unit"] = con.execute("SELECT COUNT(*) FROM dim_prod_unit").fetchone()[0]
    _log("DIM_PROD_UNIT: %d rows", counts["dim_prod_unit"])

    # Step 7: FACT_EVENT + FACT_PRODUCTION_DETAIL + BRIDGE
    # Build a staging table that resolves prod_unit_key per silver row
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _stg_events AS
        SELECT
            s.*,
            COALESCE(pu_comp.prod_unit_key, pu_lease.prod_unit_key) AS prod_unit_key,
            COALESCE(pu_comp.prod_unit_id, pu_lease.prod_unit_id) AS prod_unit_id,
            COALESCE(pu_comp.prod_unit_type, pu_lease.prod_unit_type) AS prod_unit_type,
            s.source_system_key,
            CAST(strftime(s.production_date, '%Y%m') AS INTEGER) AS month_key,
            s.production_date::TIMESTAMP AS event_ts
        FROM _stg_with_source s
        -- Well-level -> COMPLETION prod unit
        LEFT JOIN dim_well w ON s.api_number = w.api_number AND s.state = w.state AND w.is_current = TRUE
        LEFT JOIN dim_wellbore wb ON w.well_entity_key = wb.well_entity_key
        LEFT JOIN dim_completion c ON wb.wellbore_entity_key = c.wellbore_entity_key AND c.is_current = TRUE
        LEFT JOIN dim_prod_unit pu_comp ON c.completion_entity_key = pu_comp.completion_entity_key
            AND pu_comp.prod_unit_type = 'COMPLETION'
        -- Lease-level -> LEASE_COMBINED prod unit
        LEFT JOIN dim_lease l ON s.entity_type = 'lease'
            AND s.state || '-' || s.district || '-' || s.lease_number = l.lease_id
        LEFT JOIN dim_prod_unit pu_lease ON l.lease_entity_key = pu_lease.lease_entity_key
            AND pu_lease.prod_unit_type = 'LEASE_COMBINED'
        WHERE COALESCE(pu_comp.prod_unit_key, pu_lease.prod_unit_key) IS NOT NULL
    """)

    # Insert FACT_EVENT
    con.execute(f"""
        INSERT INTO fact_event (
            event_id, natural_event_id, event_version, event_hash,
            event_type_key, event_date_key, month_key, event_ts,
            source_system_key, event_status_key,
            is_current_version, superseded_at, source_file, ingested_at
        )
        SELECT
            ROW_NUMBER() OVER () AS event_id,
            'PRODUCTION:' ||
                CASE source_system_key
                    WHEN 1 THEN 'TX_RRC_PDQ'
                    WHEN 3 THEN 'NM_OCD_FTP'
                END || ':' ||
                prod_unit_id || ':' ||
                strftime(production_date, '%Y%m') AS natural_event_id,
            1 AS event_version,
            '' AS event_hash,  -- placeholder, updated below
            {PRODUCTION_EVENT_KEY} AS event_type_key,
            NULL AS event_date_key,
            month_key,
            event_ts,
            source_system_key,
            {REPORTED_STATUS_KEY} AS event_status_key,
            TRUE AS is_current_version,
            NULL AS superseded_at,
            source_file,
            ingested_at
        FROM _stg_events
    """)
    counts["fact_event"] = con.execute("SELECT COUNT(*) FROM fact_event").fetchone()[0]
    _log("FACT_EVENT: %d rows", counts["fact_event"])

    # Insert FACT_PRODUCTION_DETAIL
    con.execute("""
        INSERT INTO fact_production_detail (
            event_id, oil_bbl, gas_mcf, condensate_bbl, casinghead_gas_mcf,
            water_bbl, total_oil_bbl, total_gas_mcf, days_produced,
            oil_rate_bbl_per_day, gas_rate_mcf_per_day, gor, water_cut_pct
        )
        SELECT
            fe.event_id,
            s.oil_bbl, s.gas_mcf, s.condensate_bbl, s.casinghead_gas_mcf,
            s.water_bbl,
            COALESCE(s.oil_bbl, 0) + COALESCE(s.condensate_bbl, 0) AS total_oil_bbl,
            COALESCE(s.gas_mcf, 0) + COALESCE(s.casinghead_gas_mcf, 0) AS total_gas_mcf,
            s.days_produced,
            (COALESCE(s.oil_bbl, 0) + COALESCE(s.condensate_bbl, 0))
                / NULLIF(s.days_produced, 0) AS oil_rate_bbl_per_day,
            (COALESCE(s.gas_mcf, 0) + COALESCE(s.casinghead_gas_mcf, 0))
                / NULLIF(s.days_produced, 0) AS gas_rate_mcf_per_day,
            (COALESCE(s.gas_mcf, 0) + COALESCE(s.casinghead_gas_mcf, 0))
                / NULLIF(COALESCE(s.oil_bbl, 0) + COALESCE(s.condensate_bbl, 0), 0)
                * 1000 AS gor,
            s.water_bbl
                / NULLIF(s.water_bbl + COALESCE(s.oil_bbl, 0) + COALESCE(s.condensate_bbl, 0), 0)
                * 100 AS water_cut_pct
        FROM _stg_events s
        JOIN fact_event fe ON fe.natural_event_id =
            'PRODUCTION:' ||
            CASE s.source_system_key
                WHEN 1 THEN 'TX_RRC_PDQ'
                WHEN 3 THEN 'NM_OCD_FTP'
            END || ':' ||
            s.prod_unit_id || ':' ||
            strftime(s.production_date, '%Y%m')
        WHERE fe.is_current_version = TRUE
    """)
    counts["fact_production_detail"] = con.execute(
        "SELECT COUNT(*) FROM fact_production_detail"
    ).fetchone()[0]
    _log("FACT_PRODUCTION_DETAIL: %d rows", counts["fact_production_detail"])

    # Insert BRIDGE rows
    # PRIMARY: PROD_UNIT
    con.execute(f"""
        INSERT INTO fact_event_object_bridge (event_id, object_type_key, object_key, object_role_key)
        SELECT
            fe.event_id,
            {PROD_UNIT_TYPE_KEY},
            s.prod_unit_key,
            {PRIMARY_ROLE_KEY}
        FROM _stg_events s
        JOIN fact_event fe ON fe.natural_event_id =
            'PRODUCTION:' ||
            CASE s.source_system_key WHEN 1 THEN 'TX_RRC_PDQ' WHEN 3 THEN 'NM_OCD_FTP' END
            || ':' || s.prod_unit_id || ':' || strftime(s.production_date, '%Y%m')
        WHERE fe.is_current_version = TRUE
    """)

    # PARENT: WELL (for COMPLETION-type prod units)
    con.execute(f"""
        INSERT INTO fact_event_object_bridge (event_id, object_type_key, object_key, object_role_key)
        SELECT
            fe.event_id,
            {WELL_TYPE_KEY},
            w.well_entity_key,
            {PARENT_ROLE_KEY}
        FROM _stg_events s
        JOIN fact_event fe ON fe.natural_event_id =
            'PRODUCTION:' ||
            CASE s.source_system_key WHEN 1 THEN 'TX_RRC_PDQ' WHEN 3 THEN 'NM_OCD_FTP' END
            || ':' || s.prod_unit_id || ':' || strftime(s.production_date, '%Y%m')
        JOIN dim_prod_unit pu ON s.prod_unit_key = pu.prod_unit_key
        JOIN dim_completion c ON pu.completion_entity_key = c.completion_entity_key AND c.is_current = TRUE
        JOIN dim_wellbore wb ON c.wellbore_entity_key = wb.wellbore_entity_key
        JOIN dim_well w ON wb.well_entity_key = w.well_entity_key AND w.is_current = TRUE
        WHERE fe.is_current_version = TRUE
          AND pu.prod_unit_type = 'COMPLETION'
    """)

    # ASSOCIATED: LEASE
    con.execute(f"""
        INSERT INTO fact_event_object_bridge (event_id, object_type_key, object_key, object_role_key)
        SELECT
            fe.event_id,
            {LEASE_TYPE_KEY},
            COALESCE(l_direct.lease_entity_key, l_well.lease_entity_key),
            CASE
                WHEN s.prod_unit_type = 'LEASE_COMBINED' THEN {PARENT_ROLE_KEY}
                ELSE {ASSOCIATED_ROLE_KEY}
            END
        FROM _stg_events s
        JOIN fact_event fe ON fe.natural_event_id =
            'PRODUCTION:' ||
            CASE s.source_system_key WHEN 1 THEN 'TX_RRC_PDQ' WHEN 3 THEN 'NM_OCD_FTP' END
            || ':' || s.prod_unit_id || ':' || strftime(s.production_date, '%Y%m')
        -- LEASE_COMBINED: direct from prod_unit
        LEFT JOIN dim_prod_unit pu_lc ON s.prod_unit_key = pu_lc.prod_unit_key
            AND pu_lc.prod_unit_type = 'LEASE_COMBINED'
        LEFT JOIN dim_lease l_direct ON pu_lc.lease_entity_key = l_direct.lease_entity_key
        -- COMPLETION: walk up to well -> lease
        LEFT JOIN dim_prod_unit pu_c ON s.prod_unit_key = pu_c.prod_unit_key
            AND pu_c.prod_unit_type = 'COMPLETION'
        LEFT JOIN dim_completion c ON pu_c.completion_entity_key = c.completion_entity_key AND c.is_current = TRUE
        LEFT JOIN dim_wellbore wb ON c.wellbore_entity_key = wb.wellbore_entity_key
        LEFT JOIN dim_well w ON wb.well_entity_key = w.well_entity_key AND w.is_current = TRUE
        LEFT JOIN dim_lease l_well ON w.lease_entity_key = l_well.lease_entity_key
        WHERE fe.is_current_version = TRUE
          AND COALESCE(l_direct.lease_entity_key, l_well.lease_entity_key) IS NOT NULL
    """)

    counts["fact_event_object_bridge"] = con.execute(
        "SELECT COUNT(*) FROM fact_event_object_bridge"
    ).fetchone()[0]
    _log("BRIDGE: %d rows", counts["fact_event_object_bridge"])

    # Cleanup staging
    con.execute("DROP VIEW IF EXISTS _stg_silver")
    con.execute("DROP VIEW IF EXISTS _stg_with_source")
    con.execute("DROP TABLE IF EXISTS _stg_events")

    return counts
```

**Step 8: Run ETL tests**

Run: `pytest tests/test_gold/test_etl.py -v`
Expected: PASS (7 tests)

**Step 9: Commit**

```bash
git add src/gold/hash_utils.py src/gold/etl.py tests/test_gold/test_hash_utils.py tests/test_gold/test_etl.py
git commit -m "feat(gold): add silver-to-dimensional ETL with hash utils and bridge mapping"
```

---

### Task 4: Create Canonical Views + Validation Jobs

**Files:**
- Create: `src/gold/views.py`
- Create: `src/gold/validation.py`
- Test: `tests/test_gold/test_views.py`
- Test: `tests/test_gold/test_validation.py`

**Step 1: Write the failing test for views**

```python
# tests/test_gold/test_views.py
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
```

**Step 2: Run to verify failure**

Run: `pytest tests/test_gold/test_views.py -v`
Expected: FAIL

**Step 3: Write views.py**

```python
# src/gold/views.py
"""Canonical reportable views for the dimensional model.

All downstream marts MUST source from these views, never from
fact tables directly. This enforces retraction/amendment filtering.
"""

from __future__ import annotations

from src.gold.seed_constants import RETRACTED_STATUS_KEY, PRODUCTION_EVENT_KEY


def create_canonical_views(con) -> None:
    """Create the three canonical views."""

    con.execute("""
        CREATE OR REPLACE VIEW vw_fact_event_current AS
        SELECT * FROM fact_event
        WHERE is_current_version = TRUE
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW vw_fact_event_reportable AS
        SELECT * FROM fact_event
        WHERE is_current_version = TRUE
          AND event_status_key != {RETRACTED_STATUS_KEY}
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW vw_production_current AS
        SELECT
            fe.event_id,
            fe.natural_event_id,
            fe.month_key,
            fe.event_ts,
            fe.source_system_key,
            pd.*
        FROM vw_fact_event_reportable fe
        JOIN fact_production_detail pd ON fe.event_id = pd.event_id
        WHERE fe.event_type_key = {PRODUCTION_EVENT_KEY}
    """)
```

**Step 4: Write validation.py**

```python
# src/gold/validation.py
"""Post-load validation jobs for the dimensional model.

Every validation returns a list of error strings. Empty list = pass.
Any non-empty list = hard fail (pipeline should abort).
"""

from __future__ import annotations

from src.gold.seed_constants import (
    PRIMARY_ROLE_KEY, PROD_UNIT_TYPE_KEY, PRODUCTION_EVENT_KEY,
)


def validate_bridge_integrity(con) -> list[str]:
    """Run all bridge validation checks."""
    errors: list[str] = []

    # 1. Exactly one PRIMARY per event
    rows = con.execute(f"""
        SELECT event_id, COUNT(*) AS cnt
        FROM fact_event_object_bridge
        WHERE object_role_key = {PRIMARY_ROLE_KEY}
        GROUP BY event_id
        HAVING COUNT(*) != 1
    """).fetchall()
    if rows:
        errors.append(f"PRIMARY count != 1 for {len(rows)} events")

    # 2. Orphan object keys (PROD_UNIT)
    rows = con.execute(f"""
        SELECT b.object_key
        FROM fact_event_object_bridge b
        LEFT JOIN dim_prod_unit pu ON b.object_key = pu.prod_unit_key
        WHERE b.object_type_key = {PROD_UNIT_TYPE_KEY}
          AND pu.prod_unit_key IS NULL
    """).fetchall()
    if rows:
        errors.append(f"Orphan PROD_UNIT keys in bridge: {len(rows)}")

    return errors


def validate_scd2_integrity(con) -> list[str]:
    """Check SCD2 gap/overlap on DIM_WELL."""
    errors: list[str] = []
    rows = con.execute("""
        WITH versioned AS (
            SELECT well_entity_key, valid_from, valid_to,
                LEAD(valid_from) OVER (
                    PARTITION BY well_entity_key ORDER BY valid_from
                ) AS next_valid_from
            FROM dim_well
        )
        SELECT * FROM versioned
        WHERE valid_to IS NOT NULL AND valid_to != next_valid_from
    """).fetchall()
    if rows:
        errors.append(f"SCD2 gap/overlap in DIM_WELL: {len(rows)} violations")
    return errors


def validate_event_time_contract(con) -> list[str]:
    """Check that exactly one of month_key/event_date_key is set."""
    errors: list[str] = []
    rows = con.execute("""
        SELECT event_id FROM fact_event
        WHERE (month_key IS NULL AND event_date_key IS NULL)
           OR (month_key IS NOT NULL AND event_date_key IS NOT NULL)
    """).fetchall()
    if rows:
        errors.append(f"Time contract violation: {len(rows)} events have both or neither time keys")
    return errors


def validate_current_version_uniqueness(con) -> list[str]:
    """Check exactly one current version per natural_event_id."""
    errors: list[str] = []
    rows = con.execute("""
        SELECT natural_event_id, COUNT(*) AS cnt
        FROM fact_event
        WHERE is_current_version = TRUE
        GROUP BY natural_event_id
        HAVING COUNT(*) > 1
    """).fetchall()
    if rows:
        errors.append(f"Multiple current versions: {len(rows)} natural_event_ids")
    return errors


def run_all_validations(con) -> list[str]:
    """Run all validation checks. Returns combined error list."""
    errors: list[str] = []
    errors.extend(validate_bridge_integrity(con))
    errors.extend(validate_scd2_integrity(con))
    errors.extend(validate_event_time_contract(con))
    errors.extend(validate_current_version_uniqueness(con))
    return errors
```

**Step 5: Write test_validation.py**

```python
# tests/test_gold/test_validation.py
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
```

**Step 6: Run tests**

Run: `pytest tests/test_gold/test_views.py tests/test_gold/test_validation.py -v`
Expected: PASS (4 tests)

**Step 7: Commit**

```bash
git add src/gold/views.py src/gold/validation.py tests/test_gold/test_views.py tests/test_gold/test_validation.py
git commit -m "feat(gold): add canonical views and validation jobs"
```

---

### Task 5: Create Dimensional Builder (Replaces _gold_builder.py)

**Files:**
- Create: `orchestration/assets/_dimensional_builder.py`
- Test: `tests/test_gold/test_dimensional_builder.py`

**Step 1: Write the failing test**

```python
# tests/test_gold/test_dimensional_builder.py
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
```

**Step 2: Run to verify failure**

Run: `pytest tests/test_gold/test_dimensional_builder.py -v`
Expected: FAIL

**Step 3: Write _dimensional_builder.py**

```python
# orchestration/assets/_dimensional_builder.py
"""Dimensional model builder -- orchestrates DDL, seeds, ETL, views, validation.

Replaces the flat _gold_builder.py with the full digital twin dimensional model.
Called by both the production gold_models Dagster asset and E2E test assets.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from src.gold.ddl import create_all_tables, seed_reference_dimensions, populate_time_dimensions
from src.gold.etl import load_silver_to_dimensional_model
from src.gold.views import create_canonical_views
from src.gold.validation import run_all_validations
from src.gold.seed_constants import validate_seeds


def build_dimensional_model(
    *,
    duckdb_path: str | Path,
    silver_parquet_glob: str,
    log: Any = None,
    start_year: int = 2000,
    end_year: int = 2026,
) -> dict[str, Any]:
    """Build the full dimensional model from silver Parquet.

    Parameters
    ----------
    duckdb_path : path to DuckDB file
    silver_parquet_glob : glob for silver Parquet files
    log : optional logger with .info() and .error()
    start_year, end_year : range for time dimension generation

    Returns
    -------
    dict with table counts, validation results.

    Raises
    ------
    RuntimeError if seed validation or post-load validation fails.
    """
    def _log_info(msg, *a):
        if log:
            log.info(msg, *a)

    def _log_error(msg, *a):
        if log:
            log.error(msg, *a)

    con = duckdb.connect(str(duckdb_path))
    try:
        # 1. Schema
        _log_info("Creating dimensional model schema...")
        create_all_tables(con)

        # 2. Seeds
        _log_info("Seeding reference dimensions...")
        seed_reference_dimensions(con)

        # 3. Validate seeds match constants
        seed_errors = validate_seeds(con)
        if seed_errors:
            for e in seed_errors:
                _log_error("SEED ERROR: %s", e)
            raise RuntimeError(f"Seed validation failed: {seed_errors}")

        # 4. Time dimensions
        _log_info("Generating time dimensions (%d-%d)...", start_year, end_year)
        populate_time_dimensions(con, start_year=start_year, end_year=end_year)

        # 5. ETL
        _log_info("Loading silver to dimensional model...")
        counts = load_silver_to_dimensional_model(con, silver_parquet_glob, log=log)

        # 6. Views
        _log_info("Creating canonical views...")
        create_canonical_views(con)

        # 7. Validation
        _log_info("Running post-load validations...")
        validation_errors = run_all_validations(con)
        for e in validation_errors:
            _log_error("VALIDATION ERROR: %s", e)

        if validation_errors:
            raise RuntimeError(
                f"{len(validation_errors)} validation error(s): "
                + "; ".join(validation_errors)
            )

        _log_info("Dimensional model build complete.")

    finally:
        con.close()

    return {
        "tables_built": len(counts),
        "validation_errors": validation_errors,
        **counts,
    }
```

**Step 4: Run integration test**

Run: `pytest tests/test_gold/test_dimensional_builder.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add orchestration/assets/_dimensional_builder.py tests/test_gold/test_dimensional_builder.py
git commit -m "feat(gold): add dimensional model builder replacing flat gold_builder"
```

---

### Task 6: Update Dagster Gold Asset to Use Dimensional Builder

**Files:**
- Modify: `orchestration/assets/gold.py`
- Modify: `orchestration/assets/e2e.py`

**Step 1: Update gold.py**

Replace the `build_gold` import with `build_dimensional_model`. Keep the old `_gold_builder.py` for reference but stop calling it.

```python
# In orchestration/assets/gold.py, change:
# from assets._gold_builder import build_gold
# to:
from assets._dimensional_builder import build_dimensional_model
```

Update the `gold_models` function body to call `build_dimensional_model` instead of `build_gold`, mapping the return dict appropriately.

**Step 2: Update e2e.py similarly**

Same import swap in `test_gold_models`.

**Step 3: Run existing tests to verify no regressions**

Run: `pytest tests/ -v`
Expected: All existing tests PASS + new tests PASS

**Step 4: Commit**

```bash
git add orchestration/assets/gold.py orchestration/assets/e2e.py
git commit -m "feat(gold): wire Dagster assets to dimensional model builder"
```

---

### Task 7: Update Reconciliation Checks for Dimensional Model

**Files:**
- Modify: `src/quality/reconciliation.py`

**Step 1: Add dimensional model reconciliation functions**

Add new functions that validate the dimensional model's `vw_production_current` view reconciles with silver Parquet data. The existing reconciliation functions remain for backward compatibility.

**Step 2: Run reconciliation tests**

Run: `pytest tests/test_quality/test_reconciliation.py -v`
Expected: PASS

**Step 3: Commit**

```bash
git add src/quality/reconciliation.py
git commit -m "feat(quality): add dimensional model reconciliation checks"
```

---

### Task 8: Final Integration Test and Cleanup

**Step 1: Run full test suite**

Run: `pytest tests/ -v`
Expected: All PASS

**Step 2: Run linter**

Run: `ruff check src/gold/ tests/test_gold/ orchestration/assets/_dimensional_builder.py`
Run: `ruff format --check src/gold/ tests/test_gold/ orchestration/assets/_dimensional_builder.py`

**Step 3: Fix any lint issues**

**Step 4: Final commit**

```bash
git add -A
git commit -m "chore: lint fixes for dimensional model implementation"
```

---

## Dependency Graph

```
Task 1 (seed_constants)
  └── Task 2 (ddl + seeds + time dims)
        └── Task 3 (ETL + hash_utils)
              └── Task 4 (views + validation)
                    └── Task 5 (dimensional builder)
                          └── Task 6 (Dagster wiring)
                                └── Task 7 (reconciliation)
                                      └── Task 8 (integration + lint)
```

## Files Created/Modified Summary

| Action | File |
|--------|------|
| Create | `src/gold/__init__.py` |
| Create | `src/gold/seed_constants.py` |
| Create | `src/gold/ddl.py` |
| Create | `src/gold/etl.py` |
| Create | `src/gold/hash_utils.py` |
| Create | `src/gold/views.py` |
| Create | `src/gold/validation.py` |
| Create | `orchestration/assets/_dimensional_builder.py` |
| Create | `tests/test_gold/__init__.py` |
| Create | `tests/test_gold/test_seed_constants.py` |
| Create | `tests/test_gold/test_ddl.py` |
| Create | `tests/test_gold/test_hash_utils.py` |
| Create | `tests/test_gold/test_etl.py` |
| Create | `tests/test_gold/test_views.py` |
| Create | `tests/test_gold/test_validation.py` |
| Create | `tests/test_gold/test_dimensional_builder.py` |
| Modify | `orchestration/assets/gold.py` |
| Modify | `orchestration/assets/e2e.py` |
| Modify | `src/quality/reconciliation.py` |
