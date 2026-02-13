"""DDL definitions and seed data for the gold dimensional model.

All tables are created in DuckDB via CREATE TABLE/VIEW statements.
Reference dimensions are seeded with controlled vocabularies.
Time dimensions are generated from a date range.

The surrogate key assignments for reference dimensions MUST match
the values in seed_constants.py.  Bootstrap validation (validate_seeds)
checks this at runtime.
"""

from __future__ import annotations


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
        try:
            con.execute(sql)
        except Exception:
            # DuckDB does not support partial indexes (WHERE clause on CREATE INDEX).
            # Skip those gracefully; the constraints are enforced by application logic.
            if "WHERE" in sql.upper():
                continue
            raise


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
            (
                1,
                "TX_RRC_PDQ",
                "Texas RRC Public Data Query",
                "https://www.rrc.texas.gov/resource-center/research/data-sets-available-for-download/",
                "MONTHLY",
            ),
            (
                2,
                "TX_RRC_ARCGIS",
                "Texas RRC ArcGIS Well Locations",
                "https://gis.rrc.texas.gov/server/rest/services/",
                "MONTHLY",
            ),
            (3, "NM_OCD_FTP", "New Mexico OCD FTP Production Data", "ftp://164.64.106.6/Public/OCD/", "WEEKLY"),
            (
                4,
                "NM_OCD_ARCGIS",
                "New Mexico OCD ArcGIS Well Master",
                "https://gis.emnrd.nm.gov/arcgis/rest/services/",
                "WEEKLY",
            ),
        ],
    )


def _seed_object_rules(con) -> None:
    """Seed EVENT_TYPE_OBJECT_RULES with production rules (both variants)."""
    rules = [
        # PRODUCTION + COMPLETION variant
        (1, 1, "COMPLETION", 1, True, True, "EXACTLY_ONE"),  # PROD_UNIT = PRIMARY
        (2, 1, "COMPLETION", 5, False, True, "EXACTLY_ONE"),  # COMPLETION = required
        (3, 1, "COMPLETION", 4, False, True, "EXACTLY_ONE"),  # WELLBORE = required
        (4, 1, "COMPLETION", 3, False, True, "EXACTLY_ONE"),  # WELL = required
        (5, 1, "COMPLETION", 2, False, True, "EXACTLY_ONE"),  # LEASE = required
        # PRODUCTION + LEASE_COMBINED variant
        (6, 1, "LEASE_COMBINED", 1, True, True, "EXACTLY_ONE"),  # PROD_UNIT = PRIMARY
        (7, 1, "LEASE_COMBINED", 5, False, False, "NOT_APPLICABLE"),  # COMPLETION N/A
        (8, 1, "LEASE_COMBINED", 4, False, False, "NOT_APPLICABLE"),  # WELLBORE N/A
        (9, 1, "LEASE_COMBINED", 3, False, False, "ZERO_OR_MANY"),  # WELL optional
        (10, 1, "LEASE_COMBINED", 2, False, True, "EXACTLY_ONE"),  # LEASE = required (PARENT)
        # STATUS_CHANGE (no variant)
        (11, 2, None, 5, True, True, "EXACTLY_ONE"),  # COMPLETION = PRIMARY
        (12, 2, None, 3, False, True, "EXACTLY_ONE"),  # WELL = required
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
