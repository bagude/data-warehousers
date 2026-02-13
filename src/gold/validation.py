"""Post-load validation jobs for the dimensional model.

Every validation returns a list of error strings. Empty list = pass.
Any non-empty list = hard fail (pipeline should abort).
"""

from __future__ import annotations

from src.gold.seed_constants import (
    PRIMARY_ROLE_KEY,
    PROD_UNIT_TYPE_KEY,
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
