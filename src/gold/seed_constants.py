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
            errors.append(f"{const_name}: expected key {expected_key}, got {row[0]} for {code_val} in {table}")
    return errors
