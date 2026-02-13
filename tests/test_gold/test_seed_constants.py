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
