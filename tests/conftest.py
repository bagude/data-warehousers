"""Shared fixtures for unit tests."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest


@pytest.fixture
def valid_well_row() -> dict:
    """A single valid well-level production record as a dict.

    Tests can copy and surgically inject one bad value.
    """
    return {
        "state": "NM",
        "entity_type": "well",
        "api_number": "30-015-12345",
        "lease_number": None,
        "district": None,
        "well_name": "Test Well 1",
        "operator": "Test Operator",
        "county": "Eddy",
        "field_name": "Test Field",
        "basin": "Permian",
        "well_type": "OIL",
        "well_status": "ACTIVE",
        "latitude": 32.5,
        "longitude": -104.0,
        "production_date": date(2024, 1, 1),
        "oil_bbl": 1000.0,
        "gas_mcf": 500.0,
        "condensate_bbl": None,
        "casinghead_gas_mcf": None,
        "water_bbl": 200.0,
        "days_produced": 30,
        "source_file": "test.csv",
        "ingested_at": datetime(2024, 2, 1, tzinfo=timezone.utc),
    }


@pytest.fixture
def valid_lease_row() -> dict:
    """A single valid lease-level production record as a dict."""
    return {
        "state": "TX",
        "entity_type": "lease",
        "api_number": None,
        "lease_number": "12345",
        "district": "08",
        "well_name": "Test Lease",
        "operator": "TX Operator",
        "county": "Midland",
        "field_name": "Spraberry",
        "basin": None,
        "well_type": "OIL",
        "well_status": "ACTIVE",
        "latitude": None,
        "longitude": None,
        "production_date": date(2024, 1, 1),
        "oil_bbl": 2000.0,
        "gas_mcf": 0.0,
        "condensate_bbl": 50.0,
        "casinghead_gas_mcf": 100.0,
        "water_bbl": None,
        "days_produced": None,
        "source_file": "OG_LEASE_CYCLE.dsv",
        "ingested_at": datetime(2024, 2, 1, tzinfo=timezone.utc),
    }


@pytest.fixture
def small_silver_df(valid_well_row, valid_lease_row) -> pd.DataFrame:
    """5 rows: 3 wells + 2 leases, all valid."""
    w1 = valid_well_row.copy()
    w2 = valid_well_row.copy()
    w2["api_number"] = "30-015-12346"
    w2["production_date"] = date(2024, 2, 1)
    w2["oil_bbl"] = 1100.0
    w3 = valid_well_row.copy()
    w3["api_number"] = "30-025-00001"
    w3["county"] = "Lea"
    w3["production_date"] = date(2024, 3, 1)
    l1 = valid_lease_row.copy()
    l2 = valid_lease_row.copy()
    l2["lease_number"] = "67890"
    l2["production_date"] = date(2024, 2, 1)
    l2["oil_bbl"] = 3000.0
    return pd.DataFrame([w1, w2, w3, l1, l2])


@pytest.fixture
def silver_df_with_dupes(valid_well_row, valid_lease_row) -> pd.DataFrame:
    """8 rows with well and lease duplicates for dedup testing."""
    ts1 = datetime(2024, 1, 15, tzinfo=timezone.utc)
    ts2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

    # Well duplicates: same (state, api_number, production_date), different ingested_at
    w1 = valid_well_row.copy()
    w1["ingested_at"] = ts1
    w1["oil_bbl"] = 900.0
    w2 = valid_well_row.copy()
    w2["ingested_at"] = ts2
    w2["oil_bbl"] = 1000.0  # newer, should win

    # Another well, no dupe
    w3 = valid_well_row.copy()
    w3["api_number"] = "30-015-99999"
    w3["ingested_at"] = ts2

    # Lease duplicates: same (state, lease_number, district, production_date)
    l1 = valid_lease_row.copy()
    l1["ingested_at"] = ts1
    l1["oil_bbl"] = 1800.0
    l2 = valid_lease_row.copy()
    l2["ingested_at"] = ts2
    l2["oil_bbl"] = 2000.0  # newer, should win

    # Another lease, no dupe
    l3 = valid_lease_row.copy()
    l3["lease_number"] = "99999"
    l3["ingested_at"] = ts2

    # Two more unique wells
    w4 = valid_well_row.copy()
    w4["api_number"] = "30-025-00001"
    w4["production_date"] = date(2024, 2, 1)
    w4["ingested_at"] = ts2

    w5 = valid_well_row.copy()
    w5["api_number"] = "30-025-00002"
    w5["production_date"] = date(2024, 3, 1)
    w5["ingested_at"] = ts2

    return pd.DataFrame([w1, w2, w3, l1, l2, l3, w4, w5])
