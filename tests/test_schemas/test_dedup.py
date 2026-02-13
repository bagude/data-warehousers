"""Tests for src.schemas.dedup -- deduplication and TX entity type assignment."""

from __future__ import annotations

import datetime

import pandas as pd
import pytest

from src.schemas.dedup import _dedup_partition, deduplicate, assign_tx_entity_type


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_production_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal production DataFrame with required columns.

    Columns not supplied in *rows* are filled with sensible defaults so that
    tests can focus only on the columns relevant to each scenario.
    """
    defaults = {
        "state": "TX",
        "entity_type": "well",
        "api_number": None,
        "lease_number": None,
        "district": None,
        "production_date": datetime.date(2025, 1, 1),
        "oil_bbl": 100.0,
        "ingested_at": pd.Timestamp("2025-06-01"),
    }
    full_rows = [{**defaults, **r} for r in rows]
    return pd.DataFrame(full_rows)


# ===================================================================
# _dedup_partition
# ===================================================================


class TestDedupPartition:
    """Tests for the internal ``_dedup_partition`` helper."""

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame(columns=["state", "api_number", "production_date", "ingested_at"])
        result = _dedup_partition(df, ["state", "api_number", "production_date"])
        assert result.empty
        assert list(result.columns) == list(df.columns)

    def test_no_duplicates_returns_same_rows(self):
        df = _make_production_df([
            {"api_number": "42-001-00001", "production_date": datetime.date(2025, 1, 1)},
            {"api_number": "42-001-00002", "production_date": datetime.date(2025, 1, 1)},
            {"api_number": "42-001-00001", "production_date": datetime.date(2025, 2, 1)},
        ])
        result = _dedup_partition(df, ["state", "api_number", "production_date"])
        assert len(result) == 3

    def test_duplicates_keeps_latest_ingested_at(self):
        df = _make_production_df([
            {
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 1, 1),
                "oil_bbl": 100.0,
                "ingested_at": pd.Timestamp("2025-06-01"),
            },
            {
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 1, 1),
                "oil_bbl": 200.0,
                "ingested_at": pd.Timestamp("2025-07-01"),
            },
        ])
        result = _dedup_partition(df, ["state", "api_number", "production_date"])
        assert len(result) == 1
        assert result.iloc[0]["oil_bbl"] == 200.0
        assert result.iloc[0]["ingested_at"] == pd.Timestamp("2025-07-01")

    def test_nat_ingested_at_treated_as_oldest(self):
        """NaT in the tiebreaker column should sort first (na_position='first'),
        so the non-NaT row wins."""
        df = _make_production_df([
            {
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 1, 1),
                "oil_bbl": 50.0,
                "ingested_at": pd.NaT,
            },
            {
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 1, 1),
                "oil_bbl": 300.0,
                "ingested_at": pd.Timestamp("2025-05-15"),
            },
        ])
        result = _dedup_partition(df, ["state", "api_number", "production_date"])
        assert len(result) == 1
        assert result.iloc[0]["oil_bbl"] == 300.0

    def test_missing_key_column_raises_value_error(self):
        df = _make_production_df([
            {"api_number": "42-001-00001"},
        ])
        with pytest.raises(ValueError, match="Dedup key columns missing"):
            _dedup_partition(df, ["state", "api_number", "nonexistent_column"])


# ===================================================================
# deduplicate
# ===================================================================


class TestDeduplicate:
    """Tests for the public ``deduplicate`` function."""

    def test_empty_dataframe_returns_empty(self):
        df = _make_production_df([])
        result = deduplicate(df)
        assert result.empty

    def test_no_duplicates_returns_same_count(self):
        df = _make_production_df([
            {
                "entity_type": "well",
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 1, 1),
            },
            {
                "entity_type": "well",
                "api_number": "42-001-00002",
                "production_date": datetime.date(2025, 1, 1),
            },
            {
                "entity_type": "lease",
                "lease_number": "L001",
                "district": "01",
                "production_date": datetime.date(2025, 1, 1),
            },
        ])
        result = deduplicate(df)
        assert len(result) == 3

    def test_well_duplicates_keeps_latest(self):
        """Same (state, api_number, production_date) with different ingested_at
        -- the later ingestion should survive."""
        df = _make_production_df([
            {
                "entity_type": "well",
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 1, 1),
                "oil_bbl": 100.0,
                "ingested_at": pd.Timestamp("2025-06-01"),
            },
            {
                "entity_type": "well",
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 1, 1),
                "oil_bbl": 150.0,
                "ingested_at": pd.Timestamp("2025-07-01"),
            },
        ])
        result = deduplicate(df)
        assert len(result) == 1
        assert result.iloc[0]["oil_bbl"] == 150.0

    def test_lease_duplicates_keeps_latest(self):
        """Same (state, lease_number, district, production_date) with different
        ingested_at -- the later ingestion should survive."""
        df = _make_production_df([
            {
                "entity_type": "lease",
                "lease_number": "L001",
                "district": "01",
                "production_date": datetime.date(2025, 3, 1),
                "oil_bbl": 500.0,
                "ingested_at": pd.Timestamp("2025-04-01"),
            },
            {
                "entity_type": "lease",
                "lease_number": "L001",
                "district": "01",
                "production_date": datetime.date(2025, 3, 1),
                "oil_bbl": 550.0,
                "ingested_at": pd.Timestamp("2025-05-01"),
            },
        ])
        result = deduplicate(df)
        assert len(result) == 1
        assert result.iloc[0]["oil_bbl"] == 550.0

    def test_mixed_entity_types_deduped_independently(self):
        """A well record and a lease record with overlapping fields should
        both survive because they are deduped in separate partitions."""
        df = _make_production_df([
            {
                "entity_type": "well",
                "api_number": "42-001-00001",
                "lease_number": "L001",
                "district": "01",
                "production_date": datetime.date(2025, 1, 1),
                "oil_bbl": 100.0,
                "ingested_at": pd.Timestamp("2025-06-01"),
            },
            {
                "entity_type": "lease",
                "api_number": None,
                "lease_number": "L001",
                "district": "01",
                "production_date": datetime.date(2025, 1, 1),
                "oil_bbl": 200.0,
                "ingested_at": pd.Timestamp("2025-06-01"),
            },
        ])
        result = deduplicate(df)
        assert len(result) == 2

    def test_different_production_dates_not_deduped(self):
        """Records for the same entity but different months are distinct
        and must NOT be collapsed."""
        df = _make_production_df([
            {
                "entity_type": "well",
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 1, 1),
                "ingested_at": pd.Timestamp("2025-06-01"),
            },
            {
                "entity_type": "well",
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 2, 1),
                "ingested_at": pd.Timestamp("2025-06-01"),
            },
            {
                "entity_type": "well",
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 3, 1),
                "ingested_at": pd.Timestamp("2025-06-01"),
            },
        ])
        result = deduplicate(df)
        assert len(result) == 3

    def test_result_has_contiguous_index(self):
        """After deduplication the result should have a 0-based contiguous
        index (ignore_index=True in pd.concat)."""
        df = _make_production_df([
            {
                "entity_type": "well",
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 1, 1),
                "ingested_at": pd.Timestamp("2025-06-01"),
            },
            {
                "entity_type": "well",
                "api_number": "42-001-00001",
                "production_date": datetime.date(2025, 1, 1),
                "ingested_at": pd.Timestamp("2025-07-01"),
            },
            {
                "entity_type": "lease",
                "lease_number": "L001",
                "district": "01",
                "production_date": datetime.date(2025, 1, 1),
                "ingested_at": pd.Timestamp("2025-06-01"),
            },
        ])
        result = deduplicate(df)
        assert list(result.index) == list(range(len(result)))


# ===================================================================
# assign_tx_entity_type
# ===================================================================


class TestAssignTxEntityType:
    """Tests for TX lease-to-well entity type assignment."""

    def test_no_well_completion_df_all_lease(self):
        """When well_completion_df is None, all records default to 'lease'."""
        df = _make_production_df([
            {"lease_number": "L001", "district": "01"},
            {"lease_number": "L002", "district": "02"},
        ])
        result = assign_tx_entity_type(df, well_completion_df=None)
        assert (result["entity_type"] == "lease").all()

    def test_single_well_lease_becomes_well(self):
        """A lease with exactly one well in the completions table should get
        entity_type='well' and api_number filled from the completions."""
        df = _make_production_df([
            {"lease_number": "1001", "district": "01", "entity_type": None, "api_number": None},
        ])
        completions = pd.DataFrame({
            "LEASE_NO": ["1001"],
            "DISTRICT_NO": ["01"],
            "API_NO": ["42-001-00001"],
        })
        result = assign_tx_entity_type(df, well_completion_df=completions)
        assert result.iloc[0]["entity_type"] == "well"
        assert result.iloc[0]["api_number"] == "42-001-00001"

    def test_multi_well_lease_stays_lease(self):
        """A lease with multiple wells should remain entity_type='lease' with
        api_number staying None."""
        df = _make_production_df([
            {"lease_number": "2001", "district": "03", "entity_type": None, "api_number": None},
        ])
        completions = pd.DataFrame({
            "LEASE_NO": ["2001", "2001"],
            "DISTRICT_NO": ["03", "03"],
            "API_NO": ["42-003-00001", "42-003-00002"],
        })
        result = assign_tx_entity_type(df, well_completion_df=completions)
        assert result.iloc[0]["entity_type"] == "lease"
        assert result.iloc[0]["api_number"] is None or pd.isna(result.iloc[0]["api_number"])

    def test_mix_of_single_and_multi_well_leases(self):
        """A DataFrame with both single-well and multi-well leases should
        assign entity types correctly for each."""
        df = _make_production_df([
            {"lease_number": "1001", "district": "01", "entity_type": None, "api_number": None},
            {"lease_number": "2001", "district": "03", "entity_type": None, "api_number": None},
        ])
        completions = pd.DataFrame({
            "LEASE_NO": ["1001", "2001", "2001"],
            "DISTRICT_NO": ["01", "03", "03"],
            "API_NO": ["42-001-00001", "42-003-00001", "42-003-00002"],
        })
        result = assign_tx_entity_type(df, well_completion_df=completions)

        single = result[result["lease_number"] == "1001"].iloc[0]
        multi = result[result["lease_number"] == "2001"].iloc[0]

        assert single["entity_type"] == "well"
        assert single["api_number"] == "42-001-00001"
        assert multi["entity_type"] == "lease"
        assert multi["api_number"] is None or pd.isna(multi["api_number"])

    def test_production_record_not_in_completions_defaults_to_lease(self):
        """If a production record has no matching lease in the completions
        DataFrame (left join produces NaN _well_count), it should default
        to entity_type='lease'."""
        df = _make_production_df([
            {"lease_number": "9999", "district": "99", "entity_type": None, "api_number": None},
        ])
        completions = pd.DataFrame({
            "LEASE_NO": ["1001"],
            "DISTRICT_NO": ["01"],
            "API_NO": ["42-001-00001"],
        })
        result = assign_tx_entity_type(df, well_completion_df=completions)
        assert result.iloc[0]["entity_type"] == "lease"

    def test_temp_columns_cleaned_up(self):
        """Internal merge columns (_well_count, LEASE_NO, DISTRICT_NO, etc.)
        must not appear in the final output."""
        df = _make_production_df([
            {"lease_number": "1001", "district": "01", "entity_type": None, "api_number": None},
        ])
        completions = pd.DataFrame({
            "LEASE_NO": ["1001"],
            "DISTRICT_NO": ["01"],
            "API_NO": ["42-001-00001"],
        })
        result = assign_tx_entity_type(df, well_completion_df=completions)

        forbidden = {"_well_count", "_single_api", "LEASE_NO", "DISTRICT_NO",
                     "LEASE_NO_api", "DISTRICT_NO_api"}
        leftover = forbidden & set(result.columns)
        assert leftover == set(), f"Temp columns not cleaned up: {leftover}"
