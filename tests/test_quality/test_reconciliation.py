"""Tests for src.quality.reconciliation — silver-to-gold reconciliation checks."""

from datetime import date

import pandas as pd
import pytest

from src.quality.reconciliation import (
    check_month_completeness,
    check_pk_uniqueness,
    reconcile_entity_counts,
    reconcile_volumes,
)


# ===================================================================
# reconcile_volumes
# ===================================================================

class TestReconcileVolumes:
    """Tests for reconcile_volumes()."""

    def test_silver_and_gold_match_exactly(self):
        """Silver and gold volume sums match -> passed=True."""
        silver = pd.DataFrame({
            "county": ["Eddy", "Eddy", "Lea"],
            "state": ["NM", "NM", "NM"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)],
            "oil_bbl": [100.0, 200.0, 300.0],
        })
        gold = pd.DataFrame({
            "county": ["Eddy", "Lea"],
            "state": ["NM", "NM"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1)],
            "total_oil": [300.0, 300.0],
        })
        result = reconcile_volumes(
            silver,
            gold,
            volume_column="oil_bbl",
            gold_volume_column="total_oil",
            group_keys=["county", "state", "production_date"],
        )

        assert result.passed is True
        assert result.details["silver_only_count"] == 0
        assert result.details["gold_only_count"] == 0
        assert result.details["mismatch_count"] == 0

    def test_volume_mismatch_beyond_tolerance(self):
        """Volume totals differ more than tolerance -> passed=False."""
        silver = pd.DataFrame({
            "county": ["Eddy"],
            "state": ["NM"],
            "production_date": [date(2024, 1, 1)],
            "oil_bbl": [1000.0],
        })
        gold = pd.DataFrame({
            "county": ["Eddy"],
            "state": ["NM"],
            "production_date": [date(2024, 1, 1)],
            "total_oil": [500.0],  # 50% off -- way beyond 1% tolerance
        })
        result = reconcile_volumes(
            silver,
            gold,
            volume_column="oil_bbl",
            gold_volume_column="total_oil",
            group_keys=["county", "state", "production_date"],
            tolerance=0.01,
        )

        assert result.passed is False
        assert result.details["mismatch_count"] > 0

    def test_groups_in_silver_but_not_gold(self):
        """Silver has groups that gold does not -> passed=False, silver_only_count > 0."""
        silver = pd.DataFrame({
            "county": ["Eddy", "Lea"],
            "state": ["NM", "NM"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1)],
            "oil_bbl": [100.0, 200.0],
        })
        gold = pd.DataFrame({
            "county": ["Eddy"],
            "state": ["NM"],
            "production_date": [date(2024, 1, 1)],
            "total_oil": [100.0],
        })
        result = reconcile_volumes(
            silver,
            gold,
            volume_column="oil_bbl",
            gold_volume_column="total_oil",
            group_keys=["county", "state", "production_date"],
        )

        assert result.passed is False
        assert result.details["silver_only_count"] > 0

    def test_orphan_groups_in_gold_not_in_silver(self):
        """Gold has groups not in silver -> passed=False, gold_only_count > 0."""
        silver = pd.DataFrame({
            "county": ["Eddy"],
            "state": ["NM"],
            "production_date": [date(2024, 1, 1)],
            "oil_bbl": [100.0],
        })
        gold = pd.DataFrame({
            "county": ["Eddy", "Lea"],
            "state": ["NM", "NM"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1)],
            "total_oil": [100.0, 999.0],
        })
        result = reconcile_volumes(
            silver,
            gold,
            volume_column="oil_bbl",
            gold_volume_column="total_oil",
            group_keys=["county", "state", "production_date"],
        )

        assert result.passed is False
        assert result.details["gold_only_count"] > 0


# ===================================================================
# reconcile_entity_counts
# ===================================================================

class TestReconcileEntityCounts:
    """Tests for reconcile_entity_counts()."""

    def test_counts_match(self):
        """Silver row counts and gold entity_count match -> passed=True."""
        silver = pd.DataFrame({
            "county": ["Eddy", "Eddy", "Lea"],
            "state": ["NM", "NM", "NM"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)],
        })
        gold = pd.DataFrame({
            "county": ["Eddy", "Lea"],
            "state": ["NM", "NM"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1)],
            "entity_count": [2, 1],
        })
        result = reconcile_entity_counts(
            silver,
            gold,
            group_keys=["county", "state", "production_date"],
        )

        assert result.passed is True
        assert result.name == "entity_count_reconciliation"

    def test_count_mismatch(self):
        """Gold entity_count does not match silver row count -> passed=False."""
        silver = pd.DataFrame({
            "county": ["Eddy", "Eddy"],
            "state": ["NM", "NM"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1)],
        })
        gold = pd.DataFrame({
            "county": ["Eddy"],
            "state": ["NM"],
            "production_date": [date(2024, 1, 1)],
            "entity_count": [5],  # Should be 2
        })
        result = reconcile_entity_counts(
            silver,
            gold,
            group_keys=["county", "state", "production_date"],
        )

        assert result.passed is False
        assert result.details["count_mismatches"] > 0

    def test_missing_groups(self):
        """Silver has groups not in gold -> passed=False."""
        silver = pd.DataFrame({
            "county": ["Eddy", "Lea"],
            "state": ["NM", "NM"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1)],
        })
        gold = pd.DataFrame({
            "county": ["Eddy"],
            "state": ["NM"],
            "production_date": [date(2024, 1, 1)],
            "entity_count": [1],
        })
        result = reconcile_entity_counts(
            silver,
            gold,
            group_keys=["county", "state", "production_date"],
        )

        assert result.passed is False
        assert result.details["silver_only"] > 0


# ===================================================================
# check_pk_uniqueness
# ===================================================================

class TestCheckPkUniqueness:
    """Tests for check_pk_uniqueness()."""

    def test_unique_pks(self):
        """All primary keys unique -> passed=True."""
        df = pd.DataFrame({
            "county": ["Eddy", "Lea"],
            "state": ["NM", "NM"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1)],
            "total_oil": [100.0, 200.0],
        })
        result = check_pk_uniqueness(
            df,
            pk_columns=["county", "state", "production_date"],
            table_name="test_table",
        )

        assert result.passed is True
        assert "unique" in result.message.lower() or "unique" in result.message

    def test_duplicate_pks(self):
        """Duplicate primary keys -> passed=False, details show count."""
        df = pd.DataFrame({
            "county": ["Eddy", "Eddy"],
            "state": ["NM", "NM"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1)],
            "total_oil": [100.0, 200.0],
        })
        result = check_pk_uniqueness(
            df,
            pk_columns=["county", "state", "production_date"],
            table_name="test_table",
        )

        assert result.passed is False
        assert result.details["duplicate_rows"] == 2  # Both rows flagged by keep=False
        assert "test_table" in result.message

    def test_empty_dataframe(self):
        """Empty DataFrame -> passed=True (trivially satisfied)."""
        df = pd.DataFrame(columns=["county", "state", "production_date"])
        result = check_pk_uniqueness(
            df,
            pk_columns=["county", "state", "production_date"],
            table_name="empty_table",
        )

        assert result.passed is True
        assert "empty" in result.message.lower()


# ===================================================================
# check_month_completeness
# ===================================================================

class TestCheckMonthCompleteness:
    """Tests for check_month_completeness()."""

    def test_no_gaps(self):
        """Consecutive months with no gaps -> passed=True."""
        df = pd.DataFrame({
            "entity_id": ["W001", "W001", "W001", "W001"],
            "state": ["TX", "TX", "TX", "TX"],
            "production_date": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 3, 1),
                date(2024, 4, 1),
            ],
        })
        result = check_month_completeness(df, max_gap_months=3)

        assert result.passed is True
        assert result.details["flagged_entities"] == 0

    def test_gap_exceeds_max_gap_months(self):
        """Gap larger than max_gap_months -> passed=False, flagged_entities > 0."""
        df = pd.DataFrame({
            "entity_id": ["W001", "W001"],
            "state": ["TX", "TX"],
            "production_date": [
                date(2024, 1, 1),
                date(2025, 1, 1),  # 12 month gap
            ],
        })
        result = check_month_completeness(df, max_gap_months=3)

        assert result.passed is False
        assert result.details["flagged_entities"] > 0

    def test_empty_dataframe(self):
        """Empty DataFrame -> passed=True."""
        df = pd.DataFrame(columns=["entity_id", "state", "production_date"])
        result = check_month_completeness(df)

        assert result.passed is True

    def test_single_record_entity(self):
        """Entity with only one date -> passed=True (no gap possible with < 2 dates)."""
        df = pd.DataFrame({
            "entity_id": ["W001"],
            "state": ["TX"],
            "production_date": [date(2024, 6, 1)],
        })
        result = check_month_completeness(df, max_gap_months=3)

        assert result.passed is True
        assert result.details["flagged_entities"] == 0
