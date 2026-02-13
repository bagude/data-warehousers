"""Tests for helpers in src.transforms.tx_parser and src.ingestion.tx_rrc."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src.transforms.tx_parser import _safe_mode, _normalize_tx_arcgis_api, TxParser
from src.ingestion.tx_rrc import TxRrcIngester


# ---------------------------------------------------------------------------
# _safe_mode
# ---------------------------------------------------------------------------

class TestSafeMode:
    @pytest.mark.parametrize(
        "values, default, expected",
        [
            # Single dominant value -> returns that value
            (["OIL", "OIL", "GAS"], "OTHER", "OIL"),
            # Empty series -> returns default
            ([], "OTHER", "OTHER"),
            # Tie -> mode() returns first alphabetically; either tied value acceptable
            (["GAS", "OIL", "GAS", "OIL"], "OTHER", "GAS"),
            # All same value -> returns that value
            (["ACTIVE", "ACTIVE", "ACTIVE"], "OTHER", "ACTIVE"),
        ],
        ids=[
            "single_dominant",
            "empty_returns_default",
            "tie_returns_first_alpha",
            "all_same_value",
        ],
    )
    def test_safe_mode(self, values, default, expected):
        series = pd.Series(values)
        result = _safe_mode(series, default)
        if not values:
            assert result == default
        else:
            assert result == expected


# ---------------------------------------------------------------------------
# _normalize_tx_arcgis_api
# ---------------------------------------------------------------------------

class TestNormalizeTxArcgisApi:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            # 8-digit -> prepend TX state code 42
            ("12345678", "42-123-45678"),
            # 10-digit starting with 42 -> format as SS-CCC-WWWWW
            ("4212345678", "42-123-45678"),
            # Too short (7 digits) -> None
            ("1234567", None),
            # 10-digit NOT starting with 42 -> None
            ("3001512345", None),
            # Non-digit chars stripped, resulting 8 digits -> valid
            ("123-456-78", "42-123-45678"),
            # Empty string -> None
            ("", None),
            # 9 digits -> None (neither 8 nor 10)
            ("123456789", None),
        ],
        ids=[
            "eight_digit_prepend_42",
            "ten_digit_starts_42",
            "too_short_seven_digits",
            "ten_digit_not_42_prefix",
            "non_digit_chars_stripped",
            "empty_string",
            "nine_digits_invalid",
        ],
    )
    def test_normalize_tx_arcgis_api(self, raw, expected):
        assert _normalize_tx_arcgis_api(raw) == expected


# ---------------------------------------------------------------------------
# TxParser._build_production_date  (static method)
# ---------------------------------------------------------------------------

class TestBuildProductionDate:
    @pytest.mark.parametrize(
        "year, month, expected_date",
        [
            # Valid year and month -> date(year, month, 1)
            ("2024", "1", date(2024, 1, 1)),
            # Invalid month 13 -> NaT which becomes None via .dt.date
            ("2024", "13", None),
            # December edge case
            ("2024", "12", date(2024, 12, 1)),
        ],
        ids=[
            "valid_jan_2024",
            "invalid_month_13",
            "december_edge_case",
        ],
    )
    def test_single_row(self, year, month, expected_date):
        df = pd.DataFrame({"CYCLE_YEAR": [year], "CYCLE_MONTH": [month]})
        result = TxParser._build_production_date(df)
        actual = result["production_date"].iloc[0]
        if expected_date is None:
            assert actual is None or pd.isna(actual)
        else:
            assert actual == expected_date

    def test_mixed_valid_invalid(self):
        """Multiple rows with mixed valid and invalid entries."""
        df = pd.DataFrame({
            "CYCLE_YEAR": ["2024", "2024", "bad", "2024"],
            "CYCLE_MONTH": ["1", "13", "6", "12"],
        })
        result = TxParser._build_production_date(df)
        dates = result["production_date"].tolist()
        assert dates[0] == date(2024, 1, 1)
        assert dates[1] is None or pd.isna(dates[1])  # month 13 invalid
        assert dates[2] is None or pd.isna(dates[2])  # non-numeric year
        assert dates[3] == date(2024, 12, 1)


# ---------------------------------------------------------------------------
# TxParser._map_volumes  (static method)
# ---------------------------------------------------------------------------

class TestMapVolumes:
    def test_all_four_columns_present(self):
        """All 4 source volume columns present -> renamed correctly."""
        df = pd.DataFrame({
            "LEASE_OIL_PROD_VOL": ["100"],
            "LEASE_GAS_PROD_VOL": ["200"],
            "LEASE_COND_PROD_VOL": ["50"],
            "LEASE_CSGD_PROD_VOL": ["75"],
        })
        result = TxParser._map_volumes(df)
        assert result["oil_bbl"].iloc[0] == 100.0
        assert result["gas_mcf"].iloc[0] == 200.0
        assert result["condensate_bbl"].iloc[0] == 50.0
        assert result["casinghead_gas_mcf"].iloc[0] == 75.0

    def test_non_numeric_values_become_nan(self):
        """Non-numeric values coerced to NaN."""
        df = pd.DataFrame({
            "LEASE_OIL_PROD_VOL": ["abc"],
            "LEASE_GAS_PROD_VOL": [""],
            "LEASE_COND_PROD_VOL": ["N/A"],
            "LEASE_CSGD_PROD_VOL": ["--"],
        })
        result = TxParser._map_volumes(df)
        assert pd.isna(result["oil_bbl"].iloc[0])
        assert pd.isna(result["gas_mcf"].iloc[0])
        assert pd.isna(result["condensate_bbl"].iloc[0])
        assert pd.isna(result["casinghead_gas_mcf"].iloc[0])

    def test_missing_source_column_fills_na(self):
        """Missing source column -> destination filled with pd.NA."""
        df = pd.DataFrame({
            "LEASE_OIL_PROD_VOL": ["100"],
            # LEASE_GAS_PROD_VOL intentionally missing
            "LEASE_COND_PROD_VOL": ["50"],
            "LEASE_CSGD_PROD_VOL": ["75"],
        })
        result = TxParser._map_volumes(df)
        assert result["oil_bbl"].iloc[0] == 100.0
        assert pd.isna(result["gas_mcf"].iloc[0])
        assert result["condensate_bbl"].iloc[0] == 50.0
        assert result["casinghead_gas_mcf"].iloc[0] == 75.0

    def test_existing_values_preserved(self):
        """Numeric values are correctly coerced and preserved."""
        df = pd.DataFrame({
            "LEASE_OIL_PROD_VOL": ["1234.56"],
            "LEASE_GAS_PROD_VOL": ["0"],
            "LEASE_COND_PROD_VOL": ["0.001"],
            "LEASE_CSGD_PROD_VOL": ["99999"],
        })
        result = TxParser._map_volumes(df)
        assert result["oil_bbl"].iloc[0] == pytest.approx(1234.56)
        assert result["gas_mcf"].iloc[0] == 0.0
        assert result["condensate_bbl"].iloc[0] == pytest.approx(0.001)
        assert result["casinghead_gas_mcf"].iloc[0] == 99999.0


# ---------------------------------------------------------------------------
# TxRrcIngester._find_zip_entry  (static method)
# ---------------------------------------------------------------------------

class TestFindZipEntry:
    @pytest.mark.parametrize(
        "zip_contents, table_name, expected",
        [
            # Exact match with .dsv extension
            (
                ["OG_LEASE_CYCLE.dsv", "OG_WELL_COMPLETION.dsv"],
                "OG_LEASE_CYCLE",
                "OG_LEASE_CYCLE.dsv",
            ),
            # Exact match with .csv extension
            (
                ["OG_LEASE_CYCLE.csv", "OG_WELL_COMPLETION.csv"],
                "OG_LEASE_CYCLE",
                "OG_LEASE_CYCLE.csv",
            ),
            # In a subdirectory
            (
                ["PDQ_DSV/OG_LEASE_CYCLE.dsv", "PDQ_DSV/OG_WELL_COMPLETION.dsv"],
                "OG_LEASE_CYCLE",
                "PDQ_DSV/OG_LEASE_CYCLE.dsv",
            ),
            # Case insensitive match
            (
                ["og_lease_cycle.dsv"],
                "OG_LEASE_CYCLE",
                "og_lease_cycle.dsv",
            ),
            # Not found -> None
            (
                ["SOME_OTHER_FILE.dsv"],
                "OG_LEASE_CYCLE",
                None,
            ),
            # Partial match: entry *contains* the table name (fallback match)
            (
                ["OG_LEASE_CYCLE_EXTRA.dsv"],
                "OG_LEASE_CYCLE",
                "OG_LEASE_CYCLE_EXTRA.dsv",
            ),
            # No extension match -- bare name matches stem exactly
            (
                ["OG_LEASE_CYCLE"],
                "OG_LEASE_CYCLE",
                "OG_LEASE_CYCLE",
            ),
            # Multiple candidates -> returns first exact stem match
            (
                ["OG_LEASE_CYCLE.dsv", "OG_LEASE_CYCLE.csv"],
                "OG_LEASE_CYCLE",
                "OG_LEASE_CYCLE.dsv",
            ),
        ],
        ids=[
            "exact_dsv",
            "exact_csv",
            "in_subdirectory",
            "case_insensitive",
            "not_found",
            "partial_match_fallback",
            "no_extension_bare_name",
            "multiple_candidates_first_exact",
        ],
    )
    def test_find_zip_entry(self, zip_contents, table_name, expected):
        result = TxRrcIngester._find_zip_entry(zip_contents, table_name)
        assert result == expected
