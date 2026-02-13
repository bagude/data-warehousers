"""Tests for src.quality.checks — layer-boundary quality checks."""

import datetime
from datetime import date

import pandas as pd
import pytest

from src.quality.checks import (
    CheckResult,
    QualityReport,
    check_dedup_integrity,
    check_required_fields,
    check_value_ranges,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_silver_df(**overrides) -> pd.DataFrame:
    """Build a minimal valid silver DataFrame with optional column overrides."""
    base = {
        "state": ["TX", "TX"],
        "entity_type": ["well", "well"],
        "api_number": ["42-123-45678", "42-123-45679"],
        "lease_number": [None, None],
        "production_date": [date(2024, 1, 1), date(2024, 2, 1)],
        "oil_bbl": [100.0, 200.0],
        "gas_mcf": [50.0, 75.0],
        "condensate_bbl": [0.0, 0.0],
        "casinghead_gas_mcf": [0.0, 0.0],
        "water_bbl": [10.0, 20.0],
    }
    base.update(overrides)
    return pd.DataFrame(base)


# ===================================================================
# check_required_fields
# ===================================================================

class TestCheckRequiredFields:
    """Tests for check_required_fields()."""

    def test_all_required_fields_populated(self):
        """All required fields present and non-null -> passed=True."""
        df = _make_silver_df()
        result = check_required_fields(df)

        assert result.passed is True
        assert result.name == "required_fields"
        assert result.details["null_counts"] == {}

    def test_missing_state_for_some_rows(self):
        """Some rows have null state -> passed=False, details show null count."""
        df = _make_silver_df(state=["TX", None])
        result = check_required_fields(df)

        assert result.passed is False
        assert "state" in result.details["null_counts"]
        assert result.details["null_counts"]["state"] == 1
        assert "state" in result.message

    def test_well_entity_type_missing_api_number(self):
        """Well entity_type with null api_number -> passed=False."""
        df = _make_silver_df(
            entity_type=["well", "well"],
            api_number=["42-123-45678", None],
        )
        result = check_required_fields(df)

        assert result.passed is False
        assert "api_number (wells)" in result.details["null_counts"]
        assert result.details["null_counts"]["api_number (wells)"] == 1

    def test_lease_entity_type_missing_lease_number(self):
        """Lease entity_type with null lease_number -> passed=False."""
        df = _make_silver_df(
            entity_type=["lease", "lease"],
            api_number=[None, None],
            lease_number=["12345", None],
        )
        result = check_required_fields(df)

        assert result.passed is False
        assert "lease_number (leases)" in result.details["null_counts"]
        assert result.details["null_counts"]["lease_number (leases)"] == 1


# ===================================================================
# check_value_ranges
# ===================================================================

class TestCheckValueRanges:
    """Tests for check_value_ranges()."""

    def test_all_valid_data(self):
        """All values within valid ranges -> passed=True."""
        df = _make_silver_df()
        result = check_value_ranges(df)

        assert result.passed is True
        assert result.name == "value_ranges"
        assert result.details["failed_rows"] == 0
        assert result.details["errors"] == {}

    def test_negative_volume(self):
        """Negative oil_bbl value -> passed=False."""
        df = _make_silver_df(oil_bbl=[-100.0, 200.0])
        result = check_value_ranges(df)

        assert result.passed is False
        assert result.details["failed_rows"] > 0
        # The errors dict should contain info about the non_negative_volumes check
        assert "non_negative_volumes" in result.details["errors"]
        assert result.details["errors"]["non_negative_volumes"]["column"] == "oil_bbl"

    def test_bad_api_format(self):
        """API number not matching NN-NNN-NNNNN -> passed=False with error info."""
        df = _make_silver_df(api_number=["BADFORMAT", "42-123-45679"])
        result = check_value_ranges(df)

        assert result.passed is False
        assert "api_number_format" in result.details["errors"]
        error_info = result.details["errors"]["api_number_format"]
        assert error_info["column"] == "api_number"
        assert error_info["bad_row_count"] >= 1
        assert "message" in error_info


# ===================================================================
# check_dedup_integrity
# ===================================================================

class TestCheckDedupIntegrity:
    """Tests for check_dedup_integrity()."""

    def test_no_duplicates(self):
        """Unique records on dedup keys -> passed=True."""
        df = _make_silver_df()
        result = check_dedup_integrity(df)

        assert result.passed is True
        assert result.name == "dedup_integrity"
        assert result.details == {}

    def test_well_duplicates(self):
        """Duplicate well records (same state+api+date) -> passed=False."""
        df = _make_silver_df(
            state=["TX", "TX"],
            entity_type=["well", "well"],
            api_number=["42-123-45678", "42-123-45678"],
            production_date=[date(2024, 1, 1), date(2024, 1, 1)],
        )
        result = check_dedup_integrity(df)

        assert result.passed is False
        assert "well_duplicates" in result.details
        assert result.details["well_duplicates"] == 2  # Both rows flagged by keep=False

    def test_lease_duplicates(self):
        """Duplicate lease records (same state+lease+district+date) -> passed=False."""
        df = pd.DataFrame({
            "state": ["TX", "TX"],
            "entity_type": ["lease", "lease"],
            "api_number": [None, None],
            "lease_number": ["L001", "L001"],
            "district": ["01", "01"],
            "production_date": [date(2024, 1, 1), date(2024, 1, 1)],
            "oil_bbl": [100.0, 150.0],
        })
        result = check_dedup_integrity(df)

        assert result.passed is False
        assert "lease_duplicates" in result.details
        assert result.details["lease_duplicates"] == 2

    def test_no_entity_type_column(self):
        """DataFrame without entity_type column -> passed=True (guard clause)."""
        df = pd.DataFrame({
            "state": ["TX", "TX"],
            "api_number": ["42-123-45678", "42-123-45679"],
            "production_date": [date(2024, 1, 1), date(2024, 2, 1)],
            "oil_bbl": [100.0, 200.0],
        })
        result = check_dedup_integrity(df)

        assert result.passed is True
        assert result.details == {}


# ===================================================================
# QualityReport property tests
# ===================================================================

def _make_check(name: str, passed: bool) -> CheckResult:
    """Create a CheckResult with the given pass/fail status."""
    return CheckResult(
        name=name,
        passed=passed,
        message=f"{'OK' if passed else 'FAIL'}: {name}",
    )


def _make_report(checks: list[CheckResult]) -> QualityReport:
    """Create a QualityReport wrapping the given CheckResults."""
    return QualityReport(
        layer="bronze_to_silver",
        state="TX",
        timestamp=datetime.datetime(2024, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
        total_rows=100,
        checks=checks,
    )


class TestQualityReport:
    """Tests for QualityReport properties and summary()."""

    def test_all_checks_passed(self):
        """All checks passed -> report.passed is True."""
        report = _make_report([
            _make_check("check_a", True),
            _make_check("check_b", True),
            _make_check("check_c", True),
        ])
        assert report.passed is True

    def test_one_check_failed(self):
        """One check failed -> report.passed is False."""
        report = _make_report([
            _make_check("check_a", True),
            _make_check("check_b", False),
            _make_check("check_c", True),
        ])
        assert report.passed is False

    def test_failed_checks_returns_only_failed(self):
        """failed_checks returns only the CheckResult items that failed."""
        c_pass = _make_check("check_a", True)
        c_fail_1 = _make_check("check_b", False)
        c_fail_2 = _make_check("check_c", False)

        report = _make_report([c_pass, c_fail_1, c_fail_2])
        failed = report.failed_checks

        assert len(failed) == 2
        assert c_fail_1 in failed
        assert c_fail_2 in failed
        assert c_pass not in failed

    def test_summary_contains_passed_when_all_pass(self):
        """summary() contains 'PASSED' when every check passes."""
        report = _make_report([
            _make_check("check_a", True),
            _make_check("check_b", True),
        ])
        summary = report.summary()

        assert "PASSED" in summary
        assert "FAILED" not in summary

    def test_summary_contains_failed_and_counts(self):
        """summary() contains 'FAILED' and correct pass/fail counts when some fail."""
        report = _make_report([
            _make_check("check_a", True),
            _make_check("check_b", False),
            _make_check("check_c", False),
        ])
        summary = report.summary()

        assert "FAILED" in summary
        # 1 passed out of 3 total, 2 failed
        assert "1/3 checks passed" in summary
        assert "2 failed" in summary
