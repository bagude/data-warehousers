"""Tests for src.schemas.validation — value-range and format validation rules."""

from __future__ import annotations

import datetime

import numpy as np
import pandas as pd
import pytest

from src.schemas.validation import (
    check_non_negative,
    check_days_produced_range,
    check_production_date_range,
    check_api_number_format,
    check_state_values,
    check_entity_type_values,
    check_lease_fields_for_lease_entities,
    validate_batch,
    get_valid_rows,
)


# ===================================================================
# check_non_negative
# ===================================================================

class TestCheckNonNegative:
    """Tests for check_non_negative()."""

    def test_column_not_present_returns_all_true(self):
        df = pd.DataFrame({"other_col": [1, 2, 3]})
        result = check_non_negative(df, "oil_bbl")
        assert result.all()
        assert len(result) == 3

    def test_all_positive_values(self):
        df = pd.DataFrame({"oil_bbl": [0.0, 100.5, 9999.9]})
        result = check_non_negative(df, "oil_bbl")
        assert result.all()

    def test_negative_value_flagged(self):
        df = pd.DataFrame({"oil_bbl": [10.0, -5.0, 20.0]})
        result = check_non_negative(df, "oil_bbl")
        assert result.tolist() == [True, False, True]

    def test_nan_is_allowed(self):
        df = pd.DataFrame({"oil_bbl": [np.nan, np.nan]})
        result = check_non_negative(df, "oil_bbl")
        assert result.all()

    def test_mix_of_valid_negative_and_nan(self):
        df = pd.DataFrame({"gas_mcf": [100.0, -1.0, np.nan, 50.0, -0.5]})
        result = check_non_negative(df, "gas_mcf")
        assert result.tolist() == [True, False, True, True, False]


# ===================================================================
# check_days_produced_range
# ===================================================================

class TestCheckDaysProducedRange:
    """Tests for check_days_produced_range()."""

    def test_column_not_present_returns_all_true(self):
        df = pd.DataFrame({"other_col": [1, 2, 3]})
        result = check_days_produced_range(df)
        assert result.all()
        assert len(result) == 3

    def test_valid_range(self):
        df = pd.DataFrame({"days_produced": [0, 15, 31]})
        result = check_days_produced_range(df)
        assert result.all()

    def test_value_32_is_invalid(self):
        df = pd.DataFrame({"days_produced": [10, 32]})
        result = check_days_produced_range(df)
        assert result.tolist() == [True, False]

    def test_negative_value_is_invalid(self):
        df = pd.DataFrame({"days_produced": [-1, 5]})
        result = check_days_produced_range(df)
        assert result.tolist() == [False, True]

    def test_nan_is_allowed(self):
        df = pd.DataFrame({"days_produced": [np.nan, 10.0]})
        result = check_days_produced_range(df)
        assert result.all()

    def test_value_zero_is_valid(self):
        df = pd.DataFrame({"days_produced": [0]})
        result = check_days_produced_range(df)
        assert result.tolist() == [True]


# ===================================================================
# check_production_date_range
# ===================================================================

class TestCheckProductionDateRange:
    """Tests for check_production_date_range()."""

    def test_column_not_present_returns_all_true(self):
        df = pd.DataFrame({"other_col": [1, 2]})
        result = check_production_date_range(df)
        assert result.all()
        assert len(result) == 2

    def test_valid_date(self):
        df = pd.DataFrame({"production_date": [datetime.date(2024, 1, 1)]})
        result = check_production_date_range(df)
        assert result.all()

    def test_future_date_is_invalid(self):
        df = pd.DataFrame({"production_date": [datetime.date(2099, 1, 1)]})
        result = check_production_date_range(df)
        assert result.tolist() == [False]

    def test_date_before_1900_is_invalid(self):
        df = pd.DataFrame({"production_date": [datetime.date(1899, 12, 31)]})
        result = check_production_date_range(df)
        assert result.tolist() == [False]

    def test_date_exactly_1900_is_valid(self):
        df = pd.DataFrame({"production_date": [datetime.date(1900, 1, 1)]})
        result = check_production_date_range(df)
        assert result.tolist() == [True]


# ===================================================================
# check_api_number_format
# ===================================================================

class TestCheckApiNumberFormat:
    """Tests for check_api_number_format()."""

    def test_column_not_present_returns_all_true(self):
        df = pd.DataFrame({"other_col": [1, 2]})
        result = check_api_number_format(df)
        assert result.all()
        assert len(result) == 2

    def test_valid_format(self):
        df = pd.DataFrame({"api_number": ["42-123-45678"]})
        result = check_api_number_format(df)
        assert result.tolist() == [True]

    def test_nan_is_allowed(self):
        df = pd.DataFrame({"api_number": [np.nan]})
        result = check_api_number_format(df)
        assert result.tolist() == [True]

    def test_missing_dashes(self):
        df = pd.DataFrame({"api_number": ["4212345678"]})
        result = check_api_number_format(df)
        assert result.tolist() == [False]

    def test_too_short(self):
        df = pd.DataFrame({"api_number": ["42-12-4567"]})
        result = check_api_number_format(df)
        assert result.tolist() == [False]

    def test_letters_are_invalid(self):
        df = pd.DataFrame({"api_number": ["AB-123-45678"]})
        result = check_api_number_format(df)
        assert result.tolist() == [False]

    def test_valid_nm_format(self):
        df = pd.DataFrame({"api_number": ["30-015-12345"]})
        result = check_api_number_format(df)
        assert result.tolist() == [True]


# ===================================================================
# check_state_values
# ===================================================================

class TestCheckStateValues:
    """Tests for check_state_values()."""

    def test_column_not_present_returns_all_true(self):
        df = pd.DataFrame({"other_col": [1]})
        result = check_state_values(df)
        assert result.all()

    def test_tx_is_valid(self):
        df = pd.DataFrame({"state": ["TX"]})
        result = check_state_values(df)
        assert result.tolist() == [True]

    def test_nm_is_valid(self):
        df = pd.DataFrame({"state": ["NM"]})
        result = check_state_values(df)
        assert result.tolist() == [True]

    def test_ca_is_invalid(self):
        df = pd.DataFrame({"state": ["CA"]})
        result = check_state_values(df)
        assert result.tolist() == [False]

    def test_empty_string_is_invalid(self):
        df = pd.DataFrame({"state": [""]})
        result = check_state_values(df)
        assert result.tolist() == [False]


# ===================================================================
# check_entity_type_values
# ===================================================================

class TestCheckEntityTypeValues:
    """Tests for check_entity_type_values()."""

    def test_column_not_present_returns_all_true(self):
        df = pd.DataFrame({"other_col": [1]})
        result = check_entity_type_values(df)
        assert result.all()

    def test_well_is_valid(self):
        df = pd.DataFrame({"entity_type": ["well"]})
        result = check_entity_type_values(df)
        assert result.tolist() == [True]

    def test_lease_is_valid(self):
        df = pd.DataFrame({"entity_type": ["lease"]})
        result = check_entity_type_values(df)
        assert result.tolist() == [True]

    def test_uppercase_well_is_invalid(self):
        df = pd.DataFrame({"entity_type": ["WELL"]})
        result = check_entity_type_values(df)
        assert result.tolist() == [False]

    def test_other_is_invalid(self):
        df = pd.DataFrame({"entity_type": ["other"]})
        result = check_entity_type_values(df)
        assert result.tolist() == [False]


# ===================================================================
# check_lease_fields_for_lease_entities
# ===================================================================

class TestCheckLeaseFieldsForLeaseEntities:
    """Tests for check_lease_fields_for_lease_entities()."""

    def test_entity_type_not_present_returns_all_true(self):
        df = pd.DataFrame({"other_col": [1, 2]})
        result = check_lease_fields_for_lease_entities(df)
        assert result.all()
        assert len(result) == 2

    def test_well_without_lease_number_passes(self):
        df = pd.DataFrame({
            "entity_type": ["well"],
            "lease_number": [np.nan],
        })
        result = check_lease_fields_for_lease_entities(df)
        assert result.tolist() == [True]

    def test_lease_with_lease_number_passes(self):
        df = pd.DataFrame({
            "entity_type": ["lease"],
            "lease_number": ["12345"],
        })
        result = check_lease_fields_for_lease_entities(df)
        assert result.tolist() == [True]

    def test_lease_without_lease_number_fails(self):
        df = pd.DataFrame({
            "entity_type": ["lease"],
            "lease_number": [np.nan],
        })
        result = check_lease_fields_for_lease_entities(df)
        assert result.tolist() == [False]

    def test_mix_of_well_and_lease_rows(self):
        df = pd.DataFrame({
            "entity_type": ["well", "lease", "lease", "well"],
            "lease_number": [np.nan, "ABC", np.nan, "XYZ"],
        })
        result = check_lease_fields_for_lease_entities(df)
        # well with nan -> True, lease with value -> True,
        # lease with nan -> False, well with value -> True
        assert result.tolist() == [True, True, False, True]


# ===================================================================
# validate_batch
# ===================================================================

class TestValidateBatch:
    """Tests for validate_batch()."""

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = validate_batch(df)
        assert result.total_rows == 0
        assert result.failed_rows == 0
        assert result.passed is True
        assert result.pass_rate == 1.0
        assert result.errors == []

    def test_all_valid_rows(self):
        df = pd.DataFrame({
            "state": ["TX", "TX"],
            "entity_type": ["well", "well"],
            "api_number": ["42-123-45678", "42-456-78901"],
            "production_date": [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)],
            "oil_bbl": [100.0, 200.0],
            "gas_mcf": [50.0, 60.0],
        })
        result = validate_batch(df)
        assert result.passed is True
        assert result.pass_rate == 1.0
        assert result.failed_rows == 0
        assert len(result.errors) == 0

    def test_single_failure_negative_oil(self):
        df = pd.DataFrame({
            "state": ["TX", "TX"],
            "entity_type": ["well", "well"],
            "api_number": ["42-123-45678", "42-456-78901"],
            "production_date": [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)],
            "oil_bbl": [100.0, -5.0],
            "gas_mcf": [50.0, 60.0],
        })
        result = validate_batch(df)
        assert result.failed_rows == 1
        assert len(result.errors) == 1
        assert result.errors[0].check_name == "non_negative_volumes"
        assert result.errors[0].column == "oil_bbl"
        assert result.errors[0].bad_row_count == 1

    def test_multiple_different_failures(self):
        df = pd.DataFrame({
            "state": ["TX", "CA"],  # row 1 has invalid state
            "entity_type": ["well", "well"],
            "api_number": ["42-123-45678", "42-456-78901"],
            "production_date": [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)],
            "oil_bbl": [-10.0, 200.0],  # row 0 has negative oil
            "gas_mcf": [50.0, 60.0],
        })
        result = validate_batch(df)
        # Row 0 fails non_negative (oil_bbl), row 1 fails state_values
        assert result.failed_rows == 2
        check_names = {e.check_name for e in result.errors}
        assert "non_negative_volumes" in check_names
        assert "state_values" in check_names

    def test_pass_rate_calculation_partial_failures(self):
        df = pd.DataFrame({
            "state": ["TX", "TX", "TX", "TX"],
            "entity_type": ["well", "well", "well", "well"],
            "oil_bbl": [100.0, -5.0, 200.0, 300.0],
            "production_date": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 2, 1),
                datetime.date(2024, 3, 1),
                datetime.date(2024, 4, 1),
            ],
        })
        result = validate_batch(df)
        # 1 out of 4 rows failed
        assert result.total_rows == 4
        assert result.failed_rows == 1
        assert result.pass_rate == pytest.approx(0.75)

    def test_passed_is_false_when_failures_exist(self):
        df = pd.DataFrame({
            "state": ["TX"],
            "entity_type": ["well"],
            "oil_bbl": [-1.0],
            "production_date": [datetime.date(2024, 1, 1)],
        })
        result = validate_batch(df)
        assert result.passed is False

    def test_max_sample_values_limits_samples(self):
        # Create 10 rows all with negative oil_bbl
        df = pd.DataFrame({
            "state": ["TX"] * 10,
            "entity_type": ["well"] * 10,
            "oil_bbl": [-float(i) for i in range(1, 11)],
            "production_date": [datetime.date(2024, 1, 1)] * 10,
        })
        result = validate_batch(df, max_sample_values=3)
        oil_error = [e for e in result.errors if e.column == "oil_bbl"][0]
        assert len(oil_error.sample_values) <= 3


# ===================================================================
# get_valid_rows
# ===================================================================

class TestGetValidRows:
    """Tests for get_valid_rows()."""

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame()
        result = get_valid_rows(df)
        assert result.empty

    def test_all_valid_returns_all_rows(self):
        df = pd.DataFrame({
            "state": ["TX", "NM"],
            "entity_type": ["well", "well"],
            "api_number": ["42-123-45678", "30-015-12345"],
            "production_date": [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)],
            "oil_bbl": [100.0, 200.0],
            "gas_mcf": [50.0, 60.0],
        })
        result = get_valid_rows(df)
        assert len(result) == 2

    def test_invalid_rows_are_filtered_out(self):
        df = pd.DataFrame({
            "state": ["TX", "TX", "TX"],
            "entity_type": ["well", "well", "well"],
            "api_number": ["42-123-45678", "42-456-78901", "42-789-01234"],
            "production_date": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 2, 1),
                datetime.date(2024, 3, 1),
            ],
            "oil_bbl": [100.0, -5.0, 300.0],
            "gas_mcf": [50.0, 60.0, 70.0],
        })
        result = get_valid_rows(df)
        assert len(result) == 2
        # The valid rows should have oil_bbl 100.0 and 300.0
        assert result["oil_bbl"].tolist() == [100.0, 300.0]

    def test_returned_dataframe_has_reset_index(self):
        df = pd.DataFrame({
            "state": ["TX", "TX", "TX"],
            "entity_type": ["well", "well", "well"],
            "production_date": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 2, 1),
                datetime.date(2024, 3, 1),
            ],
            "oil_bbl": [100.0, -5.0, 300.0],
        })
        result = get_valid_rows(df)
        # After filtering, index should be reset to 0-based
        assert list(result.index) == [0, 1]
