"""Tests for src.schemas.field_mappings — per-state field mapping helpers."""

from __future__ import annotations

import pytest

from src.schemas.field_mappings import get_source_columns, get_null_fields


# ===================================================================
# get_source_columns
# ===================================================================

class TestGetSourceColumns:
    """Tests for get_source_columns()."""

    def test_tx_returns_expected_columns(self):
        cols = get_source_columns("TX")
        # Spot-check key TX source columns
        assert "LEASE_OIL_PROD_VOL" in cols
        assert "LEASE_GAS_PROD_VOL" in cols
        assert "API_NO" in cols
        assert "LEASE_NO" in cols
        assert "DISTRICT_NO" in cols
        assert "OPERATOR_NAME" in cols

    def test_nm_returns_expected_columns(self):
        cols = get_source_columns("NM")
        # Spot-check key NM source columns
        assert "id" in cols
        assert "Oil" in cols
        assert "Gas" in cols
        assert "Water" in cols
        assert "Days" in cols
        assert "well_name" in cols
        assert "ogrid_name" in cols

    def test_tx_does_not_include_none_sources(self):
        cols = get_source_columns("TX")
        # water_bbl has source=None for TX, so None should not appear
        assert None not in cols
        # Also verify that fields which are known to have source=None
        # do not contribute any column name
        # (water_bbl and days_produced are NULL for TX)

    def test_nm_does_not_include_none_sources(self):
        cols = get_source_columns("NM")
        assert None not in cols

    def test_list_sources_are_flattened(self):
        cols = get_source_columns("TX")
        # production_date has source=["CYCLE_YEAR", "CYCLE_MONTH"] for TX
        assert "CYCLE_YEAR" in cols
        assert "CYCLE_MONTH" in cols

    def test_invalid_state_raises_key_error(self):
        with pytest.raises(KeyError):
            get_source_columns("CA")


# ===================================================================
# get_null_fields
# ===================================================================

class TestGetNullFields:
    """Tests for get_null_fields()."""

    def test_tx_includes_water_bbl(self):
        null_fields = get_null_fields("TX")
        assert "water_bbl" in null_fields

    def test_tx_includes_days_produced(self):
        null_fields = get_null_fields("TX")
        assert "days_produced" in null_fields

    def test_nm_includes_condensate_bbl(self):
        null_fields = get_null_fields("NM")
        assert "condensate_bbl" in null_fields

    def test_nm_includes_casinghead_gas_mcf(self):
        null_fields = get_null_fields("NM")
        assert "casinghead_gas_mcf" in null_fields

    def test_fields_with_transform_not_included(self):
        # state has source=None but transform="constant:TX", so it should
        # NOT be in the null fields list (it has a non-None transform)
        tx_null_fields = get_null_fields("TX")
        assert "state" not in tx_null_fields

        nm_null_fields = get_null_fields("NM")
        assert "state" not in nm_null_fields
        assert "entity_type" not in nm_null_fields
