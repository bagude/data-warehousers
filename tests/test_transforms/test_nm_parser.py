"""Tests for pure/module-level helper functions in src.transforms.nm_parser."""

from __future__ import annotations

import math

import pytest

from src.transforms.nm_parser import (
    _normalize_api,
    _normalize_api_from_parts,
    _normalize_well_type,
    _normalize_well_status,
    _derive_basin,
    _normalize_county_from_dir,
)


# ---------------------------------------------------------------------------
# _normalize_api
# ---------------------------------------------------------------------------

class TestNormalizeApi:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            # Already formatted -- returned as-is
            ("30-015-12345", "30-015-12345"),
            # 10-digit string -> SS-CCC-WWWWW
            ("3001512345", "30-015-12345"),
            # 8-digit string (missing state code) -> prepends 30
            ("01512345", "30-015-12345"),
            # NaN -> None
            (float("nan"), None),
            # Empty string -> None
            ("", None),
            # 7 digits -> ambiguous length -> None
            ("0151234", None),
            # 12 digits -> too long -> None
            ("300151234567", None),
            # Dashes and spaces stripped before parsing
            ("30 015-12345", "30-015-12345"),
        ],
        ids=[
            "already_formatted",
            "ten_digit",
            "eight_digit_prepends_30",
            "nan_returns_none",
            "empty_string_returns_none",
            "seven_digit_ambiguous",
            "twelve_digit_too_long",
            "dashes_and_spaces_stripped",
        ],
    )
    def test_normalize_api(self, raw, expected):
        assert _normalize_api(raw) == expected


# ---------------------------------------------------------------------------
# _normalize_api_from_parts
# ---------------------------------------------------------------------------

class TestNormalizeApiFromParts:
    @pytest.mark.parametrize(
        "st, cnty, well, expected",
        [
            # Valid integer inputs
            (30, 15, 12345, "30-015-12345"),
            # String inputs that can be converted to int
            ("30", "15", "12345", "30-015-12345"),
            # Float inputs that can be converted to int via int(float(...))
            (30.0, 15.0, 12345.0, "30-015-12345"),
            # None for any part -> None (triggers ValueError/TypeError)
            (None, 15, 12345, None),
            # Non-numeric string -> None
            ("abc", 15, 12345, None),
            # State code out of range (>99) -> None
            (100, 15, 12345, None),
            # Well number out of range (>99999) -> None
            (30, 15, 100000, None),
        ],
        ids=[
            "valid_ints",
            "string_inputs",
            "float_inputs",
            "none_part_returns_none",
            "non_numeric_string",
            "state_out_of_range",
            "well_out_of_range",
        ],
    )
    def test_normalize_api_from_parts(self, st, cnty, well, expected):
        assert _normalize_api_from_parts(st, cnty, well) == expected


# ---------------------------------------------------------------------------
# _normalize_well_type
# ---------------------------------------------------------------------------

class TestNormalizeWellType:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("OIL", "OIL"),
            ("GAS", "GAS"),
            ("INJ", "INJ"),
            # SWD maps to INJ
            ("SWD", "INJ"),
            # Unknown code maps to OTHER
            ("UNKNOWN_CODE", "OTHER"),
            # NaN -> None
            (float("nan"), None),
            # Lowercase input -> case insensitive (uppercased before lookup)
            ("oil", "OIL"),
        ],
        ids=[
            "oil",
            "gas",
            "inj",
            "swd_maps_to_inj",
            "unknown_maps_to_other",
            "nan_returns_none",
            "lowercase_case_insensitive",
        ],
    )
    def test_normalize_well_type(self, raw, expected):
        assert _normalize_well_type(raw) == expected


# ---------------------------------------------------------------------------
# _normalize_well_status
# ---------------------------------------------------------------------------

class TestNormalizeWellStatus:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            # OCD single-char codes (checked first via _WC_STAT_MAP)
            ("A", "ACTIVE"),
            ("T", "SHUT-IN"),
            ("P", "P&A"),
            # Long-form codes (checked via _WELL_STATUS_MAP)
            ("ACTIVE", "ACTIVE"),
            ("SHUT-IN", "SHUT-IN"),
            # Unknown code -> OTHER
            ("UNKNOWN", "OTHER"),
            # NaN -> None
            (float("nan"), None),
        ],
        ids=[
            "ocd_char_A_active",
            "ocd_char_T_shutin",
            "ocd_char_P_pna",
            "long_form_active",
            "long_form_shutin",
            "unknown_maps_to_other",
            "nan_returns_none",
        ],
    )
    def test_normalize_well_status(self, raw, expected):
        assert _normalize_well_status(raw) == expected


# ---------------------------------------------------------------------------
# _derive_basin
# ---------------------------------------------------------------------------

class TestDeriveBasin:
    @pytest.mark.parametrize(
        "county, expected",
        [
            ("Eddy", "Permian"),
            ("San Juan", "San Juan"),
            ("Colfax", "Raton"),
            # Unmapped county -> None
            ("Bernalillo", None),
            # NaN -> None
            (float("nan"), None),
        ],
        ids=[
            "eddy_permian",
            "san_juan_san_juan",
            "colfax_raton",
            "bernalillo_unmapped",
            "nan_returns_none",
        ],
    )
    def test_derive_basin(self, county, expected):
        assert _derive_basin(county) == expected


# ---------------------------------------------------------------------------
# _normalize_county_from_dir
# ---------------------------------------------------------------------------

class TestNormalizeCountyFromDir:
    @pytest.mark.parametrize(
        "dirname, expected",
        [
            # GO-TECH directory patterns
            ("allwells_SanJuan", "San Juan"),
            ("allwells_Lea", "Lea"),
            ("allwells_RioArriba", "Rio Arriba"),
            # Plain county name (in _COUNTY_TO_BASIN)
            ("Eddy", "Eddy"),
            # Empty string -> None
            ("", None),
        ],
        ids=[
            "allwells_san_juan",
            "allwells_lea",
            "allwells_rio_arriba",
            "plain_eddy",
            "empty_string_returns_none",
        ],
    )
    def test_normalize_county_from_dir(self, dirname, expected):
        assert _normalize_county_from_dir(dirname) == expected
