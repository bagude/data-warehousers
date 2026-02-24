"""Tests for pure/module-level helper functions in src.transforms.ok_parser."""

from __future__ import annotations

import pytest

from src.transforms.ok_parser import (
    _normalize_api,
    _normalize_well_type,
    _normalize_well_status,
    _parse_county_code,
    _derive_basin,
)


# ---------------------------------------------------------------------------
# _normalize_api
# ---------------------------------------------------------------------------

class TestNormalizeApi:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            # Already formatted -- returned as-is
            ("35-017-12345", "35-017-12345"),
            # 10-digit string -> SS-CCC-WWWWW
            ("3501712345", "35-017-12345"),
            # 8-digit string (missing state code) -> prepends 35
            ("01712345", "35-017-12345"),
            # NaN -> None
            (float("nan"), None),
            # Empty string -> None
            ("", None),
            # 7 digits -> ambiguous length -> None
            ("0171234", None),
            # 12 digits -> too long -> None
            ("350171234567", None),
            # Dashes and spaces stripped before parsing
            ("35 017-12345", "35-017-12345"),
            # Float representation (from ArcGIS numeric API field)
            ("3501712345.0", "35-017-12345"),
            # Dots stripped
            ("35.017.12345", "35-017-12345"),
        ],
        ids=[
            "already_formatted",
            "ten_digit",
            "eight_digit_prepends_35",
            "nan_returns_none",
            "empty_string_returns_none",
            "seven_digit_ambiguous",
            "twelve_digit_too_long",
            "dashes_and_spaces_stripped",
            "float_representation",
            "dots_stripped",
        ],
    )
    def test_normalize_api(self, raw, expected):
        assert _normalize_api(raw) == expected


# ---------------------------------------------------------------------------
# _normalize_well_type
# ---------------------------------------------------------------------------

class TestNormalizeWellType:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("OIL", "OIL"),
            ("GAS", "GAS"),
            ("OIL/GAS", "OIL"),
            ("UIC", "INJ"),
            ("WATER_INJECTION", "INJ"),
            ("DRY", "OTHER"),
            ("PLUGGED", "OTHER"),
            # Unknown code maps to OTHER
            ("UNKNOWN_CODE", "OTHER"),
            # NaN -> None
            (float("nan"), None),
            # Lowercase input -> case insensitive
            ("oil", "OIL"),
            ("gas", "GAS"),
        ],
        ids=[
            "oil",
            "gas",
            "oil_gas",
            "uic_maps_to_inj",
            "water_injection_maps_to_inj",
            "dry_maps_to_other",
            "plugged_maps_to_other",
            "unknown_maps_to_other",
            "nan_returns_none",
            "lowercase_oil",
            "lowercase_gas",
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
            ("ACTIVE", "ACTIVE"),
            ("SHUT-IN", "SHUT-IN"),
            ("P&A", "P&A"),
            ("PLUGGED", "P&A"),
            ("TEMPORARILY_ABANDONED", "SHUT-IN"),
            ("TERMINATED", "INACTIVE"),
            ("ORPHAN", "INACTIVE"),
            ("DRY", "P&A"),
            # Unknown code -> OTHER
            ("UNKNOWN", "OTHER"),
            # NaN -> None
            (float("nan"), None),
        ],
        ids=[
            "active",
            "shutin",
            "pna",
            "plugged_maps_to_pna",
            "temp_abandoned_maps_to_shutin",
            "terminated_maps_to_inactive",
            "orphan_maps_to_inactive",
            "dry_maps_to_pna",
            "unknown_maps_to_other",
            "nan_returns_none",
        ],
    )
    def test_normalize_well_status(self, raw, expected):
        assert _normalize_well_status(raw) == expected


# ---------------------------------------------------------------------------
# _parse_county_code
# ---------------------------------------------------------------------------

class TestParseCountyCode:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("003-ADAIR", "ADAIR"),
            ("009-BECKHAM", "BECKHAM"),
            ("113-ROGER MILLS", "ROGER MILLS"),
            # No dash — upper-case the raw value
            ("Caddo", "CADDO"),
            # NaN -> None
            (float("nan"), None),
            # Empty -> None
            ("", None),
        ],
        ids=[
            "adair_with_code",
            "beckham_with_code",
            "roger_mills_with_code",
            "plain_county_name",
            "nan_returns_none",
            "empty_returns_none",
        ],
    )
    def test_parse_county_code(self, raw, expected):
        assert _parse_county_code(raw) == expected


# ---------------------------------------------------------------------------
# _derive_basin
# ---------------------------------------------------------------------------

class TestDeriveBasin:
    @pytest.mark.parametrize(
        "county, expected",
        [
            ("Woodward", "Anadarko"),
            ("Canadian", "Anadarko"),
            ("Caddo", "Anadarko"),
            ("Pittsburg", "Arkoma"),
            ("Latimer", "Arkoma"),
            ("Carter", "Ardmore"),
            ("Garfield", "Cherokee Platform"),
            ("Osage", "Cherokee Platform"),
            ("Beaver", "Hugoton"),
            ("Cimarron", "Hugoton"),
            # Unmapped county -> None
            ("Tulsa", None),
            # NaN -> None
            (float("nan"), None),
        ],
        ids=[
            "woodward_anadarko",
            "canadian_anadarko",
            "caddo_anadarko",
            "pittsburg_arkoma",
            "latimer_arkoma",
            "carter_ardmore",
            "garfield_cherokee_platform",
            "osage_cherokee_platform",
            "beaver_hugoton",
            "cimarron_hugoton",
            "tulsa_unmapped",
            "nan_returns_none",
        ],
    )
    def test_derive_basin(self, county, expected):
        assert _derive_basin(county) == expected
