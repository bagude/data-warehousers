"""Per-state field mapping configurations for bronze-to-silver transforms.

Each state has a mapping dict that tells the parser how to translate raw
source column names into the common silver-layer schema defined in
``production.py``.

Mapping entries
===============

Each key in the mapping dict is a common schema field name.  The value is a
``FieldMapping`` describing how to derive that field from the source data:

- ``source``: Raw column name(s) in the bronze file.  A single string for
  direct 1:1 mappings; a list of strings when multiple source columns must be
  combined (e.g. CYCLE_YEAR + CYCLE_MONTH -> production_date).
- ``transform``: Optional callable or string identifier describing the
  transformation to apply.  ``None`` means direct copy (possibly with a type
  cast).  Named transforms (strings) are resolved by the parser at runtime.
- ``dtype``: Target PyArrow / pandas dtype for the field.
- ``nullable``: Whether NULL is expected/allowed for this state.
- ``notes``: Human-readable notes explaining the mapping or known caveats.

NULL conventions
================

When ``source`` is ``None``, the field is not available from that state's
data and the parser should fill it with NULL.  This is distinct from a field
that *is* available but happens to be zero.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FieldMapping:
    """Describes how one common-schema field is derived from a source dataset."""

    source: str | list[str] | None
    """Raw column name(s) in the bronze file, or None if not available."""

    transform: str | None = None
    """Named transform to apply: None = direct copy, or a string like
    'combine_year_month', 'normalize_api', etc."""

    dtype: str = "string"
    """Target data type (pandas/pyarrow type name)."""

    nullable: bool = True
    """Whether NULL values are expected for this field from this state."""

    notes: str = ""
    """Human-readable explanation of the mapping or caveats."""


# ===================================================================
# Texas RRC — PDQ CSV Dump
# ===================================================================
# Primary production table: OG_LEASE_CYCLE
# Well-level reference:     OG_WELL_COMPLETION
# Operator reference:       OG_OPERATOR_DW
#
# Key join: LEASE_NO + DISTRICT_NO links OG_LEASE_CYCLE to OG_WELL_COMPLETION.
# Oil production is at *lease* level; gas is effectively well-level (1:1).
# ===================================================================

TX_FIELD_MAPPING: dict[str, FieldMapping] = {
    "state": FieldMapping(
        source=None,
        transform="constant:TX",
        dtype="string",
        nullable=False,
        notes="Hard-coded to 'TX' for all Texas records.",
    ),
    "entity_type": FieldMapping(
        source=None,
        transform="derive_entity_type",
        dtype="string",
        nullable=False,
        notes=(
            "Derived from well type: gas wells are 'well' (1:1 lease-to-well), "
            "oil leases are 'lease' (many wells per lease). When a lease has "
            "only one well, entity_type may be set to 'well' with the API number."
        ),
    ),
    "api_number": FieldMapping(
        source="API_NO",
        transform="normalize_api",
        dtype="string",
        nullable=True,
        notes=(
            "From OG_WELL_COMPLETION via LEASE_NO + DISTRICT_NO join. "
            "Format: 42-CCC-WWWWW (state code 42 = Texas). "
            "NULL for multi-well oil leases where a single API cannot represent "
            "the lease."
        ),
    ),
    "lease_number": FieldMapping(
        source="LEASE_NO",
        dtype="string",
        nullable=True,
        notes="TX RRC lease number. Primary identifier for lease-level records.",
    ),
    "district": FieldMapping(
        source="DISTRICT_NO",
        dtype="string",
        nullable=True,
        notes=(
            "TX RRC district number. Part of the composite key with LEASE_NO. "
            "Values: 01-12 plus suffixed districts (6E, 7B, 7C, 8A, 9A)."
        ),
    ),
    "well_name": FieldMapping(
        source="LEASE_NAME",
        dtype="string",
        notes="Lease name from OG_LEASE_CYCLE or well name from OG_WELL_COMPLETION.",
    ),
    "operator": FieldMapping(
        source="OPERATOR_NAME",
        dtype="string",
        notes=(
            "Operator name from OG_LEASE_CYCLE. Can also be joined via "
            "OPERATOR_NO to OG_OPERATOR_DW for canonical name."
        ),
    ),
    "county": FieldMapping(
        source="COUNTY_CODE",
        transform="tx_county_code_to_name",
        dtype="string",
        notes=(
            "County code from OG_WELL_COMPLETION, translated to county name "
            "via TX RRC county code lookup. Requires join to well table."
        ),
    ),
    "field_name": FieldMapping(
        source="FIELD_NAME",
        dtype="string",
        notes="Field name from OG_LEASE_CYCLE (via FIELD_NO).",
    ),
    "basin": FieldMapping(
        source=None,
        transform="derive_basin_from_county_or_field",
        dtype="string",
        nullable=True,
        notes=(
            "Not directly in TX RRC data. Derived from county or field name "
            "using a lookup table. May be NULL if mapping is unavailable."
        ),
    ),
    "well_type": FieldMapping(
        source="WELL_TYPE_CODE",
        transform="normalize_well_type",
        dtype="string",
        notes=(
            "From OG_WELL_COMPLETION. TX codes include: OL=Oil, GW=Gas, "
            "GC=Gas Condensate, IN=Injection, etc. Normalized to standard "
            "values: 'OIL', 'GAS', 'GAS_CONDENSATE', 'INJ', 'OTHER'."
        ),
    ),
    "well_status": FieldMapping(
        source="WELL_STATUS",
        transform="normalize_well_status",
        dtype="string",
        notes=(
            "From OG_WELL_COMPLETION. Normalized to: 'ACTIVE', 'SHUT-IN', "
            "'P&A', 'INACTIVE', 'OTHER'."
        ),
    ),
    "production_date": FieldMapping(
        source=["CYCLE_YEAR", "CYCLE_MONTH"],
        transform="combine_year_month",
        dtype="date32",
        nullable=False,
        notes=(
            "Constructed as the first day of the month from CYCLE_YEAR (int, "
            "e.g. 2025) and CYCLE_MONTH (int, 1-12). Result: date(2025, 1, 1)."
        ),
    ),
    "oil_bbl": FieldMapping(
        source="LEASE_OIL_PROD_VOL",
        dtype="float64",
        notes=(
            "Crude oil production in barrels from OG_LEASE_CYCLE. "
            "Does NOT include condensate (reported separately)."
        ),
    ),
    "gas_mcf": FieldMapping(
        source="LEASE_GAS_PROD_VOL",
        dtype="float64",
        notes=(
            "Gas well gas production in MCF from OG_LEASE_CYCLE. "
            "Does NOT include casinghead gas (reported separately)."
        ),
    ),
    "condensate_bbl": FieldMapping(
        source="LEASE_COND_PROD_VOL",
        dtype="float64",
        notes=(
            "Condensate production in barrels from OG_LEASE_CYCLE. "
            "Condensate = liquid hydrocarbons recovered from gas wells. "
            "TX reports this separately from crude oil."
        ),
    ),
    "casinghead_gas_mcf": FieldMapping(
        source="LEASE_CSGD_PROD_VOL",
        dtype="float64",
        notes=(
            "Casinghead gas production in MCF from OG_LEASE_CYCLE. "
            "Casinghead gas = gas produced from oil wells. "
            "TX reports this separately from gas-well gas."
        ),
    ),
    "water_bbl": FieldMapping(
        source=None,
        dtype="float64",
        nullable=True,
        notes="TX RRC does NOT collect water production volumes. Always NULL.",
    ),
    "days_produced": FieldMapping(
        source=None,
        dtype="int32",
        nullable=True,
        notes=(
            "Not available in standard TX RRC PDQ tables. Always NULL. "
            "Some operators file H-10 forms with days, but these are not "
            "in the bulk CSV dump."
        ),
    ),
    "source_file": FieldMapping(
        source=None,
        transform="inject_source_filename",
        dtype="string",
        notes="Set by the parser to the bronze file path for lineage.",
    ),
    "ingested_at": FieldMapping(
        source=None,
        transform="inject_ingestion_timestamp",
        dtype="timestamp[us, tz=UTC]",
        notes="Set by the parser to the current UTC timestamp at ingest time.",
    ),
}


# ===================================================================
# New Mexico OCD — GO-TECH County ZIPs + ArcGIS Wells_Public
# ===================================================================
# Production data: GO-TECH county ZIP downloads (C-115 form data)
# Well master:     ArcGIS Wells_Public feature service
#
# Production is at *well* level — one row per API per month.
# ===================================================================

NM_FIELD_MAPPING: dict[str, FieldMapping] = {
    "state": FieldMapping(
        source=None,
        transform="constant:NM",
        dtype="string",
        nullable=False,
        notes="Hard-coded to 'NM' for all New Mexico records.",
    ),
    "entity_type": FieldMapping(
        source=None,
        transform="constant:well",
        dtype="string",
        nullable=False,
        notes="NM OCD reports at well level. Always 'well'.",
    ),
    "api_number": FieldMapping(
        source="id",
        transform="normalize_api",
        dtype="string",
        nullable=False,
        notes=(
            "API well number from Wells_Public 'id' field or C-115 API column. "
            "Format: 30-XXX-XXXXX (state code 30 = New Mexico). "
            "Normalized to NN-NNN-NNNNN format."
        ),
    ),
    "lease_number": FieldMapping(
        source=None,
        dtype="string",
        nullable=True,
        notes="NM does not use TX-style lease numbers. Always NULL.",
    ),
    "district": FieldMapping(
        source=None,
        dtype="string",
        nullable=True,
        notes="NM does not have TX RRC-style districts. Always NULL.",
    ),
    "well_name": FieldMapping(
        source="well_name",
        dtype="string",
        notes="Well name from Wells_Public or C-115 form data.",
    ),
    "operator": FieldMapping(
        source="ogrid_name",
        dtype="string",
        notes="Operator name from Wells_Public 'ogrid_name' field.",
    ),
    "county": FieldMapping(
        source="county",
        dtype="string",
        notes="County name from Wells_Public or derived from the GO-TECH file name.",
    ),
    "field_name": FieldMapping(
        source="pool",
        dtype="string",
        notes=(
            "Pool or field name from NM OCD data. NM uses 'pool' terminology "
            "rather than 'field'; mapped to field_name for consistency."
        ),
    ),
    "basin": FieldMapping(
        source=None,
        transform="derive_basin_from_county_or_field",
        dtype="string",
        nullable=True,
        notes=(
            "Derived from county using a lookup table (e.g. Lea/Eddy -> Permian, "
            "San Juan/Rio Arriba -> San Juan). May be NULL."
        ),
    ),
    "well_type": FieldMapping(
        source="type",
        transform="normalize_well_type",
        dtype="string",
        notes=(
            "Well type from Wells_Public 'type' field or C-115 well type. "
            "Normalized to: 'OIL', 'GAS', 'INJ', 'OTHER'."
        ),
    ),
    "well_status": FieldMapping(
        source="status",
        transform="normalize_well_status",
        dtype="string",
        notes=(
            "Well status from Wells_Public 'status' field. "
            "Normalized to: 'ACTIVE', 'SHUT-IN', 'P&A', 'INACTIVE', 'OTHER'."
        ),
    ),
    "production_date": FieldMapping(
        source="production_month",
        transform="parse_production_month",
        dtype="date32",
        nullable=False,
        notes=(
            "From the GO-TECH monthly column or C-115 report period. "
            "Normalized to the first of the month: date(YYYY, MM, 1)."
        ),
    ),
    "oil_bbl": FieldMapping(
        source="Oil",
        dtype="float64",
        notes=(
            "Oil production in barrels from GO-TECH data (column 'Oil' in BBL). "
            "NM may include condensate in this figure — see condensate_bbl notes."
        ),
    ),
    "gas_mcf": FieldMapping(
        source="Gas",
        dtype="float64",
        notes=(
            "Gas production in MCF from GO-TECH data (column 'Gas'). "
            "NM may include casinghead gas in this figure — see "
            "casinghead_gas_mcf notes."
        ),
    ),
    "condensate_bbl": FieldMapping(
        source=None,
        dtype="float64",
        nullable=True,
        notes=(
            "NM OCD does not report condensate separately. It is typically "
            "included in the oil_bbl figure. Always NULL at silver layer. "
            "Gold layer may estimate condensate using well type if needed."
        ),
    ),
    "casinghead_gas_mcf": FieldMapping(
        source=None,
        dtype="float64",
        nullable=True,
        notes=(
            "NM OCD does not report casinghead gas separately. It is typically "
            "included in the gas_mcf figure. Always NULL at silver layer."
        ),
    ),
    "water_bbl": FieldMapping(
        source="Water",
        dtype="float64",
        notes="Produced water in barrels from GO-TECH data (column 'Water' in BBL).",
    ),
    "days_produced": FieldMapping(
        source="Days",
        dtype="int32",
        notes="Number of producing days from GO-TECH data (column 'Days'). 0-31.",
    ),
    "source_file": FieldMapping(
        source=None,
        transform="inject_source_filename",
        dtype="string",
        notes="Set by the parser to the bronze file path for lineage.",
    ),
    "ingested_at": FieldMapping(
        source=None,
        transform="inject_ingestion_timestamp",
        dtype="timestamp[us, tz=UTC]",
        notes="Set by the parser to the current UTC timestamp at ingest time.",
    ),
}


# ===================================================================
# Oklahoma OCC — RBDMS Wells ArcGIS + RBDMS CSV
# ===================================================================
# Well master:     ArcGIS RBDMS_WELLS FeatureServer/220
# Production data: RBDMS nightly CSV (well_data.csv)
#
# Production is at *well* level — one row per API per month.
# ===================================================================

OK_FIELD_MAPPING: dict[str, FieldMapping] = {
    "state": FieldMapping(
        source=None,
        transform="constant:OK",
        dtype="string",
        nullable=False,
        notes="Hard-coded to 'OK' for all Oklahoma records.",
    ),
    "entity_type": FieldMapping(
        source=None,
        transform="constant:well",
        dtype="string",
        nullable=False,
        notes="OK OCC reports at well level. Always 'well'.",
    ),
    "api_number": FieldMapping(
        source="api",
        transform="normalize_api",
        dtype="string",
        nullable=False,
        notes=(
            "API well number from RBDMS_WELLS 'api' field or RBDMS CSV. "
            "Format: 35-XXX-XXXXX (state code 35 = Oklahoma). "
            "Normalized to NN-NNN-NNNNN format."
        ),
    ),
    "lease_number": FieldMapping(
        source=None,
        dtype="string",
        nullable=True,
        notes="OK does not use TX-style lease numbers. Always NULL.",
    ),
    "district": FieldMapping(
        source=None,
        dtype="string",
        nullable=True,
        notes="OK does not have TX RRC-style districts. Always NULL.",
    ),
    "well_name": FieldMapping(
        source="well_name",
        dtype="string",
        notes="Well name from RBDMS_WELLS ArcGIS 'well_name' field.",
    ),
    "operator": FieldMapping(
        source="operator",
        dtype="string",
        notes="Operator name from RBDMS_WELLS ArcGIS 'operator' field.",
    ),
    "county": FieldMapping(
        source="county",
        dtype="string",
        notes="County name from RBDMS_WELLS ArcGIS 'county' field.",
    ),
    "field_name": FieldMapping(
        source=None,
        dtype="string",
        nullable=True,
        notes=(
            "Not directly available in the RBDMS wells ArcGIS layer. "
            "May be derived from formation data if available. NULL by default."
        ),
    ),
    "basin": FieldMapping(
        source=None,
        transform="derive_basin_from_county",
        dtype="string",
        nullable=True,
        notes=(
            "Derived from county using a lookup table (e.g. Woodward -> Anadarko, "
            "Pontotoc -> Arkoma). May be NULL."
        ),
    ),
    "well_type": FieldMapping(
        source="welltype",
        transform="normalize_well_type",
        dtype="string",
        notes=(
            "Well type from RBDMS_WELLS 'welltype' field. "
            "OCC symbol_class values: OIL, GAS, OIL/GAS, DRY, PLUGGED, etc. "
            "Normalized to: 'OIL', 'GAS', 'INJ', 'OTHER'."
        ),
    ),
    "well_status": FieldMapping(
        source="wellstatus",
        transform="normalize_well_status",
        dtype="string",
        notes=(
            "Well status from RBDMS_WELLS 'wellstatus' field. "
            "Normalized to: 'ACTIVE', 'SHUT-IN', 'P&A', 'INACTIVE', 'OTHER'."
        ),
    ),
    "production_date": FieldMapping(
        source=["production_year", "production_month"],
        transform="combine_year_month",
        dtype="date32",
        nullable=False,
        notes=(
            "Constructed as the first day of the month from production year "
            "and month fields. Result: date(YYYY, MM, 1)."
        ),
    ),
    "oil_bbl": FieldMapping(
        source="oil",
        dtype="float64",
        notes="Oil production in barrels from RBDMS CSV or production data.",
    ),
    "gas_mcf": FieldMapping(
        source="gas",
        dtype="float64",
        notes="Gas production in MCF from RBDMS CSV or production data.",
    ),
    "condensate_bbl": FieldMapping(
        source=None,
        dtype="float64",
        nullable=True,
        notes=(
            "OK OCC does not report condensate separately. It is typically "
            "included in the oil_bbl figure. Always NULL at silver layer."
        ),
    ),
    "casinghead_gas_mcf": FieldMapping(
        source=None,
        dtype="float64",
        nullable=True,
        notes=(
            "OK OCC does not report casinghead gas separately. It is typically "
            "included in the gas_mcf figure. Always NULL at silver layer."
        ),
    ),
    "water_bbl": FieldMapping(
        source="water",
        dtype="float64",
        nullable=True,
        notes="Produced water in barrels, if available in production data.",
    ),
    "days_produced": FieldMapping(
        source="days",
        dtype="int32",
        nullable=True,
        notes="Number of producing days, if available in production data. 0-31.",
    ),
    "source_file": FieldMapping(
        source=None,
        transform="inject_source_filename",
        dtype="string",
        notes="Set by the parser to the bronze file path for lineage.",
    ),
    "ingested_at": FieldMapping(
        source=None,
        transform="inject_ingestion_timestamp",
        dtype="timestamp[us, tz=UTC]",
        notes="Set by the parser to the current UTC timestamp at ingest time.",
    ),
}


# ===================================================================
# Helpers
# ===================================================================

STATE_MAPPINGS: dict[str, dict[str, FieldMapping]] = {
    "TX": TX_FIELD_MAPPING,
    "NM": NM_FIELD_MAPPING,
    "OK": OK_FIELD_MAPPING,
}


def get_source_columns(state: str) -> list[str]:
    """Return the list of raw source column names needed for a given state.

    Useful for selecting only the required columns when reading large CSVs.
    """
    mapping = STATE_MAPPINGS[state]
    cols: list[str] = []
    for fm in mapping.values():
        if fm.source is None:
            continue
        if isinstance(fm.source, list):
            cols.extend(fm.source)
        else:
            cols.append(fm.source)
    return cols


def get_null_fields(state: str) -> list[str]:
    """Return field names that are always NULL for a given state."""
    mapping = STATE_MAPPINGS[state]
    return [
        name
        for name, fm in mapping.items()
        if fm.source is None
        and fm.transform is None
    ]
