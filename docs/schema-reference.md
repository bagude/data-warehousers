# Silver Layer Schema Reference

Canonical schema for monthly oil and gas production records. All state-specific
parsers produce DataFrames conforming to this schema before writing to
`data/silver/production/`.

Defined in: `src/schemas/production.py` (`PRODUCTION_SCHEMA`)

## Design Decisions

1. **Entity granularity**: TX RRC reports oil at lease level (many wells per
   lease); NM OCD reports at well level. The schema carries `entity_type`
   ("well" or "lease") so consumers know the granularity of each row.

2. **Condensate and casinghead gas**: TX RRC reports these separately from
   crude oil and gas-well gas. Kept as distinct columns at silver to avoid
   information loss. The gold layer combines them into `total_oil_bbl` and
   `total_gas_mcf`.

3. **NULL means "not reported"**: NULL is distinct from zero. Zero means the
   operator reported zero production; NULL means the field is not collected
   or not applicable for that state.

4. **Units**: Oil/condensate/water in barrels (BBL). Gas/casinghead gas in
   thousand cubic feet (MCF). These are native industry units -- no conversion
   needed.

5. **production_date**: Always the first day of the production month
   (e.g. 2025-01-01 for January 2025).

## Field Reference

| # | Field | Type | Nullable | Description |
|---|-------|------|----------|-------------|
| 1 | `state` | string | No | Two-letter state code: "TX" or "NM" |
| 2 | `entity_type` | string | No | "well" (NM, TX gas wells) or "lease" (TX oil leases) |
| 3 | `api_number` | string | Yes | API well number in NN-NNN-NNNNN format. NULL for TX multi-well oil leases |
| 4 | `lease_number` | string | Yes | TX RRC lease number. NULL for NM records |
| 5 | `district` | string | Yes | TX RRC district number (01-12, 6E, 7B, 7C, 8A, 9A). NULL for NM |
| 6 | `well_name` | string | Yes | Well or lease name as reported by operator |
| 7 | `operator` | string | Yes | Operator name |
| 8 | `county` | string | Yes | County where the well/lease is located |
| 9 | `field_name` | string | Yes | Oil/gas field (TX) or pool (NM) |
| 10 | `basin` | string | Yes | Basin name (Permian, San Juan, etc.). Derived from county |
| 11 | `well_type` | string | Yes | OIL, GAS, GAS_CONDENSATE, INJ, OTHER |
| 12 | `well_status` | string | Yes | ACTIVE, SHUT-IN, P&A, INACTIVE, OTHER |
| 13 | `production_date` | date32 | No | First day of production month |
| 14 | `oil_bbl` | float64 | Yes | Crude oil in barrels (excludes condensate) |
| 15 | `gas_mcf` | float64 | Yes | Gas-well gas in MCF (excludes casinghead gas) |
| 16 | `condensate_bbl` | float64 | Yes | Condensate in barrels. TX only; NULL for NM |
| 17 | `casinghead_gas_mcf` | float64 | Yes | Casinghead gas in MCF. TX only; NULL for NM |
| 18 | `water_bbl` | float64 | Yes | Produced water in barrels. NM only; NULL for TX |
| 19 | `days_produced` | int32 | Yes | Days producing (0-31). NM only; NULL for TX |
| 20 | `source_file` | string | Yes | Bronze file path for data lineage |
| 21 | `ingested_at` | timestamp[us, UTC] | Yes | UTC timestamp when record entered silver layer |

## Known NULLs by State

| Field | TX | NM | Reason |
|-------|----|----|--------|
| `api_number` | NULL for multi-well oil leases | Always populated | TX oil leases can span many wells |
| `lease_number` | Populated | Always NULL | NM uses API, not lease/district |
| `district` | Populated | Always NULL | NM has no RRC district concept |
| `condensate_bbl` | Populated | Always NULL | NM combines condensate into oil |
| `casinghead_gas_mcf` | Populated | Always NULL | NM combines casinghead into gas |
| `water_bbl` | Always NULL | Populated | TX RRC does not collect water volumes |
| `days_produced` | Always NULL | Populated | TX PDQ tables lack day counts |

## Deduplication

Defined in: `src/schemas/dedup.py`

- **Well records**: Keyed on `(state, api_number, production_date)`
- **Lease records**: Keyed on `(state, lease_number, district, production_date)`
- **Tie-break**: Latest `ingested_at` wins (most recent filing)

## Validation

Defined in: `src/schemas/validation.py`

Checks run after parsing, before writing silver Parquet:

| Check | Column(s) | Rule |
|-------|-----------|------|
| `non_negative_volumes` | oil_bbl, gas_mcf, condensate_bbl, casinghead_gas_mcf, water_bbl | >= 0 or NULL |
| `days_produced_range` | days_produced | 0-31 or NULL |
| `production_date_range` | production_date | >= 1900-01-01 and <= today |
| `api_number_format` | api_number | NN-NNN-NNNNN or NULL |
| `state_values` | state | TX or NM |
| `entity_type_values` | entity_type | "well" or "lease" |
| `lease_fields_consistency` | lease_number | Must be non-NULL when entity_type="lease" |
