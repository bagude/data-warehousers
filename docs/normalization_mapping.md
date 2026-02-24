# Bronze-to-Silver Normalization Mapping

This document describes how each bronze source column maps to the 23-field silver `PRODUCTION_SCHEMA` for Texas (TX), New Mexico (NM), and Oklahoma (OK).

All mappings are defined in `src/schemas/field_mappings.py` as `FieldMapping` dataclass instances. Transformations are implemented in the state-specific parser classes (`TxParser`, `NmParser`, `OkParser`) under `src/transforms/`.

---

## 1. Well Identifier

Maps bronze well/entity identifiers to the silver identity columns.

| Silver Field | TX Source | NM Source | OK Source | Transform |
|-------------|-----------|-----------|-----------|-----------|
| `api_number` | `API_NO` (from `OG_WELL_COMPLETION` via join) | `id` (Wells_Public ArcGIS) | `api` (RBDMS_WELLS ArcGIS) | `normalize_api`: Format to `NN-NNN-NNNNN`. TX state code = 42, NM = 30, OK = 35. NULL for TX multi-well oil leases. |
| `lease_number` | `LEASE_NO` (from `OG_LEASE_CYCLE`) | NULL | NULL | Direct copy for TX. Always NULL for NM/OK (no lease-based reporting). |
| `district` | `DISTRICT_NO` (from `OG_LEASE_CYCLE`) | NULL | NULL | Direct copy for TX. Values: 01-12 plus 6E, 7B, 7C, 8A, 9A. Always NULL for NM/OK. |

### Notes:
- **TX entity granularity**: Oil production is at lease level (many wells per lease); gas production is at well level (1:1). The `api_number` is NULL for multi-well oil leases. Single-well leases have both `api_number` and `lease_number` populated.
- **NM/OK entity granularity**: All production is at well level (`entity_type = 'well'`). Lease number and district are not applicable.

---

## 2. Operator

Maps operator name to the silver `operator` column.

| Silver Field | TX Source | NM Source | OK Source | Transform |
|-------------|-----------|-----------|-----------|-----------|
| `operator` | `OPERATOR_NAME` (from `OG_LEASE_CYCLE` or `OG_OPERATOR_DW` via `OPERATOR_NO` join) | `ogrid_name` (Wells_Public ArcGIS) or `ogrid_nam` (OCD FTP ogrid.xml) | `operator` (RBDMS_WELLS ArcGIS) | String strip + title-case. |

---

## 3. Location

Maps geographic identifiers and coordinates to the silver location columns.

| Silver Field | TX Source | NM Source | OK Source | Transform |
|-------------|-----------|-----------|-----------|-----------|
| `county` | `COUNTY_CODE` (from `OG_WELL_COMPLETION` via join) | `county` (Wells_Public ArcGIS or derived from GO-TECH file name) | `county` (RBDMS_WELLS ArcGIS) | TX: Code-to-name lookup via RRC county table. NM: Direct or extracted from directory name (e.g., "allwells_SanJuan" -> "San Juan"). OK: Parsed from "NNN-COUNTYNAME" format to title-case. |
| `latitude` | `GIS_LAT83` (from ArcGIS RRC wells API) | `latitude` (Wells_Public ArcGIS) | `sh_lat` (RBDMS_WELLS ArcGIS) or `Surf_Lat_Y` (completions XLSX) | Coerce to float64. WGS 84 / NAD 83. |
| `longitude` | `GIS_LONG83` (from ArcGIS RRC wells API) | `longitude` (Wells_Public ArcGIS) | `sh_lon` (RBDMS_WELLS ArcGIS) or `Surf_Long_X` (completions XLSX) | Coerce to float64. WGS 84 / NAD 83. |

---

## 4. Field/Pool

Maps field or pool names and basins to the silver descriptive columns.

| Silver Field | TX Source | NM Source | OK Source | Transform |
|-------------|-----------|-----------|-----------|-----------|
| `field_name` | `FIELD_NAME` (from `OG_LEASE_CYCLE` via `FIELD_NO`) | `pool` (Wells_Public) or `pool_nam` (OCD FTP pool.xml) | NULL (not directly available in RBDMS) or `Formation_Name` (completions XLSX) | Direct copy for TX/NM. OK uses formation as proxy if available from completions. |
| `basin` | Derived from county or field via lookup table | Derived from county (e.g., Lea/Eddy -> Permian, San Juan -> San Juan) | Derived from county (e.g., Woodward -> Anadarko, Pontotoc -> Arkoma) | `derive_basin_from_county_or_field` (TX) or `derive_basin_from_county` (NM/OK). May be NULL if mapping unavailable. |

### Notes:
- **TX field names** come from the lease cycle table via `FIELD_NO`.
- **NM pool names** are the NM OCD equivalent of "field"; mapped to `field_name` for consistency.
- **OK field names** are not in the standard RBDMS data. The completions XLSX provides `Formation_Name` which is used as a proxy.

---

## 5. Well Attributes

Maps well type, status, and name to the silver descriptive columns.

| Silver Field | TX Source | NM Source | OK Source | Transform |
|-------------|-----------|-----------|-----------|-----------|
| `well_type` | `OIL_GAS_CODE` (from `OG_WELL_COMPLETION`) | `type` (Wells_Public ArcGIS) or `well_typ_cde` (wchistory.xml) | `welltype` (RBDMS_WELLS ArcGIS) or `Well_Type` (completions XLSX) | `normalize_well_type`: Map to standard values ('OIL', 'GAS', 'GAS_CONDENSATE', 'INJ', 'OTHER'). TX infers from O/G code; NM/OK normalize from source type codes. |
| `well_status` | `WELL_14B2_STATUS_CODE` (from `OG_WELL_COMPLETION`) | `status` (Wells_Public ArcGIS) or `wc_stat_cde` (wchistory.xml) | `wellstatus` (RBDMS_WELLS ArcGIS) or `Well_Status` (completions XLSX) | `normalize_well_status`: Map to standard values ('ACTIVE', 'SHUT-IN', 'P&A', 'INACTIVE', 'OTHER'). |
| `well_name` | `LEASE_NAME` (from `OG_LEASE_CYCLE`) or well name (from `OG_WELL_COMPLETION`) | `well_name` (Wells_Public ArcGIS) or `well_nbr_idn` (wchistory.xml) | `well_name` (RBDMS_WELLS ArcGIS) or `Well_Name` (completions XLSX) | String strip + title-case. |

### Well Type Normalization:
- **TX**: `OIL_GAS_CODE` values: O=OIL, G=GAS. Legacy `WELL_TYPE_CODE` values: OL=OIL, GW=GAS, GC=GAS_CONDENSATE, IN=INJ.
- **NM**: OCD codes: OIL, OW (Oil Well), GAS, GW (Gas Well), INJ, IW (Injection Well), SWD (Salt Water Disposal).
- **OK**: OCC codes: OIL, GAS, OIL/GAS, WATER_INJECTION, PLUGGED, DRY, etc.

### Well Status Normalization:
- **TX**: Single-character codes: A=ACTIVE, I=INACTIVE, P=P&A, S=SHUT-IN, etc.
- **NM**: OCD single-character codes: A=ACTIVE, C=INACTIVE (Cancelled), D=P&A (Dry Hole), T=SHUT-IN (Temporary Abandonment), etc.
- **OK**: OCC status codes: ACTIVE, SHUT-IN, PLUGGED, TEMPORARILY_ABANDONED, ORPHAN, etc.

---

## 6. Time Grain

Maps production reporting period to the silver `production_date` column.

| Silver Field | TX Source | NM Source | OK Source | Transform |
|-------------|-----------|-----------|-----------|-----------|
| `production_date` | `CYCLE_YEAR` + `CYCLE_MONTH` (from `OG_LEASE_CYCLE`) | `production_month` (GO-TECH CSV) or `prodn_yr` + `prodn_mth` (wcproduction.xml) | `production_year` + `production_month` (RBDMS CSV) or `Test_Date` (completions XLSX) | `combine_year_month`: Construct first of month as `date(YYYY, MM, 1)`. Format: `date32`. Not nullable. |

### Notes:
- **TX**: Year (int, e.g., 2025) + Month (int, 1-12) -> `date(2025, 1, 1)`.
- **NM GO-TECH**: Combined month column or separate year/month columns parsed to first of month.
- **NM OCD FTP**: Integer fields `prodn_yr` and `prodn_mth` combined to first of month.
- **OK completions**: `Test_Date` or `First_Prod` parsed to first of month for IP test records.

---

## 7. Volumes

Maps production volumes and days produced to the silver volume columns.

| Silver Field | TX Source | NM Source | OK Source | Transform |
|-------------|-----------|-----------|-----------|-----------|
| `oil_bbl` | `LEASE_OIL_PROD_VOL` (from `OG_LEASE_CYCLE`) | `Oil` (GO-TECH CSV) or pivoted from `prod_amt` where `prd_knd_cde='O'` (wcproduction.xml) | `oil` (RBDMS CSV) or `Oil_BBL_Per_Day` (completions XLSX) | Coerce to float64. TX does NOT include condensate. NM may include condensate. |
| `gas_mcf` | `LEASE_GAS_PROD_VOL` (from `OG_LEASE_CYCLE`) | `Gas` (GO-TECH CSV) or pivoted from `prod_amt` where `prd_knd_cde='G'` (wcproduction.xml) | `gas` (RBDMS CSV) or `Gas_MCF_Per_Day` (completions XLSX) | Coerce to float64. TX does NOT include casinghead gas. NM may include casinghead gas. |
| `condensate_bbl` | `LEASE_COND_PROD_VOL` (from `OG_LEASE_CYCLE`) | NULL | NULL | TX: Direct copy. NM/OK: Always NULL (condensate typically included in `oil_bbl`). |
| `casinghead_gas_mcf` | `LEASE_CSGD_PROD_VOL` (from `OG_LEASE_CYCLE`) | NULL | NULL | TX: Direct copy. NM/OK: Always NULL (casinghead gas typically included in `gas_mcf`). |
| `water_bbl` | NULL | `Water` (GO-TECH CSV) or pivoted from `prod_amt` where `prd_knd_cde='W'` (wcproduction.xml) | `water` (RBDMS CSV) or `Water_BBL_Per_Day` (completions XLSX) | TX: Always NULL (RRC does not collect water volumes). NM/OK: Coerce to float64. |
| `days_produced` | NULL | `Days` (GO-TECH CSV) or max of `prodn_day_num` across product kinds (wcproduction.xml) | `days` (RBDMS CSV) or 1 (completions XLSX IP tests) | TX: Always NULL (not in PDQ tables). NM/OK: Coerce to int32, range 0-31. |

### Notes:
- **Condensate vs. oil**: TX RRC reports condensate separately in `LEASE_COND_PROD_VOL`. NM OCD combines condensate with oil (no separate column). OK OCC does the same. Gold layer computes `total_oil_bbl = oil_bbl + condensate_bbl`.
- **Casinghead gas vs. gas**: TX RRC reports casinghead gas separately in `LEASE_CSGD_PROD_VOL`. NM/OK combine it with gas. Gold layer computes `total_gas_mcf = gas_mcf + casinghead_gas_mcf`.
- **Water volumes**: TX RRC does NOT collect water production. NM OCD and OK OCC do.
- **Days produced**: TX RRC does not include this in standard PDQ tables. NM OCD and OK OCC do.

---

## 8. Lineage

Maps data lineage and ingestion metadata to the silver lineage columns.

| Silver Field | TX Source | NM Source | OK Source | Transform |
|-------------|-----------|-----------|-----------|-----------|
| `source_file` | `{pull_date}/OG_LEASE_CYCLE.dsv` | GO-TECH CSV file name or `wcproduction.xml` | `rbdms_well_data.csv` or `completions_wells_formations.xlsx` | `inject_source_filename`: Set by parser to bronze file path for lineage. |
| `ingested_at` | Current UTC timestamp at parse time | Current UTC timestamp at parse time | Current UTC timestamp at parse time | `inject_ingestion_timestamp`: Set by parser to `datetime.now(tz=UTC)`. Format: `timestamp[us, tz=UTC]`. |

---

## 9. Structural Notes

### Entity Type Assignment Logic

The `entity_type` field distinguishes between well-level and lease-level reporting.

| State | Entity Type | Logic |
|-------|-------------|-------|
| **TX** | `'well'` or `'lease'` | Derived via `assign_tx_entity_type()` in `src/schemas/dedup.py`. Gas wells (1:1 lease-to-well) are `'well'`. Oil leases with multiple wells are `'lease'`. Oil leases with a single well may be promoted to `'well'` with the API number populated. |
| **NM** | `'well'` | Always `'well'`. NM OCD reports at well level (one row per API per month). |
| **OK** | `'well'` | Always `'well'`. OK OCC reports at well level (one row per API per month). |

**Deduplication keys**:
- **Well-level records**: `(state, api_number, production_date)`
- **Lease-level records**: `(state, lease_number, district, production_date)`

When duplicates exist, the row with the latest `ingested_at` timestamp wins.

### NULL Conventions

**NULL means "not reported / not applicable"**. Zero means "reported as zero production."

When a bronze source does not provide a field:
- The `FieldMapping.source` is `None`, indicating the field is not available.
- The parser fills the field with NULL in the silver output.
- This is distinct from a field that is available but happens to be zero (e.g., 0 BBL oil production for a gas well).

**Transform placeholders**:
- **`constant:XX`**: Hard-code the field to a constant value (e.g., `state='TX'`).
- **`derive_entity_type`**: Compute `entity_type` from well/lease structure (TX only).
- **`normalize_api`**: Format API number to `NN-NNN-NNNNN`.
- **`combine_year_month`**: Build `production_date` from year + month integers.
- **`derive_basin_from_county_or_field`**: Look up basin from county or field name.
- **`normalize_well_type`** / **`normalize_well_status`**: Map source codes to standard values.
- **`inject_source_filename`** / **`inject_ingestion_timestamp`**: Set lineage metadata at parse time.

---

## NULL Field Summary by State

The following table shows which silver fields are **always NULL** for each state (no bronze source available).

| Silver Field | TX | NM | OK | Reason |
|-------------|----|----|----|----|
| `state` | ✅ | ✅ | ✅ | Hard-coded constant per state. |
| `entity_type` | ✅ | ✅ | ✅ | Derived or constant. |
| `api_number` | NULL for multi-well leases | ✅ | ✅ | NULL when lease has multiple wells (TX only). |
| `lease_number` | ✅ | ❌ NULL | ❌ NULL | Only TX uses lease numbers. |
| `district` | ✅ | ❌ NULL | ❌ NULL | Only TX has RRC districts. |
| `well_name` | ✅ | ✅ | ✅ | Available from all states. |
| `operator` | ✅ | ✅ | ✅ | Available from all states. |
| `county` | ✅ | ✅ | ✅ | Available from all states (derived or direct). |
| `field_name` | ✅ | ✅ | NULL or derived | OK: Not in standard RBDMS; may come from completions XLSX. |
| `basin` | Derived | Derived | Derived | Derived from county or field lookup; may be NULL if mapping unavailable. |
| `well_type` | ✅ | ✅ | ✅ | Available from all states (normalized). |
| `well_status` | ✅ | ✅ | ✅ | Available from all states (normalized). |
| `latitude` | ✅ | ✅ | ✅ | Available from ArcGIS or well master; may be NULL if coordinates missing. |
| `longitude` | ✅ | ✅ | ✅ | Available from ArcGIS or well master; may be NULL if coordinates missing. |
| `production_date` | ✅ | ✅ | ✅ | Constructed from year+month; not nullable. |
| `oil_bbl` | ✅ | ✅ | ✅ | Available from all states. |
| `gas_mcf` | ✅ | ✅ | ✅ | Available from all states. |
| `condensate_bbl` | ✅ | ❌ NULL | ❌ NULL | TX reports separately; NM/OK include in `oil_bbl`. |
| `casinghead_gas_mcf` | ✅ | ❌ NULL | ❌ NULL | TX reports separately; NM/OK include in `gas_mcf`. |
| `water_bbl` | ❌ NULL | ✅ | ✅ | TX RRC does NOT collect water volumes. |
| `days_produced` | ❌ NULL | ✅ | ✅ | TX PDQ tables do not include days produced. |
| `source_file` | ✅ | ✅ | ✅ | Injected by parser. |
| `ingested_at` | ✅ | ✅ | ✅ | Injected by parser. |

**Legend**:
- ✅ = Field is available and populated from bronze source.
- ❌ NULL = Field is always NULL for this state (no bronze source).
- **NULL for multi-well leases** = Field is conditionally NULL depending on entity structure.
- **Derived** = Field is computed from other fields or lookup tables; may be NULL if mapping unavailable.

---

## References

- **Field mappings**: `src/schemas/field_mappings.py` (TX_FIELD_MAPPING, NM_FIELD_MAPPING, OK_FIELD_MAPPING)
- **Production schema**: `src/schemas/production.py` (PRODUCTION_SCHEMA, 23 fields)
- **Transform logic**: `src/transforms/tx_parser.py`, `src/transforms/nm_parser.py`, `src/transforms/ok_parser.py`
- **Deduplication**: `src/schemas/dedup.py` (entity-type-aware dedup keys)
- **Validation**: `src/schemas/validation.py` (row-level checks, `validate_batch()`)

---

**Last updated**: 2026-02-14
