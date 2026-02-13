# Source Field Mapping: Bronze to Silver

Per-state mapping from raw source columns to the common silver schema.

Defined in: `src/schemas/field_mappings.py`

---

## Texas RRC (PDQ CSV Dumps)

**Bronze sources:**
- `OG_LEASE_CYCLE.csv` -- monthly production volumes per lease
- `OG_WELL_COMPLETION.csv` -- well/completion attributes (API, type, county)
- `OG_OPERATOR_DW.csv` -- operator name lookup

**Join key:** `LEASE_NO + DISTRICT_NO` links OG_LEASE_CYCLE to OG_WELL_COMPLETION.

| Silver Field | Source Column(s) | Source Table | Transform | Notes |
|-------------|-----------------|--------------|-----------|-------|
| `state` | (none) | -- | Constant "TX" | Hard-coded |
| `entity_type` | (derived) | OG_WELL_COMPLETION | `derive_entity_type` | "well" for single-well leases/gas; "lease" for multi-well oil |
| `api_number` | `API_NO` | OG_WELL_COMPLETION | `normalize_api` | Format: 42-CCC-WWWWW. NULL for multi-well oil leases |
| `lease_number` | `LEASE_NO` | OG_LEASE_CYCLE | Direct copy | TX RRC lease number |
| `district` | `DISTRICT_NO` | OG_LEASE_CYCLE | Direct copy | Values: 01-12, 6E, 7B, 7C, 8A, 9A |
| `well_name` | `LEASE_NAME` | OG_LEASE_CYCLE | Strip + Title case | Lease name (used as well_name proxy) |
| `operator` | `OPERATOR_NAME` | OG_LEASE_CYCLE / OG_OPERATOR_DW | Strip + Title case | Fallback join on OPERATOR_NO |
| `county` | `COUNTY_CODE` / `COUNTY_NAME` | OG_WELL_COMPLETION | `tx_county_code_to_name` | County name via well join |
| `field_name` | `FIELD_NAME` | OG_LEASE_CYCLE | Strip + Title case | -- |
| `basin` | (none) | -- | `derive_basin_from_county` | Lookup table; may be NULL |
| `well_type` | `WELL_TYPE_CODE` | OG_WELL_COMPLETION | `normalize_well_type` | OL->OIL, GW->GAS, GC->GAS_CONDENSATE, IN->INJ |
| `well_status` | `WELL_STATUS` | OG_WELL_COMPLETION | `normalize_well_status` | A->ACTIVE, S->SHUT-IN, P->P&A, I->INACTIVE |
| `production_date` | `CYCLE_YEAR` + `CYCLE_MONTH` | OG_LEASE_CYCLE | `combine_year_month` | date(YYYY, MM, 1) |
| `oil_bbl` | `LEASE_OIL_PROD_VOL` | OG_LEASE_CYCLE | Numeric coerce | Crude oil only (no condensate) |
| `gas_mcf` | `LEASE_GAS_PROD_VOL` | OG_LEASE_CYCLE | Numeric coerce | Gas-well gas only (no casinghead) |
| `condensate_bbl` | `LEASE_COND_PROD_VOL` | OG_LEASE_CYCLE | Numeric coerce | Condensate from gas wells |
| `casinghead_gas_mcf` | `LEASE_CSGD_PROD_VOL` | OG_LEASE_CYCLE | Numeric coerce | Casinghead gas from oil wells |
| `water_bbl` | (none) | -- | -- | TX RRC does not collect. Always NULL |
| `days_produced` | (none) | -- | -- | Not in PDQ tables. Always NULL |
| `source_file` | (injected) | -- | Parser injects | Bronze file path |
| `ingested_at` | (injected) | -- | Parser injects | UTC timestamp |

**Well-type code reference:**

| TX Code | Normalized |
|---------|-----------|
| OL, O | OIL |
| GW, G | GAS |
| GC | GAS_CONDENSATE |
| IN, I | INJ |
| SW | SALT_WATER_DISPOSAL |
| S | STORAGE |
| D | DISPOSAL |
| W | WATER |

---

## New Mexico OCD (GO-TECH + ArcGIS)

**Bronze sources:**
- `wells/wells_batch_*.json` -- ArcGIS well master (paginated JSON)
- `production/*.csv` -- GO-TECH county production data (extracted from ZIPs)

**Join key:** API number (normalized to NN-NNN-NNNNN) links production records to well master.

| Silver Field | Source Column(s) | Source | Transform | Notes |
|-------------|-----------------|--------|-----------|-------|
| `state` | (none) | -- | Constant "NM" | Hard-coded |
| `entity_type` | (none) | -- | Constant "well" | NM always well-level |
| `api_number` | `id` / `API` | ArcGIS / GO-TECH | `normalize_api` | Format: 30-CCC-WWWWW |
| `lease_number` | (none) | -- | -- | NM has no lease numbers. Always NULL |
| `district` | (none) | -- | -- | NM has no RRC districts. Always NULL |
| `well_name` | `well_name` / `Well Name` | ArcGIS / GO-TECH | Strip | -- |
| `operator` | `ogrid_name` / `Operator` | ArcGIS / GO-TECH | Strip | ArcGIS enrichment preferred |
| `county` | `county` / directory name | ArcGIS / GO-TECH | `normalize_county_from_dir` | Derived from GO-TECH ZIP folder name |
| `field_name` | `pool` / `Pool` | ArcGIS | Strip | NM "pool" = "field" |
| `basin` | (derived from county) | -- | `derive_basin` | Eddy/Lea->Permian, San Juan->San Juan, etc. |
| `well_type` | `type` | ArcGIS | `normalize_well_type` | OIL, GAS, INJ, OTHER |
| `well_status` | `status` | ArcGIS | `normalize_well_status` | ACTIVE, SHUT-IN, P&A, INACTIVE, OTHER |
| `production_date` | Year + Month / date column | GO-TECH | `parse_production_date` | date(YYYY, MM, 1) |
| `oil_bbl` | `Oil` | GO-TECH | Numeric coerce | May include condensate |
| `gas_mcf` | `Gas` | GO-TECH | Numeric coerce | May include casinghead |
| `condensate_bbl` | (none) | -- | -- | NM combines with oil. Always NULL |
| `casinghead_gas_mcf` | (none) | -- | -- | NM combines with gas. Always NULL |
| `water_bbl` | `Water` | GO-TECH | Numeric coerce | -- |
| `days_produced` | `Days` | GO-TECH | Integer coerce | 0-31 |
| `source_file` | (injected) | -- | Parser injects | Bronze file path |
| `ingested_at` | (injected) | -- | Parser injects | UTC timestamp |

**NM county-to-basin lookup:**

| County | Basin |
|--------|-------|
| Chaves, Eddy, Lea, Roosevelt, Otero, Lincoln | Permian |
| San Juan, Rio Arriba, Sandoval, McKinley | San Juan |
| Colfax, Mora, Union, Harding | Raton |
| Hidalgo | Pedregosa |
| Santa Fe | Estancia |
