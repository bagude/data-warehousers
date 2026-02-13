# Gold Layer Data Dictionary

The gold layer consists of dbt models materialized as DuckDB tables.
All gold models read from `stg_production` (a view over silver Parquet)
and provide analysis-ready aggregations and derived metrics.

dbt project: `dbt/`
Database: `data/warehouse.duckdb`

---

## stg_production (staging view)

**Source:** `dbt/models/staging/stg_production.sql`
**Materialization:** View (no storage overhead)
**Grain:** Same as silver -- one row per (entity_id, state, production_date)

Reads all silver Parquet files via `read_parquet('../../data/silver/production/*.parquet')`.
Adds derived columns on top of the silver schema.

| Column | Type | Description |
|--------|------|-------------|
| `entity_id` | string | Unified identifier: api_number for wells, lease_number for leases |
| `state` | string | TX or NM |
| `entity_type` | string | "well" or "lease" |
| `api_number` | string | API well number (NN-NNN-NNNNN) |
| `lease_number` | string | TX RRC lease number |
| `district` | string | TX RRC district |
| `well_name` | string | Well or lease name |
| `operator` | string | Operator name |
| `county` | string | County |
| `field_name` | string | Field or pool name |
| `basin` | string | Basin (Permian, San Juan, etc.) |
| `well_type` | string | OIL, GAS, INJ, etc. |
| `well_status` | string | ACTIVE, SHUT-IN, P&A, etc. |
| `production_date` | date | First of production month |
| `production_year` | int | Extracted from production_date |
| `production_month` | int | Extracted from production_date |
| `oil_bbl` | float | Crude oil (BBL) |
| `gas_mcf` | float | Gas-well gas (MCF) |
| `condensate_bbl` | float | Condensate (BBL) |
| `casinghead_gas_mcf` | float | Casinghead gas (MCF) |
| `water_bbl` | float | Produced water (BBL) |
| **`total_oil_bbl`** | float | **oil_bbl + condensate_bbl** (NULLs coalesced to 0) |
| **`total_gas_mcf`** | float | **gas_mcf + casinghead_gas_mcf** (NULLs coalesced to 0) |
| `days_produced` | int | Days producing in the month |
| `source_file` | string | Bronze source file |
| `ingested_at` | timestamp | Ingestion timestamp (UTC) |

---

## well_production_history

**Source:** `dbt/models/gold/well_production_history.sql`
**Materialization:** Table
**Grain:** One row per (entity_id, state, production_date)

Complete time series of monthly production for every well and lease.
Includes cumulative volumes and a months-on-production counter.

| Column | Type | Description |
|--------|------|-------------|
| `entity_id` | string | api_number (wells) or lease_number (leases) |
| `state` | string | TX or NM |
| `entity_type` | string | "well" or "lease" |
| `api_number` | string | API well number |
| `lease_number` | string | TX lease number |
| `district` | string | TX district |
| `well_name` | string | Well/lease name |
| `operator` | string | Operator name |
| `county` | string | County |
| `field_name` | string | Field/pool |
| `basin` | string | Basin |
| `well_type` | string | Well type |
| `well_status` | string | Well status |
| `production_date` | date | Production month |
| `production_year` | int | Year |
| `production_month` | int | Month |
| **`months_on_production`** | int | **Ordinal counter (1 = first month for this entity)** |
| `total_oil_bbl` | float | Crude + condensate (BBL) |
| `total_gas_mcf` | float | Well gas + casinghead gas (MCF) |
| `oil_bbl` | float | Crude oil only |
| `condensate_bbl` | float | Condensate only |
| `gas_mcf` | float | Well gas only |
| `casinghead_gas_mcf` | float | Casinghead gas only |
| `water_bbl` | float | Produced water |
| `days_produced` | int | Days producing |
| **`cumulative_oil_bbl`** | float | **Running sum of total_oil_bbl** |
| **`cumulative_gas_mcf`** | float | **Running sum of total_gas_mcf** |
| **`cumulative_water_bbl`** | float | **Running sum of water_bbl** |

**Primary key:** `(entity_id, state, production_date)`

**Use cases:**
- Plot a single well's monthly oil/gas production over time
- Compare cumulative production across wells in a field
- Identify when a well started producing and how long it has been active
- Calculate EUR (estimated ultimate recovery) from cumulative curves

---

## monthly_production_by_county

**Source:** `dbt/models/gold/monthly_production_by_county.sql`
**Materialization:** Table
**Grain:** One row per (county, state, production_date)

Geographic roll-up for mapping and regional analysis. Excludes rows
where county is NULL.

| Column | Type | Description |
|--------|------|-------------|
| `county` | string | County name |
| `state` | string | TX or NM |
| `production_date` | date | Production month |
| `production_year` | int | Year |
| `production_month` | int | Month |
| `entity_count` | int | Total records (wells + leases) |
| `well_count` | int | Well-level records |
| `lease_count` | int | Lease-level records |
| `total_oil_bbl` | float | Sum of crude + condensate |
| `total_gas_mcf` | float | Sum of well gas + casinghead |
| `total_water_bbl` | float | Sum of produced water |
| `crude_oil_bbl` | float | Sum of crude oil only |
| `condensate_bbl` | float | Sum of condensate only |
| `gas_well_gas_mcf` | float | Sum of well gas only |
| `casinghead_gas_mcf` | float | Sum of casinghead gas only |

**Primary key:** `(county, state, production_date)`

**Use cases:**
- County-level production heatmaps and choropleth maps
- Compare production trends across counties within a state
- Regional supply forecasting
- Identify top-producing counties by oil or gas volume

---

## monthly_production_by_operator

**Source:** `dbt/models/gold/monthly_production_by_operator.sql`
**Materialization:** Table
**Grain:** One row per (operator, state, county, production_date)

Operator-level aggregation. Includes county in the group key since
operators may span multiple counties. Excludes rows where operator is NULL.

| Column | Type | Description |
|--------|------|-------------|
| `operator` | string | Operator name |
| `state` | string | TX or NM |
| `county` | string | County |
| `production_date` | date | Production month |
| `production_year` | int | Year |
| `production_month` | int | Month |
| `entity_count` | int | Total records |
| `well_count` | int | Well-level records |
| `lease_count` | int | Lease-level records |
| `total_oil_bbl` | float | Sum of crude + condensate |
| `total_gas_mcf` | float | Sum of well gas + casinghead |
| `total_water_bbl` | float | Sum of produced water |
| `crude_oil_bbl` | float | Sum of crude oil only |
| `condensate_bbl` | float | Sum of condensate only |
| `gas_well_gas_mcf` | float | Sum of well gas only |
| `casinghead_gas_mcf` | float | Sum of casinghead gas only |

**Primary key:** `(operator, state, county, production_date)`

**Use cases:**
- Rank operators by total production in a region
- Track an operator's production growth or decline over time
- Compare operator efficiency (production per well/lease)
- Identify which operators dominate a given county

---

## monthly_production_by_field

**Source:** `dbt/models/gold/monthly_production_by_field.sql`
**Materialization:** Table
**Grain:** One row per (field_name, basin, state, production_date)

Field/pool-level aggregation for field performance analysis and basin
comparisons. Excludes rows where field_name is NULL.

| Column | Type | Description |
|--------|------|-------------|
| `field_name` | string | Field (TX) or pool (NM) name |
| `basin` | string | Basin (Permian, San Juan, etc.) |
| `state` | string | TX or NM |
| `production_date` | date | Production month |
| `production_year` | int | Year |
| `production_month` | int | Month |
| `entity_count` | int | Total records |
| `well_count` | int | Well-level records |
| `lease_count` | int | Lease-level records |
| `total_oil_bbl` | float | Sum of crude + condensate |
| `total_gas_mcf` | float | Sum of well gas + casinghead |
| `total_water_bbl` | float | Sum of produced water |
| `crude_oil_bbl` | float | Sum of crude oil only |
| `condensate_bbl` | float | Sum of condensate only |
| `gas_well_gas_mcf` | float | Sum of well gas only |
| `casinghead_gas_mcf` | float | Sum of casinghead gas only |

**Primary key:** `(field_name, basin, state, production_date)`

**Use cases:**
- Compare production performance across fields within a basin
- Identify mature vs. growing fields
- Basin-level production trend analysis (Permian vs. San Juan)
- Field-level reserves estimation inputs

---

## decline_curve_inputs

**Source:** `dbt/models/gold/decline_curve_inputs.sql`
**Materialization:** Table
**Grain:** One row per (api_number, state, production_date)

Per-well monthly data with decline curve analysis (DCA) inputs. Only
includes `entity_type = 'well'` records with non-NULL api_number.

| Column | Type | Description |
|--------|------|-------------|
| `api_number` | string | API well number |
| `state` | string | TX or NM |
| `well_name` | string | Well name |
| `operator` | string | Operator |
| `county` | string | County |
| `field_name` | string | Field/pool |
| `basin` | string | Basin |
| `well_type` | string | Well type |
| `well_status` | string | Well status |
| `production_date` | date | Production month |
| `production_year` | int | Year |
| `production_month` | int | Month |
| **`months_on_production`** | int | **1-indexed month counter from first producing month** |
| `total_oil_bbl` | float | Current month crude + condensate |
| `total_gas_mcf` | float | Current month well gas + casinghead |
| `water_bbl` | float | Current month water |
| `days_produced` | int | Days producing |
| **`initial_oil_rate`** | float | **Total oil in this well's first producing month (BBL)** |
| **`initial_gas_rate`** | float | **Total gas in this well's first producing month (MCF)** |
| **`oil_rate_pct_of_initial`** | float | **(current month oil / initial oil) * 100. NULL if initial = 0** |
| **`cumulative_oil`** | float | **Running sum of total oil through this month** |
| **`cumulative_gas`** | float | **Running sum of total gas through this month** |
| **`cumulative_water`** | float | **Running sum of water through this month** |

**Primary key:** `(api_number, state, production_date)`

**Use cases:**
- Fit Arps decline curves (exponential, hyperbolic, harmonic) to individual wells
- Forecast future production and estimate EUR (estimated ultimate recovery)
- Identify wells with anomalous decline behavior (recompletions, workovers)
- Type-curve construction for a field or basin (average decline profile)
- Economic analysis: break-even, NPV, rate of return per well

**Usage notes:**
- Filter to `months_on_production <= 60` for typical DCA fitting windows
- `oil_rate_pct_of_initial` shows the decline trajectory directly
- Use `initial_oil_rate` and `cumulative_oil` for Arps decline fitting
