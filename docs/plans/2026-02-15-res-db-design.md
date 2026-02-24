# res_db Design — Reserves & Forecasting Database

**Date:** 2026-02-15
**Status:** Approved

## Overview

An oil & gas reserves, forecasting, and scenario-tracking database layer built on DuckDB, exposed to AI agents via MCP tools. Inspired by ARIES petroleum economics but with an agent-optimized architecture: fewer wide tables, semantic tool surface, no join reasoning required.

## Architecture

```
+-------------------------------------+
|  Agent Orchestrators (Claude Code)   |
|  guided by CLAUDE.md workflows       |
+-------------------------------------+
|  MCP Server (res_db_server.py)       |
|  12 semantic tools                   |
|  ATTACHes warehouse for reads        |
+-------------------------------------+
|  res_db.duckdb      warehouse.duckdb |
|  (read-write)       (read-only)      |
|  4 tables + meta    production_monthly|
+-------------------------------------+
```

- `warehouse.duckdb` stays read-only. Production history lives there.
- `res_db.duckdb` is a separate file for agent-generated data (forecasts, scenarios, reserves, price decks).
- Single MCP server (`res_db_server.py`) ATTACHes `warehouse.duckdb` as read-only alias `prod` for production history access.
- Schema evolution via `ALTER TABLE ADD/DROP COLUMN` + `res_db_schema_registry_columns` for agent self-discovery.

## Tables

### 1. `res_db_properties` — Well Master

One row per well. The agent's entry point for any evaluation.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| entity_key | VARCHAR | No | PK. Same as production_monthly |
| api_number | VARCHAR | Yes | API well number NN-NNN-NNNNN |
| state | VARCHAR | No | TX, NM, OK |
| entity_type | VARCHAR | No | well or lease |
| well_name | VARCHAR | Yes | Well/lease name |
| operator | VARCHAR | Yes | Operator name |
| county | VARCHAR | Yes | County |
| field_name | VARCHAR | Yes | Field |
| basin | VARCHAR | Yes | Basin |
| well_type | VARCHAR | Yes | OIL, GAS, INJ, OTHER |
| well_status | VARCHAR | Yes | ACTIVE, SHUT-IN, P&A |
| latitude | DOUBLE | Yes | WGS 84 |
| longitude | DOUBLE | Yes | WGS 84 |
| nri | DOUBLE | Yes | Net revenue interest (0-1) |
| wi | DOUBLE | Yes | Working interest (0-1) |
| orri | DOUBLE | Yes | Overriding royalty interest (0-1) |
| interest_effective_date | DATE | Yes | When current interests took effect |
| loe_monthly | DOUBLE | Yes | Lease operating expense $/month |
| production_tax_rate | DOUBLE | Yes | Production/severance tax rate (0-1) |
| ad_valorem_annual | DOUBLE | Yes | Ad valorem tax $/year |
| first_production_date | DATE | Yes | Earliest production_date from warehouse |
| last_production_date | DATE | Yes | Latest production_date from warehouse |
| vintage | INTEGER | Yes | First year with oil or gas > 0 |
| created_at | TIMESTAMP | No | UTC row creation |
| updated_at | TIMESTAMP | No | UTC last update |

### 2. `res_db_cases` — Evaluation Cases

One row per well x scenario. Decline params + reserves + economics in one wide row.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| case_id | VARCHAR | No | PK. {entity_key}::{scenario_name} |
| entity_key | VARCHAR | No | FK -> res_db_properties |
| scenario_name | VARCHAR | No | e.g. "base", "high_price", "low_decline" |
| scenario_type | VARCHAR | No | base, upside, downside, sensitivity |
| parent_case_id | VARCHAR | Yes | FK -> self. Cloned from which case |
| decline_model | VARCHAR | Yes | exponential, hyperbolic, harmonic |
| qi_oil | DOUBLE | Yes | Initial oil rate (BBL/month) |
| qi_gas | DOUBLE | Yes | Initial gas rate (MCF/month) |
| di | DOUBLE | Yes | Initial decline rate (annual, decimal) |
| b_factor | DOUBLE | Yes | Arps b exponent (0-2) |
| d_min | DOUBLE | Yes | Minimum terminal decline rate (annual) |
| decline_start_date | DATE | Yes | When forecast begins |
| fit_r_squared | DOUBLE | Yes | Goodness of fit (0-1) |
| fit_rmse | DOUBLE | Yes | Root mean square error |
| reserves_category | VARCHAR | Yes | PDP, PDNP, PUD, PROBABLE, POSSIBLE |
| eur_oil_bbl | DOUBLE | Yes | Estimated ultimate recovery - oil |
| eur_gas_mcf | DOUBLE | Yes | Estimated ultimate recovery - gas |
| remaining_oil_bbl | DOUBLE | Yes | EUR minus cumulative to date |
| remaining_gas_mcf | DOUBLE | Yes | EUR minus cumulative to date |
| price_deck_id | VARCHAR | Yes | FK -> res_db_price_decks |
| economic_limit_date | DATE | Yes | Month where revenue <= opex |
| economic_life_months | INTEGER | Yes | Months from decline_start to economic limit |
| pv10 | DOUBLE | Yes | Present value at 10% discount ($) |
| npv_undiscounted | DOUBLE | Yes | Undiscounted net cash flow ($) |
| irr | DOUBLE | Yes | Internal rate of return (decimal) |
| effective_date | DATE | No | Evaluation as-of date |
| notes | VARCHAR | Yes | Free text - agent reasoning |
| created_at | TIMESTAMP | No | UTC row creation |
| updated_at | TIMESTAMP | No | UTC last update |

### 3. `res_db_forecast_series` — Monthly Forecast Detail

One row per case x month. The time series behind each case.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| case_id | VARCHAR | No | PK part 1. FK -> res_db_cases |
| forecast_month | DATE | No | PK part 2. First of month |
| months_on_production | INTEGER | No | Sequential month from decline_start_date |
| oil_bbl | DOUBLE | Yes | Forecast oil production |
| gas_mcf | DOUBLE | Yes | Forecast gas production |
| water_bbl | DOUBLE | Yes | Forecast water production |
| cumulative_oil_bbl | DOUBLE | Yes | Running sum oil from decline start |
| cumulative_gas_mcf | DOUBLE | Yes | Running sum gas from decline start |
| gross_revenue | DOUBLE | Yes | (oil x price) + (gas x price) |
| net_revenue | DOUBLE | Yes | gross_revenue x NRI |
| production_tax | DOUBLE | Yes | net_revenue x tax_rate |
| opex | DOUBLE | Yes | LOE + ad_valorem/12 |
| net_cash_flow | DOUBLE | Yes | net_revenue - production_tax - opex |
| discount_factor | DOUBLE | Yes | 1 / (1 + 0.10)^(months/12) |
| discounted_cash_flow | DOUBLE | Yes | net_cash_flow x discount_factor |
| is_economic_limit | BOOLEAN | No | True on month where net_cash_flow <= 0 |

### 4. `res_db_price_decks` — Pricing

One row per deck x month. No header/detail split.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| price_deck_id | VARCHAR | No | PK part 1. e.g. "sec-2025-12-31" |
| forecast_month | DATE | No | PK part 2. First of month |
| deck_type | VARCHAR | No | sec, strip, forecast |
| oil_price_bbl | DOUBLE | Yes | $/BBL |
| gas_price_mcf | DOUBLE | Yes | $/MCF |
| ngl_price_bbl | DOUBLE | Yes | $/BBL |
| oil_differential | DOUBLE | Yes | Basin/transport deduct $/BBL |
| gas_differential | DOUBLE | Yes | Basin/transport deduct $/MCF |
| created_at | TIMESTAMP | No | UTC row creation |

### 5-6. `res_db_schema_registry` + `res_db_schema_registry_columns`

Same pattern as existing warehouse. Agent self-orientation metadata.

## MCP Tool Surface

Single server: `res_db_server.py`. 12 tools.

| Tool | Reads | Writes | Purpose |
|------|-------|--------|---------|
| register_well(entity_key) | warehouse production_monthly | res_db_properties | Pull header from warehouse, create property row |
| get_well(entity_key) | res_db_properties | - | Return full property record |
| search_wells(filters) | res_db_properties | - | Filter by state/county/basin/operator/status |
| set_price_deck(id, type, prices[]) | - | res_db_price_decks | Bulk-insert monthly prices |
| get_price_deck(id) | res_db_price_decks | - | Return all months for a deck |
| fit_decline(entity_key, scenario, params) | warehouse production_monthly | res_db_cases | Create/update case with Arps params |
| generate_forecast(case_id, price_deck_id) | cases, properties, price_decks | forecast_series, cases | Extrapolate decline, apply economics, write series + update case |
| book_reserves(case_id, category) | res_db_cases | res_db_cases | Set reserves_category, calc remaining |
| clone_case(case_id, new_scenario, overrides) | res_db_cases | cases, forecast_series | Clone case with param overrides, re-run forecast |
| compare_cases(case_ids[]) | cases, forecast_series | - | Side-by-side summary |
| list_cases(entity_key) | res_db_cases | - | All cases for a well |
| query_res_db(sql) | any res_db table | - | Read-only SQL escape hatch |

## Agent Workflow (PDP Forecasting Skill)

```
1. register_well(entity_key)          # pull well into res_db
2. fit_decline(entity_key, "base")    # fit Arps to production history
3. set_price_deck("sec-2025-12-31")   # create SEC price deck
4. generate_forecast(case_id, deck)   # extrapolate + economics
5. book_reserves(case_id, "PDP")      # classify as PDP
6. clone_case(case_id, "high_price")  # sensitivity scenario
7. compare_cases([base, high_price])  # evaluate
```

## Key Design Decisions

1. **4 wide tables, not 13 normalized.** Agents don't reason about joins. Each MCP tool is a single-table operation.
2. **Separate DuckDB file.** Warehouse stays read-only source of record. Agent-generated data is isolated in res_db.duckdb.
3. **Human-readable IDs.** case_id = `{entity_key}::base`, price_deck_id = `sec-2025-12-31`. No UUIDs.
4. **Schema evolution friendly.** DuckDB ALTER TABLE ADD/DROP COLUMN + schema_registry_columns for agent discovery.
5. **Interests and costs on property row.** No separate tables for NRI/WI or LOE. interest_effective_date tracks changes. Full history can be added later if needed.
6. **Reserves roll-forward derived.** Query cases by entity_key + effective_date range, diff remaining columns. No separate rollforward table.
7. **Notes field on cases.** Agent documents its reasoning for decline parameter choices.

## Reserves Categories Supported

- PDP (Proved Developed Producing)
- PDNP (Proved Developed Non-Producing)
- PUD (Proved Undeveloped)
- PROBABLE (2P minus 1P)
- POSSIBLE (3P minus 2P)

## Out of Scope (for now)

- Type curve library
- NGL yield / plant processing models
- Multi-well pad economics
- AFE / capital budgeting
- Regulatory filing generation
- Full interest history table (can be added via ALTER TABLE)
