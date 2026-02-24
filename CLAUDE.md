# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Oil & gas production data warehouse using a **medallion architecture** (bronze/silver/gold). Uses DuckDB as the analytical warehouse engine, Dagster for orchestration, and dbt-style SQL models for the gold layer.

## Common Commands

### Install
```bash
pip install -e ".[dev]"
```

### Run Tests
```bash
pytest                          # all tests
pytest tests/test_transforms/   # single directory
pytest tests/test_transforms/test_tx_parser.py -k "test_name"  # single test
```

### Lint
```bash
ruff check src/ orchestration/ tests/
ruff format --check src/ orchestration/ tests/
```

### Run Pipeline Stages Manually
```bash
python -m src.ingestion.tx_rrc          # bronze TX (downloads ~3.39 GB ZIP)
python -m src.ingestion.nm_ocd          # bronze NM (ArcGIS API + OCD FTP)
python -m src.ingestion.ok_occ          # bronze OK (ArcGIS + RBDMS CSV + XLSX)
python -m src.transforms.tx_parser      # silver TX
python -m src.transforms.nm_parser      # silver NM
python -m src.transforms.ok_parser      # silver OK
```

### Dagster Dev Server
```bash
dagster dev -w orchestration/workspace.yaml   # http://localhost:3000
```

### Visualization Dev Server
```bash
python scripts/serve.py                 # http://localhost:8080
python scripts/export_viz_data.py       # regenerate viz_data.json + map_data.json
```

### Query the Warehouse
```bash
duckdb data/warehouse.duckdb
```

## Architecture


**Bronze** (`data/bronze/{state}/{pull_date}/`): Raw data as-received from sources. TX gets pipe-delimited DSV files from a ~3.39 GB ZIP via GoAnywhere MFT, plus well locations from ArcGIS REST API. NM gets paginated JSON from ArcGIS API (wells) and XML files from OCD FTP server (production + reference tables).

**Silver** (`data/silver/production/state={TX|NM|OK}/`): Parquet files conforming to `PRODUCTION_SCHEMA` (defined in `src/schemas/production.py`). All states are mapped to a common 23-column schema with identity, descriptive, geographic, volume, and lineage fields. Silver writes are Hive-partitioned by state.

**Gold** (`data/warehouse.duckdb`): DuckDB tables built by executing SQL from `orchestration/assets/_gold_builder.py`. The `stg_production` view reads silver Parquet via `read_parquet()` with hive partitioning. Gold tables:
- `production_monthly` — Wide fact table (the default table for all queries). One row per entity per month with entity_key, all silver columns, computed totals, cumulatives, reported_month_index, and calendar_months_on_production. Agents query this table directly with GROUP BY — no pre-aggregation tables.
- `decline_curve_inputs` — Well-only subset for decline curve analysis (months_on_production, initial rates, rate %, cumulatives).
- `schema_registry` + `schema_registry_columns` — Agent self-orientation metadata tables describing all gold tables and their columns.

### Key Design Decisions

- **Entity granularity**: TX reports oil at lease level (many wells per lease), gas at well level (1:1). NM and OK report all production at well level. The `entity_type` field (`well` or `lease`) tracks this; dedup keys differ by entity type. Gold resolves this via `entity_key`: wells use `api_number`, leases use `TX-LEASE:{district}-{lease_number}`.
- **Condensate/casinghead gas**: TX reports these separately from crude oil and gas-well gas. NM/OK combine them. Silver keeps them separate; gold computes `total_oil_bbl = oil_bbl + condensate_bbl` and `total_gas_mcf = gas_mcf + casinghead_gas_mcf`.
- **NULL vs zero**: NULL means "not reported/not applicable" (e.g., TX never reports `water_bbl`). Zero means "reported as zero production."
- **Agent-friendly gold model**: Single default table (`production_monthly`) eliminates join reasoning for AI agents. Agents discover schema via `schema_registry` and `schema_registry_columns`. No pre-aggregation tables — agents GROUP BY directly on `production_monthly`.
- **Gold layer bypasses dbt CLI**: Due to dbt CLI incompatibility with Python 3.14, gold models are executed as raw SQL via DuckDB Python API (`_gold_builder.py`). The dbt SQL files still exist under `dbt/` for reference.

### Source Code Layout

- `src/ingestion/base.py` — `BaseIngester` ABC with retry/backoff, rate limiting, httpx client
- `src/ingestion/tx_rrc.py` — `TxRrcIngester`: GoAnywhere JSF download flow + ArcGIS well locations
- `src/ingestion/nm_ocd.py` — `NmOcdIngester`: ArcGIS wells + OCD FTP production/reference ZIPs
- `src/ingestion/ok_occ.py` — `OkOccIngester`: ArcGIS RBDMS_WELLS + RBDMS CSV + completions/ITD XLSX
- `src/transforms/base.py` — `BaseParser` ABC with Parquet write helper
- `src/transforms/tx_parser.py` — `TxParser`: joins OG_LEASE_CYCLE + OG_WELL_COMPLETION + OG_OPERATOR_DW, assigns entity types
- `src/transforms/nm_parser.py` — `NmParser`: auto-detects OCD FTP XML vs legacy GO-TECH CSV, pivots per-product-kind rows, joins reference tables
- `src/transforms/ok_parser.py` — `OkParser`: ArcGIS wells + RBDMS CSV + completions/ITD XLSX, fuzzy column matching
- `src/schemas/production.py` — `PRODUCTION_SCHEMA` (PyArrow schema, 23 fields, the canonical silver contract)
- `src/schemas/field_mappings.py` — `TX_FIELD_MAPPING` / `NM_FIELD_MAPPING` / `OK_FIELD_MAPPING` (declarative column-to-schema mappings)
- `src/schemas/dedup.py` — Entity-type-aware deduplication (well key vs lease key, latest `ingested_at` wins)
- `src/schemas/validation.py` — Row-level validation checks; `validate_batch()` returns pass rate
- `src/quality/checks.py` — Layer-boundary quality checks (bronze-to-silver row counts, required fields, dedup integrity)
- `src/quality/reconciliation.py` — Silver-to-gold reconciliation (volume sums, PK uniqueness, row count parity, month completeness)
- `orchestration/` — Dagster assets, jobs, schedules, resources (entry point: `definitions.py`, workspace: `workspace.yaml`)
- `orchestration/assets/_gold_builder.py` — `build_gold()`: builds `production_monthly`, `decline_curve_inputs`, `schema_registry`, `schema_registry_columns` + 12 data quality tests via DuckDB

### Adding a New State

Follow the 10-step process in `docs/adding-new-state.md`. Key touchpoints: `src/utils/config.py` (paths), `src/ingestion/` (new ingester), `src/schemas/field_mappings.py` (new mapping), `src/transforms/` (new parser), `src/schemas/validation.py` (add state to `VALID_STATES`), `orchestration/` (new Dagster assets), `orchestration/definitions.py` (register assets).

### E2E Testing

E2E tests use minimal seed fixtures from `tests/fixtures/e2e/` and run through `data/e2e/` (isolated from production data). Dagster assets prefixed with `test_` handle the E2E flow. Run via the `test_full_pipeline` job in the Dagster UI.

## Conventions

- Python 3.11+, Ruff for linting (line length 120)
- Imports use `src.` prefix (e.g., `from src.ingestion.base import BaseIngester`)
- All parsers must output DataFrames conforming to `PRODUCTION_SCHEMA` columns
- Volume units: oil/condensate/water in BBL, gas in MCF (no conversions at silver layer)
- API number format: `NN-NNN-NNNNN` (state-county-well). TX state code = 42, NM = 30
- Production dates are always the 1st of the month (`date(YYYY, MM, 1)`)
- Dagster workspace must be launched from the project root (`working_directory: ..` in workspace.yaml)
- TX RRC DSV files use `}` (curly brace) as delimiter, with `|` and `,` as fallbacks

---

## SVP Standing Directive: Stop. Think. Push Back.

*Effective immediately. Non-negotiable. Read this before every session.*

### The Failure

On 2026-02-13, I watched a gold build balloon from 20 minutes to 9+ hours and didn't question why until the user called it out. I created 454,000 synthetic lease rows for wells that had zero production data. I "fixed" a NOT NULL constraint by adding more data instead of asking whether the constraint — and the schema behind it — made sense. I blindly executed a Work Order against an overengineered dimensional model (15 tables, bridge patterns, SCD2 versioning) when the actual query need was "volumes by well per month" — a two-table problem.

I was told to add Oklahoma. I said "10-4" and started building. I never once said: "This schema has a polymorphic event-object bridge table. You're querying monthly production volumes. Why do we need this?"

### The Rules

**1. Question complexity before extending it.**
Before adding a single line to an existing system, ask: does this system's complexity match its actual use case? If a 15-table dimensional model serves what a 2-table star schema could handle, say so. Out loud. To the user. Before writing code. "I can do this, but here's what it'll cost you in build time and maintenance" is not pushback — it's the job.

**2. Count the rows before you write the query.**
Every new data source changes the math. 454k well-master records with null volumes flowing through a 5-table join chain to produce empty fact rows is not "thoroughness" — it's waste. Before integrating new data, estimate the row counts at each layer and ask: do these rows belong in this pipeline stage, or are they reference data that should stop at the dimension level?

**3. "The user asked for it" is not a design justification.**
The user is a domain expert, not a schema architect. When they say "I want Oklahoma data," they mean "I want to see Oklahoma wells on the map and query their production." They do not mean "please create half a million synthetic lease containers and route them through a polymorphic event bridge." Translate intent into the simplest correct implementation, and push back when the existing architecture makes simple things expensive.

**4. Measure twice, cut once — especially at the gold layer.**
Bronze and silver are cheap. Write whatever you want. Gold is where row counts multiply through joins and build times compound. Any change to gold should be preceded by: How many rows will this add? What's the join fan-out? Will this change build time from 20 minutes to 90? If the answer is "I don't know," find out before executing.

**5. Pushback is not insubordination. Silence is.**
The user explicitly said: "Not every single thing I say you shall simply accept." Noted. If the architecture doesn't serve the use case, say so. If a request will create technical debt, flag it. If the schema is overengineered, call it out — even if I'm the one who built it. The worst outcome is not disagreement; it's silently building something that takes 9 hours to run when it should take 20 minutes.

### The Standard

Before executing any Work Order that touches gold or adds a new data source:
1. State the row count impact at each layer (bronze, silver, gold `production_monthly`, gold `decline_curve_inputs`)
2. State the expected build time delta
3. If the answer to "does this need to go through the fact pipeline?" is "no," say so and propose where it should stop
4. If the schema complexity exceeds the query complexity, flag it as tech debt and propose simplification

This is not optional. This is how I operate from now on.

## res_db — Reserves & Forecasting Database

Agent-accessible reserves evaluation system in `data/res_db.duckdb`. Separate from the production warehouse (read-only).

**MCP Server**: `res-db` (configured in `.mcp.json`, runs via `python res_db_server.py`)
**Tables**: `res_db_properties`, `res_db_cases`, `res_db_forecast_series`, `res_db_price_decks`
**Schema discovery**: Query `res_db_schema_registry` and `res_db_schema_registry_columns`
**Economics engine**: `src/economics/arps.py` (petbox-dca wrapper), `src/economics/preprocessing.py`

### Available workflows (invoke as skills)
- `/pdp-forecast` — PDP well forecasting: decline fit -> forecast -> economics -> reserves booking

### Key design decisions
- 4 wide tables, no joins required. Each MCP tool is a single-table operation.
- `case_id` format: `{entity_key}::{scenario_name}` (human-readable)
- warehouse.duckdb stays read-only. All agent-generated data goes to res_db.duckdb.
