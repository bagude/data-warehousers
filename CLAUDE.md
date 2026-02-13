# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Oil & gas production data warehouse using a **medallion architecture** (bronze/silver/gold) for Texas RRC and New Mexico OCD public data. Uses DuckDB as the analytical warehouse engine, Dagster for orchestration, and dbt-style SQL models for the gold layer.

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
python -m src.transforms.tx_parser      # silver TX
python -m src.transforms.nm_parser      # silver NM
cd dbt && dbt build --profiles-dir . --project-dir . && cd ..  # gold (DuckDB)
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

### Medallion Data Flow

```
bronze_tx ──> silver_tx ──\
                           ├──> gold_models ──> DuckDB tables
bronze_nm ──> silver_nm ──/
```

**Bronze** (`data/bronze/{state}/{pull_date}/`): Raw data as-received from sources. TX gets pipe-delimited DSV files from a ~3.39 GB ZIP via GoAnywhere MFT, plus well locations from ArcGIS REST API. NM gets paginated JSON from ArcGIS API (wells) and XML files from OCD FTP server (production + reference tables).

**Silver** (`data/silver/production/state={TX|NM}/`): Parquet files conforming to `PRODUCTION_SCHEMA` (defined in `src/schemas/production.py`). Both states are mapped to a common 23-column schema with identity, descriptive, geographic, volume, and lineage fields. Silver writes are Hive-partitioned by state.

**Gold** (`data/warehouse.duckdb`): DuckDB tables built by executing SQL from `orchestration/assets/_gold_builder.py`. The `stg_production` view reads silver Parquet via `read_parquet()` with hive partitioning, then gold tables aggregate by county, operator, field, and per-entity time series. Includes `decline_curve_inputs` for production decline analysis.

### Key Design Decisions

- **Entity granularity**: TX reports oil at lease level (many wells per lease), gas at well level (1:1). NM reports all production at well level. The `entity_type` field (`well` or `lease`) tracks this; dedup keys differ by entity type.
- **Condensate/casinghead gas**: TX reports these separately from crude oil and gas-well gas. NM combines them. Silver keeps them separate; gold computes `total_oil_bbl = oil_bbl + condensate_bbl` and `total_gas_mcf = gas_mcf + casinghead_gas_mcf`.
- **NULL vs zero**: NULL means "not reported/not applicable" (e.g., TX never reports `water_bbl`). Zero means "reported as zero production."
- **Gold layer bypasses dbt CLI**: Due to dbt CLI incompatibility with Python 3.14, gold models are executed as raw SQL via DuckDB Python API (`_gold_builder.py`). The dbt SQL files still exist under `dbt/` for reference.

### Source Code Layout

- `src/ingestion/base.py` — `BaseIngester` ABC with retry/backoff, rate limiting, httpx client
- `src/ingestion/tx_rrc.py` — `TxRrcIngester`: GoAnywhere JSF download flow + ArcGIS well locations
- `src/ingestion/nm_ocd.py` — `NmOcdIngester`: ArcGIS wells + OCD FTP production/reference ZIPs
- `src/transforms/base.py` — `BaseParser` ABC with Parquet write helper
- `src/transforms/tx_parser.py` — `TxParser`: joins OG_LEASE_CYCLE + OG_WELL_COMPLETION + OG_OPERATOR_DW, assigns entity types
- `src/transforms/nm_parser.py` — `NmParser`: auto-detects OCD FTP XML vs legacy GO-TECH CSV, pivots per-product-kind rows, joins reference tables
- `src/schemas/production.py` — `PRODUCTION_SCHEMA` (PyArrow schema, 23 fields, the canonical silver contract)
- `src/schemas/field_mappings.py` — `TX_FIELD_MAPPING` / `NM_FIELD_MAPPING` (declarative column-to-schema mappings)
- `src/schemas/dedup.py` — Entity-type-aware deduplication (well key vs lease key, latest `ingested_at` wins)
- `src/schemas/validation.py` — Row-level validation checks; `validate_batch()` returns pass rate
- `src/quality/checks.py` — Layer-boundary quality checks (bronze-to-silver row counts, required fields, dedup integrity)
- `src/quality/reconciliation.py` — Silver-to-gold reconciliation (volume sums, entity counts, PK uniqueness, month completeness)
- `orchestration/` — Dagster assets, jobs, schedules, resources (entry point: `definitions.py`, workspace: `workspace.yaml`)
- `orchestration/assets/_gold_builder.py` — `build_gold()`: executes gold model SQL + data quality tests via DuckDB

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
