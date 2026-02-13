# Pipeline Runbook

How to run the og-data-warehouse pipeline locally and in production.

---

## Prerequisites

- Python 3.11+
- Install dependencies:
  ```
  pip install -e ".[dev]"
  ```
- Ensure `dbt` CLI is available:
  ```
  dbt --version
  ```

## Project Layout

```
data-warehousers/
  src/              Python source (ingestion, transforms, schemas, quality, utils)
  dagster/          Dagster orchestration config
  dbt/              dbt project (staging + gold models)
  data/             Medallion data directories
    bronze/tx/      Raw TX RRC CSV dumps
    bronze/nm/      Raw NM OCD JSON + ZIPs
    silver/         Parquet files (common schema)
    gold/           (materialized in DuckDB via dbt)
  tests/            Test suite
```

## Running with Dagster (Recommended)

### Start the dev server

```bash
cd data-warehousers
dagster dev -w dagster/workspace.yaml
```

Open `http://localhost:3000` in your browser.

### Trigger a full refresh

1. In the Dagster UI, go to **Jobs** > **full_refresh**
2. Click **Materialize All**
3. Select a partition key (date in YYYY-MM-DD format)
4. Click **Launch**

This runs: bronze_tx -> silver_tx -> gold_models
           bronze_nm -> silver_nm ->

### Trigger an incremental run

1. Go to **Jobs** > **incremental**
2. Select today's date as the partition key
3. Click **Launch**

### Schedule

The `monthly_refresh` schedule is configured to run on the 1st of each
month at 06:00 UTC. Enable it in the Dagster UI under **Schedules**.

This aligns with TX RRC updating data on the last Saturday of each month.

### Materialize individual assets

In the Dagster UI, navigate to the **Asset Graph** and click on any
individual asset to materialize it. Useful for:

- Re-running a failed bronze ingestion
- Re-parsing silver after a parser fix
- Rebuilding gold models after a dbt change

## Running Without Dagster

### Bronze ingestion (TX)

```bash
python -m src.ingestion.tx_rrc
```

Downloads PDQ CSV dumps to `data/bronze/tx/{today}/`.

### Bronze ingestion (NM)

```bash
python -m src.ingestion.nm_ocd
```

Downloads ArcGIS well master + GO-TECH county ZIPs to
`data/bronze/nm/{today}/`.

### Silver transforms (TX)

```bash
python -m src.transforms.tx_parser
```

Reads the most recent `data/bronze/tx/{date}/` and writes
`data/silver/production/state=TX/tx_production.parquet`.

### Silver transforms (NM)

```bash
python -m src.transforms.nm_parser
```

Reads the most recent `data/bronze/nm/{date}/` and writes
`data/silver/production/state=NM/nm_production.parquet`.

### Gold models (dbt)

```bash
cd dbt
dbt build --profiles-dir . --project-dir .
```

Reads silver Parquet via DuckDB `read_parquet()`, builds staging view and
all gold tables into `data/warehouse.duckdb`.

## Full Refresh (Manual)

Run all steps in order:

```bash
# 1. Bronze
python -m src.ingestion.tx_rrc
python -m src.ingestion.nm_ocd

# 2. Silver
python -m src.transforms.tx_parser
python -m src.transforms.nm_parser

# 3. Gold
cd dbt && dbt build --profiles-dir . --project-dir . && cd ..
```

## Querying the Warehouse

After a full run, query gold tables directly in DuckDB:

```bash
python -c "
import duckdb
con = duckdb.connect('data/warehouse.duckdb', read_only=True)
print(con.sql('SELECT * FROM monthly_production_by_county LIMIT 10').df())
"
```

Or use the DuckDB CLI:

```bash
duckdb data/warehouse.duckdb
```

```sql
SELECT state, county, production_date, total_oil_bbl, total_gas_mcf
FROM monthly_production_by_county
WHERE production_date >= '2025-01-01'
ORDER BY total_oil_bbl DESC
LIMIT 20;
```

## Troubleshooting

### Bronze ingestion fails with timeout

The TX RRC MFT server can be slow. Increase the timeout:

```python
with TxRrcIngester(timeout=600.0) as ingester:
    ingester.ingest()
```

### "No pull-date subdirectories found"

The silver parser auto-selects the most recent date directory under
`data/bronze/{state}/`. If bronze hasn't run yet, run it first.

### dbt build fails with "file not found"

The staging model reads from `data/silver/production/*.parquet`. If no
silver Parquet files exist yet, run the silver transforms first.

### Dagster can't find definitions

Ensure you are running from the project root directory:

```bash
cd data-warehousers
dagster dev -w dagster/workspace.yaml
```

The `workspace.yaml` sets `working_directory: ..` which must resolve to
the project root for `src.*` imports to work.
