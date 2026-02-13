# State Onboarding Runbook

Step-by-step guide for adding a new state to the pipeline. Uses the existing
TX and NM implementations as reference.

---

## Prerequisites

- Access to the new state's public production data portal
- Understanding of the data format (CSV, JSON, API, etc.)
- Knowledge of the state's well identifier system (API numbers, state-specific IDs)

## Steps

### 1. Research the data source

Identify:
- Where the data lives (download page, REST API, FTP)
- Data format and encoding
- Update frequency (monthly, quarterly)
- Coverage period
- Key fields: well/lease ID, operator, county, volumes, dates
- Authentication requirements (if any)

Document findings in `docs/` for the team.

### 2. Add bronze directory

Create the state subdirectory under `data/bronze/`:

```
data/bronze/{state_code}/
```

Update `src/utils/config.py` to add the new path constant:

```python
BRONZE_XX_DIR = BRONZE_DIR / "xx"  # Replace xx with state code
```

Update `ensure_data_dirs()` to include the new directory.

### 3. Write the ingester

Create `src/ingestion/{state}_scraper.py` extending `BaseIngester`:

```python
from src.ingestion.base import BaseIngester
from src.utils.config import BRONZE_XX_DIR

class XxIngester(BaseIngester):
    def __init__(self, *, pull_date=None, **kwargs):
        super().__init__(output_dir=BRONZE_XX_DIR, **kwargs)
        # ...

    def discover(self) -> list[str]:
        """Return URLs or identifiers to scrape."""
        # ...

    def ingest(self) -> Path:
        """Download raw data into bronze/{state}/{pull_date}/."""
        # ...
```

`BaseIngester` provides: retry with backoff, rate limiting, httpx client,
logging. See `src/ingestion/tx_rrc.py` and `src/ingestion/nm_ocd.py` for
examples.

### 4. Add field mappings

Add the state's field mapping to `src/schemas/field_mappings.py`:

```python
XX_FIELD_MAPPING: dict[str, FieldMapping] = {
    "state": FieldMapping(source=None, transform="constant:XX", ...),
    "entity_type": FieldMapping(...),
    "api_number": FieldMapping(source="<source_column>", transform="normalize_api", ...),
    # ... map all 21 fields
}
```

Add to the `STATE_MAPPINGS` dict:

```python
STATE_MAPPINGS: dict[str, dict[str, FieldMapping]] = {
    "TX": TX_FIELD_MAPPING,
    "NM": NM_FIELD_MAPPING,
    "XX": XX_FIELD_MAPPING,  # new state
}
```

### 5. Write the parser

Create `src/transforms/{state}_parser.py` extending `BaseParser`:

```python
from src.transforms.base import BaseParser
from src.utils.config import BRONZE_XX_DIR, SILVER_DIR

class XxParser(BaseParser):
    def __init__(self):
        super().__init__(
            input_dir=BRONZE_XX_DIR,
            output_dir=SILVER_DIR / "production" / "state=XX",
        )

    def parse(self) -> pd.DataFrame:
        """Read bronze data, map to silver schema, validate, dedup."""
        # 1. Load raw files from self.input_dir
        # 2. Map columns per XX_FIELD_MAPPING
        # 3. Normalize well_type, well_status, api_number
        # 4. Validate with validate_batch()
        # 5. Deduplicate with deduplicate()
        # 6. Write with self.write_parquet()
```

`BaseParser` provides: `write_parquet()` helper with optional schema
enforcement, logging. See `src/transforms/tx_parser.py` and
`src/transforms/nm_parser.py` for examples.

### 6. Update validation

In `src/schemas/validation.py`, add the new state code to `VALID_STATES`:

```python
VALID_STATES = frozenset({"TX", "NM", "XX"})
```

If the state has unique validation needs (e.g. different API format),
add new check functions.

### 7. Add Dagster assets

In `dagster/assets/bronze.py`, add a new bronze asset:

```python
@asset(group_name="bronze", partitions_def=daily_partitions)
def bronze_xx(context):
    # ... run XxIngester
```

In `dagster/assets/silver.py`, add a new silver asset:

```python
@asset(group_name="silver", deps=["bronze_xx"], partitions_def=daily_partitions)
def silver_xx(context):
    # ... run XxParser
```

In `dagster/assets/gold.py`, add the new silver asset to `gold_models` deps:

```python
@asset(group_name="gold", deps=["silver_tx", "silver_nm", "silver_xx"])
def gold_models(context):
    # ...
```

Register the new assets in `dagster/definitions.py`.

### 8. Update dbt staging

The dbt staging model `dbt/models/staging/stg_production.sql` reads all
Parquet files via glob:

```sql
select * from read_parquet('../../data/silver/production/*.parquet')
```

If the new state's Parquet files land in
`data/silver/production/state=XX/`, they will be picked up automatically.
No dbt changes needed unless the state requires special handling.

### 9. Add tests

Create `tests/test_ingestion/test_{state}_ingester.py` and
`tests/test_transforms/test_{state}_parser.py`.

At minimum, test:
- Parser produces a DataFrame matching `PRODUCTION_SCHEMA` columns
- Validation passes on sample data
- Deduplication handles the state's entity type correctly

### 10. Update documentation

- Add the state to `docs/source_mapping.md`
- Update `docs/schema_reference.md` "Known NULLs by State" table
- Update `docs/data_dictionary.md` if the state affects gold models
