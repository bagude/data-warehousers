"""Shared gold-layer builder -- agent-friendly flat model via DuckDB Python API.

Tables built:
  1. production_monthly    -- wide fact table (the default table for all queries)
  2. decline_curve_inputs  -- well-only subset for decline curve analysis
  3. schema_registry       -- table-level metadata for agent self-orientation
  4. schema_registry_columns -- column-level metadata for agent self-orientation

Design principles:
  - Single default table (production_monthly) eliminates join reasoning for agents
  - Stable canonical column names across states
  - entity_key resolves mixed grain (wells=api_number, leases=TX-LEASE:{district}-{lease_number})
  - NULL = not reported; 0 = reported as zero
"""

from pathlib import Path
from typing import Any

import duckdb

# ---------------------------------------------------------------------------
# SQL templates  (silver_path is the only variable, injected via .format())
# ---------------------------------------------------------------------------

STG_PRODUCTION_SQL = """
create or replace view stg_production as
select
    -- entity_key: resolves mixed grain
    case
        when entity_type = 'well'  then api_number
        when entity_type = 'lease' then 'TX-LEASE:' || district || '-' || lease_number
    end as entity_key,
    state, entity_type, api_number, lease_number, district,
    well_name, operator, county, field_name, basin, well_type, well_status,
    production_date,
    extract(year from production_date)::int   as production_year,
    extract(month from production_date)::int  as production_month,
    oil_bbl, gas_mcf, condensate_bbl, casinghead_gas_mcf, water_bbl,
    coalesce(oil_bbl, 0) + coalesce(condensate_bbl, 0)     as total_oil_bbl,
    coalesce(gas_mcf, 0) + coalesce(casinghead_gas_mcf, 0) as total_gas_mcf,
    latitude, longitude,
    days_produced, source_file, ingested_at
from read_parquet('{silver_path}', hive_partitioning=true)
"""

GOLD_MODELS: dict[str, str] = {
    "production_monthly": """
create or replace table production_monthly as
with base as (
    select *,
        -- reported_month_index: sequential count of rows with data per entity
        row_number() over (
            partition by entity_key, state
            order by production_date
        ) as reported_month_index,
        -- calendar_months_on_production: dense month count from first production
        (extract(year from production_date) * 12 + extract(month from production_date))
        - (extract(year from first_value(production_date) over (
            partition by entity_key, state
            order by production_date
            rows between unbounded preceding and unbounded following
          )) * 12
          + extract(month from first_value(production_date) over (
            partition by entity_key, state
            order by production_date
            rows between unbounded preceding and unbounded following
          )))
        + 1 as calendar_months_on_production,
        -- vintage: first production year when volumes exist
        extract(year from min(
            case when total_oil_bbl > 0 or total_gas_mcf > 0 then production_date end
        ) over (
            partition by entity_key, state
        ))::int as vintage,
        -- peak oil & gas per entity for initial GOR calc
        max(total_oil_bbl) over (partition by entity_key, state) as _peak_oil,
        max(total_gas_mcf) over (partition by entity_key, state) as _peak_gas
    from stg_production
)
select
    entity_key, state, entity_type, api_number, lease_number, district,
    well_name, operator, county, field_name, basin, well_type, well_status,
    latitude, longitude,
    production_date, production_year, production_month,
    reported_month_index,
    calendar_months_on_production,
    vintage,
    case when _peak_oil > 0
         then round(_peak_gas / _peak_oil * 1000, 1)
         else null
    end as initial_gor,
    total_oil_bbl, total_gas_mcf,
    oil_bbl, condensate_bbl, gas_mcf, casinghead_gas_mcf, water_bbl,
    days_produced,
    sum(total_oil_bbl) over (
        partition by entity_key, state
        order by production_date
        rows between unbounded preceding and current row
    ) as cumulative_oil_bbl,
    sum(total_gas_mcf) over (
        partition by entity_key, state
        order by production_date
        rows between unbounded preceding and current row
    ) as cumulative_gas_mcf,
    sum(water_bbl) over (
        partition by entity_key, state
        order by production_date
        rows between unbounded preceding and current row
    ) as cumulative_water_bbl,
    source_file, ingested_at
from base
""",
    "decline_curve_inputs": """
create or replace table decline_curve_inputs as
with producing_months as (
    select entity_key, api_number, state, well_name, operator,
           county, field_name, basin, well_type, well_status,
           latitude, longitude,
           production_date, production_year, production_month,
           total_oil_bbl, total_gas_mcf, water_bbl, days_produced
    from stg_production
    where entity_type = 'well'
      and api_number is not null
      and (total_oil_bbl > 0 or total_gas_mcf > 0)
),
with_dca_fields as (
    select *,
        row_number() over (partition by api_number, state order by production_date)
            as months_on_production,
        first_value(total_oil_bbl) over (partition by api_number, state
            order by production_date rows between unbounded preceding and unbounded following)
            as initial_oil_rate,
        first_value(total_gas_mcf) over (partition by api_number, state
            order by production_date rows between unbounded preceding and unbounded following)
            as initial_gas_rate,
        sum(total_oil_bbl) over (partition by api_number, state order by production_date
            rows between unbounded preceding and current row) as cumulative_oil,
        sum(total_gas_mcf) over (partition by api_number, state order by production_date
            rows between unbounded preceding and current row) as cumulative_gas,
        sum(water_bbl) over (partition by api_number, state order by production_date
            rows between unbounded preceding and current row) as cumulative_water
    from producing_months
)
select entity_key, api_number, state, well_name, operator,
       county, field_name, basin, well_type, well_status,
       latitude, longitude,
       production_date, production_year, production_month,
       months_on_production, total_oil_bbl, total_gas_mcf, water_bbl, days_produced,
       initial_oil_rate, initial_gas_rate,
       case when initial_oil_rate > 0
            then round(total_oil_bbl / initial_oil_rate * 100, 2)
            else null end as oil_rate_pct_of_initial,
       cumulative_oil, cumulative_gas, cumulative_water
from with_dca_fields
""",
}

# ---------------------------------------------------------------------------
# Schema registry -- agent self-orientation metadata
# ---------------------------------------------------------------------------

SCHEMA_REGISTRY_SQL = """
create or replace table schema_registry as
select * from (values
    ('production_monthly',
     'Wide fact table with one row per entity per production month. '
     'The default table for all production queries. Contains identity, '
     'descriptive, geographic, time, volume, computed, and lineage columns. '
     'entity_key resolves mixed grain: wells use api_number, leases use '
     'TX-LEASE:{district}-{lease_number}.',
     'entity_key, state, production_date',
     'monthly',
     current_timestamp),
    ('decline_curve_inputs',
     'Well-only subset of production_monthly for decline curve analysis. '
     'Filtered to entity_type=well with non-zero production. Includes '
     'months_on_production, initial rates, rate percentages, and cumulatives.',
     'api_number, state, production_date',
     'monthly',
     current_timestamp),
    ('schema_registry',
     'Table-level metadata for agent self-orientation. Describes purpose, '
     'primary key, and grain of each gold table.',
     'table_name',
     'static',
     current_timestamp),
    ('schema_registry_columns',
     'Column-level metadata for agent self-orientation. Describes each column '
     'in each gold table with type, nullability, and semantic description.',
     'table_name, column_name',
     'static',
     current_timestamp)
) as t(table_name, description, primary_key, grain, updated_at)
"""

# Column registry built dynamically after tables exist
SCHEMA_REGISTRY_COLUMNS_SQL = """
create or replace table schema_registry_columns as
with raw_cols as (
    select
        table_name,
        column_name,
        data_type,
        is_nullable,
        ordinal_position
    from information_schema.columns
    where table_schema = 'main'
      and table_name in ('production_monthly', 'decline_curve_inputs')
)
select
    table_name,
    column_name,
    data_type,
    case when is_nullable = 'YES' then true else false end as nullable,
    ordinal_position,
    case column_name
        -- Identity
        when 'entity_key' then 'Unique entity identifier. Wells=api_number, leases=TX-LEASE:{district}-{lease_number}'
        when 'state' then 'Two-letter state code (TX, NM, OK)'
        when 'entity_type' then 'Record granularity: well or lease'
        when 'api_number' then 'API well number NN-NNN-NNNNN. NULL for lease-level records'
        when 'lease_number' then 'TX RRC lease number. NULL for well-level records'
        when 'district' then 'TX RRC district number. NULL for non-TX records'
        -- Descriptive
        when 'well_name' then 'Well or lease name as reported by operator'
        when 'operator' then 'Operator name'
        when 'county' then 'County where entity is located'
        when 'field_name' then 'Oil/gas field name'
        when 'basin' then 'Basin name (Permian, San Juan, etc). May be NULL'
        when 'well_type' then 'Well type: OIL, GAS, INJ, OTHER'
        when 'well_status' then 'Well status: ACTIVE, SHUT-IN, P&A, etc'
        -- Geographic
        when 'latitude' then 'Decimal degrees WGS84. NULL when unavailable'
        when 'longitude' then 'Decimal degrees WGS84. NULL when unavailable'
        -- Time
        when 'production_date' then 'First day of production month (YYYY-MM-01)'
        when 'production_year' then 'Year extracted from production_date'
        when 'production_month' then 'Month extracted from production_date (1-12)'
        when 'reported_month_index' then 'Sequential count of rows with data per entity (1,2,3...)'
        when 'calendar_months_on_production' then 'Dense month count from first production date, including gaps'
        when 'vintage' then 'First production year when oil or gas volumes > 0 for this entity'
        when 'initial_gor' then 'Initial gas-oil ratio: peak_gas / peak_oil * 1000 (MCF/BBL). NULL when peak oil is zero'
        when 'months_on_production' then 'Sequential month count for producing months only (DCA)'
        -- Volumes (reported)
        when 'oil_bbl' then 'Crude oil BBL. Excludes condensate. NULL=not reported, 0=reported zero'
        when 'gas_mcf' then 'Gas well gas MCF. Excludes casinghead. NULL=not reported, 0=reported zero'
        when 'condensate_bbl' then 'Condensate BBL. TX-only. NULL for NM/OK'
        when 'casinghead_gas_mcf' then 'Casinghead gas MCF. TX-only. NULL for NM/OK'
        when 'water_bbl' then 'Produced water BBL. NULL for TX'
        when 'days_produced' then 'Days producing in month (0-31). NULL for TX'
        -- Volumes (computed)
        when 'total_oil_bbl' then 'oil_bbl + condensate_bbl (COALESCE nulls to 0)'
        when 'total_gas_mcf' then 'gas_mcf + casinghead_gas_mcf (COALESCE nulls to 0)'
        when 'cumulative_oil_bbl' then 'Running sum of total_oil_bbl per entity'
        when 'cumulative_gas_mcf' then 'Running sum of total_gas_mcf per entity'
        when 'cumulative_water_bbl' then 'Running sum of water_bbl per entity'
        -- DCA-specific
        when 'initial_oil_rate' then 'First month total_oil_bbl for this well'
        when 'initial_gas_rate' then 'First month total_gas_mcf for this well'
        when 'oil_rate_pct_of_initial' then 'Current month oil as % of initial rate'
        when 'cumulative_oil' then 'Running sum of total_oil_bbl (DCA context)'
        when 'cumulative_gas' then 'Running sum of total_gas_mcf (DCA context)'
        when 'cumulative_water' then 'Running sum of water_bbl (DCA context)'
        -- Lineage
        when 'source_file' then 'Bronze file this record was parsed from'
        when 'ingested_at' then 'UTC timestamp of silver-layer ingestion'
        else 'No description available'
    end as description
from raw_cols
order by table_name, ordinal_position
"""

# ---------------------------------------------------------------------------
# Data quality tests
# ---------------------------------------------------------------------------

TESTS: dict[str, str] = {
    "assert_no_null_entity_key": """
select * from production_monthly where entity_key is null
""",
    "assert_entity_key_format_wells": """
select * from production_monthly
where entity_type = 'well' and entity_key != api_number
""",
    "assert_entity_key_format_leases": """
select * from production_monthly
where entity_type = 'lease'
  and entity_key != 'TX-LEASE:' || district || '-' || lease_number
""",
    "assert_no_negative_volumes": """
select * from production_monthly
where oil_bbl < 0 or gas_mcf < 0 or condensate_bbl < 0
   or casinghead_gas_mcf < 0 or water_bbl < 0
""",
    "assert_no_future_production_dates": """
select * from production_monthly where production_date > current_date
""",
    "assert_no_orphan_gold_records": """
select pm.entity_key, pm.state, pm.production_date
from production_monthly pm
left join stg_production stg
    on pm.entity_key = stg.entity_key and pm.state = stg.state
    and pm.production_date = stg.production_date
where stg.entity_key is null
""",
    "assert_reported_month_index_sequential": """
with entity_stats as (
    select entity_key, state, count(*) as row_count,
           max(reported_month_index) as max_idx,
           min(reported_month_index) as min_idx
    from production_monthly group by entity_key, state
)
select * from entity_stats where min_idx != 1 or max_idx != row_count
""",
    "assert_calendar_months_positive": """
select * from production_monthly where calendar_months_on_production < 1
""",
    "assert_gold_volume_reconciles_with_silver": """
with silver_totals as (
    select state,
           sum(coalesce(oil_bbl, 0) + coalesce(condensate_bbl, 0)) as silver_oil,
           sum(coalesce(gas_mcf, 0) + coalesce(casinghead_gas_mcf, 0)) as silver_gas
    from stg_production
    group by state
),
gold_totals as (
    select state,
           sum(total_oil_bbl) as gold_oil,
           sum(total_gas_mcf) as gold_gas
    from production_monthly
    group by state
)
select coalesce(s.state, g.state) as state,
       s.silver_oil, g.gold_oil, s.silver_gas, g.gold_gas,
       abs(coalesce(s.silver_oil, 0) - coalesce(g.gold_oil, 0)) as oil_diff,
       abs(coalesce(s.silver_gas, 0) - coalesce(g.gold_gas, 0)) as gas_diff
from silver_totals s
full outer join gold_totals g on s.state = g.state
where abs(coalesce(s.silver_oil, 0) - coalesce(g.gold_oil, 0)) > 0.01
   or abs(coalesce(s.silver_gas, 0) - coalesce(g.gold_gas, 0)) > 0.01
   or s.silver_oil is null or g.gold_oil is null
""",
    "assert_decline_curve_well_only": """
select dca.api_number, dca.state, dca.production_date
from decline_curve_inputs dca
left join stg_production stg
    on dca.api_number = stg.api_number and dca.state = stg.state
    and dca.production_date = stg.production_date
where stg.entity_type != 'well' or stg.api_number is null
""",
    "assert_decline_curve_completeness": """
with well_stats as (
    select api_number, state, count(*) as row_count,
           min(months_on_production) as min_month,
           max(months_on_production) as max_month
    from decline_curve_inputs group by api_number, state
)
select * from well_stats where min_month != 1 or max_month != row_count
""",
    "assert_row_count_silver_equals_gold": """
with silver_count as (
    select count(*) as cnt from stg_production
),
gold_count as (
    select count(*) as cnt from production_monthly
)
select * from silver_count cross join gold_count
where silver_count.cnt != gold_count.cnt
""",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_gold(
    *,
    duckdb_path: str | Path,
    silver_parquet_glob: str,
    log: Any = None,
    run_tests: bool = True,
) -> dict[str, Any]:
    """Build all gold tables and optionally run data quality tests.

    Parameters
    ----------
    duckdb_path:
        Path to the DuckDB database file.
    silver_parquet_glob:
        Glob pattern pointing at silver Parquet files.
    log:
        Optional logger with ``.info`` and ``.error`` methods.
    run_tests:
        If True (default) run all data-quality tests after building.

    Returns
    -------
    dict with keys: stg_rows, models (dict of name->row_count),
    tests_passed, tests_failed (list of names).

    Raises
    ------
    RuntimeError if any test fails.
    """

    def _log_info(msg, *args):
        if log:
            log.info(msg, *args)

    def _log_error(msg, *args):
        if log:
            log.error(msg, *args)

    con = duckdb.connect(str(duckdb_path))
    try:
        # 1. Staging view
        stg_sql = STG_PRODUCTION_SQL.format(silver_path=silver_parquet_glob)
        con.execute(stg_sql)
        stg_count = con.execute("select count(*) from stg_production").fetchone()[0]
        _log_info("stg_production: %d rows", stg_count)

        # 2. Gold tables
        model_counts: dict[str, int] = {}
        for name, sql in GOLD_MODELS.items():
            con.execute(sql)
            cnt = con.execute(f"select count(*) from {name}").fetchone()[0]
            model_counts[name] = cnt
            _log_info("  %s: %d rows", name, cnt)

        # 3. Schema registry (metadata tables)
        con.execute(SCHEMA_REGISTRY_SQL)
        _log_info("  schema_registry: built")
        con.execute(SCHEMA_REGISTRY_COLUMNS_SQL)
        col_count = con.execute(
            "select count(*) from schema_registry_columns"
        ).fetchone()[0]
        _log_info("  schema_registry_columns: %d entries", col_count)

        # 4. Tests
        passed = 0
        failed: list[str] = []
        if run_tests:
            for test_name, sql in TESTS.items():
                failures = con.execute(sql).fetchall()
                if failures:
                    failed.append(test_name)
                    _log_error("FAIL %s: %d violation(s)", test_name, len(failures))
                    for row in failures[:5]:
                        _log_error("  %s", row)
                else:
                    passed += 1
                    _log_info("PASS %s", test_name)

            _log_info(
                "Tests: %d passed, %d failed out of %d",
                passed, len(failed), len(TESTS),
            )

    finally:
        con.close()

    if failed:
        raise RuntimeError(
            f"{len(failed)} test(s) failed: {', '.join(failed)}"
        )

    return {
        "stg_rows": stg_count,
        "models": model_counts,
        "tests_passed": passed,
        "tests_failed": failed,
    }
