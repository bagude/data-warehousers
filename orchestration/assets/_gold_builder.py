"""Shared gold-layer builder -- executes dbt model SQL via DuckDB Python API.

dbt CLI is incompatible with Python 3.14 (mashumaro / pydantic v1 breakage),
so both the production ``gold_models`` and E2E ``test_gold_models`` assets
call :func:`build_gold` instead of shelling out to ``dbt build``.

The SQL here is extracted verbatim from the dbt model and test files under
``dbt/models/`` and ``dbt/tests/``, with Jinja ``{{ ref(...) }}`` and
``{{ var(...) }}`` resolved to plain table names and parameters.
"""

import os
from pathlib import Path
from typing import Any

import duckdb

# ---------------------------------------------------------------------------
# SQL templates  (silver_path is the only variable, injected via .format())
# ---------------------------------------------------------------------------

STG_PRODUCTION_SQL = """
create or replace view stg_production as
select
    case
        when entity_type = 'well'  then api_number
        when entity_type = 'lease' then state || '-' || district || '-' || lease_number
    end as entity_id,
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
    "well_production_history": """
create or replace table well_production_history as
select
    entity_id, state, entity_type, api_number, lease_number, district,
    well_name, operator, county, field_name, basin, well_type, well_status,
    latitude, longitude,
    production_date, production_year, production_month,
    row_number() over (partition by entity_id, state order by production_date)
        as months_on_production,
    total_oil_bbl, total_gas_mcf,
    oil_bbl, condensate_bbl, gas_mcf, casinghead_gas_mcf, water_bbl,
    days_produced,
    sum(total_oil_bbl) over (partition by entity_id, state order by production_date
        rows between unbounded preceding and current row) as cumulative_oil_bbl,
    sum(total_gas_mcf) over (partition by entity_id, state order by production_date
        rows between unbounded preceding and current row) as cumulative_gas_mcf,
    sum(water_bbl) over (partition by entity_id, state order by production_date
        rows between unbounded preceding and current row) as cumulative_water_bbl
from stg_production
""",
    "monthly_production_by_county": """
create or replace table monthly_production_by_county as
select
    county, state, production_date, production_year, production_month,
    count(*) as entity_count,
    count(*) filter (where entity_type = 'well')  as well_count,
    count(*) filter (where entity_type = 'lease') as lease_count,
    sum(total_oil_bbl) as total_oil_bbl, sum(total_gas_mcf) as total_gas_mcf,
    sum(oil_bbl) as crude_oil_bbl, sum(condensate_bbl) as condensate_bbl,
    sum(gas_mcf) as gas_well_gas_mcf, sum(casinghead_gas_mcf) as casinghead_gas_mcf,
    sum(water_bbl) as total_water_bbl
from stg_production
where county is not null
group by county, state, production_date, production_year, production_month
""",
    "monthly_production_by_operator": """
create or replace table monthly_production_by_operator as
select
    operator, state, county, production_date, production_year, production_month,
    count(*) as entity_count,
    count(*) filter (where entity_type = 'well')  as well_count,
    count(*) filter (where entity_type = 'lease') as lease_count,
    sum(total_oil_bbl) as total_oil_bbl, sum(total_gas_mcf) as total_gas_mcf,
    sum(oil_bbl) as crude_oil_bbl, sum(condensate_bbl) as condensate_bbl,
    sum(gas_mcf) as gas_well_gas_mcf, sum(casinghead_gas_mcf) as casinghead_gas_mcf,
    sum(water_bbl) as total_water_bbl
from stg_production
where operator is not null
group by operator, state, county, production_date, production_year, production_month
""",
    "monthly_production_by_field": """
create or replace table monthly_production_by_field as
select
    field_name, basin, state, production_date, production_year, production_month,
    count(*) as entity_count,
    count(*) filter (where entity_type = 'well')  as well_count,
    count(*) filter (where entity_type = 'lease') as lease_count,
    sum(total_oil_bbl) as total_oil_bbl, sum(total_gas_mcf) as total_gas_mcf,
    sum(oil_bbl) as crude_oil_bbl, sum(condensate_bbl) as condensate_bbl,
    sum(gas_mcf) as gas_well_gas_mcf, sum(casinghead_gas_mcf) as casinghead_gas_mcf,
    sum(water_bbl) as total_water_bbl
from stg_production
where field_name is not null
group by field_name, basin, state, production_date, production_year, production_month
""",
    "decline_curve_inputs": """
create or replace table decline_curve_inputs as
with producing_months as (
    select api_number, state, well_name, operator, county, field_name, basin,
           well_type, well_status, latitude, longitude,
           production_date, production_year, production_month,
           total_oil_bbl, total_gas_mcf, water_bbl, days_produced
    from stg_production
    where entity_type = 'well'
      and api_number is not null
      and (total_oil_bbl > 0 or total_gas_mcf > 0)
),
with_dca_fields as (
    select *, -- includes latitude, longitude from producing_months
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
select api_number, state, well_name, operator, county, field_name, basin,
       well_type, well_status, latitude, longitude,
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

TESTS: dict[str, str] = {
    "assert_no_negative_volumes": """
select * from stg_production
where oil_bbl < 0 or gas_mcf < 0 or condensate_bbl < 0
   or casinghead_gas_mcf < 0 or water_bbl < 0
""",
    "assert_no_future_production_dates": """
select * from stg_production where production_date > current_date
""",
    "assert_no_orphan_gold_records": """
select wph.entity_id, wph.state, wph.production_date
from well_production_history wph
left join stg_production stg
    on wph.entity_id = stg.entity_id and wph.state = stg.state
    and wph.production_date = stg.production_date
where stg.entity_id is null
""",
    "assert_months_on_production_sequential": """
with entity_stats as (
    select entity_id, state, count(*) as row_count,
           max(months_on_production) as max_month,
           min(months_on_production) as min_month
    from well_production_history group by entity_id, state
)
select * from entity_stats where min_month != 1 or max_month != row_count
""",
    "assert_county_entity_count_reconciles": """
with silver_count as (
    select count(*) as silver_rows from stg_production where county is not null
),
gold_count as (
    select sum(entity_count) as gold_entities from monthly_production_by_county
)
select * from silver_count cross join gold_count
where silver_rows != gold_entities
""",
    "assert_gold_reconciles_with_silver": """
with silver_totals as (
    select state, production_date,
           sum(total_oil_bbl) as silver_oil, sum(total_gas_mcf) as silver_gas,
           sum(water_bbl) as silver_water
    from stg_production where county is not null
    group by state, production_date
),
gold_totals as (
    select state, production_date,
           sum(total_oil_bbl) as gold_oil, sum(total_gas_mcf) as gold_gas,
           sum(total_water_bbl) as gold_water
    from monthly_production_by_county group by state, production_date
),
compared as (
    select coalesce(s.state, g.state) as state,
           coalesce(s.production_date, g.production_date) as production_date,
           s.silver_oil, g.gold_oil, s.silver_gas, g.gold_gas,
           s.silver_water, g.gold_water,
           abs(coalesce(s.silver_oil, 0) - coalesce(g.gold_oil, 0)) as oil_diff,
           abs(coalesce(s.silver_gas, 0) - coalesce(g.gold_gas, 0)) as gas_diff,
           abs(coalesce(s.silver_water, 0) - coalesce(g.gold_water, 0)) as water_diff
    from silver_totals s
    full outer join gold_totals g on s.state = g.state
        and s.production_date = g.production_date
)
select * from compared
where oil_diff > 0.01 or gas_diff > 0.01 or water_diff > 0.01
   or silver_oil is null or gold_oil is null
""",
    "assert_county_oil_reconciles": """
with silver_total as (
    select sum(oil_bbl) as silver_oil from stg_production where county is not null
),
gold_total as (
    select sum(crude_oil_bbl) as gold_oil from monthly_production_by_county
)
select * from silver_total cross join gold_total
where abs(silver_oil - gold_oil) > 0.01
""",
    "assert_county_gas_reconciles": """
with silver_total as (
    select sum(gas_mcf) as silver_gas from stg_production where county is not null
),
gold_total as (
    select sum(gas_well_gas_mcf) as gold_gas from monthly_production_by_county
)
select * from silver_total cross join gold_total
where abs(silver_gas - gold_gas) > 0.01
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
    """Build all gold models and optionally run tests via DuckDB.

    Parameters
    ----------
    duckdb_path:
        Path to the DuckDB database file.
    silver_parquet_glob:
        Glob pattern pointing at silver Parquet files
        (e.g. ``C:/…/data/silver/production/**/*.parquet``).
    log:
        Optional logger (Dagster context.log or similar) with ``.info``
        and ``.error`` methods.
    run_tests:
        If True (default) run all data-quality tests after building models.

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

        # 3. Tests
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
