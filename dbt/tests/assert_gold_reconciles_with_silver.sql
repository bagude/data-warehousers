/*
    Reconciliation test: sum of gold monthly_production_by_county volumes must
    equal the sum of staging volumes for the same state and period.

    Compares total_oil_bbl and total_gas_mcf (the combined columns in staging
    vs the gold aggregation).  A tolerance of 0.01 is used for floating-point
    rounding.

    Returns rows where any volume mismatch exceeds tolerance -- any results
    mean failure.
*/

with silver_totals as (

    select
        state,
        production_date,
        sum(total_oil_bbl)          as silver_oil,
        sum(total_gas_mcf)          as silver_gas,
        sum(water_bbl)              as silver_water
    from {{ ref('stg_production') }}
    where county is not null
    group by state, production_date

),

gold_totals as (

    select
        state,
        production_date,
        sum(total_oil_bbl)      as gold_oil,
        sum(total_gas_mcf)      as gold_gas,
        sum(total_water_bbl)    as gold_water
    from {{ ref('monthly_production_by_county') }}
    group by state, production_date

),

compared as (

    select
        coalesce(s.state, g.state)                   as state,
        coalesce(s.production_date, g.production_date) as production_date,
        s.silver_oil,
        g.gold_oil,
        s.silver_gas,
        g.gold_gas,
        s.silver_water,
        g.gold_water,
        abs(coalesce(s.silver_oil, 0) - coalesce(g.gold_oil, 0))     as oil_diff,
        abs(coalesce(s.silver_gas, 0) - coalesce(g.gold_gas, 0))     as gas_diff,
        abs(coalesce(s.silver_water, 0) - coalesce(g.gold_water, 0)) as water_diff
    from silver_totals s
    full outer join gold_totals g
        on s.state = g.state
        and s.production_date = g.production_date

)

select *
from compared
where oil_diff > 0.01
   or gas_diff > 0.01
   or water_diff > 0.01
   or silver_oil is null
   or gold_oil is null
