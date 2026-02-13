/*
    Reconciliation test: total gas well gas in monthly_production_by_county must
    equal the sum from stg_production (for rows with non-null county).

    Returns rows where the difference exceeds 0.01 MCF — any results mean failure.
*/

with silver_total as (

    select
        sum(gas_mcf) as silver_gas
    from {{ ref('stg_production') }}
    where county is not null

),

gold_total as (

    select
        sum(gas_well_gas_mcf) as gold_gas
    from {{ ref('monthly_production_by_county') }}

)

select
    silver_total.silver_gas,
    gold_total.gold_gas,
    abs(silver_total.silver_gas - gold_total.gold_gas) as diff
from silver_total
cross join gold_total
where abs(silver_total.silver_gas - gold_total.gold_gas) > 0.01
