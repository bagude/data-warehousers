/*
    Reconciliation test: total crude oil in monthly_production_by_county must
    equal the sum from stg_production (for rows with non-null county).

    Returns rows where the difference exceeds 0.01 BBL — any results mean failure.
*/

with silver_total as (

    select
        sum(oil_bbl) as silver_oil
    from {{ ref('stg_production') }}
    where county is not null

),

gold_total as (

    select
        sum(crude_oil_bbl) as gold_oil
    from {{ ref('monthly_production_by_county') }}

)

select
    silver_total.silver_oil,
    gold_total.gold_oil,
    abs(silver_total.silver_oil - gold_total.gold_oil) as diff
from silver_total
cross join gold_total
where abs(silver_total.silver_oil - gold_total.gold_oil) > 0.01
