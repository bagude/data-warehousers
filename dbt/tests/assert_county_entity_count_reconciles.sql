/*
    Reconciliation test: total entity_count in monthly_production_by_county
    must equal the number of silver rows with non-null county.

    Returns rows where counts don't match — any results mean failure.
*/

with silver_count as (

    select
        count(*) as silver_rows
    from {{ ref('stg_production') }}
    where county is not null

),

gold_count as (

    select
        sum(entity_count) as gold_entities
    from {{ ref('monthly_production_by_county') }}

)

select
    silver_count.silver_rows,
    gold_count.gold_entities,
    silver_count.silver_rows - gold_count.gold_entities as diff
from silver_count
cross join gold_count
where silver_count.silver_rows != gold_count.gold_entities
