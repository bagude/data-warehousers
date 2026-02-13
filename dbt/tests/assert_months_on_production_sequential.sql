/*
    Integrity test: months_on_production in well_production_history must be
    a contiguous 1-indexed sequence per entity.

    For each entity, the max(months_on_production) should equal count(*).
    If there are gaps or duplicates this will fail.

    Returns entities where the sequence is broken — any results mean failure.
*/

with entity_stats as (

    select
        entity_id,
        state,
        count(*)                        as row_count,
        max(months_on_production)       as max_month,
        min(months_on_production)       as min_month
    from {{ ref('well_production_history') }}
    group by entity_id, state

)

select
    entity_id,
    state,
    row_count,
    max_month,
    min_month
from entity_stats
where min_month != 1
   or max_month != row_count
