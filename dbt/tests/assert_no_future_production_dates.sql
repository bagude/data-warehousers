/*
    Data quality test: no production records should have a future date.

    Returns rows with production_date > current_date — any results mean failure.
*/

select
    entity_id,
    state,
    production_date
from {{ ref('stg_production') }}
where production_date > current_date
