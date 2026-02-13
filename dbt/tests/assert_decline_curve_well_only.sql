/*
    Integrity test: decline_curve_inputs should contain only well-level
    records (no lease entities).

    Checks that every api_number in the DCA table exists in stg_production
    with entity_type = 'well'.

    Returns rows that violate the constraint — any results mean failure.
*/

select
    dca.api_number,
    dca.state,
    dca.production_date
from {{ ref('decline_curve_inputs') }} dca
left join {{ ref('stg_production') }} stg
    on dca.api_number = stg.api_number
    and dca.state = stg.state
    and dca.production_date = stg.production_date
where stg.entity_type != 'well'
   or stg.api_number is null
