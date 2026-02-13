/*
    Orphan check: every record in well_production_history must trace back to
    at least one silver record.  There should be no gold rows whose
    (entity_id, state, production_date) does not exist in stg_production.

    Returns orphan gold rows -- any results mean failure.
*/

select
    wph.entity_id,
    wph.state,
    wph.production_date
from {{ ref('well_production_history') }} wph
left join {{ ref('stg_production') }} stg
    on wph.entity_id = stg.entity_id
    and wph.state = stg.state
    and wph.production_date = stg.production_date
where stg.entity_id is null
