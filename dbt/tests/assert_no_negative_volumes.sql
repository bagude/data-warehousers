/*
    Data quality test: no volume columns should contain negative values
    in the staging layer.

    Returns rows with any negative volume — any results mean failure.
*/

select
    entity_id,
    state,
    production_date,
    oil_bbl,
    gas_mcf,
    condensate_bbl,
    casinghead_gas_mcf,
    water_bbl
from {{ ref('stg_production') }}
where oil_bbl < 0
   or gas_mcf < 0
   or condensate_bbl < 0
   or casinghead_gas_mcf < 0
   or water_bbl < 0
