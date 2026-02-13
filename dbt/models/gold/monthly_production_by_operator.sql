/*
    Gold model: monthly_production_by_operator

    Aggregates monthly production volumes by operator, state, and county.
    Includes well and lease counts so analysts can normalize per-entity.

    NULL handling: all volume columns use plain sum() — NULL propagates
    correctly (NULL means "not applicable for this state", not zero).

    Grain: one row per (operator, state, county, production_date).
*/

select
    operator,
    state,
    county,
    production_date,
    production_year,
    production_month,

    -- Entity counts
    count(*)                                         as entity_count,
    count(*) filter (where entity_type = 'well')     as well_count,
    count(*) filter (where entity_type = 'lease')    as lease_count,

    -- Combined volumes (crude + condensate, well gas + casinghead)
    sum(total_oil_bbl)                               as total_oil_bbl,
    sum(total_gas_mcf)                               as total_gas_mcf,

    -- Raw component volumes (NULL = not applicable for this state)
    sum(oil_bbl)                                     as crude_oil_bbl,
    sum(condensate_bbl)                              as condensate_bbl,
    sum(gas_mcf)                                     as gas_well_gas_mcf,
    sum(casinghead_gas_mcf)                          as casinghead_gas_mcf,
    sum(water_bbl)                                   as total_water_bbl

from {{ ref('stg_production') }}

where operator is not null

group by
    operator,
    state,
    county,
    production_date,
    production_year,
    production_month
