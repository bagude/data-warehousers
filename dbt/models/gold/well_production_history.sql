/*
    Gold model: well_production_history

    Well-level (and lease-level) time series of all monthly production records.
    Includes:
    - months_on_production: ordinal counter (1 = first month for this entity)
    - Cumulative production columns (running sums of oil, gas, water)
    - Both combined totals and raw component volumes

    Entity ID construction:
    - Wells:  api_number (globally unique)
    - Leases: state || '-' || district || '-' || lease_number
      (TX lease_number is only unique within a district)

    NULL handling: cumulative sums use plain sum() over window — NULL volumes
    (e.g. water_bbl for TX) stay NULL in cumulatives.

    Grain: one row per (entity_id, state, production_date).
*/

select
    entity_id,
    state,
    entity_type,
    api_number,
    lease_number,
    district,
    well_name,
    operator,
    county,
    field_name,
    basin,
    well_type,
    well_status,

    production_date,
    production_year,
    production_month,

    -- Ordinal month counter per entity
    row_number() over (
        partition by entity_id, state
        order by production_date
    ) as months_on_production,

    -- Current-month combined volumes
    total_oil_bbl,
    total_gas_mcf,

    -- Current-month raw component volumes
    oil_bbl,
    condensate_bbl,
    gas_mcf,
    casinghead_gas_mcf,
    water_bbl,

    days_produced,

    -- Cumulative production (running sums through this month)
    sum(total_oil_bbl) over (
        partition by entity_id, state
        order by production_date
        rows between unbounded preceding and current row
    ) as cumulative_oil_bbl,

    sum(total_gas_mcf) over (
        partition by entity_id, state
        order by production_date
        rows between unbounded preceding and current row
    ) as cumulative_gas_mcf,

    sum(water_bbl) over (
        partition by entity_id, state
        order by production_date
        rows between unbounded preceding and current row
    ) as cumulative_water_bbl

from {{ ref('stg_production') }}
