/*
    Gold model: decline_curve_inputs

    Per-well monthly production with derived fields for decline curve analysis
    (DCA).  Only well-level records (entity_type = 'well') are included since
    decline curves are a per-well concept.

    Only months with non-zero oil or gas production are included -- shut-in
    months (zero production) are excluded so that months_on_production counts
    actual producing months, not calendar months.

    Derived columns:
    - months_on_production:    1-indexed counter of producing months
    - initial_oil_rate:        total oil in the first producing month (BBL)
    - initial_gas_rate:        total gas in the first producing month (MCF)
    - cumulative_oil:          running sum of total oil through this month
    - cumulative_gas:          running sum of total gas through this month
    - oil_rate_pct_of_initial: (current month oil / initial oil) * 100

    Grain: one row per (api_number, state, production_date).
*/

with producing_months as (

    select
        api_number,
        state,
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

        total_oil_bbl,
        total_gas_mcf,
        water_bbl,
        days_produced

    from {{ ref('stg_production') }}

    where entity_type = 'well'
      and api_number is not null
      and (total_oil_bbl > 0 or total_gas_mcf > 0)

),

with_dca_fields as (

    select
        *,

        -- Ordinal counter: 1 = first producing month (shut-in months excluded)
        row_number() over (
            partition by api_number, state
            order by production_date
        ) as months_on_production,

        -- First-month production (initial rate for DCA)
        first_value(total_oil_bbl) over (
            partition by api_number, state
            order by production_date
            rows between unbounded preceding and unbounded following
        ) as initial_oil_rate,

        first_value(total_gas_mcf) over (
            partition by api_number, state
            order by production_date
            rows between unbounded preceding and unbounded following
        ) as initial_gas_rate,

        -- Cumulative production through this month
        sum(total_oil_bbl) over (
            partition by api_number, state
            order by production_date
            rows between unbounded preceding and current row
        ) as cumulative_oil,

        sum(total_gas_mcf) over (
            partition by api_number, state
            order by production_date
            rows between unbounded preceding and current row
        ) as cumulative_gas,

        sum(water_bbl) over (
            partition by api_number, state
            order by production_date
            rows between unbounded preceding and current row
        ) as cumulative_water

    from producing_months

)

select
    api_number,
    state,
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
    months_on_production,

    -- Current month volumes
    total_oil_bbl,
    total_gas_mcf,
    water_bbl,
    days_produced,

    -- Initial rates (first producing month)
    initial_oil_rate,
    initial_gas_rate,

    -- Decline rate: current month as percentage of initial
    case
        when initial_oil_rate > 0
        then round(total_oil_bbl / initial_oil_rate * 100, 2)
        else null
    end as oil_rate_pct_of_initial,

    -- Cumulative volumes
    cumulative_oil,
    cumulative_gas,
    cumulative_water

from with_dca_fields
