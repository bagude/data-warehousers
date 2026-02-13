/*
    Decline curve completeness: for each well in decline_curve_inputs,
    months_on_production must be a contiguous 1-indexed sequence with no gaps.

    For a well with N rows, min(months_on_production) must be 1 and
    max(months_on_production) must equal N.  If there are gaps or duplicates,
    this test fails.

    Returns wells with non-contiguous month sequences -- any results mean failure.
*/

with well_stats as (

    select
        api_number,
        state,
        count(*)                        as row_count,
        min(months_on_production)       as min_month,
        max(months_on_production)       as max_month
    from {{ ref('decline_curve_inputs') }}
    group by api_number, state

)

select
    api_number,
    state,
    row_count,
    min_month,
    max_month
from well_stats
where min_month != 1
   or max_month != row_count
