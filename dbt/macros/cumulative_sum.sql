/*
    Macro: cumulative_sum

    Generates a running-sum window expression partitioned by entity and ordered
    by production_date.  Used in well_production_history and decline_curve_inputs
    to avoid repeating the verbose window clause.

    Usage:
        {{ cumulative_sum('total_oil_bbl', 'entity_id', 'state') }}

    Expands to:
        sum(total_oil_bbl) over (
            partition by entity_id, state
            order by production_date
            rows between unbounded preceding and current row
        )
*/

{% macro cumulative_sum(column, partition_col, partition_col_2='state') %}
sum({{ column }}) over (
    partition by {{ partition_col }}, {{ partition_col_2 }}
    order by production_date
    rows between unbounded preceding and current row
)
{% endmacro %}
