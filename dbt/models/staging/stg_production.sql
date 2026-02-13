/*
    Staging model: stg_production

    Reads silver-layer Parquet files directly using DuckDB's read_parquet().
    The glob uses ** to support Hive-style state= partitioned directories
    under data/silver/production/.

    Transformations applied:
    - total_oil_bbl  = oil_bbl + condensate_bbl  (NULLs coalesced to 0)
    - total_gas_mcf  = gas_mcf + casinghead_gas_mcf (NULLs coalesced to 0)
    - production_year and production_month extracted for convenience
    - entity_id: unified identifier
        - wells:  api_number
        - leases: state || '-' || district || '-' || lease_number
          (lease_number is only unique within a TX RRC district)

    Materialized as a view — no storage overhead.
*/

with source as (

    select *
    from read_parquet('{{ var("silver_parquet_path", "../data/silver/production/**/*.parquet") }}', hive_partitioning=true)

)

select
    -- Unified entity identifier
    case
        when entity_type = 'well'  then api_number
        when entity_type = 'lease' then state || '-' || district || '-' || lease_number
    end as entity_id,

    -- Identity
    state,
    entity_type,
    api_number,
    lease_number,
    district,

    -- Descriptive
    well_name,
    operator,
    county,
    field_name,
    basin,
    well_type,
    well_status,

    -- Time
    production_date,
    extract(year from production_date)::int   as production_year,
    extract(month from production_date)::int  as production_month,

    -- Raw volumes (preserved for auditability)
    oil_bbl,
    gas_mcf,
    condensate_bbl,
    casinghead_gas_mcf,
    water_bbl,

    -- Combined volumes (condensate folded into oil, casinghead folded into gas)
    coalesce(oil_bbl, 0) + coalesce(condensate_bbl, 0)     as total_oil_bbl,
    coalesce(gas_mcf, 0) + coalesce(casinghead_gas_mcf, 0)  as total_gas_mcf,

    -- Operational
    days_produced,

    -- Lineage
    source_file,
    ingested_at

from source
