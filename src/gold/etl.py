"""Silver-to-dimensional-model ETL.

Reads silver Parquet via DuckDB read_parquet(), then populates all
dimension and fact tables in dependency order.

Mapping rules:
- DIM_LEASE: TX real leases from (district, lease_number). NM synthetic from api_number.
- DIM_WELL: SCD2 on (operator, field_name, well_status). Keyed by api_number.
  Only created for rows that have an api_number (NM wells, TX gas wells).
- DIM_WELLBORE: 1:1 with well, default WB01 suffix.
- DIM_COMPLETION: 1:1 with wellbore, default C01 suffix. SCD2 on status.
- DIM_PROD_UNIT: COMPLETION type for wells, LEASE_COMBINED for TX multi-well leases.
- FACT_EVENT: One per prod_unit-month. natural_event_id is source-stable.
- FACT_EVENT_OBJECT_BRIDGE: Per-event hierarchy walk.
- FACT_PRODUCTION_DETAIL: Measures from silver.
"""

from __future__ import annotations

from typing import Any

from src.gold.seed_constants import (
    PRIMARY_ROLE_KEY,
    ASSOCIATED_ROLE_KEY,
    PARENT_ROLE_KEY,
    PROD_UNIT_TYPE_KEY,
    LEASE_TYPE_KEY,
    WELL_TYPE_KEY,
    PRODUCTION_EVENT_KEY,
    REPORTED_STATUS_KEY,
)


def load_silver_to_dimensional_model(
    con,
    silver_parquet_glob: str,
    *,
    log: Any = None,
) -> dict[str, int]:
    """Load silver Parquet into the dimensional model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
    silver_parquet_glob : str
        Glob pattern for silver Parquet files.
    log : optional
        Logger with .info() and .error() methods.

    Returns
    -------
    dict with row counts per table.
    """

    def _log(msg, *args):
        if log:
            log.info(msg, *args)

    counts: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Step 0: Create staging view over silver Parquet
    # ------------------------------------------------------------------
    con.execute(f"""
        CREATE OR REPLACE VIEW _stg_silver AS
        SELECT *
        FROM read_parquet('{silver_parquet_glob}', hive_partitioning=true)
    """)
    stg_count = con.execute("SELECT COUNT(*) FROM _stg_silver").fetchone()[0]
    _log("Staging: %d silver rows", stg_count)

    # ------------------------------------------------------------------
    # Step 1: Resolve source_system_key from state
    # ------------------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW _stg_with_source AS
        SELECT s.*,
            CASE
                WHEN s.state = 'TX' THEN 1  -- TX_RRC_PDQ
                WHEN s.state = 'NM' THEN 3  -- NM_OCD_FTP
            END AS source_system_key
        FROM _stg_silver s
    """)

    # ------------------------------------------------------------------
    # Step 2: DIM_LEASE
    # ------------------------------------------------------------------
    # TX real leases (entity_type = 'lease')
    con.execute("""
        INSERT INTO dim_lease (
            lease_entity_key, lease_id, state, lease_number, district,
            lease_name, county, basin, is_synthetic, lease_grain_type
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY lease_id) AS lease_entity_key,
            lease_id, state, lease_number, district,
            lease_name, county, basin, is_synthetic, lease_grain_type
        FROM (
            SELECT DISTINCT ON (state, district, lease_number)
                state || '-' || district || '-' || lease_number AS lease_id,
                state,
                lease_number,
                district,
                well_name AS lease_name,
                county,
                basin,
                FALSE AS is_synthetic,
                'REAL_LEASE' AS lease_grain_type
            FROM _stg_silver
            WHERE entity_type = 'lease'
              AND lease_number IS NOT NULL
              AND district IS NOT NULL
            ORDER BY state, district, lease_number, production_date DESC
        ) sub
    """)
    tx_lease_count = con.execute("SELECT COUNT(*) FROM dim_lease").fetchone()[0]
    _log("DIM_LEASE after TX real leases: %d rows", tx_lease_count)

    # NM synthetic leases (1:1 with well)
    con.execute("""
        INSERT INTO dim_lease (
            lease_entity_key, lease_id, state, lease_number, district,
            lease_name, county, basin, is_synthetic, lease_grain_type
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY api_number)
                + (SELECT COALESCE(MAX(lease_entity_key), 0) FROM dim_lease) AS lease_entity_key,
            'NM-' || api_number AS lease_id,
            'NM' AS state,
            NULL AS lease_number,
            NULL AS district,
            well_name AS lease_name,
            county,
            basin,
            TRUE AS is_synthetic,
            'SYNTHETIC_CONTAINER' AS lease_grain_type
        FROM (
            SELECT DISTINCT ON (api_number)
                api_number, well_name, county, basin
            FROM _stg_silver
            WHERE state = 'NM' AND api_number IS NOT NULL
            ORDER BY api_number, production_date DESC
        ) sub
    """)
    _log("DIM_LEASE after NM synthetic: %d rows", con.execute("SELECT COUNT(*) FROM dim_lease").fetchone()[0])

    counts["dim_lease"] = con.execute("SELECT COUNT(*) FROM dim_lease").fetchone()[0]
    _log("DIM_LEASE: %d rows", counts["dim_lease"])

    # ------------------------------------------------------------------
    # Step 3: DIM_WELL (SCD2 - initial load = all current)
    # Only for rows with api_number (NM wells, TX gas well-level entities)
    # ------------------------------------------------------------------
    con.execute("""
        INSERT INTO dim_well (
            well_version_key, well_entity_key, well_id,
            lease_entity_key, api_number, state,
            well_name, well_number, operator, operator_id,
            county, field_name, basin,
            well_type_key, well_status_key,
            latitude, longitude,
            spud_date, completion_date, total_depth_ft,
            valid_from, valid_to, is_current
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY w.api_number) AS well_version_key,
            ROW_NUMBER() OVER (ORDER BY w.api_number) AS well_entity_key,
            w.api_number AS well_id,
            l.lease_entity_key,
            w.api_number,
            w.state,
            w.well_name,
            NULL AS well_number,
            w.operator,
            NULL AS operator_id,
            w.county,
            w.field_name,
            w.basin,
            wt.well_type_key,
            ws.well_status_key,
            w.latitude,
            w.longitude,
            NULL AS spud_date,
            NULL AS completion_date,
            NULL AS total_depth_ft,
            TIMESTAMP '2000-01-01' AS valid_from,
            NULL AS valid_to,
            TRUE AS is_current
        FROM (
            SELECT DISTINCT ON (api_number, state)
                api_number, state, well_name, operator, county,
                field_name, basin, well_type, well_status,
                latitude, longitude
            FROM _stg_silver
            WHERE api_number IS NOT NULL
            ORDER BY api_number, state, production_date DESC
        ) w
        LEFT JOIN dim_lease l ON (
            CASE
                WHEN w.state = 'NM' THEN 'NM-' || w.api_number
                WHEN w.state = 'TX' THEN 'TX-WELL-' || w.api_number
            END = l.lease_id
        )
        LEFT JOIN dim_well_type wt ON wt.well_type_code = COALESCE(w.well_type, 'UNKNOWN')
        LEFT JOIN dim_well_status ws ON ws.well_status_code = COALESCE(w.well_status, 'UNKNOWN')
    """)
    counts["dim_well"] = con.execute("SELECT COUNT(*) FROM dim_well").fetchone()[0]
    _log("DIM_WELL: %d rows", counts["dim_well"])

    # ------------------------------------------------------------------
    # Step 4: DIM_WELLBORE (1:1 with well)
    # ------------------------------------------------------------------
    con.execute("""
        INSERT INTO dim_wellbore (
            wellbore_entity_key, well_entity_key, wellbore_id
        )
        SELECT
            well_entity_key AS wellbore_entity_key,
            well_entity_key,
            api_number || '-WB01' AS wellbore_id
        FROM dim_well
        WHERE is_current = TRUE
    """)
    counts["dim_wellbore"] = con.execute("SELECT COUNT(*) FROM dim_wellbore").fetchone()[0]
    _log("DIM_WELLBORE: %d rows", counts["dim_wellbore"])

    # ------------------------------------------------------------------
    # Step 5: DIM_COMPLETION (1:1 with wellbore, initial load)
    # ------------------------------------------------------------------
    con.execute("""
        INSERT INTO dim_completion (
            completion_version_key, completion_entity_key,
            wellbore_entity_key, completion_id,
            completion_type, producing_formation,
            completion_status_key,
            valid_from, valid_to, is_current
        )
        SELECT
            wb.wellbore_entity_key AS completion_version_key,
            wb.wellbore_entity_key AS completion_entity_key,
            wb.wellbore_entity_key,
            wb.wellbore_id || '-C01' AS completion_id,
            NULL AS completion_type,
            NULL AS producing_formation,
            NULL AS completion_status_key,
            DATE '2000-01-01' AS valid_from,
            NULL AS valid_to,
            TRUE AS is_current
        FROM dim_wellbore wb
        JOIN dim_well w ON wb.well_entity_key = w.well_entity_key AND w.is_current = TRUE
    """)
    counts["dim_completion"] = con.execute("SELECT COUNT(*) FROM dim_completion").fetchone()[0]
    _log("DIM_COMPLETION: %d rows", counts["dim_completion"])

    # ------------------------------------------------------------------
    # Step 6: DIM_PROD_UNIT
    # ------------------------------------------------------------------
    # COMPLETION-type prod units (well-level production)
    con.execute("""
        INSERT INTO dim_prod_unit (
            prod_unit_key, prod_unit_id, prod_unit_type,
            completion_entity_key, lease_entity_key, state
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY c.completion_id) AS prod_unit_key,
            c.completion_id AS prod_unit_id,
            'COMPLETION' AS prod_unit_type,
            c.completion_entity_key,
            NULL AS lease_entity_key,
            w.state
        FROM dim_completion c
        JOIN dim_wellbore wb ON c.wellbore_entity_key = wb.wellbore_entity_key
        JOIN dim_well w ON wb.well_entity_key = w.well_entity_key AND w.is_current = TRUE
        WHERE c.is_current = TRUE
    """)

    # LEASE_COMBINED prod units (TX multi-well oil leases)
    con.execute("""
        INSERT INTO dim_prod_unit (
            prod_unit_key, prod_unit_id, prod_unit_type,
            completion_entity_key, lease_entity_key, state
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY l.lease_id)
                + (SELECT COALESCE(MAX(prod_unit_key), 0) FROM dim_prod_unit) AS prod_unit_key,
            l.lease_id || '-COMBINED' AS prod_unit_id,
            'LEASE_COMBINED' AS prod_unit_type,
            NULL AS completion_entity_key,
            l.lease_entity_key,
            l.state
        FROM dim_lease l
        WHERE l.lease_grain_type = 'REAL_LEASE'
          AND l.state = 'TX'
          AND l.lease_number IS NOT NULL
    """)

    counts["dim_prod_unit"] = con.execute("SELECT COUNT(*) FROM dim_prod_unit").fetchone()[0]
    _log("DIM_PROD_UNIT: %d rows", counts["dim_prod_unit"])

    # ------------------------------------------------------------------
    # Step 7: Build staging table that resolves prod_unit_key per silver row
    # ------------------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _stg_events AS
        SELECT
            s.*,
            COALESCE(pu_comp.prod_unit_key, pu_lease.prod_unit_key) AS resolved_prod_unit_key,
            COALESCE(pu_comp.prod_unit_id, pu_lease.prod_unit_id) AS resolved_prod_unit_id,
            COALESCE(pu_comp.prod_unit_type, pu_lease.prod_unit_type) AS resolved_prod_unit_type,
            s.source_system_key,
            CAST(strftime(s.production_date, '%Y%m') AS INTEGER) AS month_key,
            s.production_date::TIMESTAMP AS event_ts
        FROM _stg_with_source s
        -- Well-level -> COMPLETION prod unit (join chain: well -> wellbore -> completion -> prod_unit)
        LEFT JOIN dim_well w
            ON s.api_number IS NOT NULL
            AND s.api_number = w.api_number
            AND s.state = w.state
            AND w.is_current = TRUE
        LEFT JOIN dim_wellbore wb
            ON w.well_entity_key = wb.well_entity_key
        LEFT JOIN dim_completion c
            ON wb.wellbore_entity_key = c.wellbore_entity_key
            AND c.is_current = TRUE
        LEFT JOIN dim_prod_unit pu_comp
            ON c.completion_entity_key = pu_comp.completion_entity_key
            AND pu_comp.prod_unit_type = 'COMPLETION'
        -- Lease-level -> LEASE_COMBINED prod unit
        LEFT JOIN dim_lease l
            ON s.entity_type = 'lease'
            AND s.state || '-' || s.district || '-' || s.lease_number = l.lease_id
        LEFT JOIN dim_prod_unit pu_lease
            ON l.lease_entity_key = pu_lease.lease_entity_key
            AND pu_lease.prod_unit_type = 'LEASE_COMBINED'
        WHERE COALESCE(pu_comp.prod_unit_key, pu_lease.prod_unit_key) IS NOT NULL
    """)

    stg_events_count = con.execute("SELECT COUNT(*) FROM _stg_events").fetchone()[0]
    _log("Staging events: %d rows", stg_events_count)

    # ------------------------------------------------------------------
    # Step 8: FACT_EVENT
    # ------------------------------------------------------------------
    con.execute(f"""
        INSERT INTO fact_event (
            event_id, natural_event_id, event_version, event_hash,
            event_type_key, event_date_key, month_key, event_ts,
            source_system_key, event_status_key,
            is_current_version, superseded_at, source_file, ingested_at
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY resolved_prod_unit_id, production_date) AS event_id,
            'PRODUCTION:' ||
                CASE source_system_key
                    WHEN 1 THEN 'TX_RRC_PDQ'
                    WHEN 3 THEN 'NM_OCD_FTP'
                END || ':' ||
                resolved_prod_unit_id || ':' ||
                strftime(production_date, '%Y%m') AS natural_event_id,
            1 AS event_version,
            '' AS event_hash,
            {PRODUCTION_EVENT_KEY} AS event_type_key,
            NULL AS event_date_key,
            month_key,
            event_ts,
            source_system_key,
            {REPORTED_STATUS_KEY} AS event_status_key,
            TRUE AS is_current_version,
            NULL AS superseded_at,
            source_file,
            ingested_at
        FROM _stg_events
    """)
    counts["fact_event"] = con.execute("SELECT COUNT(*) FROM fact_event").fetchone()[0]
    _log("FACT_EVENT: %d rows", counts["fact_event"])

    # ------------------------------------------------------------------
    # Step 9: FACT_PRODUCTION_DETAIL
    # ------------------------------------------------------------------
    # Build natural_event_id expression for joining
    _nat_id_expr = """
        'PRODUCTION:' ||
        CASE s.source_system_key
            WHEN 1 THEN 'TX_RRC_PDQ'
            WHEN 3 THEN 'NM_OCD_FTP'
        END || ':' ||
        s.resolved_prod_unit_id || ':' ||
        strftime(s.production_date, '%Y%m')
    """

    con.execute(f"""
        INSERT INTO fact_production_detail (
            event_id, oil_bbl, gas_mcf, condensate_bbl, casinghead_gas_mcf,
            water_bbl, total_oil_bbl, total_gas_mcf, days_produced,
            oil_rate_bbl_per_day, gas_rate_mcf_per_day, gor, water_cut_pct
        )
        SELECT
            fe.event_id,
            s.oil_bbl,
            s.gas_mcf,
            s.condensate_bbl,
            s.casinghead_gas_mcf,
            s.water_bbl,
            COALESCE(s.oil_bbl, 0) + COALESCE(s.condensate_bbl, 0) AS total_oil_bbl,
            COALESCE(s.gas_mcf, 0) + COALESCE(s.casinghead_gas_mcf, 0) AS total_gas_mcf,
            s.days_produced,
            CASE
                WHEN NULLIF(s.days_produced, 0) IS NOT NULL THEN
                    (COALESCE(s.oil_bbl, 0) + COALESCE(s.condensate_bbl, 0))
                    / s.days_produced
                ELSE NULL
            END AS oil_rate_bbl_per_day,
            CASE
                WHEN NULLIF(s.days_produced, 0) IS NOT NULL THEN
                    (COALESCE(s.gas_mcf, 0) + COALESCE(s.casinghead_gas_mcf, 0))
                    / s.days_produced
                ELSE NULL
            END AS gas_rate_mcf_per_day,
            CASE
                WHEN NULLIF(COALESCE(s.oil_bbl, 0) + COALESCE(s.condensate_bbl, 0), 0) IS NOT NULL THEN
                    (COALESCE(s.gas_mcf, 0) + COALESCE(s.casinghead_gas_mcf, 0))
                    / (COALESCE(s.oil_bbl, 0) + COALESCE(s.condensate_bbl, 0))
                    * 1000
                ELSE NULL
            END AS gor,
            CASE
                WHEN NULLIF(s.water_bbl + COALESCE(s.oil_bbl, 0) + COALESCE(s.condensate_bbl, 0), 0) IS NOT NULL THEN
                    s.water_bbl
                    / (s.water_bbl + COALESCE(s.oil_bbl, 0) + COALESCE(s.condensate_bbl, 0))
                    * 100
                ELSE NULL
            END AS water_cut_pct
        FROM _stg_events s
        JOIN fact_event fe ON fe.natural_event_id = {_nat_id_expr}
        WHERE fe.is_current_version = TRUE
    """)
    counts["fact_production_detail"] = con.execute("SELECT COUNT(*) FROM fact_production_detail").fetchone()[0]
    _log("FACT_PRODUCTION_DETAIL: %d rows", counts["fact_production_detail"])

    # ------------------------------------------------------------------
    # Step 10: FACT_EVENT_OBJECT_BRIDGE
    # ------------------------------------------------------------------
    # PRIMARY: PROD_UNIT (every event gets one)
    con.execute(f"""
        INSERT INTO fact_event_object_bridge (event_id, object_type_key, object_key, object_role_key)
        SELECT
            fe.event_id,
            {PROD_UNIT_TYPE_KEY},
            s.resolved_prod_unit_key,
            {PRIMARY_ROLE_KEY}
        FROM _stg_events s
        JOIN fact_event fe ON fe.natural_event_id = {_nat_id_expr}
        WHERE fe.is_current_version = TRUE
    """)

    # PARENT: WELL (for COMPLETION-type prod units only)
    con.execute(f"""
        INSERT INTO fact_event_object_bridge (event_id, object_type_key, object_key, object_role_key)
        SELECT
            fe.event_id,
            {WELL_TYPE_KEY},
            w.well_entity_key,
            {PARENT_ROLE_KEY}
        FROM _stg_events s
        JOIN fact_event fe ON fe.natural_event_id = {_nat_id_expr}
        JOIN dim_prod_unit pu ON s.resolved_prod_unit_key = pu.prod_unit_key
        JOIN dim_completion c ON pu.completion_entity_key = c.completion_entity_key AND c.is_current = TRUE
        JOIN dim_wellbore wb ON c.wellbore_entity_key = wb.wellbore_entity_key
        JOIN dim_well w ON wb.well_entity_key = w.well_entity_key AND w.is_current = TRUE
        WHERE fe.is_current_version = TRUE
          AND pu.prod_unit_type = 'COMPLETION'
    """)

    # PARENT/ASSOCIATED: LEASE
    # For LEASE_COMBINED -> PARENT role (the lease is the parent container)
    # For COMPLETION -> ASSOCIATED role (the lease is associated via the well)
    con.execute(f"""
        INSERT INTO fact_event_object_bridge (event_id, object_type_key, object_key, object_role_key)
        SELECT
            fe.event_id,
            {LEASE_TYPE_KEY},
            COALESCE(l_direct.lease_entity_key, l_well.lease_entity_key),
            CASE
                WHEN s.resolved_prod_unit_type = 'LEASE_COMBINED' THEN {PARENT_ROLE_KEY}
                ELSE {ASSOCIATED_ROLE_KEY}
            END
        FROM _stg_events s
        JOIN fact_event fe ON fe.natural_event_id = {_nat_id_expr}
        -- LEASE_COMBINED: direct from prod_unit
        LEFT JOIN dim_prod_unit pu_lc ON s.resolved_prod_unit_key = pu_lc.prod_unit_key
            AND pu_lc.prod_unit_type = 'LEASE_COMBINED'
        LEFT JOIN dim_lease l_direct ON pu_lc.lease_entity_key = l_direct.lease_entity_key
        -- COMPLETION: walk up to well -> lease
        LEFT JOIN dim_prod_unit pu_c ON s.resolved_prod_unit_key = pu_c.prod_unit_key
            AND pu_c.prod_unit_type = 'COMPLETION'
        LEFT JOIN dim_completion c ON pu_c.completion_entity_key = c.completion_entity_key AND c.is_current = TRUE
        LEFT JOIN dim_wellbore wb ON c.wellbore_entity_key = wb.wellbore_entity_key
        LEFT JOIN dim_well w ON wb.well_entity_key = w.well_entity_key AND w.is_current = TRUE
        LEFT JOIN dim_lease l_well ON w.lease_entity_key = l_well.lease_entity_key
        WHERE fe.is_current_version = TRUE
          AND COALESCE(l_direct.lease_entity_key, l_well.lease_entity_key) IS NOT NULL
    """)

    counts["fact_event_object_bridge"] = con.execute("SELECT COUNT(*) FROM fact_event_object_bridge").fetchone()[0]
    _log("BRIDGE: %d rows", counts["fact_event_object_bridge"])

    # ------------------------------------------------------------------
    # Cleanup staging
    # ------------------------------------------------------------------
    con.execute("DROP VIEW IF EXISTS _stg_silver")
    con.execute("DROP VIEW IF EXISTS _stg_with_source")
    con.execute("DROP TABLE IF EXISTS _stg_events")

    return counts
