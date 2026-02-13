"""Canonical reportable views for the dimensional model.

All downstream marts MUST source from these views, never from
fact tables directly. This enforces retraction/amendment filtering.
"""

from __future__ import annotations

from src.gold.seed_constants import RETRACTED_STATUS_KEY, PRODUCTION_EVENT_KEY


def create_canonical_views(con) -> None:
    """Create the three canonical views."""

    con.execute("""
        CREATE OR REPLACE VIEW vw_fact_event_current AS
        SELECT * FROM fact_event
        WHERE is_current_version = TRUE
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW vw_fact_event_reportable AS
        SELECT * FROM fact_event
        WHERE is_current_version = TRUE
          AND event_status_key != {RETRACTED_STATUS_KEY}
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW vw_production_current AS
        SELECT
            fe.event_id,
            fe.natural_event_id,
            fe.month_key,
            fe.event_ts,
            fe.source_system_key,
            pd.oil_bbl,
            pd.gas_mcf,
            pd.condensate_bbl,
            pd.casinghead_gas_mcf,
            pd.water_bbl,
            pd.total_oil_bbl,
            pd.total_gas_mcf,
            pd.days_produced,
            pd.oil_rate_bbl_per_day,
            pd.gas_rate_mcf_per_day,
            pd.gor,
            pd.water_cut_pct
        FROM vw_fact_event_reportable fe
        JOIN fact_production_detail pd ON fe.event_id = pd.event_id
        WHERE fe.event_type_key = {PRODUCTION_EVENT_KEY}
    """)
