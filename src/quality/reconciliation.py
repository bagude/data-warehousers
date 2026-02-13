"""Silver-to-gold aggregation reconciliation checks.

These checks verify that the gold layer is a faithful aggregation of the
silver layer — no rows were silently dropped or double-counted during
the dbt transformation.

Checks
======

1. **Volume reconciliation**: The sum of a volume column across all gold
   rows for a given period must equal the sum from silver for the same
   period (within a small floating-point tolerance).

2. **Entity count reconciliation**: The total entity_count in the gold
   aggregation table must equal the row count in silver for the same
   group-by dimensions.

3. **Primary key uniqueness**: Gold tables must have no duplicate primary
   keys.

4. **Completeness / no orphans**: Every gold row must trace back to at
   least one silver row.  There should be no "orphan" gold records that
   have no silver source.

5. **Month completeness**: For well_production_history, check for
   unexpected gaps (missing months) in each entity's time series.

Usage
=====

These checks are designed to be called after ``dbt run`` and ``dbt test``
as an additional Python-side validation layer.  They read both the silver
Parquet files and the DuckDB gold tables to cross-check.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ReconciliationResult:
    """Outcome of a silver-to-gold reconciliation check."""

    name: str
    passed: bool
    message: str
    details: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Volume reconciliation
# ---------------------------------------------------------------------------

def reconcile_volumes(
    silver_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    *,
    volume_column: str,
    gold_volume_column: str | None = None,
    group_keys: list[str],
    tolerance: float = 0.01,
) -> ReconciliationResult:
    """Check that summed volumes in gold match silver for matching groups.

    Parameters
    ----------
    silver_df:
        Silver-layer production DataFrame.
    gold_df:
        Gold-layer aggregated DataFrame.
    volume_column:
        Column name in silver to sum (e.g. 'oil_bbl').
    gold_volume_column:
        Column name in gold that holds the aggregated value.  Defaults to
        ``volume_column`` if not specified.
    group_keys:
        Columns to group by for comparison (e.g. ['state', 'production_date']).
    tolerance:
        Maximum acceptable absolute difference between silver sum and gold
        sum, as a fraction of the silver sum.
    """
    gold_vol = gold_volume_column or volume_column

    # Sum silver by group keys
    silver_agg = (
        silver_df
        .groupby(group_keys, dropna=False)[volume_column]
        .sum()
        .reset_index(name="silver_total")
    )

    # Sum gold by group keys (gold should already be aggregated, but re-sum
    # in case multiple gold rows map to the same group)
    gold_agg = (
        gold_df
        .groupby(group_keys, dropna=False)[gold_vol]
        .sum()
        .reset_index(name="gold_total")
    )

    merged = silver_agg.merge(gold_agg, on=group_keys, how="outer", indicator=True)

    # Check for groups missing from gold
    silver_only = merged[merged["_merge"] == "left_only"]
    gold_only = merged[merged["_merge"] == "right_only"]
    both = merged[merged["_merge"] == "both"]

    mismatches: list[dict] = []
    if not both.empty:
        both = both.copy()
        both["diff"] = (both["gold_total"] - both["silver_total"]).abs()
        both["rel_diff"] = both["diff"] / both["silver_total"].abs().clip(lower=1e-10)
        bad = both[both["rel_diff"] > tolerance]
        if not bad.empty:
            mismatches = bad.head(5).to_dict("records")

    n_issues = len(silver_only) + len(gold_only) + len(mismatches)
    passed = n_issues == 0

    parts: list[str] = []
    if len(silver_only) > 0:
        parts.append(f"{len(silver_only)} group(s) in silver but missing from gold")
    if len(gold_only) > 0:
        parts.append(f"{len(gold_only)} orphan group(s) in gold not in silver")
    if mismatches:
        parts.append(f"{len(mismatches)} group(s) with volume mismatch > {tolerance:.0%}")

    message = (
        f"Volume reconciliation ({volume_column}): "
        + ("; ".join(parts) if parts else "all groups match")
    )

    return ReconciliationResult(
        name=f"volume_reconciliation_{volume_column}",
        passed=passed,
        message=message,
        details={
            "silver_only_count": len(silver_only),
            "gold_only_count": len(gold_only),
            "mismatch_count": len(mismatches),
            "sample_mismatches": mismatches,
            "tolerance": tolerance,
        },
    )


# ---------------------------------------------------------------------------
# Entity count reconciliation
# ---------------------------------------------------------------------------

def reconcile_entity_counts(
    silver_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    *,
    group_keys: list[str],
    gold_count_column: str = "entity_count",
) -> ReconciliationResult:
    """Check that entity counts in gold match row counts from silver.

    Parameters
    ----------
    silver_df:
        Silver-layer DataFrame.
    gold_df:
        Gold aggregation DataFrame with an entity count column.
    group_keys:
        Columns to group by.
    gold_count_column:
        Name of the count column in the gold table.
    """
    silver_counts = (
        silver_df
        .groupby(group_keys, dropna=False)
        .size()
        .reset_index(name="silver_count")
    )

    gold_counts = (
        gold_df
        .groupby(group_keys, dropna=False)[gold_count_column]
        .sum()
        .reset_index(name="gold_count")
    )

    merged = silver_counts.merge(gold_counts, on=group_keys, how="outer", indicator=True)

    silver_only = len(merged[merged["_merge"] == "left_only"])
    gold_only = len(merged[merged["_merge"] == "right_only"])

    both = merged[merged["_merge"] == "both"].copy()
    mismatches = 0
    if not both.empty:
        mismatches = int((both["silver_count"] != both["gold_count"]).sum())

    n_issues = silver_only + gold_only + mismatches
    passed = n_issues == 0

    message = (
        f"Entity count reconciliation: "
        f"{silver_only} missing from gold, {gold_only} orphans in gold, "
        f"{mismatches} count mismatches."
        if not passed
        else "Entity count reconciliation: all groups match."
    )

    return ReconciliationResult(
        name="entity_count_reconciliation",
        passed=passed,
        message=message,
        details={
            "silver_only": silver_only,
            "gold_only": gold_only,
            "count_mismatches": mismatches,
        },
    )


# ---------------------------------------------------------------------------
# Primary key uniqueness
# ---------------------------------------------------------------------------

def check_pk_uniqueness(
    df: pd.DataFrame,
    pk_columns: list[str],
    *,
    table_name: str = "unknown",
) -> ReconciliationResult:
    """Verify that primary key columns are unique in a gold table.

    Parameters
    ----------
    df:
        Gold-layer DataFrame.
    pk_columns:
        Columns forming the primary key.
    table_name:
        Name of the gold table (for reporting).
    """
    if df.empty:
        return ReconciliationResult(
            name=f"pk_uniqueness_{table_name}",
            passed=True,
            message=f"{table_name}: empty table, PK uniqueness trivially satisfied.",
        )

    n_dupes = int(df.duplicated(subset=pk_columns, keep=False).sum())
    passed = n_dupes == 0

    message = (
        f"{table_name}: PK {pk_columns} is unique."
        if passed
        else f"{table_name}: {n_dupes} rows violate PK uniqueness on {pk_columns}."
    )

    return ReconciliationResult(
        name=f"pk_uniqueness_{table_name}",
        passed=passed,
        message=message,
        details={"duplicate_rows": n_dupes, "pk_columns": pk_columns},
    )


# ---------------------------------------------------------------------------
# Month completeness for well histories
# ---------------------------------------------------------------------------

def check_month_completeness(
    df: pd.DataFrame,
    *,
    entity_col: str = "entity_id",
    state_col: str = "state",
    date_col: str = "production_date",
    max_gap_months: int = 3,
    sample_size: int = 5,
) -> ReconciliationResult:
    """Check for unexpected gaps in well production time series.

    A "gap" is defined as a stretch of more than ``max_gap_months``
    consecutive months with no production record for an entity.  Short gaps
    (1-2 months) are normal in oil & gas data; long gaps may indicate
    missing data.

    Parameters
    ----------
    df:
        well_production_history or similar DataFrame.
    entity_col:
        Column identifying the entity (api_number, entity_id, etc.).
    state_col:
        State column (part of the entity partition).
    date_col:
        Production date column.
    max_gap_months:
        Maximum acceptable gap before flagging.
    sample_size:
        Number of sample entities with gaps to include in details.
    """
    if df.empty:
        return ReconciliationResult(
            name="month_completeness",
            passed=True,
            message="Empty DataFrame; no gaps to check.",
        )

    dates = pd.to_datetime(df[date_col])
    df_work = df[[entity_col, state_col]].copy()
    df_work["_date"] = dates

    entities_with_gaps: list[dict] = []

    for (entity, state), group in df_work.groupby([entity_col, state_col]):
        sorted_dates = group["_date"].sort_values()
        if len(sorted_dates) < 2:
            continue

        diffs = sorted_dates.diff().dt.days
        # A normal month gap is ~28-31 days.  Flag gaps > max_gap_months * 31.
        max_allowed_days = max_gap_months * 31
        big_gaps = diffs[diffs > max_allowed_days]

        if not big_gaps.empty:
            entities_with_gaps.append({
                "entity": entity,
                "state": state,
                "gap_count": len(big_gaps),
                "max_gap_days": int(big_gaps.max()),
            })

    n_flagged = len(entities_with_gaps)
    passed = n_flagged == 0

    message = (
        f"Month completeness: no entities have gaps > {max_gap_months} months."
        if passed
        else (
            f"Month completeness: {n_flagged} entities have gaps "
            f"> {max_gap_months} months."
        )
    )

    return ReconciliationResult(
        name="month_completeness",
        passed=passed,
        message=message,
        details={
            "flagged_entities": n_flagged,
            "max_gap_months_threshold": max_gap_months,
            "samples": entities_with_gaps[:sample_size],
        },
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_silver_to_gold_checks(
    silver_df: pd.DataFrame,
    gold_tables: dict[str, pd.DataFrame],
) -> list[ReconciliationResult]:
    """Run all silver-to-gold reconciliation checks.

    Parameters
    ----------
    silver_df:
        The silver-layer production DataFrame.
    gold_tables:
        Dict mapping gold table name to its DataFrame.  Expected keys:
        'monthly_production_by_county', 'monthly_production_by_operator',
        'monthly_production_by_field', 'well_production_history',
        'decline_curve_inputs'.

    Returns
    -------
    List of ReconciliationResult objects.
    """
    results: list[ReconciliationResult] = []

    # --- Volume reconciliation on county aggregation ---
    county_df = gold_tables.get("monthly_production_by_county")
    if county_df is not None:
        for vol_col, gold_col in [
            ("oil_bbl", "crude_oil_bbl"),
            ("gas_mcf", "gas_well_gas_mcf"),
        ]:
            results.append(reconcile_volumes(
                silver_df[silver_df["county"].notna()],
                county_df,
                volume_column=vol_col,
                gold_volume_column=gold_col,
                group_keys=["county", "state", "production_date"],
            ))

        results.append(reconcile_entity_counts(
            silver_df[silver_df["county"].notna()],
            county_df,
            group_keys=["county", "state", "production_date"],
        ))

        results.append(check_pk_uniqueness(
            county_df,
            ["county", "state", "production_date"],
            table_name="monthly_production_by_county",
        ))

    # --- PK checks on other gold tables ---
    operator_df = gold_tables.get("monthly_production_by_operator")
    if operator_df is not None:
        results.append(check_pk_uniqueness(
            operator_df,
            ["operator", "state", "county", "production_date"],
            table_name="monthly_production_by_operator",
        ))

    field_df = gold_tables.get("monthly_production_by_field")
    if field_df is not None:
        results.append(check_pk_uniqueness(
            field_df,
            ["field_name", "basin", "state", "production_date"],
            table_name="monthly_production_by_field",
        ))

    history_df = gold_tables.get("well_production_history")
    if history_df is not None:
        results.append(check_pk_uniqueness(
            history_df,
            ["entity_id", "state", "production_date"],
            table_name="well_production_history",
        ))
        results.append(check_month_completeness(history_df))

    dca_df = gold_tables.get("decline_curve_inputs")
    if dca_df is not None:
        results.append(check_pk_uniqueness(
            dca_df,
            ["api_number", "state", "production_date"],
            table_name="decline_curve_inputs",
        ))

    for r in results:
        level = logging.INFO if r.passed else logging.WARNING
        logger.log(level, "%s: %s", "PASS" if r.passed else "FAIL", r.message)

    return results


# ---------------------------------------------------------------------------
# Dimensional model reconciliation (new gold layer)
# ---------------------------------------------------------------------------

def reconcile_dimensional_model(con) -> list[ReconciliationResult]:
    """Run basic reconciliation checks against the dimensional model.

    This function validates the new dimensional model's ``vw_production_current``
    view.  It checks:

    1. **Row count**: ``vw_production_current`` has at least one row.
    2. **Event-to-detail integrity**: Every ``fact_event`` with
       ``event_type_key = 1`` (PRODUCTION) has a matching
       ``fact_production_detail`` row.
    3. **No negative volumes**: ``total_oil_bbl`` and ``total_gas_mcf`` in
       ``vw_production_current`` are non-negative.
    4. **Bridge completeness**: Every current event has at least one bridge row.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection with the dimensional model populated
        (tables + views must exist).

    Returns
    -------
    List of ReconciliationResult objects.
    """
    results: list[ReconciliationResult] = []

    # 1. Row count check
    row_count = con.execute("SELECT COUNT(*) FROM vw_production_current").fetchone()[0]
    results.append(ReconciliationResult(
        name="dimensional_production_row_count",
        passed=row_count > 0,
        message=(
            f"vw_production_current: {row_count} rows."
            if row_count > 0
            else "vw_production_current: 0 rows -- no production loaded."
        ),
        details={"row_count": row_count},
    ))

    # 2. Event-to-detail integrity
    orphan_events = con.execute("""
        SELECT COUNT(*) FROM vw_fact_event_current fe
        WHERE fe.event_type_key = 1
          AND NOT EXISTS (
              SELECT 1 FROM fact_production_detail pd
              WHERE pd.event_id = fe.event_id
          )
    """).fetchone()[0]
    results.append(ReconciliationResult(
        name="dimensional_event_detail_integrity",
        passed=orphan_events == 0,
        message=(
            "All production events have matching detail rows."
            if orphan_events == 0
            else f"{orphan_events} production event(s) missing detail rows."
        ),
        details={"orphan_production_events": orphan_events},
    ))

    # 3. No negative volumes
    neg_volumes = con.execute("""
        SELECT COUNT(*) FROM vw_production_current
        WHERE total_oil_bbl < 0 OR total_gas_mcf < 0
    """).fetchone()[0]
    results.append(ReconciliationResult(
        name="dimensional_no_negative_volumes",
        passed=neg_volumes == 0,
        message=(
            "No negative volumes in vw_production_current."
            if neg_volumes == 0
            else f"{neg_volumes} row(s) with negative volumes."
        ),
        details={"negative_volume_rows": neg_volumes},
    ))

    # 4. Bridge completeness
    events_without_bridge = con.execute("""
        SELECT COUNT(*) FROM vw_fact_event_current fe
        WHERE NOT EXISTS (
            SELECT 1 FROM fact_event_object_bridge b
            WHERE b.event_id = fe.event_id
        )
    """).fetchone()[0]
    results.append(ReconciliationResult(
        name="dimensional_bridge_completeness",
        passed=events_without_bridge == 0,
        message=(
            "All current events have bridge rows."
            if events_without_bridge == 0
            else f"{events_without_bridge} event(s) missing bridge rows."
        ),
        details={"events_without_bridge": events_without_bridge},
    ))

    for r in results:
        level = logging.INFO if r.passed else logging.WARNING
        logger.log(level, "%s: %s", "PASS" if r.passed else "FAIL", r.message)

    return results
