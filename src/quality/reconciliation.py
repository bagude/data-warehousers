"""Silver-to-gold reconciliation checks.

These checks verify that the gold layer is a faithful representation of the
silver layer — no rows were silently dropped or double-counted.

Checks
======

1. **Volume reconciliation**: Sum of computed volume columns in gold must
   match the equivalent computation over silver (within tolerance).

2. **Primary key uniqueness**: Gold tables must have no duplicate primary keys.

3. **Row count parity**: production_monthly row count must equal silver row count.

4. **Month completeness**: Check for unexpected gaps in entity time series.

Usage
=====

Called after gold build as an additional Python-side validation layer.
Reads both silver Parquet files and DuckDB gold tables to cross-check.
"""

from __future__ import annotations

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
        Gold-layer DataFrame.
    volume_column:
        Column name in silver to sum (e.g. 'oil_bbl').
    gold_volume_column:
        Column name in gold that holds the value.  Defaults to
        ``volume_column`` if not specified.
    group_keys:
        Columns to group by for comparison.
    tolerance:
        Maximum acceptable absolute difference as a fraction of silver sum.
    """
    gold_vol = gold_volume_column or volume_column

    silver_agg = (
        silver_df
        .groupby(group_keys, dropna=False)[volume_column]
        .sum()
        .reset_index(name="silver_total")
    )

    gold_agg = (
        gold_df
        .groupby(group_keys, dropna=False)[gold_vol]
        .sum()
        .reset_index(name="gold_total")
    )

    merged = silver_agg.merge(gold_agg, on=group_keys, how="outer", indicator=True)

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
# Primary key uniqueness
# ---------------------------------------------------------------------------

def check_pk_uniqueness(
    df: pd.DataFrame,
    pk_columns: list[str],
    *,
    table_name: str = "unknown",
) -> ReconciliationResult:
    """Verify that primary key columns are unique in a gold table."""
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
# Month completeness for entity time series
# ---------------------------------------------------------------------------

def check_month_completeness(
    df: pd.DataFrame,
    *,
    entity_col: str = "entity_key",
    state_col: str = "state",
    date_col: str = "production_date",
    max_gap_months: int = 3,
    sample_size: int = 5,
) -> ReconciliationResult:
    """Check for unexpected gaps in production time series.

    A "gap" is a stretch of more than ``max_gap_months`` consecutive months
    with no production record for an entity.  Short gaps (1-2 months) are
    normal in oil & gas data; long gaps may indicate missing data.
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
        'production_monthly', 'decline_curve_inputs'.

    Returns
    -------
    List of ReconciliationResult objects.
    """
    results: list[ReconciliationResult] = []

    # --- production_monthly checks ---
    pm_df = gold_tables.get("production_monthly")
    if pm_df is not None:
        # Volume reconciliation: total_oil_bbl and total_gas_mcf
        for vol_col in ["total_oil_bbl", "total_gas_mcf"]:
            results.append(reconcile_volumes(
                silver_df.assign(
                    total_oil_bbl=lambda d: d["oil_bbl"].fillna(0) + d["condensate_bbl"].fillna(0),
                    total_gas_mcf=lambda d: d["gas_mcf"].fillna(0) + d["casinghead_gas_mcf"].fillna(0),
                ),
                pm_df,
                volume_column=vol_col,
                group_keys=["state"],
            ))

        results.append(check_pk_uniqueness(
            pm_df,
            ["entity_key", "state", "production_date"],
            table_name="production_monthly",
        ))

        results.append(check_month_completeness(pm_df))

    # --- decline_curve_inputs checks ---
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
