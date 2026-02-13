"""Deduplication strategy for silver-layer production records.

Problem
=======

Both TX RRC and NM OCD can produce duplicate records for the same entity
and month:

- **TX RRC**: Operators may file amended reports.  The OG_LEASE_CYCLE table
  can contain multiple rows for the same (LEASE_NO, DISTRICT_NO, CYCLE_YEAR,
  CYCLE_MONTH) if corrections were filed.  The most recent filing should win.

- **NM OCD**: C-115 forms may be re-submitted or corrected.  GO-TECH data
  may contain revised figures for the same API + month.

Strategy
========

1. **Composite dedup key** — identifies a unique production record:

   - Well-level records: ``(state, api_number, production_date)``
   - Lease-level records: ``(state, lease_number, district, production_date)``

   Because the schema supports both well and lease granularity, the dedup
   key depends on the ``entity_type`` field.

2. **Tie-breaking rule** — when duplicates exist on the same key, keep the
   record from the most recent source file (latest ``ingested_at`` timestamp).
   This implements "latest report wins" semantics, which matches how both
   agencies handle amended filings.

3. **Entity mapping notes** (TX lease-to-well):

   - Gas wells in TX are typically 1:1 with a lease, so the lease-level
     record effectively represents a single well.  These can be promoted
     to ``entity_type='well'`` with the API number filled in.
   - Oil leases in TX may cover multiple wells.  These stay as
     ``entity_type='lease'`` with ``lease_number`` as the entity ID and
     ``api_number`` set to NULL.
   - NM records are always ``entity_type='well'``.

Usage
=====

Call ``deduplicate(df)`` after parsing and before writing the silver Parquet
file.  The function handles both entity types in a single pass.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dedup key definitions
# ---------------------------------------------------------------------------

WELL_DEDUP_KEY: list[str] = ["state", "api_number", "production_date"]
"""Composite key for well-level records (NM, TX gas wells)."""

LEASE_DEDUP_KEY: list[str] = ["state", "lease_number", "district", "production_date"]
"""Composite key for TX lease-level records (oil leases)."""

TIEBREAKER_COLUMN: str = "ingested_at"
"""When duplicates exist on the dedup key, keep the row with the latest value
in this column (most recent ingestion wins)."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate production records, keeping the latest filing.

    The DataFrame is split by ``entity_type``, deduped with the appropriate
    key for each type, and then recombined.

    Parameters
    ----------
    df:
        Silver-layer DataFrame that conforms to ``PRODUCTION_SCHEMA``.
        Must contain ``entity_type`` and ``ingested_at`` columns.

    Returns
    -------
    DataFrame with duplicates removed.  Row order is not preserved.
    """
    if df.empty:
        return df

    well_mask = df["entity_type"] == "well"
    lease_mask = df["entity_type"] == "lease"

    wells = df.loc[well_mask]
    leases = df.loc[lease_mask]

    wells_deduped = _dedup_partition(wells, WELL_DEDUP_KEY)
    leases_deduped = _dedup_partition(leases, LEASE_DEDUP_KEY)

    result = pd.concat([wells_deduped, leases_deduped], ignore_index=True)

    n_dropped = len(df) - len(result)
    if n_dropped > 0:
        logger.info(
            "Deduplication removed %d rows (%d well dupes, %d lease dupes).",
            n_dropped,
            len(wells) - len(wells_deduped),
            len(leases) - len(leases_deduped),
        )

    return result


def _dedup_partition(
    df: pd.DataFrame,
    key_columns: list[str],
) -> pd.DataFrame:
    """Keep only the latest-ingested row for each unique key.

    Parameters
    ----------
    df:
        Subset of records (e.g. all well-level or all lease-level).
    key_columns:
        Columns that form the composite dedup key.

    Returns
    -------
    Deduplicated DataFrame.
    """
    if df.empty:
        return df

    # Verify all key columns are present.
    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"Dedup key columns missing from DataFrame: {missing}"
        )

    # Sort so that the latest ingestion comes last, then drop earlier dupes.
    sorted_df = df.sort_values(TIEBREAKER_COLUMN, ascending=True, na_position="first")
    return sorted_df.drop_duplicates(subset=key_columns, keep="last")


# ---------------------------------------------------------------------------
# Helpers for TX entity type determination
# ---------------------------------------------------------------------------

def assign_tx_entity_type(
    df: pd.DataFrame,
    well_completion_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Assign ``entity_type`` for TX records based on well/lease structure.

    Logic:
    - If a (LEASE_NO, DISTRICT_NO) has exactly one well in
      ``well_completion_df``, set entity_type='well' and populate api_number.
    - If a lease has multiple wells (common for oil leases), set
      entity_type='lease' and leave api_number as NULL.
    - Gas wells (well_type in ('GAS', 'GAS_CONDENSATE')) are typically 1:1
      and get entity_type='well'.

    Parameters
    ----------
    df:
        TX production DataFrame with lease_number and district columns.
    well_completion_df:
        OG_WELL_COMPLETION DataFrame with LEASE_NO, DISTRICT_NO, API_NO.
        If None, all TX records default to entity_type='lease'.

    Returns
    -------
    DataFrame with entity_type and api_number populated.
    """
    if well_completion_df is None:
        df["entity_type"] = "lease"
        return df

    # Count wells per lease+district.
    well_counts = (
        well_completion_df
        .groupby(["LEASE_NO", "DISTRICT_NO"])["API_NO"]
        .nunique()
        .reset_index(name="_well_count")
    )

    # Get the API number for single-well leases.
    single_well_apis = (
        well_completion_df
        .drop_duplicates(subset=["LEASE_NO", "DISTRICT_NO", "API_NO"])
        .merge(
            well_counts[well_counts["_well_count"] == 1],
            on=["LEASE_NO", "DISTRICT_NO"],
        )[["LEASE_NO", "DISTRICT_NO", "API_NO"]]
    )

    # Merge well count info into production records.
    df = df.merge(
        well_counts,
        left_on=["lease_number", "district"],
        right_on=["LEASE_NO", "DISTRICT_NO"],
        how="left",
    )

    # Assign entity type.
    df["entity_type"] = "lease"
    single_well_mask = df["_well_count"] == 1
    df.loc[single_well_mask, "entity_type"] = "well"

    # Fill API number for single-well leases.
    df = df.merge(
        single_well_apis.rename(columns={"API_NO": "_single_api"}),
        left_on=["lease_number", "district"],
        right_on=["LEASE_NO", "DISTRICT_NO"],
        how="left",
        suffixes=("", "_api"),
    )
    df.loc[single_well_mask, "api_number"] = df.loc[single_well_mask, "_single_api"]

    # Clean up temp columns.
    drop_cols = [c for c in df.columns if c.startswith("_") or c.endswith("_api")]
    # Also drop duplicate join columns from the merge.
    for col in ["LEASE_NO", "DISTRICT_NO", "LEASE_NO_api", "DISTRICT_NO_api"]:
        if col in df.columns:
            drop_cols.append(col)
    df = df.drop(columns=[c for c in set(drop_cols) if c in df.columns])

    return df
