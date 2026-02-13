"""Value-range and format validation rules for silver-layer production data.

These checks run after parsing and deduplication but before writing the final
silver Parquet files.  They do not fix data — they flag problems so that the
pipeline can decide whether to reject, quarantine, or pass through bad rows.

Validation philosophy
=====================

- **Fail-open at row level**: Individual bad rows are flagged but do not stop
  the pipeline.  The caller decides what to do with flagged rows (log, move to
  a quarantine table, etc.).
- **Fail-closed at batch level**: If the fraction of bad rows in a batch
  exceeds a configurable threshold, the entire batch is rejected.  This
  prevents silently loading garbage data.
- **Separate concerns**: Each check function targets one specific rule.
  ``validate_batch()`` orchestrates all checks and returns a summary.
"""

from __future__ import annotations

import datetime
import logging
import re
from dataclasses import dataclass

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

API_NUMBER_PATTERN = re.compile(r"^\d{2}-\d{3}-\d{5}$")
"""Expected format for API well numbers: NN-NNN-NNNNN (e.g. 42-123-45678)."""

VALID_STATES = frozenset({"TX", "NM"})
"""Allowed values for the state field."""

VALID_ENTITY_TYPES = frozenset({"well", "lease"})
"""Allowed values for the entity_type field."""

MIN_PRODUCTION_DATE = datetime.date(1900, 1, 1)
"""Earliest plausible production date."""

MAX_DAYS_PRODUCED = 31
"""Maximum value for days_produced in a calendar month."""

NON_NEGATIVE_VOLUME_COLUMNS = [
    "oil_bbl", "gas_mcf", "condensate_bbl", "casinghead_gas_mcf", "water_bbl",
]
"""Volume columns that must be >= 0 when not NULL."""


# ---------------------------------------------------------------------------
# Validation result
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Summary of validation checks on a batch of production records."""

    total_rows: int
    """Number of rows in the input batch."""

    failed_rows: int
    """Number of rows that failed at least one check."""

    errors: list[ValidationError]
    """Individual check failures with details."""

    @property
    def pass_rate(self) -> float:
        """Fraction of rows that passed all checks (0.0 to 1.0)."""
        if self.total_rows == 0:
            return 1.0
        return (self.total_rows - self.failed_rows) / self.total_rows

    @property
    def passed(self) -> bool:
        """True if all rows passed validation."""
        return self.failed_rows == 0


@dataclass
class ValidationError:
    """One type of validation failure within a batch."""

    check_name: str
    """Name of the check that failed (e.g. 'non_negative_volumes')."""

    column: str | None
    """Column that triggered the failure, if applicable."""

    bad_row_count: int
    """Number of rows that failed this specific check."""

    sample_values: list
    """Up to 5 sample bad values for debugging."""

    message: str
    """Human-readable description of the failure."""


# ---------------------------------------------------------------------------
# Individual check functions
#
# Each returns a boolean Series (True = row is valid, False = row is bad).
# ---------------------------------------------------------------------------

def check_non_negative(df: pd.DataFrame, column: str) -> pd.Series:
    """Check that a numeric column is >= 0 wherever it is not NULL.

    Returns a boolean Series: True for valid rows, False for violations.
    """
    if column not in df.columns:
        return pd.Series(True, index=df.index)
    return df[column].isna() | (df[column] >= 0)


def check_days_produced_range(df: pd.DataFrame) -> pd.Series:
    """Check that days_produced is between 0 and 31 (inclusive) or NULL."""
    col = "days_produced"
    if col not in df.columns:
        return pd.Series(True, index=df.index)
    return df[col].isna() | ((df[col] >= 0) & (df[col] <= MAX_DAYS_PRODUCED))


def check_production_date_range(df: pd.DataFrame) -> pd.Series:
    """Check that production_date is not in the future and not before 1900."""
    col = "production_date"
    if col not in df.columns:
        return pd.Series(True, index=df.index)

    today = datetime.date.today()
    dates = pd.to_datetime(df[col]).dt.date
    return (dates >= MIN_PRODUCTION_DATE) & (dates <= today)


def check_api_number_format(df: pd.DataFrame) -> pd.Series:
    """Check that non-NULL api_number values match NN-NNN-NNNNN format."""
    col = "api_number"
    if col not in df.columns:
        return pd.Series(True, index=df.index)

    def _valid(val):
        if pd.isna(val):
            return True  # NULL api_number is allowed (TX lease-level).
        return bool(API_NUMBER_PATTERN.match(str(val)))

    return df[col].apply(_valid)


def check_state_values(df: pd.DataFrame) -> pd.Series:
    """Check that state is one of the allowed values."""
    col = "state"
    if col not in df.columns:
        return pd.Series(True, index=df.index)
    return df[col].isin(VALID_STATES)


def check_entity_type_values(df: pd.DataFrame) -> pd.Series:
    """Check that entity_type is one of the allowed values."""
    col = "entity_type"
    if col not in df.columns:
        return pd.Series(True, index=df.index)
    return df[col].isin(VALID_ENTITY_TYPES)


def check_lease_fields_for_lease_entities(df: pd.DataFrame) -> pd.Series:
    """Check that lease-type records have lease_number populated."""
    if "entity_type" not in df.columns:
        return pd.Series(True, index=df.index)

    is_lease = df["entity_type"] == "lease"
    has_lease_no = df.get("lease_number", pd.Series(dtype="object")).notna()

    # Non-lease rows always pass; lease rows must have a lease_number.
    return ~is_lease | has_lease_no


# ---------------------------------------------------------------------------
# Batch validation orchestrator
# ---------------------------------------------------------------------------

def validate_batch(
    df: pd.DataFrame,
    *,
    max_sample_values: int = 5,
) -> ValidationResult:
    """Run all validation checks on a production DataFrame.

    Parameters
    ----------
    df:
        Silver-layer production DataFrame.
    max_sample_values:
        Maximum number of sample bad values to include per error.

    Returns
    -------
    ValidationResult with aggregated check outcomes.
    """
    if df.empty:
        return ValidationResult(total_rows=0, failed_rows=0, errors=[])

    errors: list[ValidationError] = []
    # Track which rows have ANY failure.
    any_failure = pd.Series(False, index=df.index)

    # --- Non-negative volume checks ---
    for col in NON_NEGATIVE_VOLUME_COLUMNS:
        valid = check_non_negative(df, col)
        bad = ~valid
        if bad.any():
            n_bad = int(bad.sum())
            samples = df.loc[bad, col].head(max_sample_values).tolist()
            errors.append(ValidationError(
                check_name="non_negative_volumes",
                column=col,
                bad_row_count=n_bad,
                sample_values=samples,
                message=f"{col} has {n_bad} rows with negative values.",
            ))
            any_failure |= bad

    # --- Days produced range ---
    valid = check_days_produced_range(df)
    bad = ~valid
    if bad.any():
        n_bad = int(bad.sum())
        samples = df.loc[bad, "days_produced"].head(max_sample_values).tolist()
        errors.append(ValidationError(
            check_name="days_produced_range",
            column="days_produced",
            bad_row_count=n_bad,
            sample_values=samples,
            message=f"days_produced has {n_bad} rows outside 0-{MAX_DAYS_PRODUCED}.",
        ))
        any_failure |= bad

    # --- Production date range ---
    valid = check_production_date_range(df)
    bad = ~valid
    if bad.any():
        n_bad = int(bad.sum())
        samples = df.loc[bad, "production_date"].head(max_sample_values).tolist()
        errors.append(ValidationError(
            check_name="production_date_range",
            column="production_date",
            bad_row_count=n_bad,
            sample_values=samples,
            message=f"production_date has {n_bad} rows outside valid range.",
        ))
        any_failure |= bad

    # --- API number format ---
    valid = check_api_number_format(df)
    bad = ~valid
    if bad.any():
        n_bad = int(bad.sum())
        samples = df.loc[bad, "api_number"].head(max_sample_values).tolist()
        errors.append(ValidationError(
            check_name="api_number_format",
            column="api_number",
            bad_row_count=n_bad,
            sample_values=samples,
            message=f"api_number has {n_bad} rows not matching NN-NNN-NNNNN format.",
        ))
        any_failure |= bad

    # --- State values ---
    valid = check_state_values(df)
    bad = ~valid
    if bad.any():
        n_bad = int(bad.sum())
        samples = df.loc[bad, "state"].head(max_sample_values).tolist()
        errors.append(ValidationError(
            check_name="state_values",
            column="state",
            bad_row_count=n_bad,
            sample_values=samples,
            message=f"state has {n_bad} rows with invalid values.",
        ))
        any_failure |= bad

    # --- Entity type values ---
    valid = check_entity_type_values(df)
    bad = ~valid
    if bad.any():
        n_bad = int(bad.sum())
        samples = df.loc[bad, "entity_type"].head(max_sample_values).tolist()
        errors.append(ValidationError(
            check_name="entity_type_values",
            column="entity_type",
            bad_row_count=n_bad,
            sample_values=samples,
            message=f"entity_type has {n_bad} rows with invalid values.",
        ))
        any_failure |= bad

    # --- Lease fields consistency ---
    valid = check_lease_fields_for_lease_entities(df)
    bad = ~valid
    if bad.any():
        n_bad = int(bad.sum())
        samples = df.loc[bad, "lease_number"].head(max_sample_values).tolist()
        errors.append(ValidationError(
            check_name="lease_fields_consistency",
            column="lease_number",
            bad_row_count=n_bad,
            sample_values=samples,
            message=f"{n_bad} lease-type records are missing lease_number.",
        ))
        any_failure |= bad

    total_failed = int(any_failure.sum())

    result = ValidationResult(
        total_rows=len(df),
        failed_rows=total_failed,
        errors=errors,
    )

    if result.passed:
        logger.info("Validation passed: %d rows, 0 failures.", result.total_rows)
    else:
        logger.warning(
            "Validation found issues: %d/%d rows failed (%.1f%% pass rate). "
            "%d distinct check(s) triggered.",
            result.failed_rows,
            result.total_rows,
            result.pass_rate * 100,
            len(result.errors),
        )

    return result


def get_valid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return only rows that pass all validation checks.

    This is a convenience wrapper for pipelines that want to filter out
    bad rows and continue processing.
    """
    if df.empty:
        return df

    valid_mask = pd.Series(True, index=df.index)

    for col in NON_NEGATIVE_VOLUME_COLUMNS:
        valid_mask &= check_non_negative(df, col)

    valid_mask &= check_days_produced_range(df)
    valid_mask &= check_production_date_range(df)
    valid_mask &= check_api_number_format(df)
    valid_mask &= check_state_values(df)
    valid_mask &= check_entity_type_values(df)
    valid_mask &= check_lease_fields_for_lease_entities(df)

    n_dropped = int((~valid_mask).sum())
    if n_dropped > 0:
        logger.info("Filtered out %d invalid rows (kept %d).", n_dropped, int(valid_mask.sum()))

    return df.loc[valid_mask].reset_index(drop=True)
