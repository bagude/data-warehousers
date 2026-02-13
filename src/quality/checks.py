"""Data quality checks for layer-boundary validation.

This module implements checks that run at the boundaries between medallion
layers:

**Bronze -> Silver:**
- Row count comparison (no silent drops beyond a configurable threshold)
- Required fields populated (state, entity_type, production_date, plus
  api_number for wells or lease_number for leases)
- Value range checks (delegated to ``src.schemas.validation.validate_batch``)
- Duplicate detection on composite dedup keys

**Silver -> Gold:**
- Aggregation reconciliation (see ``reconciliation.py``)

Row-level value range validation (non-negative volumes, date ranges, API
format, etc.) lives in ``src.schemas.validation`` and is integrated here
via ``check_value_ranges()``.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.schemas.validation import validate_batch, ValidationResult
from src.utils.config import BRONZE_TX_DIR, BRONZE_NM_DIR, SILVER_DIR

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

_STATE_BRONZE_DIRS: dict[str, Path] = {
    "TX": BRONZE_TX_DIR,
    "NM": BRONZE_NM_DIR,
}


def _bronze_path(state: str, pull_date: str) -> Path:
    """Return the bronze directory for a given state and pull date."""
    return _STATE_BRONZE_DIRS[state] / pull_date


def _silver_path() -> Path:
    """Return the silver production directory."""
    return SILVER_DIR / "production"


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class QualityReport:
    """Aggregated report from all quality checks on a dataset."""

    layer: str
    """Which layer boundary: 'bronze_to_silver' or 'silver_to_gold'."""

    state: str
    """State code this report covers (TX, NM, or 'ALL')."""

    timestamp: datetime.datetime
    """When the checks were run (UTC)."""

    total_rows: int
    """Number of rows in the dataset being checked."""

    checks: list[CheckResult]
    """Individual check outcomes."""

    @property
    def passed(self) -> bool:
        """True if every check passed."""
        return all(c.passed for c in self.checks)

    @property
    def failed_checks(self) -> list[CheckResult]:
        """Return only the checks that failed."""
        return [c for c in self.checks if not c.passed]

    def summary(self) -> str:
        """Human-readable one-line summary."""
        n_pass = sum(1 for c in self.checks if c.passed)
        n_fail = len(self.checks) - n_pass
        status = "PASSED" if self.passed else "FAILED"
        return (
            f"[{status}] {self.layer} ({self.state}): "
            f"{n_pass}/{len(self.checks)} checks passed, "
            f"{n_fail} failed ({self.total_rows} rows)"
        )


@dataclass
class CheckResult:
    """Outcome of a single quality check."""

    name: str
    """Check identifier (e.g. 'row_count_comparison')."""

    passed: bool
    """Whether the check passed."""

    message: str
    """Human-readable description of the outcome."""

    details: dict = field(default_factory=dict)
    """Optional structured details (counts, samples, etc.)."""


# ---------------------------------------------------------------------------
# Bronze -> Silver checks
# ---------------------------------------------------------------------------

def check_bronze_silver_row_counts(
    bronze_path: Path | str,
    silver_path: Path | str,
    *,
    min_retain_pct: float = 90.0,
) -> CheckResult:
    """Compare row counts between bronze CSV(s) and silver Parquet(s).

    Reads row counts from the files on disk without loading full data into
    memory (uses Parquet metadata for silver, line counts for bronze CSVs).

    Parameters
    ----------
    bronze_path:
        Path to the bronze directory or file.  If a directory, all CSV files
        within it are counted.
    silver_path:
        Path to the silver directory or file.  If a directory, all Parquet
        files within it are counted.
    min_retain_pct:
        Minimum percentage of bronze rows that must appear in silver
        (default 90%).  Some drop is expected from validation/dedup.
    """
    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)

    # Count bronze rows
    bronze_count = 0
    if bronze_path.is_dir():
        for csv_file in bronze_path.glob("*.csv"):
            # Fast line count: subtract 1 for header
            with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
                n_lines = sum(1 for _ in f)
            bronze_count += max(0, n_lines - 1)
    elif bronze_path.is_file():
        with open(bronze_path, "r", encoding="utf-8", errors="replace") as f:
            n_lines = sum(1 for _ in f)
        bronze_count = max(0, n_lines - 1)

    # Count silver rows (via Parquet metadata — no full read needed)
    silver_count = 0
    if silver_path.is_dir():
        for pq_file in silver_path.glob("*.parquet"):
            meta = pq.read_metadata(pq_file)
            silver_count += meta.num_rows
    elif silver_path.is_file():
        meta = pq.read_metadata(silver_path)
        silver_count = meta.num_rows

    if bronze_count == 0:
        return CheckResult(
            name="row_count_comparison",
            passed=True,
            message="Bronze input has 0 rows; nothing to compare.",
            details={"bronze_count": 0, "silver_count": silver_count},
        )

    retain_pct = (silver_count / bronze_count) * 100
    passed = retain_pct >= min_retain_pct

    message = (
        f"Bronze: {bronze_count:,} rows -> Silver: {silver_count:,} rows "
        f"({retain_pct:.1f}% retained). "
        f"{'OK' if passed else f'BELOW {min_retain_pct}% threshold'}."
    )

    return CheckResult(
        name="row_count_comparison",
        passed=passed,
        message=message,
        details={
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "retain_pct": round(retain_pct, 2),
            "threshold_pct": min_retain_pct,
        },
    )


def check_required_fields(silver_df: pd.DataFrame) -> CheckResult:
    """Verify that required fields are populated for every row.

    Required fields:
    - ``state``: always required
    - ``entity_type``: always required
    - ``production_date``: always required
    - ``api_number``: required when entity_type = 'well'
    - ``lease_number``: required when entity_type = 'lease'
    """
    issues: dict[str, int] = {}

    for col in ("state", "entity_type", "production_date"):
        if col in silver_df.columns:
            n_null = int(silver_df[col].isna().sum())
            if n_null > 0:
                issues[col] = n_null

    # Conditional required: api_number for wells
    if "entity_type" in silver_df.columns and "api_number" in silver_df.columns:
        well_mask = silver_df["entity_type"] == "well"
        n_null = int(silver_df.loc[well_mask, "api_number"].isna().sum())
        if n_null > 0:
            issues["api_number (wells)"] = n_null

    # Conditional required: lease_number for leases
    if "entity_type" in silver_df.columns and "lease_number" in silver_df.columns:
        lease_mask = silver_df["entity_type"] == "lease"
        n_null = int(silver_df.loc[lease_mask, "lease_number"].isna().sum())
        if n_null > 0:
            issues["lease_number (leases)"] = n_null

    passed = len(issues) == 0
    if passed:
        message = "All required fields are populated."
    else:
        parts = [f"{col}: {n} nulls" for col, n in issues.items()]
        message = "Missing required fields: " + "; ".join(parts)

    return CheckResult(
        name="required_fields",
        passed=passed,
        message=message,
        details={"null_counts": issues},
    )


def check_value_ranges(silver_df: pd.DataFrame) -> CheckResult:
    """Run value-range validation using ``src.schemas.validation.validate_batch``.

    This delegates to the comprehensive row-level validation logic that checks
    non-negative volumes, date ranges, API format, state/entity_type enums,
    and lease field consistency.

    Parameters
    ----------
    silver_df:
        Silver-layer DataFrame.

    Returns
    -------
    CheckResult wrapping the ValidationResult from validate_batch().
    """
    result: ValidationResult = validate_batch(silver_df)

    passed = result.passed
    message = (
        f"Value range validation: {result.total_rows} rows, "
        f"{result.failed_rows} failures ({result.pass_rate:.1%} pass rate)."
    )

    error_summary = {
        e.check_name: {
            "column": e.column,
            "bad_row_count": e.bad_row_count,
            "message": e.message,
        }
        for e in result.errors
    }

    return CheckResult(
        name="value_ranges",
        passed=passed,
        message=message,
        details={
            "total_rows": result.total_rows,
            "failed_rows": result.failed_rows,
            "pass_rate": result.pass_rate,
            "errors": error_summary,
        },
    )


def check_dedup_integrity(silver_df: pd.DataFrame) -> CheckResult:
    """Verify no duplicate records on the composite dedup keys.

    Checks both entity types:
    - Wells: (state, api_number, production_date)
    - Leases: (state, lease_number, district, production_date)

    Parameters
    ----------
    silver_df:
        Silver-layer DataFrame.

    Returns
    -------
    CheckResult indicating whether any duplicates were found.
    """
    issues: dict[str, int] = {}

    # Well-level dedup check
    if "entity_type" in silver_df.columns:
        well_df = silver_df[silver_df["entity_type"] == "well"]
        if not well_df.empty:
            well_keys = ["state", "api_number", "production_date"]
            missing = [k for k in well_keys if k not in well_df.columns]
            if not missing:
                n_dupes = int(well_df.duplicated(subset=well_keys, keep=False).sum())
                if n_dupes > 0:
                    issues["well_duplicates"] = n_dupes

        # Lease-level dedup check
        lease_df = silver_df[silver_df["entity_type"] == "lease"]
        if not lease_df.empty:
            lease_keys = ["state", "lease_number", "district", "production_date"]
            missing = [k for k in lease_keys if k not in lease_df.columns]
            if not missing:
                n_dupes = int(lease_df.duplicated(subset=lease_keys, keep=False).sum())
                if n_dupes > 0:
                    issues["lease_duplicates"] = n_dupes

    passed = len(issues) == 0
    if passed:
        message = "No duplicate records found on dedup keys."
    else:
        parts = [f"{k}: {v} rows" for k, v in issues.items()]
        message = "Duplicate records found: " + "; ".join(parts)

    return CheckResult(
        name="dedup_integrity",
        passed=passed,
        message=message,
        details=issues,
    )


# ---------------------------------------------------------------------------
# Bronze -> Silver orchestrator
# ---------------------------------------------------------------------------

def run_bronze_silver_checks(
    state: str,
    pull_date: str,
    *,
    silver_df: pd.DataFrame | None = None,
    min_retain_pct: float = 90.0,
) -> QualityReport:
    """Run all bronze-to-silver layer boundary checks for a state/pull_date.

    Parameters
    ----------
    state:
        State code: 'TX' or 'NM'.
    pull_date:
        Pull date string (e.g. '2026-02-11') identifying the bronze snapshot.
    silver_df:
        Pre-loaded silver DataFrame.  If None, silver Parquet files are
        read from the standard silver directory.
    min_retain_pct:
        Minimum row retention percentage for the row count check.

    Returns
    -------
    QualityReport summarizing all check outcomes.
    """
    checks: list[CheckResult] = []

    bronze_dir = _bronze_path(state, pull_date)
    silver_dir = _silver_path()

    # 1. Row count comparison (file-based)
    checks.append(
        check_bronze_silver_row_counts(
            bronze_dir, silver_dir, min_retain_pct=min_retain_pct,
        )
    )

    # Load silver data if not provided
    if silver_df is None:
        pq_files = list(silver_dir.glob("*.parquet"))
        if pq_files:
            silver_df = pd.concat(
                [pd.read_parquet(f) for f in pq_files],
                ignore_index=True,
            )
        else:
            silver_df = pd.DataFrame()

    # Filter to the requested state
    if not silver_df.empty and "state" in silver_df.columns:
        state_df = silver_df[silver_df["state"] == state].copy()
    else:
        state_df = silver_df

    # 2. Required fields
    checks.append(check_required_fields(state_df))

    # 3. Value ranges (delegates to src.schemas.validation.validate_batch)
    checks.append(check_value_ranges(state_df))

    # 4. Dedup integrity
    checks.append(check_dedup_integrity(state_df))

    report = QualityReport(
        layer="bronze_to_silver",
        state=state,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        total_rows=len(state_df),
        checks=checks,
    )

    if report.passed:
        logger.info(report.summary())
    else:
        logger.warning(report.summary())
        for c in report.failed_checks:
            logger.warning("  FAIL: %s -- %s", c.name, c.message)

    return report
