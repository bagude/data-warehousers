"""Quality layer -- data validation and quality checks.

Bronze-to-silver checks (``checks`` module):
- ``check_bronze_silver_row_counts``: Compare row counts between layers
- ``check_required_fields``: Verify required columns are populated
- ``check_value_ranges``: Delegate to ``src.schemas.validation.validate_batch``
- ``check_dedup_integrity``: Verify no duplicate records on dedup keys
- ``run_bronze_silver_checks``: Orchestrator (state + pull_date -> QualityReport)

Silver-to-gold reconciliation (``reconciliation`` module):
- ``reconcile_volumes``: Cross-check summed volumes between layers
- ``check_pk_uniqueness``: Validate PK uniqueness on gold tables
- ``check_month_completeness``: Flag gaps in entity time series
- ``run_silver_to_gold_checks``: Orchestrator (silver_df + gold_tables -> results)

Row-level value validation (API format, date ranges, state/entity_type enums)
lives in ``src.schemas.validation``.
"""

from src.quality.checks import (
    CheckResult,
    QualityReport,
    check_bronze_silver_row_counts,
    check_dedup_integrity,
    check_required_fields,
    check_value_ranges,
    run_bronze_silver_checks,
)
from src.quality.reconciliation import (
    ReconciliationResult,
    check_month_completeness,
    check_pk_uniqueness,
    reconcile_volumes,
    run_silver_to_gold_checks,
)

__all__ = [
    # checks
    "CheckResult",
    "QualityReport",
    "check_bronze_silver_row_counts",
    "check_dedup_integrity",
    "check_required_fields",
    "check_value_ranges",
    "run_bronze_silver_checks",
    # reconciliation
    "ReconciliationResult",
    "check_month_completeness",
    "check_pk_uniqueness",
    "reconcile_volumes",
    "run_silver_to_gold_checks",
]
