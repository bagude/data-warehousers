"""Schedule definitions for the og-data-warehouse pipeline.

NOTE: This module is imported by definitions.py which adds the dagster/
directory to sys.path before importing.

TX RRC updates PDQ data on the last Saturday of each month.  The monthly
refresh schedule runs on the 1st of each month to pick up the previous
month's update.
"""

from __future__ import annotations

from dagster import ScheduleDefinition

from jobs import full_refresh_job

monthly_refresh = ScheduleDefinition(
    name="monthly_refresh",
    job=full_refresh_job,
    cron_schedule="0 6 1 * *",  # 06:00 UTC on the 1st of each month
    description=(
        "Monthly full refresh. Runs on the 1st of each month to pick up "
        "TX RRC data updated on the last Saturday of the previous month "
        "and any new NM OCD data."
    ),
)
