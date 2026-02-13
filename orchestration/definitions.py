"""Dagster Definitions -- the single entry point for the code location.

Loaded by workspace.yaml via ``python_file`` with ``working_directory``
set to the project root.  This file adds its own directory to sys.path
so that sibling modules (jobs, schedules, resources, assets/) can be
imported without colliding with the ``dagster`` library.

Asset dependency graph (medallion flow)::

    bronze_tx  -->  silver_tx  \
                                --> gold_models --> DuckDB tables
    bronze_nm  -->  silver_nm  /

Local development::

    cd data-warehousers
    dagster dev -w orchestration/workspace.yaml
"""

from __future__ import annotations

import os
import sys

# Add the orchestration/ directory to sys.path so we can import sibling
# modules (jobs.py, schedules.py, resources.py, assets/).
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

from dagster import Definitions  # noqa: E402

from assets.bronze import bronze_tx, bronze_nm  # noqa: E402
from assets.silver import silver_tx, silver_nm  # noqa: E402
from assets.gold import gold_models  # noqa: E402
from assets.e2e import (  # noqa: E402
    test_bronze_tx,
    test_bronze_nm,
    test_silver_tx,
    test_silver_nm,
    test_gold_models,
)
from jobs import (  # noqa: E402
    full_refresh_job,
    incremental_job,
    test_full_pipeline,
    test_incremental_pipeline,
)
from schedules import monthly_refresh  # noqa: E402
from resources import WarehouseConfig  # noqa: E402

defs = Definitions(
    assets=[
        bronze_tx,
        bronze_nm,
        silver_tx,
        silver_nm,
        gold_models,
        test_bronze_tx,
        test_bronze_nm,
        test_silver_tx,
        test_silver_nm,
        test_gold_models,
    ],
    jobs=[
        full_refresh_job,
        incremental_job,
        test_full_pipeline,
        test_incremental_pipeline,
    ],
    schedules=[
        monthly_refresh,
    ],
    resources={
        "warehouse_config": WarehouseConfig(),
    },
)
