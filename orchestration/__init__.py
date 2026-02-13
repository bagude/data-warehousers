"""Dagster orchestration for the og-data-warehouse pipeline.

NOTE: This package is loaded via ``python_file`` in workspace.yaml with
``working_directory: ..`` (the project root).  That means imports resolve
against the project root, so ``from src.…`` and ``from dagster.…`` both
work.  The ``dagster`` library itself is NOT shadowed because Python's
import system resolves installed packages before local directories when
the working directory is the project root (not this ``dagster/`` folder).
"""
