"""Base parser for converting raw bronze data into silver-layer Parquet files."""

from __future__ import annotations

import abc
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class _DualLogger:
    """Thin wrapper that forwards log calls to both a Python logger and an
    optional Dagster context logger so messages appear in the Dagster UI."""

    def __init__(self, py_log: logging.Logger, dagster_log=None) -> None:
        self._py = py_log
        self._dg = dagster_log

    def info(self, msg, *args):
        self._py.info(msg, *args)
        if self._dg:
            self._dg.info(msg % args if args else msg)

    def warning(self, msg, *args):
        self._py.warning(msg, *args)
        if self._dg:
            self._dg.warning(msg % args if args else msg)

    def error(self, msg, *args, **kwargs):
        self._py.error(msg, *args, **kwargs)
        if self._dg:
            self._dg.error(msg % args if args else msg)

    def debug(self, msg, *args):
        self._py.debug(msg, *args)


class BaseParser(abc.ABC):
    """Abstract base class for state-specific data parsers.

    Subclasses implement ``parse()`` to read raw files from bronze and return
    a cleaned ``pandas.DataFrame``.  The ``write_parquet()`` helper persists
    the result to the silver layer as a Parquet file.
    """

    def __init__(self, input_dir: Path, output_dir: Path, dagster_log=None) -> None:
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self._py_log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._dagster_log = dagster_log
        self.log = _DualLogger(self._py_log, dagster_log)

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def parse(self) -> pd.DataFrame:
        """Read raw data from *input_dir* and return a cleaned DataFrame."""

    # ------------------------------------------------------------------
    # Parquet output helper
    # ------------------------------------------------------------------

    def write_parquet(
        self,
        df: pd.DataFrame,
        filename: str,
        *,
        schema: pa.Schema | None = None,
    ) -> Path:
        """Write *df* to a Parquet file under *output_dir*.

        Parameters
        ----------
        df:
            The DataFrame to persist.
        filename:
            Name of the output Parquet file (e.g. ``"tx_production.parquet"``).
        schema:
            Optional PyArrow schema to enforce on write.

        Returns
        -------
        Path to the written Parquet file.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        dest = self.output_dir / filename

        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_table(table, dest)

        self.log.info("Wrote %d rows to %s", len(df), dest)
        return dest
