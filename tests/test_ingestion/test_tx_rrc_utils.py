"""Tests for utility functions in src.ingestion.tx_rrc."""

from __future__ import annotations

import pytest

from src.ingestion.tx_rrc import _fmt_bytes


class TestFmtBytes:
    """Parametrized tests for the ``_fmt_bytes`` human-readable byte formatter."""

    @pytest.mark.parametrize("n, expected", [
        (0, "0.0 B"),
        (1, "1.0 B"),
        (512, "512.0 B"),
        (1023, "1023.0 B"),
        (1024, "1.0 KB"),
        (1536, "1.5 KB"),
        (1048576, "1.0 MB"),
        (1073741824, "1.0 GB"),
        (1099511627776, "1.0 TB"),
        (1125899906842624, "1.0 PB"),
        (3639738368, "3.4 GB"),  # ~3.39 GB ZIP
        (-1024, "-1.0 KB"),  # negative value
    ])
    def test_fmt_bytes(self, n: int, expected: str) -> None:
        assert _fmt_bytes(n) == expected
