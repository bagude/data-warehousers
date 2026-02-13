"""Base ingester with retry logic, rate limiting, and logging."""

from __future__ import annotations

import abc
import logging
import time
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_USER_AGENT = (
    "og-data-warehouse/0.1 (research; +https://github.com/og-data-warehouse)"
)


class _DualLogger:
    """Forwards log calls to both a Python logger and an optional Dagster
    context logger so messages appear in the Dagster UI."""

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


class BaseIngester(abc.ABC):
    """Abstract base class for state-level production data scrapers.

    Provides:
    - Configurable delay between HTTP requests (rate limiting).
    - Automatic retry with exponential back-off (up to *max_retries*).
    - A shared ``httpx.Client`` with a proper User-Agent header.
    - Structured logging scoped to the subclass name.
    """

    def __init__(
        self,
        output_dir: Path,
        *,
        delay: float = 1.0,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        user_agent: str = _DEFAULT_USER_AGENT,
        timeout: float = 30.0,
        dagster_log=None,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.delay = delay
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.log = _DualLogger(
            logging.getLogger(f"{__name__}.{self.__class__.__name__}"),
            dagster_log,
        )

        self._client = httpx.Client(
            headers={"User-Agent": user_agent},
            timeout=timeout,
            follow_redirects=True,
        )

    # ------------------------------------------------------------------
    # HTTP helper with retry + back-off
    # ------------------------------------------------------------------

    def fetch(self, url: str, **kwargs: Any) -> httpx.Response:
        """GET *url* with retry logic and inter-request delay.

        Raises ``httpx.HTTPStatusError`` after exhausting retries.
        """
        last_exc: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                self.log.debug("GET %s (attempt %d/%d)", url, attempt, self.max_retries)
                resp = self._client.get(url, **kwargs)
                resp.raise_for_status()
                time.sleep(self.delay)
                return resp
            except (httpx.HTTPStatusError, httpx.TransportError) as exc:
                last_exc = exc
                wait = self.backoff_factor ** attempt
                self.log.warning(
                    "Request failed (%s). Retrying in %.1fs...", exc, wait
                )
                time.sleep(wait)

        raise RuntimeError(
            f"Failed to fetch {url} after {self.max_retries} retries"
        ) from last_exc

    # ------------------------------------------------------------------
    # Abstract interface — subclasses must implement these
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def discover(self) -> list[str]:
        """Return a list of URLs or identifiers to scrape."""

    @abc.abstractmethod
    def ingest(self) -> Path:
        """Run the full ingestion pipeline and return the output directory."""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> "BaseIngester":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
