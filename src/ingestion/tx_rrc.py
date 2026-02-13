"""Texas Railroad Commission (RRC) -- PDQ bulk data ingester.

Downloads the monthly Production Data Query (PDQ) consolidated ZIP archive
from the TX RRC GoAnywhere MFT server, extracts the pipe-delimited table
files we need, and stores them as-received in the bronze layer.

Also downloads well location data (latitude / longitude) from the RRC's
public ArcGIS REST API and stores paginated JSON batches alongside the
DSV files.

Source: https://www.rrc.texas.gov/resource-center/research/data-sets-available-for-download/
Server: https://mft.rrc.texas.gov  (public, no authentication)
Format: Pipe-delimited DSV (inside PDQ_DSV.zip)
Coverage: Jan 1993 to present, updated last Saturday of each month

History:
  - Prior to 2026, the RRC distributed individual CSV files per table at
    https://mft.rrc.texas.gov/link/7a5577fc-e670-4e53-b596-040eedbc66b4/{table}.csv
  - As of 2026, they switched to a single consolidated ZIP (PDQ_DSV.zip, ~3.39 GB)
    served via GoAnywhere Web Client at a new link GUID.  Files inside are
    pipe-delimited (|), not comma-separated.
"""

from __future__ import annotations

import datetime
import json
import logging
import re
import time
import zipfile
from pathlib import Path
from typing import Any

import httpx
from bs4 import BeautifulSoup

from src.ingestion.base import BaseIngester
from src.utils.config import BRONZE_TX_DIR

logger = logging.getLogger(__name__)

# Tables we care about for production analysis.  Each entry maps a logical
# name to the filename (without extension) as published inside PDQ_DSV.zip.
PDQ_TABLES: dict[str, str] = {
    "OG_LEASE_CYCLE": "OG_LEASE_CYCLE",
    "OG_WELL_COMPLETION": "OG_WELL_COMPLETION",
    "OG_OPERATOR_DW": "OG_OPERATOR_DW",
    "OG_FIELD_CYCLE": "OG_FIELD_CYCLE",
    "OG_COUNTY_CYCLE": "OG_COUNTY_CYCLE",
}

# The RRC data-download landing page that lists bulk file links.
RRC_DATA_PAGE = (
    "https://www.rrc.texas.gov/resource-center/research/"
    "data-sets-available-for-download/"
)

# New MFT link GUID for the consolidated PDQ_DSV.zip download.
MFT_LINK_GUID = "1f5ddb8d-329a-4459-b7f8-177b4f5ee60d"

# Base URL for the MFT link page (GoAnywhere Web Client).
MFT_BASE_URL = f"https://mft.rrc.texas.gov/link/{MFT_LINK_GUID}"

# Name of the consolidated ZIP archive on the MFT server.
ZIP_FILENAME = "PDQ_DSV.zip"

# GoAnywhere PrimeFaces form action path (JSF endpoint).
_GA_FORM_ACTION = "https://mft.rrc.texas.gov/webclient/godrive/PublicGoDrive.xhtml"

# After the form POST triggers the download, the server redirects to this
# path.  Following it with the session cookie yields the file stream.
_GA_DOWNLOAD_PATH = "/link/godrivedownload"

# Chunk size for streaming downloads (256 KiB).
_STREAM_CHUNK = 256 * 1024

# -- ArcGIS REST API for well location data ----------------------------------
ARCGIS_WELLS_URL = (
    "https://gis.rrc.texas.gov/server/rest/services"
    "/rrc_public/RRC_Public_Viewer_Srvs/MapServer/1/query"
)
ARCGIS_BATCH_SIZE = 1_000  # server maxRecordCount


class TxRrcIngester(BaseIngester):
    """Download TX RRC PDQ consolidated ZIP and extract bronze DSV files.

    The ingester first tries to discover the download URL dynamically from
    the RRC data-download page or by probing the GoAnywhere link page.  If
    dynamic discovery fails, it falls back to known GoAnywhere URL patterns.

    Output structure::

        data/bronze/tx/{pull_date}/
            OG_LEASE_CYCLE.dsv
            OG_WELL_COMPLETION.dsv
            ...
    """

    def __init__(
        self,
        *,
        pull_date: datetime.date | None = None,
        tables: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        # Allow callers to override the pull date (handy for testing / backfill).
        self.pull_date = pull_date or datetime.date.today()
        self.tables = tables or PDQ_TABLES

        # Increase default timeout -- these files can be very large (~3.39 GB).
        kwargs.setdefault("timeout", 600.0)
        # Be polite: 2-second delay between requests by default.
        kwargs.setdefault("delay", 2.0)

        super().__init__(output_dir=BRONZE_TX_DIR, **kwargs)

    # ------------------------------------------------------------------
    # discovery (required by BaseIngester but not used for download)
    # ------------------------------------------------------------------

    def discover(self) -> list[str]:
        """Return the GoAnywhere link page URL.

        The actual download requires a session-based interaction (see
        ``_download_pdq_zip``), so this just returns the link page URL
        for informational purposes.
        """
        return [MFT_BASE_URL]

    # ------------------------------------------------------------------
    # ingestion
    # ------------------------------------------------------------------

    def ingest(self) -> Path:
        """Download the PDQ_DSV.zip, extract target tables, and return the
        output directory.

        Each run creates a date-stamped subdirectory so that multiple
        monthly snapshots can coexist::

            data/bronze/tx/2026-02-11/OG_LEASE_CYCLE.dsv
        """
        pull_dir = self.output_dir / self.pull_date.isoformat()
        pull_dir.mkdir(parents=True, exist_ok=True)
        self.log.info(
            "Starting TX RRC PDQ ingestion for %s -> %s",
            self.pull_date.isoformat(),
            pull_dir,
        )

        # Check if all target DSV files already exist (skip download entirely)
        all_exist = True
        for table_name in self.tables:
            dsv_path = pull_dir / f"{table_name}.dsv"
            if not dsv_path.exists() or dsv_path.stat().st_size == 0:
                all_exist = False
                break

        if all_exist:
            self.log.info(
                "All %d target DSV files already exist -- skipping download.",
                len(self.tables),
            )
            return pull_dir

        zip_path = pull_dir / ZIP_FILENAME

        # Download the ZIP if we don't already have it
        if zip_path.exists() and zip_path.stat().st_size > 0:
            # Validate existing ZIP before skipping download
            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    _ = zf.namelist()
                self.log.info(
                    "ZIP already exists and is valid: %s (%s) -- skipping download.",
                    zip_path.name,
                    _fmt_bytes(zip_path.stat().st_size),
                )
            except zipfile.BadZipFile:
                self.log.warning(
                    "Existing ZIP is corrupt -- removing and re-downloading."
                )
                zip_path.unlink()
                self._download_pdq_zip(zip_path)
        else:
            self.log.info("Downloading %s (~3.39 GB) ...", ZIP_FILENAME)
            self._download_pdq_zip(zip_path)

        # Extract only the tables we need from the ZIP
        succeeded = 0
        skipped = 0
        failed = 0
        total = len(self.tables)

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zip_contents = zf.namelist()
                if not zip_contents:
                    raise RuntimeError(
                        "ZIP file is valid but empty (0 entries). "
                        "The download may have returned an incomplete archive."
                    )
                self.log.info(
                    "ZIP contains %d entries. Extracting %d target tables ...",
                    len(zip_contents),
                    total,
                )

                for idx, (table_name, _file_stem) in enumerate(
                    self.tables.items(), 1
                ):
                    dest = pull_dir / f"{table_name}.dsv"

                    if dest.exists() and dest.stat().st_size > 0:
                        self.log.info(
                            "[%d/%d] SKIP (exists) %s (%s)",
                            idx,
                            total,
                            dest.name,
                            _fmt_bytes(dest.stat().st_size),
                        )
                        skipped += 1
                        continue

                    # Find the matching entry in the ZIP.  The files inside
                    # may have various extensions or be in subdirectories.
                    zip_entry = self._find_zip_entry(zip_contents, table_name)
                    if zip_entry is None:
                        self.log.error(
                            "[%d/%d] FAILED %s -- not found in ZIP archive. "
                            "Available entries: %s",
                            idx,
                            total,
                            table_name,
                            ", ".join(zip_contents[:20]),
                        )
                        failed += 1
                        continue

                    self.log.info(
                        "[%d/%d] Extracting %s -> %s ...",
                        idx,
                        total,
                        zip_entry,
                        dest.name,
                    )
                    try:
                        self._extract_member(zf, zip_entry, dest)
                        size = dest.stat().st_size
                        self.log.info(
                            "  -> saved %s (%s)", dest.name, _fmt_bytes(size)
                        )
                        succeeded += 1
                    except Exception:
                        failed += 1
                        self.log.error(
                            "[%d/%d] FAILED extracting %s",
                            idx,
                            total,
                            table_name,
                            exc_info=True,
                        )
                        # Clean up partial extraction
                        if dest.exists():
                            dest.unlink()

        except zipfile.BadZipFile as exc:
            self.log.error(
                "ZIP file is corrupt or not a valid ZIP: %s", exc
            )
            # Remove the corrupt ZIP so next run re-downloads
            if zip_path.exists():
                zip_path.unlink()
                self.log.info("Removed corrupt ZIP: %s", zip_path)
            raise RuntimeError(
                f"Downloaded file is not a valid ZIP archive: {exc}"
            ) from exc

        self.log.info(
            "Extraction complete: %d succeeded, %d skipped, %d failed (of %d).",
            succeeded,
            skipped,
            failed,
            total,
        )

        # Fail-on-zero guard BEFORE cleanup: keep ZIP for debugging if
        # all extractions failed (avoids 3.39 GB re-download).
        if succeeded + skipped == 0:
            self.log.error(
                "All %d extractions failed -- keeping ZIP for debugging.", total
            )
            raise RuntimeError(
                f"All {total} extractions failed -- no data ingested. "
                f"Check ZIP contents and table name matching."
            )

        # Clean up the ZIP to save disk space (~3.39 GB)
        if zip_path.exists():
            self.log.info(
                "Removing ZIP to save disk space: %s (%s)",
                zip_path.name,
                _fmt_bytes(zip_path.stat().st_size),
            )
            zip_path.unlink()

        return pull_dir

    # ------------------------------------------------------------------
    # well location ingestion (ArcGIS REST API)
    # ------------------------------------------------------------------

    def ingest_well_locations(self) -> int:
        """Download paginated well location JSON from the TX RRC ArcGIS API.

        Stores batch files in ``data/bronze/tx/{pull_date}/wells/`` following
        the same pattern as the NM OCD well ingester.

        Returns the total number of feature records retrieved.
        """
        pull_dir = self.output_dir / self.pull_date.isoformat()
        wells_dir = pull_dir / "wells"
        wells_dir.mkdir(parents=True, exist_ok=True)
        self.log.info("Ingesting TX well locations from ArcGIS API -> %s", wells_dir)

        # Clear stale batch files for fresh pagination.
        stale = sorted(wells_dir.glob("wells_batch_*.json"))
        if stale:
            self.log.info("Clearing %d stale batch files", len(stale))
            for f in stale:
                f.unlink()

        offset = 0
        total_features = 0
        batch_num = 0

        while True:
            batch_num += 1
            out_file = wells_dir / f"wells_batch_{batch_num:04d}.json"

            params: dict[str, Any] = {
                "where": "1=1",
                "outFields": "API,GIS_LAT83,GIS_LONG83",
                "resultOffset": offset,
                "resultRecordCount": ARCGIS_BATCH_SIZE,
                "outSR": "4326",
                "returnGeometry": "false",
                "f": "json",
            }

            self.log.info(
                "Fetching well batch %d (offset=%d)", batch_num, offset,
            )
            resp = self.fetch(ARCGIS_WELLS_URL, params=params)
            data = resp.json()

            if "error" in data:
                err = data["error"]
                msg = (
                    f"ArcGIS server error at offset {offset}: "
                    f"code={err.get('code')}, message={err.get('message')}"
                )
                self.log.error(msg)
                raise RuntimeError(msg)

            features = data.get("features", [])
            if not features:
                self.log.info("No more features at offset %d; stopping", offset)
                break

            out_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
            size_kb = out_file.stat().st_size / 1024
            self.log.info(
                "Saved %s -- %d features (%.1f KB)",
                out_file.name, len(features), size_kb,
            )

            total_features += len(features)
            offset += ARCGIS_BATCH_SIZE

            if not data.get("exceededTransferLimit", False):
                self.log.info("exceededTransferLimit=false; all records retrieved")
                break

        self.log.info(
            "Well location ingestion complete: %d features in %d batches",
            total_features, batch_num,
        )
        return total_features

    # ------------------------------------------------------------------
    # download helpers
    # ------------------------------------------------------------------

    def _download_pdq_zip(self, dest: Path) -> None:
        """Download PDQ_DSV.zip via the GoAnywhere Web Client.

        The GoAnywhere MFT server uses a PrimeFaces/JSF web application.
        There is no direct download URL.  The flow is:

        1. GET the link page — establishes a JSESSIONID and returns the
           JSF ViewState token plus the PrimeFaces DataTable with file info.
        2. POST the JSF form with the row action parameter — the server
           responds with a 302 redirect to ``/link/godrivedownload``.
        3. GET ``/link/godrivedownload`` with the session cookie — the
           server streams the ZIP file (``application/force-download``).

        Writes to a ``.part`` file first and renames on success.
        """
        part_path = dest.with_suffix(dest.suffix + ".part")
        start = time.monotonic()

        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self._goanywhere_download(part_path, attempt)
                # Verify we got a real ZIP (check magic bytes)
                with open(part_path, "rb") as f:
                    magic = f.read(4)
                if magic[:2] != b"PK":
                    raise RuntimeError(
                        "Downloaded file is not a ZIP (magic bytes: "
                        f"{magic!r}).  The GoAnywhere server may have "
                        "returned an HTML error page instead."
                    )
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                wait = self.backoff_factor ** attempt
                self.log.warning(
                    "Download attempt %d/%d failed (%s). Retrying in %.1fs ...",
                    attempt, self.max_retries, exc, wait,
                )
                if part_path.exists():
                    part_path.unlink()
                time.sleep(wait)

        if last_exc is not None:
            if part_path.exists():
                part_path.unlink()
            raise RuntimeError(
                f"Failed to download {ZIP_FILENAME} after "
                f"{self.max_retries} retries"
            ) from last_exc

        part_path.rename(dest)
        elapsed = time.monotonic() - start
        size = dest.stat().st_size
        self.log.info(
            "  -> saved %s (%s) in %.1fs", dest.name, _fmt_bytes(size), elapsed,
        )

    def _goanywhere_download(self, dest: Path, attempt: int) -> None:
        """Execute the GoAnywhere 3-step download flow.

        Uses a *separate* httpx.Client so that session cookies don't
        pollute the main client used by BaseIngester.  A single client
        is reused across all three steps for proper cookie propagation.
        """
        self.log.info("GoAnywhere download (attempt %d/%d)", attempt, self.max_retries)

        with httpx.Client(
            timeout=httpx.Timeout(600.0, read=1800.0),
            follow_redirects=False,
        ) as session:
            # Step 1: GET the link page for session cookie + ViewState
            self.log.info("  Step 1: GET %s", MFT_BASE_URL)
            resp1 = session.get(MFT_BASE_URL, follow_redirects=True)
            resp1.raise_for_status()

            # Parse the GoAnywhere PrimeFaces page with BeautifulSoup
            soup = BeautifulSoup(resp1.text, "html.parser")

            # Extract the JSF ViewState token
            vs_input = soup.find("input", {"name": "javax.faces.ViewState"})
            if not vs_input or not vs_input.get("value"):
                raise RuntimeError(
                    "Could not extract ViewState from GoAnywhere page. "
                    "The page structure may have changed."
                )
            view_state = vs_input["value"]

            # Extract the row key (data-rk attribute on the first table row)
            first_row = soup.find("tr", attrs={"data-rk": True})
            if not first_row:
                raise RuntimeError(
                    "Could not extract row key from GoAnywhere page. "
                    "The page structure may have changed."
                )
            row_key = first_row["data-rk"]

            # Extract the command link ID for row 0 (the download action)
            file_link = soup.find("a", {"id": re.compile(r"fileTable:0:j_id_")})
            if not file_link:
                raise RuntimeError(
                    "Could not find fileTable row action in GoAnywhere page. "
                    "The page structure may have changed."
                )
            cmd_id = file_link["id"]
            self.log.debug("  ViewState=%s... row_key=%s cmd=%s",
                           str(view_state)[:30], row_key, cmd_id)

            # Step 2: POST the PrimeFaces form to trigger the download
            self.log.info("  Step 2: POST form action (%s)", cmd_id)
            resp2 = session.post(
                _GA_FORM_ACTION,
                data={
                    "fileList": "fileList",
                    "fileList_SUBMIT": "1",
                    "fileTable_selection": row_key,
                    cmd_id: cmd_id,
                    "javax.faces.ViewState": view_state,
                },
            )

            if resp2.status_code not in (302, 303):
                raise RuntimeError(
                    f"Expected 302 redirect after form POST, got "
                    f"{resp2.status_code}.  The GoAnywhere flow may "
                    f"have changed."
                )
            redirect_path = resp2.headers.get("location", "")
            if not redirect_path:
                raise RuntimeError("No Location header in form POST redirect.")

            download_url = (
                f"https://mft.rrc.texas.gov{redirect_path}"
                if redirect_path.startswith("/")
                else redirect_path
            )
            self.log.info("  Step 3: GET %s (streaming)", download_url)

            # Step 3: Stream the download with the session cookie
            with session.stream(
                "GET", download_url, follow_redirects=True,
            ) as resp3:
                resp3.raise_for_status()
                ct = resp3.headers.get("content-type", "")
                cl = resp3.headers.get("content-length", "unknown")

                if "html" in ct.lower():
                    raise RuntimeError(
                        f"Server returned HTML instead of file "
                        f"(Content-Type: {ct})"
                    )

                self.log.info(
                    "  <- %s (%s bytes, %s)", ZIP_FILENAME, cl, ct,
                )

                written = 0
                last_log = 0
                with open(dest, "wb") as fh:
                    for chunk in resp3.iter_bytes(chunk_size=_STREAM_CHUNK):
                        fh.write(chunk)
                        written += len(chunk)
                        if written - last_log >= 100 * 1024 * 1024:
                            self.log.info(
                                "  ... %s downloaded", _fmt_bytes(written)
                            )
                            last_log = written

                self.log.info("  Download complete: %s", _fmt_bytes(written))

    # ------------------------------------------------------------------
    # ZIP extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_zip_entry(
        zip_contents: list[str], table_name: str
    ) -> str | None:
        """Find the ZIP entry matching a table name.

        Handles various naming conventions that may appear inside the ZIP:
        - Direct match: ``OG_LEASE_CYCLE`` (no extension)
        - With extension: ``OG_LEASE_CYCLE.dsv``, ``OG_LEASE_CYCLE.csv``,
          ``OG_LEASE_CYCLE.txt``
        - In a subdirectory: ``PDQ_DSV/OG_LEASE_CYCLE.dsv``
        - Case-insensitive matching
        """
        table_upper = table_name.upper()

        # Try exact matches first (with common extensions)
        for entry in zip_contents:
            # Get just the filename (strip directory path)
            basename = entry.rsplit("/", 1)[-1] if "/" in entry else entry
            basename_upper = basename.upper()
            stem = basename_upper.rsplit(".", 1)[0] if "." in basename_upper else basename_upper

            if stem == table_upper:
                return entry

        # Fallback: partial match (table name anywhere in the entry name)
        for entry in zip_contents:
            if table_upper in entry.upper():
                return entry

        return None

    @staticmethod
    def _extract_member(
        zf: zipfile.ZipFile, member: str, dest: Path
    ) -> None:
        """Extract a single member from the ZIP to *dest*.

        Streams via read/write to avoid loading the entire member into memory
        at once (some tables are very large).  Validates against Zip Slip
        (path traversal) before extraction.
        """
        # Zip Slip protection: reject members with path traversal components.
        # Since we write to a fixed *dest* path (not member's own path), this
        # is a defence-in-depth check against malicious ZIP entries.
        member_parts = Path(member).parts
        if Path(member).is_absolute() or ".." in member_parts:
            raise ValueError(
                f"Zip Slip detected: member '{member}' contains unsafe path "
                f"components (absolute or parent traversal)"
            )

        part_path = dest.with_suffix(dest.suffix + ".part")
        try:
            with zf.open(member) as src, open(part_path, "wb") as dst:
                while True:
                    chunk = src.read(_STREAM_CHUNK)
                    if not chunk:
                        break
                    dst.write(chunk)
            part_path.rename(dest)
        except Exception:
            if part_path.exists():
                part_path.unlink()
            raise


# ------------------------------------------------------------------
# utilities
# ------------------------------------------------------------------


def _fmt_bytes(n: int) -> str:
    """Human-readable byte size (e.g. ``1.2 GB``)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024  # type: ignore[assignment]
    return f"{n:.1f} PB"


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------


def main() -> None:
    """Run TX RRC PDQ ingestion from the command line."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("=== TX RRC PDQ Bronze Ingestion ===")
    start = time.monotonic()

    with TxRrcIngester() as ingester:
        output = ingester.ingest()

    elapsed = time.monotonic() - start
    logger.info("Done. Output: %s  (%.1fs elapsed)", output, elapsed)


if __name__ == "__main__":
    main()
