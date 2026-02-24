"""New Mexico Oil Conservation Division (OCD) bronze ingestion scraper.

Two-phase ingestion:
  Phase 1 -- Well master data from the EMNRD ArcGIS REST API (paginated JSON).
  Phase 2 -- Production & reference data from the OCD FTP server (164.64.106.6).

All data is stored as-received in data/bronze/nm/{pull_date}/.

Phase 2 data sources (OCD FTP, updated Mon/Wed/Fri nights):
  - volumes/wcproduction/wcproduction.zip  -- well-completion production (oil/gas/water)
  - core/ogrid/ogrid.zip                   -- operator (OGRID) reference table
  - core/pool/pool.zip                     -- pool name reference table
  - core/wchistory/wchistory.zip           -- well-completion history & status
  All ZIPs contain a single XML file (UTF-16 LE, SQL Server FOR XML format).
"""

from __future__ import annotations

import ftplib
import json
import logging
import os
import time
import zipfile
from datetime import date
from pathlib import Path
from typing import Any

from src.ingestion.base import BaseIngester
from src.utils.config import BRONZE_NM_DIR

logger = logging.getLogger(__name__)

# -- ArcGIS REST API for well master data ------------------------------------
ARCGIS_WELLS_URL = (
    "https://gis.emnrd.nm.gov/arcgis/rest/services"
    "/OCDView/Wells_Public/FeatureServer/0/query"
)
ARCGIS_BATCH_SIZE = 10_000

# -- OCD FTP server ----------------------------------------------------------
OCD_FTP_HOST = "164.64.106.6"
OCD_FTP_PORT = 21
OCD_FTP_ROOT = "/Public/OCD/OCD Interface v1.1"

# Files to download for Phase 2.  Each entry is (remote_subpath, local_name).
# remote_subpath is relative to OCD_FTP_ROOT.
OCD_FTP_PRODUCTION_FILES: list[tuple[str, str]] = [
    ("volumes/wcproduction/wcproduction.zip", "wcproduction.zip"),
]

# Supplementary reference tables needed to join production data back to
# well identifiers, operators, pool names, and well status.
OCD_FTP_REFERENCE_FILES: list[tuple[str, str]] = [
    ("core/ogrid/ogrid.zip", "ogrid.zip"),
    ("core/pool/pool.zip", "pool.zip"),
    ("core/wchistory/wchistory.zip", "wchistory.zip"),
]

# Maximum number of FTP connection retries before giving up on a file.
FTP_MAX_RETRIES = 3
FTP_RETRY_BACKOFF = 5.0  # seconds; doubles each retry
FTP_TIMEOUT = 60  # seconds for FTP socket operations


def _safe_zip_members(
    zf: zipfile.ZipFile, target_dir: Path
) -> list[str]:
    """Return ZIP member names that are safe to extract (no Zip Slip).

    Rejects any member whose resolved extraction path would escape
    *target_dir* via ``..`` or absolute path components.
    """
    safe: list[str] = []
    resolved_target = target_dir.resolve()
    for name in zf.namelist():
        member_path = (resolved_target / name).resolve()
        try:
            member_path.relative_to(resolved_target)
        except ValueError:
            logger.warning("Skipping unsafe ZIP member (Zip Slip): %s", name)
            continue
        safe.append(name)
    return safe


class NmOcdIngester(BaseIngester):
    """Scraper for New Mexico OCD oil & gas production data.

    Writes raw data into ``data/bronze/nm/{pull_date}/`` with sub-folders:
    - ``wells/``       -- paginated JSON batches from the ArcGIS API
    - ``production/``  -- production & reference ZIPs from the OCD FTP server
    """

    def __init__(self, *, pull_date: str | None = None, **kwargs: Any) -> None:
        super().__init__(output_dir=BRONZE_NM_DIR, **kwargs)
        self.pull_date = pull_date or date.today().isoformat()
        self.run_dir = self.output_dir / self.pull_date
        self.wells_dir = self.run_dir / "wells"
        self.prod_dir = self.run_dir / "production"

    # ------------------------------------------------------------------
    # FTP helpers
    # ------------------------------------------------------------------

    def _ftp_connect(self) -> ftplib.FTP:
        """Open an anonymous FTP connection to the OCD server.

        Returns a connected and logged-in ``ftplib.FTP`` instance.
        Raises ``ftplib.all_errors`` on failure.
        """
        self.log.info(
            "Connecting to OCD FTP server %s:%d", OCD_FTP_HOST, OCD_FTP_PORT
        )
        ftp = ftplib.FTP()
        ftp.connect(OCD_FTP_HOST, OCD_FTP_PORT, timeout=FTP_TIMEOUT)
        ftp.login()  # anonymous
        self.log.info("FTP login successful")
        return ftp

    def _ftp_download(
        self,
        ftp: ftplib.FTP,
        remote_path: str,
        local_path: Path,
    ) -> int:
        """Download a single file from the FTP server using binary streaming.

        Uses ``retrbinary`` to stream directly to disk, avoiding holding
        the full file in memory.  Returns the number of bytes written.

        If *local_path* already exists and has the same size as the remote
        file, the download is skipped (idempotent).
        """
        # Check remote file size for skip-if-exists logic
        try:
            ftp.sendcmd("TYPE I")
            remote_size = ftp.size(remote_path)
        except ftplib.error_perm:
            remote_size = None

        if local_path.exists():
            local_size = local_path.stat().st_size
            if remote_size is not None and local_size == remote_size:
                self.log.info(
                    "Already exists with matching size (%s bytes), skipping: %s",
                    f"{local_size:,}", local_path.name,
                )
                return local_size

        size_str = f"{remote_size:,}" if remote_size else "unknown"
        self.log.info(
            "Downloading %s (%s bytes) -> %s",
            remote_path, size_str, local_path.name,
        )

        # Stream to a temporary file, then rename for atomicity
        tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")
        bytes_written = 0

        def _write_chunk(data: bytes) -> None:
            nonlocal bytes_written
            fh.write(data)
            bytes_written += len(data)

        with open(tmp_path, "wb") as fh:
            ftp.retrbinary(f"RETR {remote_path}", _write_chunk, blocksize=262_144)

        # Atomic rename
        tmp_path.replace(local_path)
        size_kb = bytes_written / 1024
        self.log.info(
            "Saved %s (%.1f KB)", local_path.name, size_kb,
        )
        return bytes_written

    def _ftp_download_with_retry(
        self,
        remote_path: str,
        local_path: Path,
    ) -> int:
        """Download a file with automatic retry on FTP errors.

        Opens a fresh FTP connection for each attempt, since FTP
        connections often cannot be reused after a timeout or reset.

        Returns bytes written, or 0 if all retries are exhausted.
        """
        last_exc: Exception | None = None

        for attempt in range(1, FTP_MAX_RETRIES + 1):
            ftp: ftplib.FTP | None = None
            try:
                ftp = self._ftp_connect()
                return self._ftp_download(ftp, remote_path, local_path)
            except ftplib.all_errors as exc:
                last_exc = exc
                wait = FTP_RETRY_BACKOFF * (2 ** (attempt - 1))
                self.log.warning(
                    "FTP download failed for %s (attempt %d/%d): %s. "
                    "Retrying in %.1fs...",
                    remote_path, attempt, FTP_MAX_RETRIES, exc, wait,
                )
                time.sleep(wait)
            except OSError as exc:
                last_exc = exc
                wait = FTP_RETRY_BACKOFF * (2 ** (attempt - 1))
                self.log.warning(
                    "Network error downloading %s (attempt %d/%d): %s. "
                    "Retrying in %.1fs...",
                    remote_path, attempt, FTP_MAX_RETRIES, exc, wait,
                )
                time.sleep(wait)
            finally:
                if ftp is not None:
                    try:
                        ftp.quit()
                    except Exception:
                        try:
                            ftp.close()
                        except Exception:
                            pass

        self.log.error(
            "Failed to download %s after %d retries: %s",
            remote_path, FTP_MAX_RETRIES, last_exc,
        )
        return 0

    # ------------------------------------------------------------------
    # Discovery -- list available files on the OCD FTP server
    # ------------------------------------------------------------------

    def discover(self) -> list[str]:
        """List available production and reference files on the OCD FTP server.

        Returns a list of remote file paths (relative to OCD_FTP_ROOT)
        that are available for download.  The file paths are deterministic
        based on the known FTP directory structure, but this method
        verifies they actually exist on the server.
        """
        self.log.info("Discovering files on OCD FTP server %s", OCD_FTP_HOST)
        all_files = OCD_FTP_PRODUCTION_FILES + OCD_FTP_REFERENCE_FILES
        available: list[str] = []

        ftp: ftplib.FTP | None = None
        try:
            ftp = self._ftp_connect()
            for remote_subpath, _local_name in all_files:
                remote_path = f"{OCD_FTP_ROOT}/{remote_subpath}"
                try:
                    ftp.sendcmd("TYPE I")
                    size = ftp.size(remote_path)
                    self.log.info(
                        "  Found: %s (%s bytes)", remote_subpath, f"{size:,}"
                    )
                    available.append(remote_subpath)
                except ftplib.error_perm:
                    self.log.warning("  Not found: %s", remote_subpath)
        except ftplib.all_errors as exc:
            self.log.error("FTP discovery failed: %s", exc)
        finally:
            if ftp is not None:
                try:
                    ftp.quit()
                except Exception:
                    try:
                        ftp.close()
                    except Exception:
                        pass

        self.log.info("Discovered %d files on OCD FTP server", len(available))
        return available

    # ------------------------------------------------------------------
    # Phase 1 -- Well master data (ArcGIS REST)
    # ------------------------------------------------------------------

    def ingest_wells(self) -> int:
        """Download paginated well master JSON from the ArcGIS API.

        Always fetches from scratch for a given pull_date to avoid
        inconsistencies when the server dataset changes between runs.
        Existing batch files are cleared before starting.

        Returns the total number of feature records retrieved.
        """
        self.wells_dir.mkdir(parents=True, exist_ok=True)
        self.log.info("Phase 1: Ingesting well master data from ArcGIS API")

        # Clear any stale batch files to avoid offset mismatches if the
        # server dataset changed since a partial prior run.
        stale = sorted(self.wells_dir.glob("wells_batch_*.json"))
        if stale:
            self.log.info(
                "Clearing %d stale batch files for fresh pagination", len(stale)
            )
            for f in stale:
                f.unlink()

        offset = 0
        total_features = 0
        batch_num = 0

        while True:
            batch_num += 1
            out_file = self.wells_dir / f"wells_batch_{batch_num:04d}.json"

            params: dict[str, Any] = {
                "where": "1=1",
                "outFields": "*",
                "resultOffset": offset,
                "resultRecordCount": ARCGIS_BATCH_SIZE,
                "outSR": "4326",
                "f": "json",
            }

            self.log.info(
                "Fetching well batch %d (offset=%d)", batch_num, offset
            )
            resp = self.fetch(ARCGIS_WELLS_URL, params=params)
            data = resp.json()

            # ArcGIS REST APIs return HTTP 200 even on errors, embedding
            # the error in the JSON body.  Detect and raise explicitly.
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

            # Persist raw JSON
            out_file.write_text(
                json.dumps(data, indent=2), encoding="utf-8"
            )
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
            "Phase 1 complete: %d total well features in %d batches",
            total_features, batch_num,
        )
        return total_features

    # ------------------------------------------------------------------
    # Phase 2 -- Production data (OCD FTP server)
    # ------------------------------------------------------------------

    def ingest_production(self) -> int:
        """Download production and reference data from the OCD FTP server.

        Downloads ZIP files containing XML data (SQL Server FOR XML format,
        UTF-16 LE encoded) for:
          - wcproduction: monthly well-completion production volumes
          - ogrid: operator reference table
          - pool: pool/field name reference table
          - wchistory: well-completion history and status

        Each ZIP is downloaded to ``data/bronze/nm/{pull_date}/production/``
        and extracted in place.  Files that already exist locally with a
        matching size are skipped (idempotent re-runs).

        Returns the number of files successfully downloaded.
        """
        self.prod_dir.mkdir(parents=True, exist_ok=True)
        self.log.info(
            "Phase 2: Ingesting production data from OCD FTP server %s",
            OCD_FTP_HOST,
        )

        all_files = OCD_FTP_PRODUCTION_FILES + OCD_FTP_REFERENCE_FILES
        downloaded = 0

        # Critical files that must succeed for the pipeline to produce data.
        critical_files = {name for _, name in OCD_FTP_PRODUCTION_FILES}

        for remote_subpath, local_name in all_files:
            remote_path = f"{OCD_FTP_ROOT}/{remote_subpath}"
            local_path = self.prod_dir / local_name

            bytes_written = self._ftp_download_with_retry(remote_path, local_path)
            if bytes_written == 0 and not local_path.exists():
                if local_name in critical_files:
                    raise RuntimeError(
                        f"Critical file {local_name} failed to download after "
                        f"{FTP_MAX_RETRIES} retries — cannot proceed without "
                        f"production data."
                    )
                self.log.warning(
                    "Optional reference file %s unavailable; continuing",
                    local_name,
                )
                continue

            # Extract ZIP contents alongside the ZIP for downstream parsing.
            extract_dir = self.prod_dir / local_path.stem
            if local_path.exists() and local_path.suffix.lower() == ".zip":
                try:
                    with zipfile.ZipFile(local_path) as zf:
                        safe_members = _safe_zip_members(zf, extract_dir)
                        zf.extractall(extract_dir, members=safe_members)
                        member_count = len(safe_members)
                    self.log.info(
                        "Extracted %d files to %s/", member_count, local_path.stem,
                    )
                except zipfile.BadZipFile:
                    self.log.warning(
                        "%s is not a valid ZIP file; raw bytes kept", local_name
                    )

            downloaded += 1

        self.log.info(
            "Phase 2 complete: %d/%d files downloaded from OCD FTP",
            downloaded, len(all_files),
        )
        return downloaded

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def ingest(self) -> Path:
        """Run both ingestion phases and return the run output directory."""
        self.log.info(
            "=== NM OCD Ingestion -- pull_date=%s ===", self.pull_date
        )
        self.run_dir.mkdir(parents=True, exist_ok=True)

        well_count = self.ingest_wells()
        prod_count = self.ingest_production()

        # Write a small manifest for downstream consumers
        manifest = {
            "pull_date": self.pull_date,
            "wells_features": well_count,
            "production_files": prod_count,
            "wells_dir": str(self.wells_dir),
            "production_dir": str(self.prod_dir),
            "source": "ocd_ftp",
            "ftp_host": OCD_FTP_HOST,
        }
        manifest_path = self.run_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        self.log.info("Manifest written to %s", manifest_path)
        self.log.info("=== NM OCD Ingestion complete ===")
        return self.run_dir


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)-36s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with NmOcdIngester(delay=1.0) as scraper:
        output = scraper.ingest()
        print(f"\nDone. Output directory: {output}")
