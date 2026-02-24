"""Oklahoma Corporation Commission (OCC) bronze ingestion scraper.

Four-phase ingestion:
  Phase 1 -- Well master data from the OCC ArcGIS REST API (paginated JSON).
  Phase 2 -- RBDMS well data CSV from the OCC Oil & Gas Data Files page.
  Phase 3 -- Well completions XLSX (IP rates, formation, dates) from OCC.
  Phase 4 -- Intent-to-Drill (ITD) permits XLSX from OCC.

All data is stored as-received in data/bronze/ok/{pull_date}/.

Data sources:
  - ArcGIS RBDMS_WELLS FeatureServer/220 (nightly, ~2000 records per page)
  - RBDMS Well Data CSV from oklahoma.gov/occ (nightly bulk download)
  - Completions XLSX from oklahoma.gov/occ (daily bulk download)
  - ITD permits XLSX from oklahoma.gov/occ (daily bulk download)
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from src.ingestion.base import BaseIngester
from src.utils.config import BRONZE_OK_DIR

logger = logging.getLogger(__name__)

# -- ArcGIS REST API for well master data ------------------------------------
ARCGIS_WELLS_URL = (
    "https://gis.occ.ok.gov/server/rest/services"
    "/Hosted/RBDMS_WELLS/FeatureServer/220/query"
)
ARCGIS_BATCH_SIZE = 2_000

# -- OCC RBDMS Well Data CSV -------------------------------------------------
# The nightly CSV download from the OCC Oil & Gas Data Files page.
# Source: https://oklahoma.gov/occ/divisions/oil-gas/oil-gas-data.html
RBDMS_CSV_URL = (
    "https://oklahoma.gov/content/dam/ok/en/occ/documents"
    "/og/ogdatafiles/rbdms-wells.csv"
)

# -- OCC Well Completions XLSX -------------------------------------------------
# Daily bulk download with IP test rates, formation, spud/completion dates.
COMPLETIONS_XLSX_URL = (
    "https://oklahoma.gov/content/dam/ok/en/occ/documents"
    "/og/ogdatafiles/completions-wells-formations-base.xlsx"
)

# -- OCC Intent-to-Drill (ITD) Permits XLSX -----------------------------------
# Daily bulk download with permit lifecycle (submit, approve, expire).
ITD_XLSX_URL = (
    "https://oklahoma.gov/content/dam/ok/en/occ/documents"
    "/og/ogdatafiles/ITD-wells-formations-base.xlsx"
)


class OkOccIngester(BaseIngester):
    """Scraper for Oklahoma OCC oil & gas well and production data.

    Writes raw data into ``data/bronze/ok/{pull_date}/`` with sub-folders:
    - ``wells/``  -- paginated JSON batches from the ArcGIS API
    - ``data/``   -- RBDMS bulk CSV + completions/ITD XLSX downloads
    """

    def __init__(self, *, pull_date: str | None = None, **kwargs: Any) -> None:
        super().__init__(output_dir=BRONZE_OK_DIR, **kwargs)
        self.pull_date = pull_date or date.today().isoformat()
        self.run_dir = self.output_dir / self.pull_date
        self.wells_dir = self.run_dir / "wells"
        self.data_dir = self.run_dir / "data"

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover(self) -> list[str]:
        """Return the list of data source URLs for OK ingestion."""
        return [ARCGIS_WELLS_URL, RBDMS_CSV_URL, COMPLETIONS_XLSX_URL, ITD_XLSX_URL]

    # ------------------------------------------------------------------
    # Phase 1 -- Well master data (ArcGIS REST)
    # ------------------------------------------------------------------

    def ingest_wells(self) -> int:
        """Download paginated well master JSON from the OCC ArcGIS API.

        Fetches RBDMS_WELLS layer 220. Max records per query = 2000.
        Returns the total number of feature records retrieved.
        """
        self.wells_dir.mkdir(parents=True, exist_ok=True)
        self.log.info("Phase 1: Ingesting OK well master data from ArcGIS API")

        # Clear stale batch files for fresh pagination.
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

            # ArcGIS returns HTTP 200 even on errors.
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
    # Phase 2 -- RBDMS CSV download
    # ------------------------------------------------------------------

    def ingest_csv(self) -> Path | None:
        """Download the RBDMS well data CSV from the OCC website.

        Returns the local path of the downloaded CSV, or None if the
        download fails after retries.
        """
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log.info("Phase 2: Downloading RBDMS CSV from OCC website")

        local_path = self.data_dir / "rbdms_well_data.csv"

        # Skip if already downloaded (idempotent)
        if local_path.exists() and local_path.stat().st_size > 0:
            self.log.info(
                "RBDMS CSV already exists (%s bytes), skipping download",
                f"{local_path.stat().st_size:,}",
            )
            return local_path

        try:
            resp = self.fetch(RBDMS_CSV_URL)
            tmp_path = local_path.with_suffix(".csv.tmp")
            tmp_path.write_bytes(resp.content)
            tmp_path.replace(local_path)

            size_kb = local_path.stat().st_size / 1024
            self.log.info("Saved RBDMS CSV (%.1f KB)", size_kb)
            return local_path

        except Exception as exc:
            self.log.error("Failed to download RBDMS CSV: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Phase 3 -- Completions XLSX download
    # ------------------------------------------------------------------

    def ingest_completions(self) -> Path | None:
        """Download the well completions XLSX from the OCC website.

        Contains IP test rates, formation, spud/completion dates, PUN
        cross-reference, and drilling details.
        Returns the local path, or None on failure.
        """
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log.info("Phase 3: Downloading completions XLSX from OCC website")

        local_path = self.data_dir / "completions_wells_formations.xlsx"

        if local_path.exists() and local_path.stat().st_size > 0:
            self.log.info(
                "Completions XLSX already exists (%s bytes), skipping",
                f"{local_path.stat().st_size:,}",
            )
            return local_path

        try:
            resp = self.fetch(COMPLETIONS_XLSX_URL)
            tmp_path = local_path.with_suffix(".xlsx.tmp")
            tmp_path.write_bytes(resp.content)
            tmp_path.replace(local_path)

            size_mb = local_path.stat().st_size / (1024 * 1024)
            self.log.info("Saved completions XLSX (%.1f MB)", size_mb)
            return local_path

        except Exception as exc:
            self.log.error("Failed to download completions XLSX: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Phase 4 -- Intent-to-Drill (ITD) permits XLSX download
    # ------------------------------------------------------------------

    def ingest_itd(self) -> Path | None:
        """Download the ITD permits XLSX from the OCC website.

        Contains permit lifecycle (submit, approve, expire), proposed
        well specs, and formation targets.
        Returns the local path, or None on failure.
        """
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log.info("Phase 4: Downloading ITD permits XLSX from OCC website")

        local_path = self.data_dir / "itd_wells_formations.xlsx"

        if local_path.exists() and local_path.stat().st_size > 0:
            self.log.info(
                "ITD XLSX already exists (%s bytes), skipping",
                f"{local_path.stat().st_size:,}",
            )
            return local_path

        try:
            resp = self.fetch(ITD_XLSX_URL)
            tmp_path = local_path.with_suffix(".xlsx.tmp")
            tmp_path.write_bytes(resp.content)
            tmp_path.replace(local_path)

            size_mb = local_path.stat().st_size / (1024 * 1024)
            self.log.info("Saved ITD permits XLSX (%.1f MB)", size_mb)
            return local_path

        except Exception as exc:
            self.log.error("Failed to download ITD XLSX: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def ingest(self) -> Path:
        """Run all ingestion phases and return the run output directory."""
        self.log.info(
            "=== OK OCC Ingestion -- pull_date=%s ===", self.pull_date
        )
        self.run_dir.mkdir(parents=True, exist_ok=True)

        well_count = self.ingest_wells()
        csv_path = self.ingest_csv()
        completions_path = self.ingest_completions()
        itd_path = self.ingest_itd()

        # Write manifest
        manifest = {
            "pull_date": self.pull_date,
            "wells_features": well_count,
            "csv_downloaded": csv_path is not None,
            "csv_path": str(csv_path) if csv_path else None,
            "completions_downloaded": completions_path is not None,
            "completions_path": str(completions_path) if completions_path else None,
            "itd_downloaded": itd_path is not None,
            "itd_path": str(itd_path) if itd_path else None,
            "wells_dir": str(self.wells_dir),
            "data_dir": str(self.data_dir),
            "source": "occ_arcgis",
            "arcgis_url": ARCGIS_WELLS_URL,
        }
        manifest_path = self.run_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        self.log.info("Manifest written to %s", manifest_path)
        self.log.info("=== OK OCC Ingestion complete ===")
        return self.run_dir


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)-36s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with OkOccIngester(delay=1.0) as scraper:
        output = scraper.ingest()
        print(f"\nDone. Output directory: {output}")
