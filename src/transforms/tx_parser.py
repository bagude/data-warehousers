"""Texas RRC production data parser -- bronze DSV/CSV to silver Parquet.

Reads the raw PDQ dump files downloaded by the TX RRC bronze ingester,
joins and transforms them into the canonical silver-layer production schema
(see ``src.schemas.production.PRODUCTION_SCHEMA``), and writes the result as
a Parquet file to ``data/silver/production/state=TX/``.

Bronze inputs (all in ``data/bronze/tx/{pull_date}/``):

  OG_LEASE_CYCLE.dsv      -- Monthly production volumes per lease
  OG_WELL_COMPLETION.dsv  -- Well/completion attributes (API, type, county)
  OG_OPERATOR_DW.dsv      -- Operator details (name, P-5 number)

As of 2026, the RRC distributes these files as pipe-delimited (|) DSVs
inside a consolidated PDQ_DSV.zip archive.  For backward compatibility,
the parser also supports the legacy comma-separated CSV format.

The parser does NOT use OG_FIELD_CYCLE or OG_COUNTY_CYCLE -- those are
pre-aggregated rollups useful for QA but not needed for silver records.

Transforms are driven by ``TX_FIELD_MAPPING`` from ``src.schemas.field_mappings``.
Deduplication uses the entity-type-aware strategy from ``src.schemas.dedup``.
Validation uses ``src.schemas.validation.validate_batch``.
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd

from src.schemas.dedup import assign_tx_entity_type, deduplicate
from src.schemas.field_mappings import TX_FIELD_MAPPING, get_source_columns
from src.schemas.production import PRODUCTION_SCHEMA
from src.schemas.validation import validate_batch
from src.transforms.base import BaseParser
from src.utils.config import BRONZE_TX_DIR, SILVER_DIR

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Bronze column selections (derived from TX_FIELD_MAPPING + join keys)
# ---------------------------------------------------------------------------

# OG_LEASE_CYCLE: production + join keys
_LC_COLS = [
    "DISTRICT_NO",
    "LEASE_NO",
    "LEASE_NAME",
    "CYCLE_YEAR",
    "CYCLE_MONTH",
    "OPERATOR_NO",
    "OPERATOR_NAME",
    "FIELD_NO",
    "FIELD_NAME",
    "LEASE_OIL_PROD_VOL",
    "LEASE_GAS_PROD_VOL",
    "LEASE_COND_PROD_VOL",
    "LEASE_CSGD_PROD_VOL",
]

# OG_WELL_COMPLETION: well attributes for join
# Note: the 2026 PDQ DSV format uses different column names than the legacy CSV.
# API_NO is constructed from API_COUNTY_CODE + API_UNIQUE_NO.
# WELL_TYPE_CODE and FIELD_NAME are not in this table (FIELD_NAME is in LEASE_CYCLE).
_WC_COLS = [
    "OIL_GAS_CODE",
    "DISTRICT_NO",
    "LEASE_NO",
    "WELL_NO",
    "API_COUNTY_CODE",
    "API_UNIQUE_NO",
    "COUNTY_NAME",
    "WELL_14B2_STATUS_CODE",
]

# OG_OPERATOR_DW: operator name lookup
_OP_COLS = [
    "OPERATOR_NO",
    "OPERATOR_NAME",
]

# ---------------------------------------------------------------------------
# Well-type code normalisation (RRC codes -> canonical labels)
# Per field_mappings.py: OL=Oil, GW=Gas, GC=Gas Condensate, IN=Injection
# ---------------------------------------------------------------------------

_WELL_TYPE_MAP: dict[str, str] = {
    "OL": "OIL",
    "O":  "OIL",
    "GW": "GAS",
    "G":  "GAS",
    "GC": "GAS_CONDENSATE",
    "IN": "INJ",
    "I":  "INJ",
    "SW": "SALT_WATER_DISPOSAL",
    "S":  "STORAGE",
    "D":  "DISPOSAL",
    "W":  "WATER",
}

_WELL_STATUS_MAP: dict[str, str] = {
    "A": "ACTIVE",
    "I": "INACTIVE",
    "N": "NEW",
    "P": "P&A",
    "S": "SHUT-IN",
    "L": "LEASED",
    "C": "CANCELLED",
    "W": "ACTIVE",
}

# Minimum pass rate for validation -- below this, the batch is rejected.
_MIN_PASS_RATE = 0.95


class TxParser(BaseParser):
    """Parse TX RRC bronze DSVs (or legacy CSVs) into silver-layer Parquet.

    Reads the most recent pull-date subdirectory under ``data/bronze/tx/``
    (or a caller-specified directory), joins OG_LEASE_CYCLE with
    OG_WELL_COMPLETION and OG_OPERATOR_DW, maps columns to the canonical
    production schema, validates, deduplicates, and writes a Parquet file
    partitioned by ``state=TX``.

    Supports both the new pipe-delimited DSV format (.dsv, sep='|') and
    the legacy comma-separated CSV format (.csv, sep=',') for backward
    compatibility during migration.

    Usage::

        parser = TxParser()
        df = parser.parse()
        # or run end-to-end:
        output_path = parser.run()
    """

    def __init__(
        self,
        *,
        pull_dir: Path | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            input_dir=BRONZE_TX_DIR,
            output_dir=SILVER_DIR / "production",
            **kwargs,
        )
        self._pull_dir = pull_dir

    # ------------------------------------------------------------------
    # public interface
    # ------------------------------------------------------------------

    def parse(self) -> pd.DataFrame:
        """Read bronze CSVs, join, transform, validate, dedup.

        Returns a silver-schema DataFrame ready for Parquet output.
        """
        pull_dir = self._resolve_pull_dir()
        self.log.info("Parsing TX RRC bronze data from %s", pull_dir)

        start = time.monotonic()

        # 1. Load raw DSVs (pipe-delimited) or fall back to legacy CSVs
        lease_cycle = self._read_csv(pull_dir / "OG_LEASE_CYCLE.dsv", _LC_COLS)
        well_comp = self._read_csv(pull_dir / "OG_WELL_COMPLETION.dsv", _WC_COLS)
        operators = self._read_csv(pull_dir / "OG_OPERATOR_DW.dsv", _OP_COLS)

        self.log.info(
            "Loaded: lease_cycle=%d rows, well_completion=%d rows, operators=%d rows",
            len(lease_cycle), len(well_comp), len(operators),
        )

        # Early exit: if the primary table is empty, nothing to transform.
        if len(lease_cycle) == 0:
            self.log.warning("No lease cycle data found — returning empty DataFrame")
            from src.schemas.production import ALL_COLUMNS
            return pd.DataFrame(columns=ALL_COLUMNS)

        # Warn about empty reference tables (non-fatal but degrades quality)
        if len(well_comp) == 0:
            self.log.warning(
                "OG_WELL_COMPLETION is empty — well attributes (API, county, "
                "type, status) will be NA; all records will be entity_type='lease'"
            )
        if len(operators) == 0:
            self.log.warning(
                "OG_OPERATOR_DW is empty — operator names may be incomplete"
            )

        # 2. Build production date from CYCLE_YEAR + CYCLE_MONTH
        lease_cycle = self._build_production_date(lease_cycle)

        # 3. Map production volume columns
        lease_cycle = self._map_volumes(lease_cycle)

        # 4. Join operator names (use OG_OPERATOR_DW as fallback for
        #    OPERATOR_NAME if not already in lease_cycle).
        #    Drop all-NA OPERATOR_NAME first to avoid _x/_y suffix collisions
        #    when merging with OG_OPERATOR_DW.
        if "OPERATOR_NAME" in lease_cycle.columns and lease_cycle["OPERATOR_NAME"].isna().all():
            lease_cycle = lease_cycle.drop(columns=["OPERATOR_NAME"])
        if "OPERATOR_NAME" not in lease_cycle.columns:
            operators_dedup = operators.drop_duplicates(subset=["OPERATOR_NO"], keep="last")
            lease_cycle = lease_cycle.merge(
                operators_dedup[["OPERATOR_NO", "OPERATOR_NAME"]],
                on="OPERATOR_NO",
                how="left",
            )

        # 5. Normalise well-completion attributes at the well level
        well_attrs = self._prepare_well_attrs(well_comp)

        # 6. Build the silver DataFrame (before entity_type assignment)
        df = self._build_silver_df(lease_cycle, well_attrs, pull_dir)

        # 7. Assign entity_type using dedup.assign_tx_entity_type
        #    (well for single-well leases / gas, lease for multi-well oil)
        self.log.info("Assigning entity types ...")
        if len(well_comp) == 0:
            self.log.warning(
                "Skipping entity_type assignment — no well completion data"
            )
        else:
            # Construct API_NO if not present (new 2026 PDQ DSV format)
            if "API_NO" not in well_comp.columns:
                cc = well_comp.get("API_COUNTY_CODE", pd.Series(dtype="object")).fillna("")
                un = well_comp.get("API_UNIQUE_NO", pd.Series(dtype="object")).fillna("")
                well_comp = well_comp.copy()
                well_comp["API_NO"] = (
                    "42-" + cc.str.strip().str.zfill(3) + "-" + un.str.strip().str.zfill(5)
                )
                well_comp.loc[
                    (cc.str.strip() == "") | (un.str.strip() == ""), "API_NO"
                ] = pd.NA
            df = assign_tx_entity_type(df, well_comp)

        # 8. Validate
        self.log.info("Running validation checks ...")
        result = validate_batch(df)
        for err in result.errors:
            self.log.warning(
                "  Validation: %s on %s -- %d bad rows: %s",
                err.check_name, err.column, err.bad_row_count, err.message,
            )
        if result.pass_rate < _MIN_PASS_RATE:
            raise RuntimeError(
                f"Validation pass rate {result.pass_rate:.1%} is below "
                f"minimum threshold {_MIN_PASS_RATE:.0%}. Aborting."
            )

        # 9. Deduplicate using entity-type-aware strategy from dedup.py
        self.log.info("Deduplicating ...")
        df = deduplicate(df)

        elapsed = time.monotonic() - start
        self.log.info("Parsed %d silver rows in %.1fs", len(df), elapsed)
        return df

    def run(self) -> Path:
        """Parse, validate, dedup, and write the silver Parquet file.

        Output is written to ``data/silver/production/state=TX/tx_production.parquet``.
        Returns the path to the written file.
        """
        df = self.parse()

        # Write to a state-partitioned subdirectory.
        partition_dir = self.output_dir / "state=TX"
        partition_dir.mkdir(parents=True, exist_ok=True)

        dest = partition_dir / "tx_production.parquet"

        if df.empty:
            self.log.warning("Empty result — writing schema-only Parquet")
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = PRODUCTION_SCHEMA.empty_table()
            pq.write_table(table, dest)
            return dest

        # Temporarily override output_dir for write_parquet.
        original_output_dir = self.output_dir
        self.output_dir = partition_dir
        try:
            path = self.write_parquet(
                df, "tx_production.parquet", schema=PRODUCTION_SCHEMA,
            )
        finally:
            self.output_dir = original_output_dir

        return path

    # ------------------------------------------------------------------
    # bronze CSV loading
    # ------------------------------------------------------------------

    def _read_csv(self, path: Path, usecols: list[str]) -> pd.DataFrame:
        """Read a bronze DSV or CSV, selecting only the columns we need.

        As of 2026, RRC files are pipe-delimited (.dsv).  For backward
        compatibility, if the .dsv file is not found but a .csv file exists
        at the same location, the parser falls back to reading the CSV with
        a comma delimiter.

        RRC dumps may have inconsistent quoting and occasional encoding
        issues.  We read as strings first and coerce later.
        """
        # Determine file path and delimiter.
        # Default: .dsv files use curly-brace delimiter (RRC PDQ format)
        sep = "}"

        if not path.exists():
            # Backward compatibility: try .csv with comma delimiter
            csv_fallback = path.with_suffix(".csv")
            if csv_fallback.exists():
                self.log.warning(
                    "DSV not found (%s), falling back to CSV: %s. "
                    "This may indicate an incomplete ingestion run.",
                    path.name,
                    csv_fallback.name,
                )
                path = csv_fallback
                sep = ","
            else:
                self.log.warning(
                    "Bronze file not found: %s (also checked %s) "
                    "-- returning empty DataFrame",
                    path,
                    csv_fallback.name,
                )
                return pd.DataFrame(columns=usecols)

        # If the file has a .csv extension, use comma delimiter
        if path.suffix.lower() == ".csv":
            sep = ","

        self.log.info("Reading %s (sep=%r) ...", path.name, sep)
        target_set = {col.upper() for col in usecols}
        df = pd.read_csv(
            path,
            sep=sep,
            usecols=lambda c: c.strip().upper() in target_set,
            dtype=str,
            low_memory=True,
            encoding="latin-1",
            on_bad_lines="warn",
        )
        # Normalise column names to upper-case stripped form.
        df.columns = df.columns.str.strip().str.upper()

        # Delimiter auto-detection: if we got very few columns compared to
        # what we expected, the delimiter may be wrong.  Try alternatives.
        matched = len(set(df.columns) & target_set)
        if matched == 0 and len(df) > 0:
            # Try common RRC delimiters in order of likelihood
            alt_seps = [s for s in ("}", "|", ",") if s != sep]
            alt_sep = alt_seps[0]
            self.log.warning(
                "No target columns matched with sep=%r — retrying with sep=%r",
                sep, alt_sep,
            )
            df = pd.read_csv(
                path,
                sep=alt_sep,
                usecols=lambda c: c.strip().upper() in target_set,
                dtype=str,
                low_memory=True,
                encoding="latin-1",
                on_bad_lines="warn",
            )
            df.columns = df.columns.str.strip().str.upper()

        self.log.info("  -> %d rows, %d columns", len(df), len(df.columns))
        return df

    # ------------------------------------------------------------------
    # transform helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_production_date(df: pd.DataFrame) -> pd.DataFrame:
        """Create ``production_date`` as the 1st of the cycle month.

        Implements the ``combine_year_month`` transform from TX_FIELD_MAPPING.
        """
        year = pd.to_numeric(df["CYCLE_YEAR"], errors="coerce")
        month = pd.to_numeric(df["CYCLE_MONTH"], errors="coerce")

        date_str = (
            year.astype("Int64").astype(str)
            + "-"
            + month.astype("Int64").astype(str).str.zfill(2)
            + "-01"
        )
        df["production_date"] = pd.to_datetime(date_str, errors="coerce").dt.date
        return df

    @staticmethod
    def _map_volumes(df: pd.DataFrame) -> pd.DataFrame:
        """Rename and coerce production volume columns per TX_FIELD_MAPPING."""
        vol_map = {
            "LEASE_OIL_PROD_VOL": "oil_bbl",
            "LEASE_GAS_PROD_VOL": "gas_mcf",
            "LEASE_COND_PROD_VOL": "condensate_bbl",
            "LEASE_CSGD_PROD_VOL": "casinghead_gas_mcf",
        }
        for src, dst in vol_map.items():
            if src in df.columns:
                df[dst] = pd.to_numeric(df[src], errors="coerce")
            else:
                df[dst] = pd.NA
        return df

    def _prepare_well_attrs(self, wc: pd.DataFrame) -> pd.DataFrame:
        """Normalise well-completion attributes and aggregate to lease level.

        Returns a DataFrame keyed on (DISTRICT_NO, LEASE_NO) with columns:
        api_number, well_type, county, well_status.

        The 2026 PDQ DSV format does not include WELL_TYPE_CODE or FIELD_NAME
        in OG_WELL_COMPLETION.  Well type is inferred from OIL_GAS_CODE
        (O=OIL, G=GAS) and field_name comes from OG_LEASE_CYCLE instead.
        """
        if wc.empty:
            return pd.DataFrame(columns=[
                "DISTRICT_NO", "LEASE_NO", "api_number", "well_type",
                "county", "well_status",
            ])

        wc = wc.copy()

        # Construct API_NO from county code + unique number.
        # TX state code = 42, format: 42-NNN-NNNNN
        county_code = wc.get("API_COUNTY_CODE", pd.Series(dtype="object")).fillna("")
        unique_no = wc.get("API_UNIQUE_NO", pd.Series(dtype="object")).fillna("")
        wc["API_NO"] = (
            "42-"
            + county_code.str.strip().str.zfill(3)
            + "-"
            + unique_no.str.strip().str.zfill(5)
        )
        # Mark badly formed API numbers as NA
        wc.loc[
            (county_code.str.strip() == "") | (unique_no.str.strip() == ""),
            "API_NO",
        ] = pd.NA

        # Infer well_type from OIL_GAS_CODE (O=OIL, G=GAS).
        # The legacy WELL_TYPE_CODE column is not in the 2026 PDQ DSV format.
        ogc = wc.get("OIL_GAS_CODE", pd.Series(dtype="object")).str.strip().str.upper()
        wc["well_type"] = ogc.map({"O": "OIL", "G": "GAS"}).fillna("OTHER")

        # Normalise well_status from WELL_14B2_STATUS_CODE.
        wc["well_status"] = (
            wc.get("WELL_14B2_STATUS_CODE", pd.Series(dtype="object"))
            .str.strip()
            .str.upper()
            .map(_WELL_STATUS_MAP)
            .fillna("OTHER")
        )

        # County from COUNTY_NAME.
        wc["county"] = (
            wc.get("COUNTY_NAME", pd.Series(dtype="object"))
            .str.strip()
            .str.upper()
        )

        # Aggregate to lease level.
        def _agg_group(g: pd.DataFrame) -> pd.Series:
            apis = g["API_NO"].dropna().unique()
            api_str = ";".join(sorted(apis)) if len(apis) > 0 else None
            return pd.Series({
                "api_number": api_str,
                "well_type": _safe_mode(g["well_type"], "OTHER"),
                "county": _safe_mode(g["county"], None),
                "well_status": _safe_mode(g["well_status"], None),
            })

        self.log.info("Aggregating well attributes to lease level ...")
        agg = wc.groupby(["DISTRICT_NO", "LEASE_NO"], as_index=False).apply(
            _agg_group, include_groups=False,
        )
        return agg

    def _load_well_locations(self, pull_dir: Path) -> pd.DataFrame:
        """Load well lat/lng from ArcGIS JSON batches under ``pull_dir/wells/``.

        Returns a DataFrame with columns: api_number, latitude, longitude.
        The API numbers are normalized to NN-NNN-NNNNN format.
        """
        wells_dir = pull_dir / "wells"
        if not wells_dir.exists():
            self.log.info("No wells/ directory at %s; coordinates will be NULL", wells_dir)
            return pd.DataFrame(columns=["api_number", "latitude", "longitude"])

        batch_files = sorted(wells_dir.glob("wells_batch_*.json"))
        if not batch_files:
            self.log.info("No well batch files found; coordinates will be NULL")
            return pd.DataFrame(columns=["api_number", "latitude", "longitude"])

        rows: list[dict[str, Any]] = []
        skipped = 0
        for bf in batch_files:
            with open(bf, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            for feat in data.get("features", []):
                attrs = feat.get("attributes", {})
                raw_api = attrs.get("API", "")
                lat = attrs.get("GIS_LAT83")
                lng = attrs.get("GIS_LONG83")
                if raw_api and lat is not None and lng is not None:
                    norm = _normalize_tx_arcgis_api(str(raw_api))
                    if norm:
                        rows.append({
                            "api_number": norm,
                            "latitude": float(lat),
                            "longitude": float(lng),
                        })
                    else:
                        skipped += 1

        if skipped:
            self.log.info(
                "Skipped %d well records with invalid/short API numbers", skipped,
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            before_dedup = len(df)
            # Keep first occurrence -- duplicate APIs with different coords
            # are ambiguous (likely different wells with bad API data).
            df = df.drop_duplicates(subset="api_number", keep="first")
            dups = before_dedup - len(df)
            if dups:
                self.log.info(
                    "Dropped %d duplicate API entries (different coords, same API)",
                    dups,
                )
        self.log.info("Loaded %d well locations with coordinates", len(df))
        return df

    def _build_silver_df(
        self,
        lc: pd.DataFrame,
        well_attrs: pd.DataFrame,
        pull_dir: Path,
    ) -> pd.DataFrame:
        """Join lease-cycle data with well attributes and map to silver schema.

        The entity_type and api_number columns are placeholders here --
        they get properly set by ``assign_tx_entity_type`` afterwards.
        """
        # Join well attributes (left join: not every lease has completions).
        merged = lc.merge(
            well_attrs,
            on=["DISTRICT_NO", "LEASE_NO"],
            how="left",
        )

        now_utc = pd.Timestamp.now(tz="UTC")
        n = len(merged)

        silver = pd.DataFrame({
            "state": ["TX"] * n,
            # Placeholder -- overwritten by assign_tx_entity_type.
            "entity_type": ["lease"] * n,
            # Start as NA; assign_tx_entity_type fills in the API number
            # only for single-well leases.  Do NOT use the semicolon-joined
            # aggregate from _prepare_well_attrs here -- it would fail the
            # NN-NNN-NNNNN format validation for multi-well leases.
            "api_number": pd.array([pd.NA] * n, dtype="string"),
            "lease_number": merged["LEASE_NO"].str.strip(),
            "district": merged["DISTRICT_NO"].str.strip(),
            "well_name": (
                merged["LEASE_NAME"].str.strip().str.upper()
                if "LEASE_NAME" in merged.columns
                else pd.array([pd.NA] * n, dtype="string")
            ),
            "operator": (
                merged["OPERATOR_NAME"].str.strip().str.upper()
                if "OPERATOR_NAME" in merged.columns
                else pd.array([pd.NA] * n, dtype="string")
            ),
            "county": merged.get("county"),
            "field_name": (
                merged["FIELD_NAME"].str.strip().str.upper()
                if "FIELD_NAME" in merged.columns
                else pd.array([pd.NA] * n, dtype="string")
            ),
            "basin": pd.array([pd.NA] * n, dtype="string"),   # Derived in gold layer.
            "well_type": merged.get("well_type"),
            "well_status": merged.get("well_status"),
            "production_date": merged["production_date"],
            "oil_bbl": merged["oil_bbl"],
            "gas_mcf": merged["gas_mcf"],
            "condensate_bbl": merged["condensate_bbl"],
            "casinghead_gas_mcf": merged["casinghead_gas_mcf"],
            "water_bbl": pd.array([pd.NA] * n, dtype="Float64"),
            "days_produced": pd.array([pd.NA] * n, dtype="Int32"),
            "source_file": [pull_dir.name + "/OG_LEASE_CYCLE.dsv"] * n,
            "ingested_at": [now_utc] * n,
        })

        # -- Enrich with well location coordinates --
        well_locs = self._load_well_locations(pull_dir)
        if not well_locs.empty and "api_number" in merged.columns:
            # Build a lookup from the semi-colon joined api_number in well_attrs.
            # For single-well leases, api_number has one API; for multi-well
            # leases it has semi-colon-joined APIs.  We extract the first API
            # to look up coordinates (representative location for the lease).
            first_api = (
                merged.get("api_number", pd.Series(dtype="string"))
                .str.split(";")
                .str[0]
            )
            loc_lookup = well_locs.set_index("api_number")
            silver["latitude"] = first_api.map(loc_lookup["latitude"]).values
            silver["longitude"] = first_api.map(loc_lookup["longitude"]).values
            n_with_coords = silver["latitude"].notna().sum()
            self.log.info(
                "Enriched %d / %d rows with coordinates from well locations",
                n_with_coords, n,
            )
        else:
            silver["latitude"] = pd.array([pd.NA] * n, dtype="Float64")
            silver["longitude"] = pd.array([pd.NA] * n, dtype="Float64")

        return silver

    # ------------------------------------------------------------------
    # pull-directory resolution
    # ------------------------------------------------------------------

    def _resolve_pull_dir(self) -> Path:
        """Return the pull-date directory to read from.

        If one was provided at init, use it.  Otherwise, find the most
        recent date-stamped subdirectory under ``BRONZE_TX_DIR``.
        """
        if self._pull_dir is not None:
            if not self._pull_dir.is_dir():
                raise FileNotFoundError(
                    f"Specified pull directory does not exist: {self._pull_dir}"
                )
            return self._pull_dir

        subdirs = sorted(
            (d for d in self.input_dir.iterdir() if d.is_dir()),
            reverse=True,
        )
        if not subdirs:
            raise FileNotFoundError(
                f"No pull-date subdirectories found in {self.input_dir}. "
                "Run the TX RRC ingester first (WO-004)."
            )

        chosen = subdirs[0]
        self.log.info("Auto-selected most recent pull directory: %s", chosen.name)
        return chosen


# ------------------------------------------------------------------
# utilities
# ------------------------------------------------------------------


def _safe_mode(series: pd.Series, default):
    """Return the mode of a Series, or *default* if empty."""
    m = series.mode()
    return m.iloc[0] if not m.empty else default


def _normalize_tx_arcgis_api(raw: str) -> str | None:
    """Normalize a TX RRC ArcGIS API number to NN-NNN-NNNNN format.

    The ArcGIS ``API`` field stores API numbers as digit strings of varying
    length.  The most common format is **8 digits** in RRC-internal notation
    (CCC-WWWWW -- county + well, *without* the ``42`` state prefix) because
    the ArcGIS layer is TX-only.  Some records carry the full 10-digit API
    (42-CCC-WWWWW).  Short junk values (< 8 digits after stripping
    non-digits) are rejected.

    Format: SS-CCC-WWWWW where SS=state (42 for TX), CCC=county, WWWWW=well.
    """
    digits = re.sub(r"\D", "", raw.strip())
    if not digits or len(digits) < 8:
        # Reject anything shorter than 8 digits -- too ambiguous / junk.
        return None

    if len(digits) == 10 and digits.startswith("42"):
        # Full 10-digit API already includes state prefix: SSCCCWWWWW
        return f"{digits[:2]}-{digits[2:5]}-{digits[5:10]}"
    if len(digits) == 8:
        # RRC-internal 8-digit format (CCCWWWWW) -- prepend TX state code.
        return f"42-{digits[:3]}-{digits[3:8]}"

    return None


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------


def main() -> None:
    """Run the TX RRC bronze-to-silver transform from the command line."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("=== TX RRC Silver Transform ===")
    start = time.monotonic()

    parser = TxParser()
    output = parser.run()

    elapsed = time.monotonic() - start
    logger.info("Done. Output: %s  (%.1fs elapsed)", output, elapsed)


if __name__ == "__main__":
    main()
