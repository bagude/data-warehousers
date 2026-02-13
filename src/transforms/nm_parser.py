"""New Mexico OCD production data parser -- bronze to silver.

Reads two bronze data sources and joins them:
  1. ArcGIS well master JSON  -> well attributes (operator, county, type, status)
  2. OCD FTP XML files         -> monthly production records (oil, gas, water, days)

The OCD FTP data comes as SQL Server FOR XML exports (UTF-16 LE) inside ZIPs:
  - wcproduction.xml: per-well-completion monthly production, one row per
    product kind (O=Oil/BBL, G=Gas/MCF, W=Water/BBL) per month
  - ogrid.xml: operator (OGRID) reference table
  - pool.xml: pool/field name reference table
  - wchistory.xml: well-completion history, status, and well type

The parser pivots the per-product-kind rows into a single row per well per
month with oil_bbl, gas_mcf, water_bbl columns, then joins with the reference
tables and well master to produce silver-layer records.

Also supports legacy GO-TECH CSV files for backward compatibility.

The joined result is mapped to the common silver-layer PRODUCTION_SCHEMA and
written as a Parquet file to data/silver/production/.
"""

from __future__ import annotations

import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from src.schemas.dedup import deduplicate
from src.schemas.production import PRODUCTION_SCHEMA
from src.schemas.validation import validate_batch, get_valid_rows
from src.transforms.base import BaseParser
from src.utils.config import BRONZE_NM_DIR, SILVER_DIR

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# County code -> county name lookup (NM FIPS county codes)
# ---------------------------------------------------------------------------

_FIPS_COUNTY_CODE: dict[int, str] = {
    1: "Bernalillo", 3: "Catron", 5: "Chaves", 6: "Cibola",
    7: "Colfax", 9: "Curry", 11: "De Baca", 13: "Dona Ana",
    15: "Eddy", 17: "Grant", 19: "Guadalupe", 21: "Harding",
    23: "Hidalgo", 25: "Lea", 27: "Lincoln", 28: "Los Alamos",
    29: "Luna", 31: "McKinley", 33: "Mora", 35: "Otero",
    37: "Quay", 39: "Rio Arriba", 41: "Roosevelt",
    43: "Sandoval", 45: "San Juan", 47: "San Miguel",
    49: "Santa Fe", 51: "Sierra", 53: "Socorro",
    55: "Taos", 57: "Torrance", 59: "Union", 61: "Valencia",
}
# NOTE: The API county codes used by OCD (api_cnty_cde) are the FIPS county
# code divided by 2 plus 1, i.e. the "odd" FIPS numbering scheme.  For
# example, Eddy County is FIPS 015 -> api_cnty_cde = 15.

# ---------------------------------------------------------------------------
# County -> basin lookup (NM)
# ---------------------------------------------------------------------------

_COUNTY_TO_BASIN: dict[str, str] = {
    "Chaves": "Permian",
    "Eddy": "Permian",
    "Lea": "Permian",
    "Roosevelt": "Permian",
    "Otero": "Permian",
    "Lincoln": "Permian",
    "San Juan": "San Juan",
    "Rio Arriba": "San Juan",
    "Sandoval": "San Juan",
    "McKinley": "San Juan",
    "Colfax": "Raton",
    "Mora": "Raton",
    "Union": "Raton",
    "Harding": "Raton",
    "Hidalgo": "Pedregosa",
    "Santa Fe": "Estancia",
}

# ---------------------------------------------------------------------------
# Well-type and well-status normalization
# ---------------------------------------------------------------------------

_WELL_TYPE_MAP: dict[str, str] = {
    "OIL": "OIL",
    "OW": "OIL",
    "GAS": "GAS",
    "GW": "GAS",
    "OG": "OIL",
    "GO": "GAS",
    "INJ": "INJ",
    "IW": "INJ",
    "WI": "INJ",
    "SWD": "INJ",
    "WD": "INJ",
    "WS": "INJ",
}

# OCD FTP wchistory.wc_stat_cde single-character codes
_WC_STAT_MAP: dict[str, str] = {
    "A": "ACTIVE",
    "C": "INACTIVE",   # Cancelled
    "D": "P&A",        # Dry Hole
    "N": "ACTIVE",     # New, Not Drilled
    "X": "INACTIVE",   # Never Drilled
    "T": "SHUT-IN",    # Temporary Abandonment
    "P": "P&A",        # Zone Permanently Plugged
    "Z": "SHUT-IN",    # Zones Temporarily Plugged
}

_WELL_STATUS_MAP: dict[str, str] = {
    "ACTIVE": "ACTIVE",
    "ACT": "ACTIVE",
    "NEW": "ACTIVE",
    "PRODUCING": "ACTIVE",
    "PROD": "ACTIVE",
    "SHUT-IN": "SHUT-IN",
    "SI": "SHUT-IN",
    "SHUT IN": "SHUT-IN",
    "TA": "SHUT-IN",
    "P&A": "P&A",
    "PA": "P&A",
    "PLUGGED": "P&A",
    "ABANDONED": "P&A",
    "ABD": "P&A",
    "INACTIVE": "INACTIVE",
    "INACT": "INACTIVE",
    "CANCELLED": "INACTIVE",
}


def _normalize_api(raw: Any) -> str | None:
    """Normalize an API number to NN-NNN-NNNNN format (state code 30 for NM).

    Only handles exactly 8-digit (CCC-WWWWW, missing state prefix) or
    10-digit (SS-CCC-WWWWW) inputs.  Other lengths are ambiguous and
    return None so validation catches them downstream.
    """
    if pd.isna(raw):
        return None
    s = str(raw).strip()
    if not s:
        return None

    # Already formatted
    if re.match(r"^\d{2}-\d{3}-\d{5}$", s):
        return s

    # Strip dashes/spaces and try to parse digits
    digits = re.sub(r"[\s\-]", "", s)

    if len(digits) == 10:
        # Full 10-digit: SS-CCC-WWWWW
        return f"{digits[:2]}-{digits[2:5]}-{digits[5:10]}"
    if len(digits) == 8:
        # Missing state code, assume NM (30)
        return f"30-{digits[:3]}-{digits[3:8]}"

    # Any other length is ambiguous -- return None and let validation flag it
    logger.debug("Could not normalize API number (unexpected length %d): %r", len(digits), raw)
    return None


def _normalize_api_from_parts(
    api_st_cde: Any, api_cnty_cde: Any, api_well_idn: Any
) -> str | None:
    """Build a normalized API number from OCD FTP component fields.

    OCD FTP data stores API as three separate integer fields:
      - api_st_cde: state code (30 for NM)
      - api_cnty_cde: county code (3 digits)
      - api_well_idn: well identifier (5 digits)

    Returns NN-NNN-NNNNN format or None if any part is missing.
    """
    try:
        st = int(float(str(api_st_cde).strip()))
        cnty = int(float(str(api_cnty_cde).strip()))
        well = int(float(str(api_well_idn).strip()))
    except (ValueError, TypeError):
        return None

    # Validate ranges to ensure NN-NNN-NNNNN format
    if not (0 <= st <= 99 and 0 <= cnty <= 999 and 0 <= well <= 99999):
        return None

    return f"{st:02d}-{cnty:03d}-{well:05d}"


def _normalize_well_type(raw: Any) -> str | None:
    """Map raw well type codes to standard values."""
    if pd.isna(raw):
        return None
    key = str(raw).strip().upper()
    return _WELL_TYPE_MAP.get(key, "OTHER")


def _normalize_well_status(raw: Any) -> str | None:
    """Map raw well status codes to standard values."""
    if pd.isna(raw):
        return None
    key = str(raw).strip().upper()
    # Try the single-char OCD FTP codes first, then the longer form
    if key in _WC_STAT_MAP:
        return _WC_STAT_MAP[key]
    return _WELL_STATUS_MAP.get(key, "OTHER")


def _derive_basin(county: Any) -> str | None:
    """Look up basin from county name."""
    if pd.isna(county):
        return None
    return _COUNTY_TO_BASIN.get(str(county).strip(), None)


# Mapping from GO-TECH directory/file stem patterns to canonical county names.
# GO-TECH ZIPs extract to directories like "allwells_SanJuan", "allwells_RioArriba",
# "allwells_Lea", etc.
_DIR_TO_COUNTY: dict[str, str] = {}
for _county in _COUNTY_TO_BASIN:
    # "San Juan" -> "sanjuan", "Rio Arriba" -> "rioarriba"
    _key = _county.replace(" ", "").lower()
    _DIR_TO_COUNTY[_key] = _county
    # Also map with allwells_ prefix stripped
    _DIR_TO_COUNTY[f"allwells_{_key}"] = _county


def _normalize_county_from_dir(dirname: str) -> str | None:
    """Convert a GO-TECH extraction directory name to a canonical county name.

    Examples:
        "allwells_SanJuan"  -> "San Juan"
        "allwells_Lea"      -> "Lea"
        "allwells_RioArriba" -> "Rio Arriba"
        "Eddy"              -> "Eddy"
    """
    if not dirname:
        return None
    key = dirname.strip().lower()
    if key in _DIR_TO_COUNTY:
        return _DIR_TO_COUNTY[key]
    # Try stripping common prefix
    for prefix in ("allwells_", "allwells-"):
        if key.startswith(prefix):
            stripped = key[len(prefix):]
            if stripped in _DIR_TO_COUNTY:
                return _DIR_TO_COUNTY[stripped]
    # Try splitting camelCase: insert space before uppercase letters
    spaced = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", dirname.strip())
    if spaced in _COUNTY_TO_BASIN:
        return spaced
    return dirname.strip() or None


# ---------------------------------------------------------------------------
# OCD FTP XML parsing helpers
# ---------------------------------------------------------------------------

def _detect_xml_encoding(xml_path: Path) -> str:
    """Detect encoding from BOM of an OCD FTP XML file."""
    with open(xml_path, "rb") as f:
        bom = f.read(4)
    if bom[:2] == b"\xff\xfe":
        return "utf-16-le"
    if bom[:2] == b"\xfe\xff":
        return "utf-16-be"
    if bom[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"
    return "utf-8"


def _iter_ocd_xml(
    xml_path: Path, tag_name: str, log=None,
) -> Iterator[dict[str, str]]:
    """Stream-parse an OCD FTP XML file, yielding one dict per record.

    Uses ``ET.iterparse`` so that memory usage stays constant regardless
    of file size (the wcproduction XML can be 40+ GB).

    Yields
    ------
    dict mapping child element names to their text content for each
    ``<tag_name>`` element in the file.
    """
    _log = log or logger
    encoding = _detect_xml_encoding(xml_path)
    _log.info(
        "Streaming OCD XML: %s (tag=%s, encoding=%s)",
        xml_path.name, tag_name, encoding,
    )

    count = 0
    try:
        with open(xml_path, "r", encoding=encoding, errors="replace") as fh:
            context = ET.iterparse(fh, events=("end",))
            for _event, elem in context:
                local_tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

                if local_tag == tag_name:
                    row: dict[str, str] = {}
                    for child in elem:
                        child_tag = (
                            child.tag.split("}")[-1]
                            if "}" in child.tag
                            else child.tag
                        )
                        text = child.text
                        if text is not None:
                            row[child_tag] = text.strip()
                    if row:
                        yield row
                        count += 1
                        if count % 1_000_000 == 0:
                            _log.info(
                                "  ... streamed %d %s records", count, tag_name,
                            )

                    # Clear processed record to free memory. Only clear
                    # record-level elements -- clearing child field elements
                    # prematurely destroys their text before the parent
                    # record's end event can read it.
                    elem.clear()

    except ET.ParseError as exc:
        _log.error("XML parse error in %s after %d records: %s",
                   xml_path.name, count, exc)

    _log.info("Streamed %d %s records from %s", count, tag_name, xml_path.name)


def _parse_ocd_xml(xml_path: Path, tag_name: str) -> list[dict[str, str]]:
    """Parse a *small* OCD FTP XML file into a list of dicts.

    Only suitable for reference tables (ogrid, pool, wchistory) that fit
    in memory.  For wcproduction, use ``_iter_ocd_xml`` with chunked
    processing instead.
    """
    return list(_iter_ocd_xml(xml_path, tag_name))


def _pivot_wcproduction(rows: list[dict[str, str]]) -> pd.DataFrame:
    """Pivot wcproduction rows from per-product-kind to per-well-per-month.

    Input rows have one entry per (well, pool, month, product_kind), where
    prd_knd_cde is O/G/W and prod_amt is the volume.

    Output has one row per (api, pool, month) with separate oil_bbl, gas_mcf,
    water_bbl columns, plus days_produced from prodn_day_num.

    The API number is constructed from api_st_cde + api_cnty_cde + api_well_idn.
    """
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Build the composite API number
    df["api_number"] = df.apply(
        lambda r: _normalize_api_from_parts(
            r.get("api_st_cde", ""),
            r.get("api_cnty_cde", ""),
            r.get("api_well_idn", ""),
        ),
        axis=1,
    )

    # Coerce numeric columns
    df["prod_amt"] = pd.to_numeric(df.get("prod_amt"), errors="coerce")
    df["prodn_day_num"] = pd.to_numeric(df.get("prodn_day_num"), errors="coerce")
    df["prodn_yr"] = pd.to_numeric(df.get("prodn_yr"), errors="coerce")
    df["prodn_mth"] = pd.to_numeric(df.get("prodn_mth"), errors="coerce")
    df["pool_idn"] = df.get("pool_idn", pd.Series(dtype=str))
    df["ogrid_cde"] = df.get("ogrid_cde", pd.Series(dtype=str))
    df["prd_knd_cde"] = df.get("prd_knd_cde", pd.Series(dtype=str)).str.strip().str.upper()
    df["api_cnty_cde"] = pd.to_numeric(df.get("api_cnty_cde"), errors="coerce")

    # Group by (api_number, pool_idn, prodn_yr, prodn_mth) and pivot product kinds
    group_cols = ["api_number", "pool_idn", "prodn_yr", "prodn_mth", "ogrid_cde", "api_cnty_cde"]

    # Separate by product kind
    oil_df = df[df["prd_knd_cde"] == "O"][group_cols + ["prod_amt", "prodn_day_num"]].copy()
    oil_df = oil_df.rename(columns={"prod_amt": "oil_bbl", "prodn_day_num": "days_produced_oil"})

    gas_df = df[df["prd_knd_cde"] == "G"][group_cols + ["prod_amt", "prodn_day_num"]].copy()
    gas_df = gas_df.rename(columns={"prod_amt": "gas_mcf", "prodn_day_num": "days_produced_gas"})

    water_df = df[df["prd_knd_cde"] == "W"][group_cols + ["prod_amt", "prodn_day_num"]].copy()
    water_df = water_df.rename(columns={"prod_amt": "water_bbl", "prodn_day_num": "days_produced_water"})

    # Aggregate: keep last value for each group (amendments are corrections,
    # not additive).  Use max for days_produced as a conservative estimate.
    agg_oil = {"oil_bbl": "last", "days_produced_oil": "max"}
    agg_gas = {"gas_mcf": "last", "days_produced_gas": "max"}
    agg_water = {"water_bbl": "last", "days_produced_water": "max"}

    if not oil_df.empty:
        oil_df = oil_df.groupby(group_cols, as_index=False, dropna=False).agg(agg_oil)
    if not gas_df.empty:
        gas_df = gas_df.groupby(group_cols, as_index=False, dropna=False).agg(agg_gas)
    if not water_df.empty:
        water_df = water_df.groupby(group_cols, as_index=False, dropna=False).agg(agg_water)

    # Merge all three product kinds on group keys
    # Start with the union of all group key combinations
    all_keys = pd.concat(
        [d[group_cols] for d in [oil_df, gas_df, water_df] if not d.empty],
        ignore_index=True,
    ).drop_duplicates()

    if all_keys.empty:
        return pd.DataFrame()

    result = all_keys
    if not oil_df.empty:
        result = result.merge(oil_df, on=group_cols, how="left")
    else:
        result["oil_bbl"] = None
        result["days_produced_oil"] = None

    if not gas_df.empty:
        result = result.merge(gas_df, on=group_cols, how="left")
    else:
        result["gas_mcf"] = None
        result["days_produced_gas"] = None

    if not water_df.empty:
        result = result.merge(water_df, on=group_cols, how="left")
    else:
        result["water_bbl"] = None
        result["days_produced_water"] = None

    # Take the max of the three days columns as the canonical days_produced
    days_cols = ["days_produced_oil", "days_produced_gas", "days_produced_water"]
    existing_days = [c for c in days_cols if c in result.columns]
    if existing_days:
        result["days_produced"] = result[existing_days].max(axis=1)
        result = result.drop(columns=existing_days)
    else:
        result["days_produced"] = None

    logger.info(
        "Pivoted wcproduction: %d rows (from %d raw product-kind rows)",
        len(result), len(df),
    )
    return result


class NmParser(BaseParser):
    """Parser for raw New Mexico OCD production data.

    Reads from ``data/bronze/nm/{pull_date}/`` and writes a single
    Parquet file to ``data/silver/production/nm_production.parquet``.

    Supports two bronze data formats:
      - OCD FTP XML files (wcproduction.xml + reference tables)
      - Legacy GO-TECH county CSV files (backward compatible)
    """

    def __init__(self, *, pull_date: str | None = None, **kwargs) -> None:
        super().__init__(
            input_dir=BRONZE_NM_DIR,
            output_dir=SILVER_DIR / "production" / "state=NM",
            **kwargs,
        )
        self.pull_date = pull_date or self._find_latest_pull_date()
        self.run_dir = self.input_dir / self.pull_date

    def _find_latest_pull_date(self) -> str:
        """Find the most recent pull_date directory under bronze/nm/."""
        candidates = sorted(
            (d for d in self.input_dir.iterdir() if d.is_dir()),
            reverse=True,
        )
        if not candidates:
            raise FileNotFoundError(
                f"No pull_date directories found under {self.input_dir}"
            )
        chosen = candidates[0].name
        self.log.info("Auto-selected latest pull_date: %s", chosen)
        return chosen

    # ------------------------------------------------------------------
    # Detect data source format
    # ------------------------------------------------------------------

    def _detect_source_format(self) -> str:
        """Detect whether the bronze data is OCD FTP XML or legacy GO-TECH CSV.

        Returns "ocd_ftp" if wcproduction.xml is found, otherwise "gotech_csv".
        """
        prod_dir = self.run_dir / "production"
        if not prod_dir.exists():
            return "gotech_csv"

        # Check for OCD FTP XML files
        wcprod_xml = prod_dir / "wcproduction" / "wcproduction.xml"
        if wcprod_xml.exists():
            return "ocd_ftp"

        # Also check for XML files at any nesting level
        xml_files = list(prod_dir.glob("**/wcproduction.xml"))
        if xml_files:
            return "ocd_ftp"

        return "gotech_csv"

    # ------------------------------------------------------------------
    # Read bronze data
    # ------------------------------------------------------------------

    def _load_wells_master(self) -> pd.DataFrame:
        """Load well master data from ArcGIS JSON batches.

        Returns a DataFrame with one row per well, columns from the
        ArcGIS ``attributes`` dicts.
        """
        wells_dir = self.run_dir / "wells"
        if not wells_dir.exists():
            self.log.warning("No wells directory at %s", wells_dir)
            return pd.DataFrame()

        batch_files = sorted(wells_dir.glob("wells_batch_*.json"))
        if not batch_files:
            self.log.warning("No well batch JSON files found in %s", wells_dir)
            return pd.DataFrame()

        all_rows: list[dict[str, Any]] = []
        for bf in batch_files:
            with open(bf, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            features = data.get("features", [])
            for feat in features:
                attrs = feat.get("attributes", {})
                all_rows.append(attrs)

        df = pd.DataFrame(all_rows)
        self.log.info(
            "Loaded %d well records from %d batch files", len(df), len(batch_files)
        )
        return df

    def _load_production_data(self) -> pd.DataFrame:
        """Load production data from extracted bronze files.

        Auto-detects the source format (OCD FTP XML or legacy GO-TECH CSV)
        and dispatches to the appropriate loader.

        Returns a combined DataFrame with standardized columns.
        """
        source_format = self._detect_source_format()
        self.log.info("Detected production data format: %s", source_format)

        if source_format == "ocd_ftp":
            return self._load_ocd_ftp_production()
        else:
            return self._load_gotech_csv_production()

    # ------------------------------------------------------------------
    # OCD FTP XML loader
    # ------------------------------------------------------------------

    def _load_ocd_ftp_production(self) -> pd.DataFrame:
        """Load production data from OCD FTP XML files.

        Reads wcproduction.xml, pivots per-product-kind rows into per-well
        per-month rows, and joins with reference tables (ogrid, pool,
        wchistory) for operator names, pool names, and well status.

        Returns a DataFrame with columns compatible with the downstream
        _build_production_records method.
        """
        prod_dir = self.run_dir / "production"

        # -- Load wcproduction.xml --
        wcprod_xml = prod_dir / "wcproduction" / "wcproduction.xml"
        if not wcprod_xml.exists():
            # Try other paths
            candidates = list(prod_dir.glob("**/wcproduction.xml"))
            if not candidates:
                self.log.warning("No wcproduction.xml found in %s", prod_dir)
                return pd.DataFrame()
            wcprod_xml = candidates[0]

        # Stream-parse the large wcproduction XML in chunks to avoid
        # loading 40+ GB of Python dicts into memory at once.
        _CHUNK_SIZE = 1_000_000
        pivoted_chunks: list[pd.DataFrame] = []
        batch: list[dict[str, str]] = []
        total_rows = 0

        for row in _iter_ocd_xml(wcprod_xml, "wcproduction", log=self.log):
            batch.append(row)
            if len(batch) >= _CHUNK_SIZE:
                total_rows += len(batch)
                self.log.info(
                    "  Pivoting chunk (%d rows, %d total so far)",
                    len(batch), total_rows,
                )
                chunk_df = _pivot_wcproduction(batch)
                if not chunk_df.empty:
                    pivoted_chunks.append(chunk_df)
                batch = []

        # Process remaining rows
        if batch:
            total_rows += len(batch)
            self.log.info(
                "  Pivoting final chunk (%d rows, %d total)", len(batch), total_rows,
            )
            chunk_df = _pivot_wcproduction(batch)
            if not chunk_df.empty:
                pivoted_chunks.append(chunk_df)
            batch = []  # free memory

        if total_rows == 0:
            file_size = wcprod_xml.stat().st_size
            if file_size > 1024:
                raise RuntimeError(
                    f"Failed to parse non-empty XML file {wcprod_xml} "
                    f"({file_size:,} bytes). Possible format change or corruption."
                )
            self.log.warning("No production records parsed from %s", wcprod_xml)
            return pd.DataFrame()

        if not pivoted_chunks:
            return pd.DataFrame()

        self.log.info(
            "Concatenating %d pivoted chunks (%d raw rows)",
            len(pivoted_chunks), total_rows,
        )
        prod_df = pd.concat(pivoted_chunks, ignore_index=True)
        del pivoted_chunks  # free memory

        # Final dedup: groups may have been split across chunks, so
        # re-aggregate with the same logic used in _pivot_wcproduction.
        group_cols = [
            "api_number", "pool_idn", "prodn_yr", "prodn_mth",
            "ogrid_cde", "api_cnty_cde",
        ]
        agg_map = {
            "oil_bbl": "last",
            "gas_mcf": "last",
            "water_bbl": "last",
            "days_produced": "max",
        }
        existing_agg = {k: v for k, v in agg_map.items() if k in prod_df.columns}
        if existing_agg:
            prod_df = prod_df.groupby(
                group_cols, as_index=False, dropna=False,
            ).agg(existing_agg)

        self.log.info("After dedup: %d production rows", len(prod_df))

        if prod_df.empty:
            return pd.DataFrame()

        # -- Load reference tables --
        ogrid_df = self._load_ocd_reference(prod_dir, "ogrid", "ogrid")
        pool_df = self._load_ocd_reference(prod_dir, "pool", "pool")
        wchistory_df = self._load_ocd_reference(prod_dir, "wchistory", "wchistory")

        # -- Join operator name from ogrid --
        if not ogrid_df.empty and "ogrid_cde" in prod_df.columns:
            ogrid_lookup = ogrid_df[["ogrid_cde", "ogrid_nam"]].copy()
            ogrid_lookup = ogrid_lookup.drop_duplicates(subset="ogrid_cde", keep="last")
            prod_df = prod_df.merge(ogrid_lookup, on="ogrid_cde", how="left")
            prod_df = prod_df.rename(columns={"ogrid_nam": "operator"})
            if "operator" in prod_df.columns:
                prod_df["operator"] = prod_df["operator"].str.strip()
        else:
            prod_df["operator"] = None

        # -- Join pool name from pool --
        if not pool_df.empty and "pool_idn" in prod_df.columns:
            pool_lookup = pool_df[["pool_idn", "pool_nam"]].copy()
            pool_lookup = pool_lookup.drop_duplicates(subset="pool_idn", keep="last")
            prod_df = prod_df.merge(pool_lookup, on="pool_idn", how="left")
            prod_df = prod_df.rename(columns={"pool_nam": "pool_name"})
            if "pool_name" in prod_df.columns:
                prod_df["pool_name"] = prod_df["pool_name"].str.strip()
        else:
            prod_df["pool_name"] = None

        # -- Join well status and type from wchistory --
        if not wchistory_df.empty:
            wch = wchistory_df.copy()
            # Build API for join
            wch["api_number"] = wch.apply(
                lambda r: _normalize_api_from_parts(
                    r.get("api_st_cde", ""),
                    r.get("api_cnty_cde", ""),
                    r.get("api_well_idn", ""),
                ),
                axis=1,
            )
            # Keep latest record per (api, pool)
            wch = wch.sort_values("eff_dte", na_position="first")
            wch_cols = ["api_number", "pool_idn"]
            keep_cols = ["api_number", "pool_idn"]
            if "wc_stat_cde" in wch.columns:
                keep_cols.append("wc_stat_cde")
            if "well_typ_cde" in wch.columns:
                keep_cols.append("well_typ_cde")
            if "well_nbr_idn" in wch.columns:
                keep_cols.append("well_nbr_idn")
            wch_lookup = wch[keep_cols].drop_duplicates(
                subset=wch_cols, keep="last"
            )
            prod_df = prod_df.merge(
                wch_lookup, on=["api_number", "pool_idn"], how="left"
            )
        else:
            prod_df["wc_stat_cde"] = None
            prod_df["well_typ_cde"] = None
            prod_df["well_nbr_idn"] = None

        # -- Derive county from api_cnty_cde --
        if "api_cnty_cde" in prod_df.columns:
            prod_df["_source_county"] = prod_df["api_cnty_cde"].apply(
                lambda c: _FIPS_COUNTY_CODE.get(int(float(c)), None)
                if pd.notna(c) else None
            )
        else:
            prod_df["_source_county"] = None

        # Tag source
        prod_df["_source_file"] = "wcproduction.xml"
        prod_df["_source_format"] = "ocd_ftp"

        self.log.info(
            "Loaded %d production rows from OCD FTP XML", len(prod_df)
        )
        return prod_df

    def _load_ocd_reference(
        self, prod_dir: Path, subdir: str, tag_name: str
    ) -> pd.DataFrame:
        """Load an OCD FTP reference XML file into a DataFrame.

        Parameters
        ----------
        prod_dir:
            The production directory.
        subdir:
            Subdirectory name within prod_dir (e.g. "ogrid").
        tag_name:
            The repeating XML element name.

        Returns
        -------
        DataFrame of reference records, or empty DataFrame if not found.
        """
        xml_path = prod_dir / subdir / f"{subdir}.xml"
        if not xml_path.exists():
            candidates = list(prod_dir.glob(f"**/{subdir}.xml"))
            if not candidates:
                self.log.info("Reference file %s.xml not found; skipping", subdir)
                return pd.DataFrame()
            xml_path = candidates[0]

        rows = _parse_ocd_xml(xml_path, tag_name)
        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Legacy GO-TECH CSV loader (backward compatible)
    # ------------------------------------------------------------------

    def _load_gotech_csv_production(self) -> pd.DataFrame:
        """Load production data from extracted GO-TECH county CSV files.

        GO-TECH county files are typically CSVs with columns like:
        API, Well Name, Operator, Year, Oil, Gas, Water, Days, etc.

        Returns a combined DataFrame across all counties.
        """
        prod_dir = self.run_dir / "production"
        if not prod_dir.exists():
            self.log.warning("No production directory at %s", prod_dir)
            return pd.DataFrame()

        # Look for CSV/TXT files in extracted subdirectories
        csv_files: list[Path] = []
        for pattern in ("**/*.csv", "**/*.txt", "**/*.CSV", "**/*.TXT"):
            csv_files.extend(prod_dir.glob(pattern))

        # Exclude the ZIP files themselves
        csv_files = [f for f in csv_files if not f.suffix.lower() == ".zip"]

        if not csv_files:
            self.log.warning("No CSV/TXT files found in %s", prod_dir)
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        for csv_path in sorted(csv_files):
            try:
                df = self._read_gotech_csv(csv_path)
                if not df.empty:
                    # Tag with source file for lineage
                    df["_source_file"] = csv_path.name
                    # Derive county from parent directory name, normalizing
                    # GO-TECH names like "allwells_SanJuan" -> "San Juan"
                    df["_source_county"] = _normalize_county_from_dir(
                        csv_path.parent.name
                    )
                    df["_source_format"] = "gotech_csv"
                    frames.append(df)
            except Exception:
                self.log.exception("Failed to read %s; skipping", csv_path)

        if not frames:
            self.log.warning("No production data successfully read")
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        self.log.info(
            "Loaded %d production rows from %d files", len(combined), len(frames)
        )
        return combined

    def _read_gotech_csv(self, path: Path) -> pd.DataFrame:
        """Read a single GO-TECH county CSV with flexible parsing.

        GO-TECH files can vary in delimiter and column naming.  This
        method tries common delimiters and normalizes column names.
        """
        # Try comma first, then tab
        for sep in (",", "\t"):
            try:
                df = pd.read_csv(
                    path,
                    sep=sep,
                    dtype=str,
                    encoding="utf-8",
                    on_bad_lines="warn",
                )
                # Need at least a few columns to be valid
                if len(df.columns) >= 3:
                    break
            except Exception:
                continue
        else:
            # Try latin-1 encoding as fallback
            try:
                df = pd.read_csv(
                    path,
                    dtype=str,
                    encoding="latin-1",
                    on_bad_lines="warn",
                )
            except Exception:
                self.log.warning("Could not parse %s with any strategy", path)
                return pd.DataFrame()

        # Normalize column names: strip whitespace, lowercase
        df.columns = [c.strip() for c in df.columns]

        return df

    # ------------------------------------------------------------------
    # Transform: map bronze columns -> silver schema
    # ------------------------------------------------------------------

    def _build_production_records(
        self,
        prod_df: pd.DataFrame,
        wells_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Join production data with well master and map to common schema.

        Uses vectorized pandas operations throughout -- no iterrows().

        Handles both OCD FTP XML format (pre-pivoted with api_number,
        oil_bbl, gas_mcf, water_bbl, days_produced columns) and legacy
        GO-TECH CSV format (raw columns needing resolution).

        Parameters
        ----------
        prod_df:
            Raw production data from OCD FTP XML or GO-TECH CSVs.
        wells_df:
            Well master data from ArcGIS JSON.

        Returns
        -------
        DataFrame conforming to PRODUCTION_SCHEMA columns.
        """
        now_utc = datetime.now(timezone.utc)

        source_format = (
            prod_df["_source_format"].iloc[0]
            if "_source_format" in prod_df.columns and len(prod_df) > 0
            else "gotech_csv"
        )

        if source_format == "ocd_ftp":
            return self._build_ocd_ftp_records(prod_df, wells_df, now_utc)
        else:
            return self._build_gotech_records(prod_df, wells_df, now_utc)

    def _build_ocd_ftp_records(
        self,
        prod_df: pd.DataFrame,
        wells_df: pd.DataFrame,
        now_utc: datetime,
    ) -> pd.DataFrame:
        """Build silver records from OCD FTP pre-pivoted production data.

        The OCD FTP data already has api_number, oil_bbl, gas_mcf, water_bbl,
        days_produced, operator, pool_name, and well status/type from the
        XML parsing and reference table joins.
        """
        n = len(prod_df)
        df = pd.DataFrame(index=range(n))

        # Constants
        df["state"] = "NM"
        df["entity_type"] = "well"
        df["lease_number"] = None
        df["district"] = None
        df["condensate_bbl"] = None
        df["casinghead_gas_mcf"] = None
        df["ingested_at"] = now_utc

        # API number (already normalized)
        df["api_number"] = prod_df["api_number"].values

        # Well name from wchistory well_nbr_idn if available
        if "well_nbr_idn" in prod_df.columns:
            df["well_name"] = prod_df["well_nbr_idn"].values
        else:
            df["well_name"] = None

        # Operator from ogrid join
        if "operator" in prod_df.columns:
            df["operator"] = prod_df["operator"].values
        else:
            df["operator"] = None

        # County from FIPS code
        if "_source_county" in prod_df.columns:
            df["county"] = prod_df["_source_county"].values
        else:
            df["county"] = None

        # Pool/field name
        if "pool_name" in prod_df.columns:
            df["field_name"] = prod_df["pool_name"].values
        else:
            df["field_name"] = None

        # Well type from wchistory
        if "well_typ_cde" in prod_df.columns:
            df["well_type"] = prod_df["well_typ_cde"].apply(_normalize_well_type)
        else:
            df["well_type"] = None

        # Well status from wchistory
        if "wc_stat_cde" in prod_df.columns:
            df["well_status"] = prod_df["wc_stat_cde"].apply(_normalize_well_status)
        else:
            df["well_status"] = None

        # Production date from prodn_yr + prodn_mth
        if "prodn_yr" in prod_df.columns and "prodn_mth" in prod_df.columns:
            years = pd.to_numeric(prod_df["prodn_yr"], errors="coerce")
            months = pd.to_numeric(prod_df["prodn_mth"], errors="coerce")
            valid = (
                years.notna() & months.notna()
                & (months >= 1) & (months <= 12)
                & (years >= 1900) & (years <= 2100)
            )
            date_strs = (
                years.astype("Int64").astype(str) + "-"
                + months.astype("Int64").astype(str).str.zfill(2) + "-01"
            )
            dates = pd.to_datetime(date_strs, format="%Y-%m-%d", errors="coerce")
            dates = dates.where(valid)
            df["production_date"] = dates.dt.date
        else:
            df["production_date"] = None

        # Volume columns (already numeric from pivot)
        for col in ("oil_bbl", "gas_mcf", "water_bbl"):
            if col in prod_df.columns:
                df[col] = pd.to_numeric(prod_df[col], errors="coerce").values
            else:
                df[col] = None

        # Days produced
        if "days_produced" in prod_df.columns:
            df["days_produced"] = (
                pd.to_numeric(prod_df["days_produced"], errors="coerce")
                .astype("Int32")
            )
        else:
            df["days_produced"] = None

        # Source file for lineage
        if "_source_file" in prod_df.columns:
            df["source_file"] = prod_df["_source_file"].values
        else:
            df["source_file"] = "wcproduction.xml"

        # Geographic coordinates (filled by well enrichment)
        df["latitude"] = None
        df["longitude"] = None

        # Basin placeholder (derived after enrichment)
        df["basin"] = None

        self.log.info("Built %d OCD FTP production records", len(df))

        # -- Enrich from ArcGIS well master data --
        well_col_map = self._resolve_well_columns(wells_df)
        if not wells_df.empty and well_col_map:
            df = self._enrich_from_wells(df, wells_df, well_col_map)

        # -- Derive basin from county --
        df["basin"] = df["county"].apply(_derive_basin)

        return df

    def _build_gotech_records(
        self,
        prod_df: pd.DataFrame,
        wells_df: pd.DataFrame,
        now_utc: datetime,
    ) -> pd.DataFrame:
        """Build silver records from legacy GO-TECH CSV data.

        This is the original logic preserved for backward compatibility.
        """
        # -- Identify production columns (case-insensitive matching) --
        col_map = self._resolve_production_columns(prod_df)
        self.log.info("Resolved production column mapping: %s", col_map)

        # -- Identify well master columns --
        well_col_map = self._resolve_well_columns(wells_df)
        self.log.info("Resolved well master column mapping: %s", well_col_map)

        # -- Vectorized column extraction and type coercion --
        n = len(prod_df)
        df = pd.DataFrame(index=range(n))

        # Constants
        df["state"] = "NM"
        df["entity_type"] = "well"
        df["lease_number"] = None
        df["district"] = None
        df["condensate_bbl"] = None
        df["casinghead_gas_mcf"] = None
        df["ingested_at"] = now_utc

        # API number (vectorized via Series.apply -- still element-wise but
        # avoids the overhead of iterrows/dict-append per row)
        if "api" in col_map:
            df["api_number"] = prod_df[col_map["api"]].apply(_normalize_api)
        else:
            df["api_number"] = None

        # Well name and operator from production data
        if "well_name" in col_map:
            df["well_name"] = prod_df[col_map["well_name"]].str.strip().replace("", None)
        else:
            df["well_name"] = None

        if "operator" in col_map:
            df["operator"] = prod_df[col_map["operator"]].str.strip().replace("", None)
        else:
            df["operator"] = None

        # County from source tagging (already normalized by _normalize_county_from_dir)
        if "_source_county" in prod_df.columns:
            df["county"] = prod_df["_source_county"]
        else:
            df["county"] = None

        # Placeholders to be filled by well enrichment
        df["field_name"] = None
        df["well_type"] = None
        df["well_status"] = None

        # Production date -- vectorized year+month or combined date column
        df["production_date"] = self._vectorized_parse_dates(prod_df, col_map)

        # Volume columns -- coerce to float, stripping commas
        for silver_col, map_key in [
            ("oil_bbl", "oil"),
            ("gas_mcf", "gas"),
            ("water_bbl", "water"),
        ]:
            if map_key in col_map:
                df[silver_col] = (
                    prod_df[col_map[map_key]]
                    .str.replace(",", "", regex=False)
                    .str.strip()
                    .replace("", None)
                    .pipe(pd.to_numeric, errors="coerce")
                )
            else:
                df[silver_col] = None

        # Days produced -- coerce to int
        if "days" in col_map:
            df["days_produced"] = (
                prod_df[col_map["days"]]
                .str.replace(",", "", regex=False)
                .str.strip()
                .replace("", None)
                .pipe(pd.to_numeric, errors="coerce")
            )
            # Convert to nullable int (Int32 supports NA)
            df["days_produced"] = df["days_produced"].astype("Int32")
        else:
            df["days_produced"] = None

        # Source file for lineage
        if "_source_file" in prod_df.columns:
            df["source_file"] = prod_df["_source_file"].values
        else:
            df["source_file"] = None

        # Geographic coordinates (filled by well enrichment)
        df["latitude"] = None
        df["longitude"] = None

        # Basin placeholder (derived after enrichment)
        df["basin"] = None

        self.log.info("Built %d raw production records (vectorized)", len(df))

        # -- Enrich from well master data --
        if not wells_df.empty and well_col_map:
            df = self._enrich_from_wells(df, wells_df, well_col_map)

        # -- Derive basin from county --
        df["basin"] = df["county"].apply(_derive_basin)

        return df

    def _vectorized_parse_dates(
        self, prod_df: pd.DataFrame, col_map: dict[str, str]
    ) -> pd.Series:
        """Parse production dates from year+month or combined date columns.

        Returns a Series of ``datetime.date`` (first of month) or None.
        """
        n = len(prod_df)

        # Try year + month columns first
        year_col = col_map.get("year", "")
        month_col = col_map.get("month", "")
        if year_col and month_col:
            years = pd.to_numeric(
                prod_df[year_col].str.replace(",", "", regex=False).str.strip(),
                errors="coerce",
            )
            months = pd.to_numeric(
                prod_df[month_col].str.replace(",", "", regex=False).str.strip(),
                errors="coerce",
            )
            # Build date strings "YYYY-MM-01" and parse
            valid = (
                years.notna() & months.notna()
                & (months >= 1) & (months <= 12)
                & (years >= 1900) & (years <= 2100)
            )
            date_strs = (
                years.astype("Int64").astype(str) + "-"
                + months.astype("Int64").astype(str).str.zfill(2) + "-01"
            )
            dates = pd.to_datetime(date_strs, format="%Y-%m-%d", errors="coerce")
            dates = dates.where(valid)
            result = dates.dt.date
            # If we got any valid dates, return this
            if result.notna().any():
                return result

        # Fallback: try combined date column
        date_col = col_map.get("date", "")
        if date_col:
            raw = prod_df[date_col].str.strip()
            # Try multiple formats
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m", "%m/%Y", "%Y%m"):
                parsed = pd.to_datetime(raw, format=fmt, errors="coerce")
                if parsed.notna().any():
                    # Normalize to first of month
                    result = parsed.apply(
                        lambda dt: date(dt.year, dt.month, 1) if pd.notna(dt) else None
                    )
                    return result

        # No date columns found
        return pd.Series([None] * n, dtype=object)

    def _enrich_from_wells(
        self,
        prod_df: pd.DataFrame,
        wells_df: pd.DataFrame,
        well_col_map: dict[str, str],
    ) -> pd.DataFrame:
        """Left-join production records with well master to fill attributes.

        Fills: well_name, operator, county, field_name, well_type, well_status
        (only where the production record has NULLs and the well master has values).
        """
        # Build a lookup keyed on normalized API
        api_col = well_col_map.get("id", "")
        if not api_col or api_col not in wells_df.columns:
            self.log.warning("No API/id column in well master; skipping enrichment")
            return prod_df

        well_lookup = wells_df.copy()
        well_lookup["_norm_api"] = well_lookup[api_col].apply(_normalize_api)
        well_lookup = well_lookup.drop_duplicates(subset="_norm_api", keep="last")

        # Build a small enrichment DataFrame
        enrich_cols: dict[str, str] = {}  # silver_col -> wells_col
        for silver_col, wells_col_name in [
            ("well_name", well_col_map.get("well_name", "")),
            ("operator", well_col_map.get("operator", "")),
            ("county", well_col_map.get("county", "")),
            ("field_name", well_col_map.get("pool", "")),
            ("well_type", well_col_map.get("type", "")),
            ("well_status", well_col_map.get("status", "")),
            ("latitude", well_col_map.get("latitude", "")),
            ("longitude", well_col_map.get("longitude", "")),
        ]:
            if wells_col_name and wells_col_name in well_lookup.columns:
                enrich_cols[silver_col] = wells_col_name

        if not enrich_cols:
            self.log.info("No enrichment columns found in well master")
            return prod_df

        # Build the right side of the join
        right_cols = ["_norm_api"] + list(enrich_cols.values())
        right_df = well_lookup[right_cols].copy()
        # Rename to _well_<silver_col> to avoid collision
        rename_map = {v: f"_well_{k}" for k, v in enrich_cols.items()}
        rename_map["_norm_api"] = "_well_api"
        right_df = right_df.rename(columns=rename_map)

        # Join
        merged = prod_df.merge(right_df, left_on="api_number", right_on="_well_api", how="left")

        # Fill NULLs in production with well master values
        for silver_col in enrich_cols:
            well_col = f"_well_{silver_col}"
            if well_col in merged.columns:
                if silver_col == "well_type":
                    merged[well_col] = merged[well_col].apply(_normalize_well_type)
                elif silver_col == "well_status":
                    merged[well_col] = merged[well_col].apply(_normalize_well_status)
                elif silver_col in ("latitude", "longitude"):
                    merged[well_col] = pd.to_numeric(merged[well_col], errors="coerce")
                else:
                    merged[well_col] = merged[well_col].apply(self._clean_string)

                mask = merged[silver_col].isna() & merged[well_col].notna()
                merged.loc[mask, silver_col] = merged.loc[mask, well_col]

        # Drop temp columns
        drop_cols = [c for c in merged.columns if c.startswith("_well_")]
        merged = merged.drop(columns=drop_cols)

        n_enriched = len(merged[merged["operator"].notna()])
        self.log.info(
            "Enriched production records from well master (%d with operator)", n_enriched
        )
        return merged

    # ------------------------------------------------------------------
    # Column resolution helpers
    # ------------------------------------------------------------------

    def _resolve_production_columns(self, df: pd.DataFrame) -> dict[str, str]:
        """Map logical field names to actual column names in GO-TECH data.

        GO-TECH files have varying column names across counties.  This
        method does case-insensitive fuzzy matching.
        """
        cols = {c.strip().lower(): c for c in df.columns}
        mapping: dict[str, str] = {}

        # API number
        for candidate in ("api", "api_no", "api no", "api number", "well_api", "apino"):
            if candidate in cols:
                mapping["api"] = cols[candidate]
                break

        # Well name
        for candidate in ("well_name", "well name", "wellname", "well", "name"):
            if candidate in cols:
                mapping["well_name"] = cols[candidate]
                break

        # Operator
        for candidate in ("operator", "operator_name", "operator name", "ogrid_name", "oper"):
            if candidate in cols:
                mapping["operator"] = cols[candidate]
                break

        # Oil production
        for candidate in ("oil", "oil_bbl", "oil bbl", "oil(bbl)", "oilbbl"):
            if candidate in cols:
                mapping["oil"] = cols[candidate]
                break

        # Gas production
        for candidate in ("gas", "gas_mcf", "gas mcf", "gas(mcf)", "gasmcf"):
            if candidate in cols:
                mapping["gas"] = cols[candidate]
                break

        # Water production
        for candidate in ("water", "water_bbl", "water bbl", "water(bbl)", "waterbbl"):
            if candidate in cols:
                mapping["water"] = cols[candidate]
                break

        # Days produced
        for candidate in ("days", "days_produced", "days produced", "proddays"):
            if candidate in cols:
                mapping["days"] = cols[candidate]
                break

        # Year
        for candidate in ("year", "prod_year", "production_year", "proxyear"):
            if candidate in cols:
                mapping["year"] = cols[candidate]
                break

        # Month
        for candidate in ("month", "prod_month", "production_month", "proxymonth", "mo"):
            if candidate in cols:
                mapping["month"] = cols[candidate]
                break

        # Combined date column
        for candidate in ("production_month", "prod_date", "date", "production_date"):
            if candidate in cols:
                mapping["date"] = cols[candidate]
                break

        return mapping

    def _resolve_well_columns(self, df: pd.DataFrame) -> dict[str, str]:
        """Map logical well attribute names to actual ArcGIS column names."""
        if df.empty:
            return {}

        cols = {c.strip().lower(): c for c in df.columns}
        mapping: dict[str, str] = {}

        for logical, candidates in [
            ("id", ("id", "api", "api_no", "apino", "well_id")),
            ("well_name", ("name", "well_name", "wellname")),
            ("operator", ("ogrid_name", "operator", "operator_name")),
            ("county", ("county", "county_name")),
            ("pool", ("pool", "pool_name", "field_name", "field")),
            ("type", ("type", "well_type", "welltype")),
            ("status", ("status", "well_status", "wellstatus")),
            ("latitude", ("latitude",)),
            ("longitude", ("longitude",)),
        ]:
            for candidate in candidates:
                if candidate in cols:
                    mapping[logical] = cols[candidate]
                    break

        return mapping

    # ------------------------------------------------------------------
    # Type coercion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_string(val: Any) -> str | None:
        """Strip and return a string, or None if empty/NA."""
        if val is None:
            return None
        if pd.isna(val):
            return None
        s = str(val).strip()
        return s if s else None

    # ------------------------------------------------------------------
    # Main parse entry point
    # ------------------------------------------------------------------

    def parse(self) -> pd.DataFrame:
        """Read raw NM OCD data from bronze and return a silver DataFrame.

        Steps:
        1. Load well master JSON batches and production data (OCD FTP XML
           or legacy GO-TECH CSVs -- auto-detected)
        2. Map and join into common schema
        3. Drop rows missing required identity fields
        4. Deduplicate (state + api_number + production_date, latest wins)
        5. Validate against schema rules
        6. Write Parquet to data/silver/production/state=NM/
        """
        self.log.info(
            "=== NM Parser -- pull_date=%s, input=%s ===",
            self.pull_date, self.run_dir,
        )

        if not self.run_dir.exists():
            raise FileNotFoundError(
                f"Bronze run directory does not exist: {self.run_dir}"
            )

        # Step 1: Load bronze data
        wells_df = self._load_wells_master()
        prod_df = self._load_production_data()

        if prod_df.empty:
            self.log.warning("No production data found; returning empty DataFrame")
            return pd.DataFrame(columns=PRODUCTION_SCHEMA.names)

        # Step 2: Build silver records
        silver_df = self._build_production_records(prod_df, wells_df)

        # Step 3: Drop rows with no API or no production date
        before = len(silver_df)
        silver_df = silver_df.dropna(subset=["api_number", "production_date"])
        dropped = before - len(silver_df)
        if dropped > 0:
            self.log.info(
                "Dropped %d rows missing api_number or production_date", dropped
            )

        # Step 4: Deduplicate
        silver_df = deduplicate(silver_df)

        # Step 5: Validate
        result = validate_batch(silver_df)
        if not result.passed:
            self.log.warning(
                "Validation flagged %d/%d rows (%.1f%% pass rate)",
                result.failed_rows,
                result.total_rows,
                result.pass_rate * 100,
            )
            for err in result.errors:
                self.log.warning(
                    "  [%s] %s -- %d bad rows, samples: %s",
                    err.check_name, err.column, err.bad_row_count,
                    err.sample_values[:3],
                )
            # Keep only valid rows for the silver output
            silver_df = get_valid_rows(silver_df)

        # Step 6: Write Parquet
        if not silver_df.empty:
            self.write_parquet(
                silver_df,
                "nm_production.parquet",
                schema=PRODUCTION_SCHEMA,
            )

        self.log.info(
            "=== NM Parser complete: %d silver records ===", len(silver_df)
        )
        return silver_df


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)-36s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = NmParser()
    df = parser.parse()
    print(f"\nDone. {len(df)} records written to silver layer.")
