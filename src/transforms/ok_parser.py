"""Oklahoma OCC production data parser -- bronze to silver.

Reads three bronze data sources and joins them:
  1. ArcGIS well master JSON     -> well attributes (operator, county, type, status, lat/lon)
  2. RBDMS CSV                   -> well data (may include production volumes)
  3. Completions XLSX            -> IP test rates, formation, spud/completion dates

The parser maps Oklahoma data to the common silver-layer PRODUCTION_SCHEMA
and writes Parquet files to data/silver/production/state=OK/.

Oklahoma wells use API state code 35 and report at well level.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd

from src.schemas.dedup import deduplicate
from src.schemas.production import PRODUCTION_SCHEMA
from src.schemas.validation import validate_batch, get_valid_rows
from src.transforms.base import BaseParser
from src.utils.config import BRONZE_OK_DIR, SILVER_DIR

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# County -> basin lookup (Oklahoma)
# ---------------------------------------------------------------------------

_COUNTY_TO_BASIN: dict[str, str] = {
    # Anadarko Basin (western OK)
    "BECKHAM": "Anadarko",
    "BLAINE": "Anadarko",
    "CADDO": "Anadarko",
    "CANADIAN": "Anadarko",
    "CUSTER": "Anadarko",
    "DEWEY": "Anadarko",
    "ELLIS": "Anadarko",
    "GRADY": "Anadarko",
    "GREER": "Anadarko",
    "HARMON": "Anadarko",
    "JACKSON": "Anadarko",
    "KIOWA": "Anadarko",
    "MAJOR": "Anadarko",
    "ROGER MILLS": "Anadarko",
    "WASHITA": "Anadarko",
    "WOODWARD": "Anadarko",
    # Arkoma Basin (eastern OK)
    "ADAIR": "Arkoma",
    "CHEROKEE": "Arkoma",
    "COAL": "Arkoma",
    "HASKELL": "Arkoma",
    "LATIMER": "Arkoma",
    "LE FLORE": "Arkoma",
    "MCINTOSH": "Arkoma",
    "MUSKOGEE": "Arkoma",
    "PITTSBURG": "Arkoma",
    "PONTOTOC": "Arkoma",
    "SEQUOYAH": "Arkoma",
    # Ardmore Basin (south-central OK)
    "CARTER": "Ardmore",
    "GARVIN": "Ardmore",
    "JOHNSTON": "Ardmore",
    "LOVE": "Ardmore",
    "MARSHALL": "Ardmore",
    "MURRAY": "Ardmore",
    "STEPHENS": "Ardmore",
    # Marietta Basin
    "JEFFERSON": "Marietta",
    # Cherokee Platform / Nemaha Ridge (north-central OK)
    "CREEK": "Cherokee Platform",
    "GARFIELD": "Cherokee Platform",
    "GRANT": "Cherokee Platform",
    "KAY": "Cherokee Platform",
    "KINGFISHER": "Cherokee Platform",
    "LINCOLN": "Cherokee Platform",
    "LOGAN": "Cherokee Platform",
    "NOBLE": "Cherokee Platform",
    "OKLAHOMA": "Cherokee Platform",
    "OSAGE": "Cherokee Platform",
    "PAWNEE": "Cherokee Platform",
    "PAYNE": "Cherokee Platform",
    "WASHINGTON": "Cherokee Platform",
    # SCOOP/STACK play area (overlaps Anadarko but commonly referenced)
    "MCCLAIN": "Anadarko",
    # Permian Basin extension (panhandle)
    "BEAVER": "Hugoton",
    "CIMARRON": "Hugoton",
    "TEXAS": "Hugoton",
}

# ---------------------------------------------------------------------------
# Well-type and well-status normalization (OCC RBDMS values)
# ---------------------------------------------------------------------------

_WELL_TYPE_MAP: dict[str, str] = {
    "OIL": "OIL",
    "GAS": "GAS",
    "OIL/GAS": "OIL",
    "GAS_STORAGE": "OTHER",
    "WATER_SUPPLY": "OTHER",
    "WATER_INJECTION": "INJ",
    "DRY": "OTHER",
    "PLUGGED": "OTHER",
    "ORPHAN": "OTHER",
    "OTHER": "OTHER",
    "UIC": "INJ",
    "TEMPORARILY_ABANDONED": "OTHER",
    "TERMINATED": "OTHER",
    "STATE_FUNDS_PLUGGING": "OTHER",
}

_WELL_STATUS_MAP: dict[str, str] = {
    "ACTIVE": "ACTIVE",
    "ACT": "ACTIVE",
    "NEW": "ACTIVE",
    "PRODUCING": "ACTIVE",
    "SHUT-IN": "SHUT-IN",
    "SI": "SHUT-IN",
    "SHUT IN": "SHUT-IN",
    "TEMPORARILY_ABANDONED": "SHUT-IN",
    "TA": "SHUT-IN",
    "P&A": "P&A",
    "PA": "P&A",
    "PLUGGED": "P&A",
    "ABANDONED": "P&A",
    "ABD": "P&A",
    "INACTIVE": "INACTIVE",
    "INACT": "INACTIVE",
    "TERMINATED": "INACTIVE",
    "ORPHAN": "INACTIVE",
    "DRY": "P&A",
    "STATE_FUNDS_PLUGGING": "P&A",
}


def _normalize_api(raw: Any) -> str | None:
    """Normalize an API number to NN-NNN-NNNNN format (state code 35 for OK).

    Handles various input formats: 10-digit numeric, formatted with dashes,
    or missing state prefix (8 digits).
    """
    if pd.isna(raw):
        return None
    s = str(raw).strip()
    if not s:
        return None

    # Already formatted
    if re.match(r"^\d{2}-\d{3}-\d{5}$", s):
        return s

    # Remove trailing ".0" from float conversion (e.g. "3501712345.0")
    s = re.sub(r"\.0+$", "", s)

    # Strip non-digit characters (dashes, spaces, dots used as separators)
    digits = re.sub(r"[^\d]", "", s)

    if len(digits) == 10:
        return f"{digits[:2]}-{digits[2:5]}-{digits[5:10]}"
    if len(digits) == 8:
        # Missing state code, assume OK (35)
        return f"35-{digits[:3]}-{digits[3:8]}"

    logger.debug("Could not normalize API number (unexpected length %d): %r", len(digits), raw)
    return None


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
    return _WELL_STATUS_MAP.get(key, "OTHER")


def _parse_county_code(raw: Any) -> str | None:
    """Parse county from OCC format '003-ADAIR' to UPPER case."""
    if pd.isna(raw):
        return None
    s = str(raw).strip()
    if not s:
        return None
    # Format: "NNN-COUNTYNAME" -> take part after dash, upper-case
    if "-" in s:
        name = s.split("-", 1)[1].strip()
        return name.upper() if name else None
    return s.upper()


def _derive_basin(county: Any) -> str | None:
    """Look up basin from county name (keys are UPPER)."""
    if pd.isna(county):
        return None
    return _COUNTY_TO_BASIN.get(str(county).strip().upper(), None)


class OkParser(BaseParser):
    """Parser for raw Oklahoma OCC well and production data.

    Reads from ``data/bronze/ok/{pull_date}/`` and writes a single
    Parquet file to ``data/silver/production/state=OK/ok_production.parquet``.
    """

    def __init__(self, *, pull_date: str | None = None, **kwargs) -> None:
        super().__init__(
            input_dir=BRONZE_OK_DIR,
            output_dir=SILVER_DIR / "production" / "state=OK",
            **kwargs,
        )
        self.pull_date = pull_date or self._find_latest_pull_date()
        self.run_dir = self.input_dir / self.pull_date

    def _find_latest_pull_date(self) -> str:
        """Find the most recent pull_date directory under bronze/ok/."""
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

    def _load_rbdms_csv(self) -> pd.DataFrame:
        """Load the RBDMS well data CSV from the data directory.

        Returns a DataFrame of the CSV contents, or empty if not found.
        """
        data_dir = self.run_dir / "data"
        if not data_dir.exists():
            self.log.warning("No data directory at %s", data_dir)
            return pd.DataFrame()

        csv_path = data_dir / "rbdms_well_data.csv"
        if not csv_path.exists():
            # Try any CSV in the data directory
            csv_files = list(data_dir.glob("*.csv"))
            if not csv_files:
                self.log.warning("No CSV files found in %s", data_dir)
                return pd.DataFrame()
            csv_path = csv_files[0]

        # Try comma first, then tab
        for sep in (",", "\t", "|"):
            try:
                df = pd.read_csv(
                    csv_path,
                    sep=sep,
                    dtype=str,
                    encoding="utf-8",
                    on_bad_lines="warn",
                    low_memory=False,
                )
                if len(df.columns) >= 3:
                    self.log.info(
                        "Loaded %d rows from %s (sep=%r)", len(df), csv_path.name, sep
                    )
                    return df
            except Exception:
                continue

        # Try latin-1 encoding as fallback
        try:
            df = pd.read_csv(
                csv_path,
                dtype=str,
                encoding="latin-1",
                on_bad_lines="warn",
                low_memory=False,
            )
            self.log.info("Loaded %d rows from %s (latin-1)", len(df), csv_path.name)
            return df
        except Exception:
            self.log.warning("Could not parse %s with any strategy", csv_path)
            return pd.DataFrame()

    def _load_completions(self) -> pd.DataFrame:
        """Load the completions XLSX from the data directory.

        Returns a DataFrame of completion records with IP test rates,
        formation, and drilling dates. Returns empty DataFrame if not found.
        """
        data_dir = self.run_dir / "data"
        xlsx_path = data_dir / "completions_wells_formations.xlsx"
        if not xlsx_path.exists():
            self.log.info("No completions XLSX found at %s", xlsx_path)
            return pd.DataFrame()

        try:
            df = pd.read_excel(xlsx_path, dtype={"API_Number": str, "OTC_Prod_Unit_No": str})
            self.log.info("Loaded %d completion records from %s", len(df), xlsx_path.name)
            return df
        except Exception as exc:
            self.log.warning("Failed to read completions XLSX: %s", exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Column resolution
    # ------------------------------------------------------------------

    def _resolve_well_columns(self, df: pd.DataFrame) -> dict[str, str]:
        """Map logical well attribute names to actual ArcGIS column names."""
        if df.empty:
            return {}

        cols = {c.strip().lower(): c for c in df.columns}
        mapping: dict[str, str] = {}

        for logical, candidates in [
            ("api", ("api", "api_no", "apino", "well_api", "api_number")),
            ("well_name", ("well_name", "wellname", "name")),
            ("well_num", ("well_num", "wellnum", "well_number")),
            ("operator", ("operator", "operator_name")),
            ("county", ("county", "county_name")),
            ("welltype", ("welltype", "well_type", "type", "symbol_class")),
            ("wellstatus", ("wellstatus", "well_status", "status")),
            ("latitude", ("sh_lat", "latitude", "lat")),
            ("longitude", ("sh_lon", "longitude", "lon", "long")),
        ]:
            for candidate in candidates:
                if candidate in cols:
                    mapping[logical] = cols[candidate]
                    break

        return mapping

    def _resolve_csv_columns(self, df: pd.DataFrame) -> dict[str, str]:
        """Map logical field names to actual column names in the RBDMS CSV.

        The RBDMS CSV column names are not documented, so this uses
        case-insensitive fuzzy matching.
        """
        if df.empty:
            return {}

        cols = {c.strip().lower(): c for c in df.columns}
        mapping: dict[str, str] = {}

        for logical, candidates in [
            ("api", ("api", "api_no", "api_number", "apino", "well_api")),
            ("well_name", ("well_name", "wellname", "name", "lease_name")),
            ("operator", ("operator", "operator_name", "oper", "company")),
            ("county", ("county", "county_name")),
            ("well_type", ("well_type", "welltype", "type")),
            ("well_status", ("well_status", "wellstatus", "status")),
            ("oil", ("oil", "oil_bbl", "oil_prod", "oil_volume", "oil production")),
            ("gas", ("gas", "gas_mcf", "gas_prod", "gas_volume", "gas production")),
            ("water", ("water", "water_bbl", "water_prod", "water_volume")),
            ("days", ("days", "days_produced", "producing_days", "prod_days")),
            ("year", ("year", "prod_year", "production_year", "rpt_year")),
            ("month", ("month", "prod_month", "production_month", "rpt_month")),
            ("date", ("production_date", "prod_date", "date", "report_date")),
            ("latitude", ("latitude", "lat", "sh_lat")),
            ("longitude", ("longitude", "lon", "long", "sh_lon")),
        ]:
            for candidate in candidates:
                if candidate in cols:
                    mapping[logical] = cols[candidate]
                    break

        return mapping

    # ------------------------------------------------------------------
    # Build silver records
    # ------------------------------------------------------------------

    def _build_from_wells(
        self,
        wells_df: pd.DataFrame,
        csv_df: pd.DataFrame,
        now_utc: datetime,
    ) -> pd.DataFrame:
        """Build silver records by combining well master and CSV data.

        The ArcGIS well master provides well attributes (name, operator,
        county, type, status, lat/lon). The RBDMS CSV may provide
        production volumes. If production data is available, each row
        represents a well-month. If not, we create one row per well
        with NULL production fields (suitable for well master only).
        """
        well_col_map = self._resolve_well_columns(wells_df)
        csv_col_map = self._resolve_csv_columns(csv_df) if not csv_df.empty else {}

        self.log.info("Well column mapping: %s", well_col_map)
        self.log.info("CSV column mapping: %s", csv_col_map)

        # Determine if CSV has production data (volumes + dates)
        has_production = (
            not csv_df.empty
            and ("oil" in csv_col_map or "gas" in csv_col_map)
            and ("year" in csv_col_map or "date" in csv_col_map)
        )

        if has_production:
            self.log.info("CSV has production data — building production records")
            return self._build_production_records(csv_df, wells_df, csv_col_map, well_col_map, now_utc)
        else:
            self.log.info("No production volumes in CSV — building well master records from ArcGIS only")
            return self._build_well_master_records(wells_df, well_col_map, now_utc)

    def _build_production_records(
        self,
        csv_df: pd.DataFrame,
        wells_df: pd.DataFrame,
        csv_col_map: dict[str, str],
        well_col_map: dict[str, str],
        now_utc: datetime,
    ) -> pd.DataFrame:
        """Build silver records from CSV production data enriched with well master."""
        n = len(csv_df)
        df = pd.DataFrame(index=range(n))

        # Constants
        df["state"] = "OK"
        df["entity_type"] = "well"
        df["lease_number"] = None
        df["district"] = None
        df["condensate_bbl"] = None
        df["casinghead_gas_mcf"] = None
        df["ingested_at"] = now_utc

        # API number
        if "api" in csv_col_map:
            df["api_number"] = csv_df[csv_col_map["api"]].apply(_normalize_api)
        else:
            df["api_number"] = None

        # Well name
        if "well_name" in csv_col_map:
            df["well_name"] = csv_df[csv_col_map["well_name"]].str.strip().replace("", None)
        else:
            df["well_name"] = None

        # Operator
        if "operator" in csv_col_map:
            df["operator"] = csv_df[csv_col_map["operator"]].str.strip().replace("", None)
        else:
            df["operator"] = None

        # County
        if "county" in csv_col_map:
            df["county"] = csv_df[csv_col_map["county"]].str.strip().replace("", None)
        else:
            df["county"] = None

        # Well type / status
        if "well_type" in csv_col_map:
            df["well_type"] = csv_df[csv_col_map["well_type"]].apply(_normalize_well_type)
        else:
            df["well_type"] = None

        if "well_status" in csv_col_map:
            df["well_status"] = csv_df[csv_col_map["well_status"]].apply(_normalize_well_status)
        else:
            df["well_status"] = None

        # Production date
        df["production_date"] = self._parse_dates(csv_df, csv_col_map)

        # Volume columns
        for silver_col, map_key in [
            ("oil_bbl", "oil"),
            ("gas_mcf", "gas"),
            ("water_bbl", "water"),
        ]:
            if map_key in csv_col_map:
                df[silver_col] = (
                    csv_df[csv_col_map[map_key]]
                    .str.replace(",", "", regex=False)
                    .str.strip()
                    .replace("", None)
                    .pipe(pd.to_numeric, errors="coerce")
                )
            else:
                df[silver_col] = None

        # Days produced
        if "days" in csv_col_map:
            df["days_produced"] = (
                csv_df[csv_col_map["days"]]
                .str.replace(",", "", regex=False)
                .str.strip()
                .replace("", None)
                .pipe(pd.to_numeric, errors="coerce")
                .astype("Int32")
            )
        else:
            df["days_produced"] = None

        # Field name placeholder
        df["field_name"] = None

        # Geographic coordinates from CSV
        if "latitude" in csv_col_map:
            df["latitude"] = pd.to_numeric(csv_df[csv_col_map["latitude"]], errors="coerce")
        else:
            df["latitude"] = None
        if "longitude" in csv_col_map:
            df["longitude"] = pd.to_numeric(csv_df[csv_col_map["longitude"]], errors="coerce")
        else:
            df["longitude"] = None

        # Basin
        df["basin"] = None

        # Source file
        df["source_file"] = "rbdms_well_data.csv"

        self.log.info("Built %d production records from CSV", len(df))

        # Enrich from ArcGIS well master
        if not wells_df.empty and well_col_map:
            df = self._enrich_from_wells(df, wells_df, well_col_map)

        # Derive basin from county
        df["basin"] = df["county"].apply(_derive_basin)

        return df

    def _build_well_master_records(
        self,
        wells_df: pd.DataFrame,
        well_col_map: dict[str, str],
        now_utc: datetime,
    ) -> pd.DataFrame:
        """Build silver records from ArcGIS well master only (no production volumes).

        Creates one record per well with NULL production fields and a
        synthetic production_date of today (first of current month).
        This allows wells to appear in the warehouse for reference even
        without production volume data.
        """
        n = len(wells_df)
        df = pd.DataFrame(index=range(n))

        # Constants
        df["state"] = "OK"
        df["entity_type"] = "well"
        df["lease_number"] = None
        df["district"] = None
        df["condensate_bbl"] = None
        df["casinghead_gas_mcf"] = None
        df["ingested_at"] = now_utc

        # API number
        if "api" in well_col_map:
            df["api_number"] = wells_df[well_col_map["api"]].apply(_normalize_api)
        else:
            df["api_number"] = None

        # Well name
        if "well_name" in well_col_map:
            df["well_name"] = wells_df[well_col_map["well_name"]].astype(str).str.strip().replace("", None)
        elif "well_num" in well_col_map:
            df["well_name"] = wells_df[well_col_map["well_num"]].astype(str).str.strip().replace("", None)
        else:
            df["well_name"] = None

        # Operator
        if "operator" in well_col_map:
            df["operator"] = wells_df[well_col_map["operator"]].astype(str).str.strip().replace("", None)
        else:
            df["operator"] = None

        # County
        if "county" in well_col_map:
            df["county"] = wells_df[well_col_map["county"]].astype(str).str.strip().replace("", None)
        else:
            df["county"] = None

        # Well type / status
        if "welltype" in well_col_map:
            df["well_type"] = wells_df[well_col_map["welltype"]].apply(_normalize_well_type)
        else:
            df["well_type"] = None

        if "wellstatus" in well_col_map:
            df["well_status"] = wells_df[well_col_map["wellstatus"]].apply(_normalize_well_status)
        else:
            df["well_status"] = None

        # Production date — first of current month as reference
        today = date.today()
        df["production_date"] = date(today.year, today.month, 1)

        # Volume columns — all NULL (well master only)
        df["oil_bbl"] = None
        df["gas_mcf"] = None
        df["water_bbl"] = None
        df["days_produced"] = None

        # Field name
        df["field_name"] = None

        # Geographic coordinates
        if "latitude" in well_col_map:
            df["latitude"] = pd.to_numeric(wells_df[well_col_map["latitude"]], errors="coerce")
        else:
            df["latitude"] = None
        if "longitude" in well_col_map:
            df["longitude"] = pd.to_numeric(wells_df[well_col_map["longitude"]], errors="coerce")
        else:
            df["longitude"] = None

        # Basin — derived after county is set
        df["basin"] = df["county"].apply(_derive_basin)

        # Source file
        df["source_file"] = "arcgis_rbdms_wells"

        self.log.info("Built %d well master records from ArcGIS", len(df))
        return df

    def _build_completion_records(
        self,
        comp_df: pd.DataFrame,
        now_utc: datetime,
    ) -> pd.DataFrame:
        """Build silver records from completions XLSX (IP test rates).

        Creates one record per (API, completion) with initial production
        test rates as oil/gas/water volumes and test_date as production_date.
        """
        if comp_df.empty:
            return pd.DataFrame(columns=PRODUCTION_SCHEMA.names)

        # Filter to rows with at least one non-zero IP rate
        has_ip = (
            (comp_df.get("Oil_BBL_Per_Day", 0) > 0)
            | (comp_df.get("Gas_MCF_Per_Day", 0) > 0)
            | (comp_df.get("Water_BBL_Per_Day", 0) > 0)
        )
        df_src = comp_df[has_ip].copy()
        if df_src.empty:
            self.log.info("No completions with non-zero IP rates")
            return pd.DataFrame(columns=PRODUCTION_SCHEMA.names)

        n = len(df_src)
        df = pd.DataFrame(index=range(n))

        # Constants
        df["state"] = "OK"
        df["entity_type"] = "well"
        df["district"] = None
        df["condensate_bbl"] = None
        df["casinghead_gas_mcf"] = None
        df["ingested_at"] = now_utc

        # API number
        df["api_number"] = df_src["API_Number"].values
        df["api_number"] = df["api_number"].apply(_normalize_api)

        # Well attributes
        df["well_name"] = df_src["Well_Name"].astype(str).str.strip().values
        df["well_name"] = df["well_name"].replace({"": None, "nan": None, "None": None})

        if "Well_Number" in df_src.columns:
            df["well_number"] = df_src["Well_Number"].astype(str).str.strip().values
        else:
            df["well_number"] = None

        df["operator"] = df_src["Operator_Name"].astype(str).str.strip().values
        df["operator"] = df["operator"].replace({"": None, "nan": None, "None": None})

        if "Operator_Number" in df_src.columns:
            df["operator_id"] = df_src["Operator_Number"].astype(str).str.strip().values
            df["operator_id"] = df["operator_id"].replace({"": None, "nan": None, "None": None})
        else:
            df["operator_id"] = None

        # County (parse "003-ADAIR" → "Adair")
        df["county"] = df_src["County"].apply(_parse_county_code).values

        # Well type / status
        df["well_type"] = df_src["Well_Type"].apply(_normalize_well_type).values
        df["well_status"] = df_src["Well_Status"].apply(_normalize_well_status).values

        # Production date — use Test_Date, fall back to First_Prod
        df["production_date"] = self._parse_completion_date(df_src)

        # IP test rates as production volumes (daily rates, days_produced=1)
        df["oil_bbl"] = pd.to_numeric(df_src["Oil_BBL_Per_Day"].values, errors="coerce")
        df["gas_mcf"] = pd.to_numeric(df_src["Gas_MCF_Per_Day"].values, errors="coerce")
        df["water_bbl"] = pd.to_numeric(df_src["Water_BBL_Per_Day"].values, errors="coerce")
        df["days_produced"] = 1

        # Replace zeros with None for consistency (zero = "not reported")
        for col in ("oil_bbl", "gas_mcf", "water_bbl"):
            df.loc[df[col] == 0, col] = None

        # Formation → field_name
        if "Formation_Name" in df_src.columns:
            df["field_name"] = df_src["Formation_Name"].astype(str).str.strip().values
            df["field_name"] = df["field_name"].replace({"": None, "nan": None, "None": None})
        else:
            df["field_name"] = None

        # Lease number from OTC PUN (production unit number)
        if "OTC_Prod_Unit_No" in df_src.columns:
            df["lease_number"] = df_src["OTC_Prod_Unit_No"].astype(str).str.strip().values
            df["lease_number"] = df["lease_number"].replace({"": None, "nan": None, "None": None})
        else:
            df["lease_number"] = None

        # Geographic coordinates
        df["latitude"] = pd.to_numeric(df_src.get("Surf_Lat_Y"), errors="coerce").values
        df["longitude"] = pd.to_numeric(df_src.get("Surf_Long_X"), errors="coerce").values

        # Basin from county
        df["basin"] = df["county"].apply(_derive_basin)

        # Source file
        df["source_file"] = "completions_wells_formations.xlsx"

        self.log.info(
            "Built %d completion records with IP rates (from %d total completions)",
            len(df), len(comp_df),
        )
        return df

    def _parse_completion_date(self, comp_df: pd.DataFrame) -> pd.Series:
        """Parse production date from completions — Test_Date or First_Prod."""
        n = len(comp_df)

        # Try Test_Date first
        for col_name in ("Test_Date", "First_Prod", "Well_Completion"):
            if col_name not in comp_df.columns:
                continue
            raw = comp_df[col_name]
            parsed = pd.to_datetime(raw, errors="coerce")
            result = parsed.apply(
                lambda dt: date(dt.year, dt.month, 1) if pd.notna(dt) and dt.year >= 1950 else None
            )
            if result.notna().any():
                self.log.info("Using %s for completion production_date", col_name)
                return result.values

        return pd.Series([None] * n, dtype=object)

    # ------------------------------------------------------------------
    # Enrichment
    # ------------------------------------------------------------------

    def _enrich_from_wells(
        self,
        prod_df: pd.DataFrame,
        wells_df: pd.DataFrame,
        well_col_map: dict[str, str],
    ) -> pd.DataFrame:
        """Left-join production records with well master to fill attributes."""
        api_col = well_col_map.get("api", "")
        if not api_col or api_col not in wells_df.columns:
            self.log.warning("No API column in well master; skipping enrichment")
            return prod_df

        well_lookup = wells_df.copy()
        well_lookup["_norm_api"] = well_lookup[api_col].apply(_normalize_api)
        well_lookup = well_lookup.drop_duplicates(subset="_norm_api", keep="last")

        # Build enrichment columns
        enrich_cols: dict[str, str] = {}
        for silver_col, wells_col_name in [
            ("well_name", well_col_map.get("well_name", "")),
            ("operator", well_col_map.get("operator", "")),
            ("county", well_col_map.get("county", "")),
            ("well_type", well_col_map.get("welltype", "")),
            ("well_status", well_col_map.get("wellstatus", "")),
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
    # Date parsing
    # ------------------------------------------------------------------

    def _parse_dates(self, df: pd.DataFrame, col_map: dict[str, str]) -> pd.Series:
        """Parse production dates from year+month or combined date columns."""
        n = len(df)

        # Try year + month columns first
        year_col = col_map.get("year", "")
        month_col = col_map.get("month", "")
        if year_col and month_col:
            years = pd.to_numeric(
                df[year_col].str.replace(",", "", regex=False).str.strip(),
                errors="coerce",
            )
            months = pd.to_numeric(
                df[month_col].str.replace(",", "", regex=False).str.strip(),
                errors="coerce",
            )
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
            if result.notna().any():
                return result

        # Fallback: try combined date column
        date_col = col_map.get("date", "")
        if date_col:
            raw = df[date_col].str.strip()
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m", "%m/%Y", "%Y%m"):
                parsed = pd.to_datetime(raw, format=fmt, errors="coerce")
                if parsed.notna().any():
                    result = parsed.apply(
                        lambda dt: date(dt.year, dt.month, 1) if pd.notna(dt) else None
                    )
                    return result

        return pd.Series([None] * n, dtype=object)

    # ------------------------------------------------------------------
    # Main parse entry point
    # ------------------------------------------------------------------

    def parse(self) -> pd.DataFrame:
        """Read raw OK OCC data from bronze and return a silver DataFrame.

        Steps:
        1. Load well master JSON, RBDMS CSV, and completions XLSX
        2. Build well master records + completion IP records
        3. Drop rows missing required identity fields
        4. Deduplicate (state + api_number + production_date, latest wins)
        5. Validate against schema rules
        6. Write Parquet to data/silver/production/state=OK/
        """
        self.log.info(
            "=== OK Parser -- pull_date=%s, input=%s ===",
            self.pull_date, self.run_dir,
        )

        if not self.run_dir.exists():
            raise FileNotFoundError(
                f"Bronze run directory does not exist: {self.run_dir}"
            )

        # Step 1: Load bronze data
        wells_df = self._load_wells_master()
        csv_df = self._load_rbdms_csv()
        comp_df = self._load_completions()

        if wells_df.empty and csv_df.empty and comp_df.empty:
            self.log.warning("No data found; returning empty DataFrame")
            return pd.DataFrame(columns=PRODUCTION_SCHEMA.names)

        # Step 2: Build silver records
        now_utc = datetime.now(timezone.utc)
        frames: list[pd.DataFrame] = []

        # Well master / CSV-based records
        if not wells_df.empty or not csv_df.empty:
            well_records = self._build_from_wells(wells_df, csv_df, now_utc)
            if not well_records.empty:
                frames.append(well_records)

        # Completion IP test records
        if not comp_df.empty:
            comp_records = self._build_completion_records(comp_df, now_utc)
            if not comp_records.empty:
                frames.append(comp_records)

        if not frames:
            self.log.warning("No silver records produced; returning empty DataFrame")
            return pd.DataFrame(columns=PRODUCTION_SCHEMA.names)

        silver_df = pd.concat(frames, ignore_index=True)

        if silver_df.empty:
            self.log.warning("No silver records produced; returning empty DataFrame")
            return pd.DataFrame(columns=PRODUCTION_SCHEMA.names)

        # Normalize text fields to UPPER case (match TX/NM convention)
        for col in ("operator", "county", "field_name", "well_name"):
            if col in silver_df.columns:
                silver_df[col] = silver_df[col].where(silver_df[col].isna(), silver_df[col].str.upper())

        # Step 3: Drop rows with no API or no production date
        before = len(silver_df)
        silver_df = silver_df.dropna(subset=["api_number", "production_date"])
        dropped = before - len(silver_df)
        if dropped > 0:
            self.log.info(
                "Dropped %d rows missing api_number or production_date", dropped
            )

        if silver_df.empty:
            self.log.warning("All rows dropped; returning empty DataFrame")
            return pd.DataFrame(columns=PRODUCTION_SCHEMA.names)

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
            silver_df = get_valid_rows(silver_df)

        # Step 6: Write Parquet
        if not silver_df.empty:
            self.write_parquet(
                silver_df,
                "ok_production.parquet",
                schema=PRODUCTION_SCHEMA,
            )

        self.log.info(
            "=== OK Parser complete: %d silver records ===", len(silver_df)
        )
        return silver_df


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)-36s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = OkParser()
    df = parser.parse()
    print(f"\nDone. {len(df)} records written to silver layer.")
