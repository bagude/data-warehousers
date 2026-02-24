"""
init_res_db.py — Create and initialize res_db.duckdb with all 6 tables + schema registry.

Idempotent: safe to run multiple times. Uses CREATE TABLE IF NOT EXISTS and
INSERT OR REPLACE for registry rows.

Usage:
    python scripts/init_res_db.py
"""

from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "res_db.duckdb"


def build_ddl() -> list[str]:
    """Return DDL statements for all 6 tables."""
    return [
        # ------------------------------------------------------------------
        # 1. res_db_properties — Well Master
        # ------------------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS res_db_properties (
            entity_key               VARCHAR NOT NULL PRIMARY KEY,
            api_number               VARCHAR,
            state                    VARCHAR NOT NULL,
            entity_type              VARCHAR NOT NULL,
            well_name                VARCHAR,
            operator                 VARCHAR,
            county                   VARCHAR,
            field_name               VARCHAR,
            basin                    VARCHAR,
            well_type                VARCHAR,
            well_status              VARCHAR,
            latitude                 DOUBLE,
            longitude                DOUBLE,
            nri                      DOUBLE,
            wi                       DOUBLE,
            orri                     DOUBLE,
            interest_effective_date   DATE,
            loe_monthly              DOUBLE,
            production_tax_rate      DOUBLE,
            ad_valorem_annual        DOUBLE,
            first_production_date    DATE,
            last_production_date     DATE,
            vintage                  INTEGER,
            created_at               TIMESTAMP NOT NULL,
            updated_at               TIMESTAMP NOT NULL
        )
        """,
        # ------------------------------------------------------------------
        # 2. res_db_cases — Evaluation Cases
        # ------------------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS res_db_cases (
            case_id                  VARCHAR NOT NULL PRIMARY KEY,
            entity_key               VARCHAR NOT NULL,
            scenario_name            VARCHAR NOT NULL,
            scenario_type            VARCHAR NOT NULL,
            parent_case_id           VARCHAR,
            decline_model            VARCHAR,
            qi_oil                   DOUBLE,
            qi_gas                   DOUBLE,
            di                       DOUBLE,
            b_factor                 DOUBLE,
            d_min                    DOUBLE,
            decline_start_date       DATE,
            fit_r_squared            DOUBLE,
            fit_rmse                 DOUBLE,
            reserves_category        VARCHAR,
            eur_oil_bbl              DOUBLE,
            eur_gas_mcf              DOUBLE,
            remaining_oil_bbl        DOUBLE,
            remaining_gas_mcf        DOUBLE,
            price_deck_id            VARCHAR,
            economic_limit_date      DATE,
            economic_life_months     INTEGER,
            pv10                     DOUBLE,
            npv_undiscounted         DOUBLE,
            irr                      DOUBLE,
            effective_date           DATE NOT NULL,
            notes                    VARCHAR,
            created_at               TIMESTAMP NOT NULL,
            updated_at               TIMESTAMP NOT NULL
        )
        """,
        # ------------------------------------------------------------------
        # 3. res_db_forecast_series — Monthly Forecast Detail
        # ------------------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS res_db_forecast_series (
            case_id                  VARCHAR NOT NULL,
            forecast_month           DATE NOT NULL,
            months_on_production     INTEGER NOT NULL,
            oil_bbl                  DOUBLE,
            gas_mcf                  DOUBLE,
            water_bbl                DOUBLE,
            cumulative_oil_bbl       DOUBLE,
            cumulative_gas_mcf       DOUBLE,
            gross_revenue            DOUBLE,
            net_revenue              DOUBLE,
            production_tax           DOUBLE,
            opex                     DOUBLE,
            net_cash_flow            DOUBLE,
            discount_factor          DOUBLE,
            discounted_cash_flow     DOUBLE,
            is_economic_limit        BOOLEAN NOT NULL,
            PRIMARY KEY (case_id, forecast_month)
        )
        """,
        # ------------------------------------------------------------------
        # 4. res_db_price_decks — Pricing
        # ------------------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS res_db_price_decks (
            price_deck_id            VARCHAR NOT NULL,
            forecast_month           DATE NOT NULL,
            deck_type                VARCHAR NOT NULL,
            oil_price_bbl            DOUBLE,
            gas_price_mcf            DOUBLE,
            ngl_price_bbl            DOUBLE,
            oil_differential         DOUBLE,
            gas_differential         DOUBLE,
            created_at               TIMESTAMP NOT NULL,
            PRIMARY KEY (price_deck_id, forecast_month)
        )
        """,
        # ------------------------------------------------------------------
        # 5. res_db_schema_registry — Table-level metadata
        # ------------------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS res_db_schema_registry (
            table_name               VARCHAR NOT NULL PRIMARY KEY,
            description              VARCHAR,
            primary_key              VARCHAR,
            grain                    VARCHAR
        )
        """,
        # ------------------------------------------------------------------
        # 6. res_db_schema_registry_columns — Column-level metadata
        # ------------------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS res_db_schema_registry_columns (
            table_name               VARCHAR NOT NULL,
            column_name              VARCHAR NOT NULL,
            data_type                VARCHAR,
            nullable                 BOOLEAN,
            ordinal_position         INTEGER,
            description              VARCHAR,
            PRIMARY KEY (table_name, column_name)
        )
        """,
    ]


# Registry rows for the 4 data tables
REGISTRY_TABLES = [
    (
        "res_db_properties",
        "Well master. One row per well/lease. Agent entry point for any evaluation.",
        "entity_key",
        "one row per entity (well or lease)",
    ),
    (
        "res_db_cases",
        "Evaluation cases. One row per well x scenario. Decline params, reserves, and economics in one wide row.",
        "case_id",
        "one row per entity per scenario",
    ),
    (
        "res_db_forecast_series",
        "Monthly forecast detail. One row per case x month. Time series behind each case.",
        "case_id, forecast_month",
        "one row per case per month",
    ),
    (
        "res_db_price_decks",
        "Pricing. One row per deck x month. Oil/gas/NGL prices and differentials.",
        "price_deck_id, forecast_month",
        "one row per price deck per month",
    ),
]

# Column metadata: (table_name, column_name, data_type, nullable, ordinal, description)
REGISTRY_COLUMNS = [
    # --- res_db_properties (25 columns) ---
    ("res_db_properties", "entity_key", "VARCHAR", False, 1, "PK. Same as production_monthly"),
    ("res_db_properties", "api_number", "VARCHAR", True, 2, "API well number NN-NNN-NNNNN"),
    ("res_db_properties", "state", "VARCHAR", False, 3, "TX, NM, OK"),
    ("res_db_properties", "entity_type", "VARCHAR", False, 4, "well or lease"),
    ("res_db_properties", "well_name", "VARCHAR", True, 5, "Well/lease name"),
    ("res_db_properties", "operator", "VARCHAR", True, 6, "Operator name"),
    ("res_db_properties", "county", "VARCHAR", True, 7, "County"),
    ("res_db_properties", "field_name", "VARCHAR", True, 8, "Field"),
    ("res_db_properties", "basin", "VARCHAR", True, 9, "Basin"),
    ("res_db_properties", "well_type", "VARCHAR", True, 10, "OIL, GAS, INJ, OTHER"),
    ("res_db_properties", "well_status", "VARCHAR", True, 11, "ACTIVE, SHUT-IN, P&A"),
    ("res_db_properties", "latitude", "DOUBLE", True, 12, "WGS 84"),
    ("res_db_properties", "longitude", "DOUBLE", True, 13, "WGS 84"),
    ("res_db_properties", "nri", "DOUBLE", True, 14, "Net revenue interest (0-1)"),
    ("res_db_properties", "wi", "DOUBLE", True, 15, "Working interest (0-1)"),
    ("res_db_properties", "orri", "DOUBLE", True, 16, "Overriding royalty interest (0-1)"),
    ("res_db_properties", "interest_effective_date", "DATE", True, 17, "When current interests took effect"),
    ("res_db_properties", "loe_monthly", "DOUBLE", True, 18, "Lease operating expense $/month"),
    ("res_db_properties", "production_tax_rate", "DOUBLE", True, 19, "Production/severance tax rate (0-1)"),
    ("res_db_properties", "ad_valorem_annual", "DOUBLE", True, 20, "Ad valorem tax $/year"),
    ("res_db_properties", "first_production_date", "DATE", True, 21, "Earliest production_date from warehouse"),
    ("res_db_properties", "last_production_date", "DATE", True, 22, "Latest production_date from warehouse"),
    ("res_db_properties", "vintage", "INTEGER", True, 23, "First year with oil or gas > 0"),
    ("res_db_properties", "created_at", "TIMESTAMP", False, 24, "UTC row creation"),
    ("res_db_properties", "updated_at", "TIMESTAMP", False, 25, "UTC last update"),
    # --- res_db_cases (29 columns) ---
    ("res_db_cases", "case_id", "VARCHAR", False, 1, "PK. {entity_key}::{scenario_name}"),
    ("res_db_cases", "entity_key", "VARCHAR", False, 2, "FK -> res_db_properties"),
    ("res_db_cases", "scenario_name", "VARCHAR", False, 3, 'e.g. "base", "high_price", "low_decline"'),
    ("res_db_cases", "scenario_type", "VARCHAR", False, 4, "base, upside, downside, sensitivity"),
    ("res_db_cases", "parent_case_id", "VARCHAR", True, 5, "FK -> self. Cloned from which case"),
    ("res_db_cases", "decline_model", "VARCHAR", True, 6, "exponential, hyperbolic, harmonic"),
    ("res_db_cases", "qi_oil", "DOUBLE", True, 7, "Initial oil rate (BBL/month)"),
    ("res_db_cases", "qi_gas", "DOUBLE", True, 8, "Initial gas rate (MCF/month)"),
    ("res_db_cases", "di", "DOUBLE", True, 9, "Initial decline rate (annual, decimal)"),
    ("res_db_cases", "b_factor", "DOUBLE", True, 10, "Arps b exponent (0-2)"),
    ("res_db_cases", "d_min", "DOUBLE", True, 11, "Minimum terminal decline rate (annual)"),
    ("res_db_cases", "decline_start_date", "DATE", True, 12, "When forecast begins"),
    ("res_db_cases", "fit_r_squared", "DOUBLE", True, 13, "Goodness of fit (0-1)"),
    ("res_db_cases", "fit_rmse", "DOUBLE", True, 14, "Root mean square error"),
    ("res_db_cases", "reserves_category", "VARCHAR", True, 15, "PDP, PDNP, PUD, PROBABLE, POSSIBLE"),
    ("res_db_cases", "eur_oil_bbl", "DOUBLE", True, 16, "Estimated ultimate recovery - oil"),
    ("res_db_cases", "eur_gas_mcf", "DOUBLE", True, 17, "Estimated ultimate recovery - gas"),
    ("res_db_cases", "remaining_oil_bbl", "DOUBLE", True, 18, "EUR minus cumulative to date"),
    ("res_db_cases", "remaining_gas_mcf", "DOUBLE", True, 19, "EUR minus cumulative to date"),
    ("res_db_cases", "price_deck_id", "VARCHAR", True, 20, "FK -> res_db_price_decks"),
    ("res_db_cases", "economic_limit_date", "DATE", True, 21, "Month where revenue <= opex"),
    ("res_db_cases", "economic_life_months", "INTEGER", True, 22, "Months from decline_start to economic limit"),
    ("res_db_cases", "pv10", "DOUBLE", True, 23, "Present value at 10% discount ($)"),
    ("res_db_cases", "npv_undiscounted", "DOUBLE", True, 24, "Undiscounted net cash flow ($)"),
    ("res_db_cases", "irr", "DOUBLE", True, 25, "Internal rate of return (decimal)"),
    ("res_db_cases", "effective_date", "DATE", False, 26, "Evaluation as-of date"),
    ("res_db_cases", "notes", "VARCHAR", True, 27, "Free text - agent reasoning"),
    ("res_db_cases", "created_at", "TIMESTAMP", False, 28, "UTC row creation"),
    ("res_db_cases", "updated_at", "TIMESTAMP", False, 29, "UTC last update"),
    # --- res_db_forecast_series (16 columns) ---
    ("res_db_forecast_series", "case_id", "VARCHAR", False, 1, "PK part 1. FK -> res_db_cases"),
    ("res_db_forecast_series", "forecast_month", "DATE", False, 2, "PK part 2. First of month"),
    ("res_db_forecast_series", "months_on_production", "INTEGER", False, 3, "Sequential month from decline_start_date"),
    ("res_db_forecast_series", "oil_bbl", "DOUBLE", True, 4, "Forecast oil production"),
    ("res_db_forecast_series", "gas_mcf", "DOUBLE", True, 5, "Forecast gas production"),
    ("res_db_forecast_series", "water_bbl", "DOUBLE", True, 6, "Forecast water production"),
    ("res_db_forecast_series", "cumulative_oil_bbl", "DOUBLE", True, 7, "Running sum oil from decline start"),
    ("res_db_forecast_series", "cumulative_gas_mcf", "DOUBLE", True, 8, "Running sum gas from decline start"),
    ("res_db_forecast_series", "gross_revenue", "DOUBLE", True, 9, "(oil x price) + (gas x price)"),
    ("res_db_forecast_series", "net_revenue", "DOUBLE", True, 10, "gross_revenue x NRI"),
    ("res_db_forecast_series", "production_tax", "DOUBLE", True, 11, "net_revenue x tax_rate"),
    ("res_db_forecast_series", "opex", "DOUBLE", True, 12, "LOE + ad_valorem/12"),
    ("res_db_forecast_series", "net_cash_flow", "DOUBLE", True, 13, "net_revenue - production_tax - opex"),
    ("res_db_forecast_series", "discount_factor", "DOUBLE", True, 14, "1 / (1 + 0.10)^(months/12)"),
    ("res_db_forecast_series", "discounted_cash_flow", "DOUBLE", True, 15, "net_cash_flow x discount_factor"),
    ("res_db_forecast_series", "is_economic_limit", "BOOLEAN", False, 16, "True on month where net_cash_flow <= 0"),
    # --- res_db_price_decks (9 columns) ---
    ("res_db_price_decks", "price_deck_id", "VARCHAR", False, 1, "PK part 1. e.g. sec-2025-12-31"),
    ("res_db_price_decks", "forecast_month", "DATE", False, 2, "PK part 2. First of month"),
    ("res_db_price_decks", "deck_type", "VARCHAR", False, 3, "sec, strip, forecast"),
    ("res_db_price_decks", "oil_price_bbl", "DOUBLE", True, 4, "$/BBL"),
    ("res_db_price_decks", "gas_price_mcf", "DOUBLE", True, 5, "$/MCF"),
    ("res_db_price_decks", "ngl_price_bbl", "DOUBLE", True, 6, "$/BBL"),
    ("res_db_price_decks", "oil_differential", "DOUBLE", True, 7, "Basin/transport deduct $/BBL"),
    ("res_db_price_decks", "gas_differential", "DOUBLE", True, 8, "Basin/transport deduct $/MCF"),
    ("res_db_price_decks", "created_at", "TIMESTAMP", False, 9, "UTC row creation"),
]


def init_res_db() -> None:
    """Create res_db.duckdb and initialize all tables + schema registry."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    try:
        # Create all 6 tables
        for ddl in build_ddl():
            con.execute(ddl)

        # Populate schema registry (tables)
        con.executemany("""
            INSERT OR REPLACE INTO res_db_schema_registry
                (table_name, description, primary_key, grain)
            VALUES (?, ?, ?, ?)
        """, REGISTRY_TABLES)

        # Populate schema registry (columns)
        con.executemany("""
            INSERT OR REPLACE INTO res_db_schema_registry_columns
                (table_name, column_name, data_type, nullable, ordinal_position, description)
            VALUES (?, ?, ?, ?, ?, ?)
        """, REGISTRY_COLUMNS)

        # Validation: print table counts
        print(f"Database: {DB_PATH}")
        print()
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name"
        ).fetchall()
        print(f"Tables created: {len(tables)}")
        for (t,) in tables:
            count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t}: {count} rows")

        # Extra validation: check registry completeness
        reg_tables = con.execute("SELECT COUNT(*) FROM res_db_schema_registry").fetchone()[0]
        reg_cols = con.execute("SELECT COUNT(*) FROM res_db_schema_registry_columns").fetchone()[0]
        print()
        print(f"Schema registry: {reg_tables} tables, {reg_cols} columns registered")

    finally:
        con.close()


if __name__ == "__main__":
    init_res_db()
