#!/usr/bin/env python3
"""End-to-end integration test for the res_db economics pipeline.

Exercises the full PDP forecasting workflow:
  production history → peak detection → outlier filtering → Arps fit →
  forecast → economic limit → EUR → PV10

Uses a real well from the warehouse. No MCP server needed — calls the
economics functions directly.

Usage:
    python scripts/test_res_db_e2e.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.economics.arps import (
    create_model,
    generate_forecast,
    find_economic_limit,
    calculate_eur,
    calculate_pv10,
    fit_arps,
)
from src.economics.preprocessing import detect_decline_start, filter_outliers


WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"

# Economic assumptions for test
NRI = 0.80
OIL_PRICE = 70.0   # $/BBL
GAS_PRICE = 3.10    # $/MCF
LOE_MONTHLY = 2000  # $/month
TAX_RATE = 0.046    # TX default
DISCOUNT_RATE = 0.10


def find_test_well(con: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    """Find a well with >24 months of production history."""
    result = con.execute("""
        SELECT entity_key, well_name, state, COUNT(*) as months,
               SUM(total_oil_bbl) as total_oil
        FROM production_monthly
        WHERE entity_type = 'well'
          AND total_oil_bbl > 0
        GROUP BY entity_key, well_name, state
        HAVING COUNT(*) > 24
        ORDER BY total_oil DESC
        LIMIT 1
    """).fetchone()

    if result is None:
        print("FAIL: No well with >24 months found in warehouse.")
        sys.exit(1)

    entity_key, well_name, state, months, total_oil = result
    print(f"Test well: {entity_key} ({well_name}, {state})")
    print(f"  History: {months} months, {total_oil:,.0f} BBL cumulative oil")
    return entity_key, well_name


def pull_production(con: duckdb.DuckDBPyConnection, entity_key: str) -> list[float]:
    """Pull monthly oil production for a well."""
    rows = con.execute("""
        SELECT production_date, total_oil_bbl
        FROM production_monthly
        WHERE entity_key = ?
        ORDER BY production_date
    """, [entity_key]).fetchall()

    production = [float(row[1]) if row[1] is not None else 0.0 for row in rows]
    print(f"  Pulled {len(production)} months of production data")
    return production


def run_test() -> None:
    """Run the full PDP forecasting pipeline."""
    print("=" * 60)
    print("res_db E2E Integration Test")
    print("=" * 60)

    # Connect to warehouse
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)

    try:
        # Step 1: Find a test well
        print("\n--- Step 1: Find test well ---")
        entity_key, well_name = find_test_well(con)

        # Step 2: Pull production history
        print("\n--- Step 2: Pull production history ---")
        production = pull_production(con, entity_key)

    finally:
        con.close()

    # Step 3: Detect decline start
    print("\n--- Step 3: Detect decline start ---")
    anchor = detect_decline_start(production)
    print(f"  Peak index: {anchor.peak_index} (value: {anchor.peak_value:,.0f} BBL)")
    print(f"  All peaks detected: {anchor.all_peaks}")
    print(f"  Trimmed to {len(anchor.production_trimmed)} months from peak onward")

    # Step 4: Filter outliers
    print("\n--- Step 4: Filter outliers ---")
    filtered = filter_outliers(
        anchor.production_trimmed,
        time_indices=anchor.time_trimmed,
    )
    print(f"  Removed {filtered.outlier_count} outlier months")
    print(f"  Clean data: {len(filtered.production_clean)} months")

    if len(filtered.production_clean) < 3:
        print("FAIL: Too few clean production months after filtering.")
        sys.exit(1)

    # Step 5: Fit Arps decline
    print("\n--- Step 5: Fit Arps decline ---")
    try:
        fit = fit_arps(
            filtered.production_clean,
            time_months=filtered.time_clean,
            model_type="hyperbolic",
        )
    except ValueError as e:
        print(f"FAIL: Fit failed: {e}")
        sys.exit(1)

    print(f"  Model: {fit['decline_model']}")
    print(f"  qi:    {fit['qi']:,.1f} BBL/month")
    print(f"  Di:    {fit['di']:.4f} (annual)")
    print(f"  b:     {fit['b']:.4f}")
    print(f"  R²:    {fit['r_squared']:.4f}")
    print(f"  RMSE:  {fit['rmse']:,.1f} BBL/month")

    if fit["r_squared"] < 0.5:
        print("WARNING: Poor fit quality (R² < 0.5). Results may be unreliable.")

    # Step 6: Generate forecast
    print("\n--- Step 6: Generate forecast ---")
    model = create_model(
        decline_model=fit["decline_model"],
        qi_oil=fit["qi"],
        di=fit["di"],
        b=fit["b"],
        d_min=fit["d_min"],
    )
    forecast = generate_forecast(model, months=360)
    print(f"  Generated {len(forecast)} months of forecast")
    print(f"  Month 1:   {forecast[0].oil_bbl:,.0f} BBL")
    print(f"  Month 12:  {forecast[11].oil_bbl:,.0f} BBL")
    print(f"  Month 60:  {forecast[59].oil_bbl:,.0f} BBL")
    print(f"  Month 360: {forecast[-1].oil_bbl:,.0f} BBL")

    # Step 7: Find economic limit
    print("\n--- Step 7: Find economic limit ---")
    limit = find_economic_limit(
        forecast,
        nri=NRI,
        oil_price=OIL_PRICE,
        gas_price=GAS_PRICE,
        loe_monthly=LOE_MONTHLY,
        production_tax_rate=TAX_RATE,
    )
    if limit is not None:
        print(f"  Economic limit at month {limit} ({limit / 12:.1f} years)")
    else:
        print(f"  No economic limit reached in {len(forecast)} months")
        limit = len(forecast)

    # Step 8: Calculate EUR
    print("\n--- Step 8: Calculate EUR ---")
    eur = calculate_eur(forecast, limit)
    print(f"  EUR oil: {eur['eur_oil_bbl']:,.0f} BBL")
    print(f"  EUR gas: {eur['eur_gas_mcf']:,.0f} MCF")

    # Step 9: Calculate PV10
    print("\n--- Step 9: Calculate PV10 ---")
    pv_result = calculate_pv10(
        forecast,
        limit,
        nri=NRI,
        oil_price=OIL_PRICE,
        gas_price=GAS_PRICE,
        loe_monthly=LOE_MONTHLY,
        production_tax_rate=TAX_RATE,
        discount_rate=DISCOUNT_RATE,
    )
    print(f"  PV10:           ${pv_result['pv10']:,.0f}")
    print(f"  Undiscounted:   ${pv_result['npv_undiscounted']:,.0f}")
    print(f"  Cash flow months: {len(pv_result['cash_flows'])}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Well:           {entity_key} ({well_name})")
    print(f"  Decline model:  {fit['decline_model']}")
    print(f"  qi:             {fit['qi']:,.0f} BBL/month")
    print(f"  Di:             {fit['di']:.2%} annual")
    print(f"  b:              {fit['b']:.3f}")
    print(f"  R²:             {fit['r_squared']:.4f}")
    econ_life = limit if limit else len(forecast)
    print(f"  Economic life:  {econ_life} months ({econ_life / 12:.1f} years)")
    print(f"  EUR oil:        {eur['eur_oil_bbl']:,.0f} BBL")
    print(f"  PV10:           ${pv_result['pv10']:,.0f}")
    print(f"  Undiscounted:   ${pv_result['npv_undiscounted']:,.0f}")
    print("=" * 60)
    print("PASS")


if __name__ == "__main__":
    run_test()
