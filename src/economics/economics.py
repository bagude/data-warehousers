"""Economic evaluation functions for decline curve forecasts.

Economic limit detection, EUR calculation, and discounted cash flow
analysis. Every function is pure — no database dependency.

Operates on ForecastMonth objects produced by src.decline.arps.

Usage from MCP tools:

    from src.decline.arps import create_model, generate_forecast
    from src.economics.economics import find_economic_limit, calculate_eur, calculate_pv10

    model = create_model("hyperbolic", qi=500, di=0.65, b=0.8, d_min=0.06)
    forecast = generate_forecast(model, months=360)
    limit = find_economic_limit(forecast, nri=0.80, oil_price=72.0,
                                gas_price=3.10, loe_monthly=5000,
                                production_tax_rate=0.046)
    eur = calculate_eur(forecast, limit)
    pv10 = calculate_pv10(forecast, limit, nri=0.80, oil_price=72.0,
                          gas_price=3.10, loe_monthly=5000,
                          production_tax_rate=0.046, ad_valorem_annual=0)
"""

from __future__ import annotations

from dataclasses import dataclass

from src.decline.arps import ForecastMonth


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EconomicResult:
    """Cash flow result for one forecast month."""

    month_index: int
    oil_bbl: float
    gas_mcf: float
    gross_revenue: float
    net_revenue: float
    production_tax: float
    opex: float
    net_cash_flow: float
    discount_factor: float
    discounted_cash_flow: float
    cumulative_oil_bbl: float
    cumulative_gas_mcf: float


# ---------------------------------------------------------------------------
# Economic limit
# ---------------------------------------------------------------------------

def find_economic_limit(
    forecast: list[ForecastMonth],
    nri: float,
    oil_price: float,
    gas_price: float,
    loe_monthly: float,
    production_tax_rate: float = 0.046,
    ad_valorem_annual: float = 0.0,
    oil_differential: float = 0.0,
    gas_differential: float = 0.0,
) -> int | None:
    """Find the month index where net cash flow first goes non-positive.

    Parameters
    ----------
    forecast : list[ForecastMonth]
        Output from generate_forecast().
    nri : float
        Net revenue interest (0-1).
    oil_price : float
        Oil price $/BBL (flat or average).
    gas_price : float
        Gas price $/MCF (flat or average).
    loe_monthly : float
        Lease operating expense $/month.
    production_tax_rate : float
        Severance/production tax as decimal (e.g. 0.046 = 4.6%).
    ad_valorem_annual : float
        Annual ad valorem tax $.
    oil_differential : float
        Basin/transport deduct $/BBL (subtracted from price).
    gas_differential : float
        Basin/transport deduct $/MCF (subtracted from price).

    Returns
    -------
    Month index (1-based) where economic limit is reached, or None if
    the well stays economic through the entire forecast.
    """
    adj_oil_price = oil_price - oil_differential
    adj_gas_price = gas_price - gas_differential
    monthly_ad_valorem = ad_valorem_annual / 12.0

    for fm in forecast:
        gross = (fm.oil_bbl * adj_oil_price) + (fm.gas_mcf * adj_gas_price)
        net = gross * nri
        tax = net * production_tax_rate
        opex = loe_monthly + monthly_ad_valorem
        ncf = net - tax - opex

        if ncf <= 0:
            return fm.month_index

    return None


# ---------------------------------------------------------------------------
# EUR
# ---------------------------------------------------------------------------

def calculate_eur(
    forecast: list[ForecastMonth],
    economic_limit_month: int | None,
) -> dict:
    """Calculate estimated ultimate recovery to economic limit.

    Parameters
    ----------
    forecast : list[ForecastMonth]
        Output from generate_forecast().
    economic_limit_month : int | None
        Month index from find_economic_limit(). If None, uses full forecast.

    Returns
    -------
    dict with eur_oil_bbl, eur_gas_mcf.
    """
    if economic_limit_month is None:
        last = forecast[-1]
    else:
        # month_index is 1-based, list is 0-based
        idx = min(economic_limit_month - 1, len(forecast) - 1)
        last = forecast[idx]

    return {
        "eur_oil_bbl": last.cumulative_oil_bbl,
        "eur_gas_mcf": last.cumulative_gas_mcf,
    }


# ---------------------------------------------------------------------------
# PV10 / discounted cash flow
# ---------------------------------------------------------------------------

def calculate_pv10(
    forecast: list[ForecastMonth],
    economic_limit_month: int | None,
    nri: float,
    oil_price: float,
    gas_price: float,
    loe_monthly: float,
    production_tax_rate: float = 0.046,
    ad_valorem_annual: float = 0.0,
    oil_differential: float = 0.0,
    gas_differential: float = 0.0,
    discount_rate: float = 0.10,
) -> dict:
    """Calculate PV10 and return full monthly cash flow series.

    Parameters
    ----------
    forecast : list[ForecastMonth]
        Output from generate_forecast().
    economic_limit_month : int | None
        Month index from find_economic_limit(). If None, uses full forecast.
    nri, oil_price, gas_price, loe_monthly, production_tax_rate,
    ad_valorem_annual, oil_differential, gas_differential : float
        Same as find_economic_limit().
    discount_rate : float
        Annual discount rate (default 0.10 = 10% for PV10).

    Returns
    -------
    dict with:
        pv10: float — sum of discounted cash flows
        npv_undiscounted: float — sum of undiscounted cash flows
        cash_flows: list[EconomicResult] — monthly detail
    """
    adj_oil_price = oil_price - oil_differential
    adj_gas_price = gas_price - gas_differential
    monthly_ad_valorem = ad_valorem_annual / 12.0

    cutoff = economic_limit_month if economic_limit_month else len(forecast)
    cutoff = min(cutoff, len(forecast))

    cash_flows: list[EconomicResult] = []
    total_pv = 0.0
    total_undiscounted = 0.0

    for i in range(cutoff):
        fm = forecast[i]
        gross = (fm.oil_bbl * adj_oil_price) + (fm.gas_mcf * adj_gas_price)
        net = gross * nri
        tax = net * production_tax_rate
        opex = loe_monthly + monthly_ad_valorem
        ncf = net - tax - opex

        # Monthly discount factor: 1 / (1 + r)^(month/12)
        df = 1.0 / ((1.0 + discount_rate) ** (fm.month_index / 12.0))
        dcf = ncf * df

        total_pv += dcf
        total_undiscounted += ncf

        cash_flows.append(EconomicResult(
            month_index=fm.month_index,
            oil_bbl=fm.oil_bbl,
            gas_mcf=fm.gas_mcf,
            gross_revenue=gross,
            net_revenue=net,
            production_tax=tax,
            opex=opex,
            net_cash_flow=ncf,
            discount_factor=df,
            discounted_cash_flow=dcf,
            cumulative_oil_bbl=fm.cumulative_oil_bbl,
            cumulative_gas_mcf=fm.cumulative_gas_mcf,
        ))

    return {
        "pv10": total_pv,
        "npv_undiscounted": total_undiscounted,
        "cash_flows": cash_flows,
    }
