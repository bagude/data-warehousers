"""Agent-optimized DCA wrapper around petbox-dca.

Thin orchestration layer that composes petbox-dca's Arps math with
model creation, forecast generation, and curve fitting. Every function
is pure — no database dependency.

Usage from MCP tools:

    model = create_model("hyperbolic", qi=500, di=0.65, b=0.8, d_min=0.06)
    forecast = generate_forecast(model, months=360)
    fit = fit_arps(production, model_type="hyperbolic")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
from petbox import dca


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

DeclineModelType = Literal["exponential", "hyperbolic", "harmonic"]


@dataclass(frozen=True)
class ForecastMonth:
    """One month of forecast output."""

    month_index: int         # 1-based sequential month
    oil_bbl: float           # monthly oil volume
    gas_mcf: float           # monthly gas volume
    cumulative_oil_bbl: float
    cumulative_gas_mcf: float


# ---------------------------------------------------------------------------
# Model creation
# ---------------------------------------------------------------------------

def create_model(
    decline_model: DeclineModelType,
    qi_oil: float,
    di: float,
    b: float = 0.0,
    d_min: float = 0.05,
    qi_gas: float = 0.0,
) -> dict:
    """Create a petbox-dca model from standard Arps parameters.

    Parameters
    ----------
    decline_model : str
        "exponential", "hyperbolic", or "harmonic".
    qi_oil : float
        Initial oil rate in BBL/month.
    di : float
        Initial nominal decline rate, annual (e.g. 0.65 = 65%/yr).
    b : float
        Arps b-exponent. 0 = exponential, 1 = harmonic, 0<b<1 typical hyperbolic.
        For shale wells b can exceed 1 (use with d_min to cap).
    d_min : float
        Terminal decline rate, annual. Used for modified hyperbolic (b > 0).
        Ignored when decline_model is "exponential".
    qi_gas : float
        Initial gas rate in MCF/month. If 0, gas forecast is zeros.

    Returns
    -------
    dict with keys: oil_model, gas_model, params
    """
    if decline_model == "exponential":
        b = 0.0

    if decline_model == "harmonic":
        b = 1.0

    # petbox-dca MH takes qi in units/day, Di as nominal annual, bi, Dterm
    # Convert monthly rates to daily for petbox-dca
    qi_oil_daily = qi_oil / 30.4375  # avg days per month
    qi_gas_daily = qi_gas / 30.4375

    oil_model = dca.MH(qi=qi_oil_daily, Di=di, bi=b, Dterm=d_min)

    gas_model = None
    if qi_gas > 0:
        gas_model = dca.MH(qi=qi_gas_daily, Di=di, bi=b, Dterm=d_min)

    return {
        "oil_model": oil_model,
        "gas_model": gas_model,
        "params": {
            "decline_model": decline_model,
            "qi_oil": qi_oil,
            "qi_gas": qi_gas,
            "di": di,
            "b": b,
            "d_min": d_min,
        },
    }


# ---------------------------------------------------------------------------
# Forecast generation
# ---------------------------------------------------------------------------

def generate_forecast(
    model: dict,
    months: int = 600,
) -> list[ForecastMonth]:
    """Generate monthly production forecast from a decline model.

    Parameters
    ----------
    model : dict
        Output from create_model().
    months : int
        Number of forecast months (default 600 = 50 years).

    Returns
    -------
    List of ForecastMonth dataclasses, one per month.
    """
    oil_model = model["oil_model"]
    gas_model = model["gas_model"]

    # Build time array in days at monthly intervals.
    # Start at 1 month (skip t=0 which gives zero interval volume).
    days_per_month = 30.4375
    t = np.arange(1, months + 1) * days_per_month

    oil_monthly = oil_model.monthly_vol(t)
    oil_cumulative = np.cumsum(oil_monthly)

    if gas_model is not None:
        gas_monthly = gas_model.monthly_vol(t)
        gas_cumulative = np.cumsum(gas_monthly)
    else:
        gas_monthly = np.zeros(len(oil_monthly))
        gas_cumulative = np.zeros(len(oil_monthly))

    forecast = []
    for i in range(len(oil_monthly)):
        forecast.append(ForecastMonth(
            month_index=i + 1,
            oil_bbl=float(oil_monthly[i]),
            gas_mcf=float(gas_monthly[i]),
            cumulative_oil_bbl=float(oil_cumulative[i]),
            cumulative_gas_mcf=float(gas_cumulative[i]),
        ))

    return forecast


# ---------------------------------------------------------------------------
# Fitting (auto-fit Arps to production history)
# ---------------------------------------------------------------------------

def fit_arps(
    production: list[float],
    time_months: list[int] | None = None,
    model_type: DeclineModelType = "hyperbolic",
) -> dict:
    """Fit Arps decline parameters to historical monthly production.

    Uses scipy.optimize.curve_fit to find best qi, Di, b for the
    given production history.

    Parameters
    ----------
    production : list[float]
        Monthly production volumes (BBL or MCF), in chronological order.
        Zeros and NaNs are excluded from fitting.
    time_months : list[int] | None
        Month indices (1-based). If None, assumes sequential 1..N.
    model_type : str
        "exponential", "hyperbolic", or "harmonic".

    Returns
    -------
    dict with: qi, di, b, d_min, r_squared, rmse, decline_model
    """
    from scipy.optimize import curve_fit

    prod = np.array(production, dtype=float)

    if time_months is None:
        t_months = np.arange(1, len(prod) + 1)
    else:
        t_months = np.array(time_months)

    # Filter out zeros and NaNs
    mask = (prod > 0) & ~np.isnan(prod)
    prod_clean = prod[mask]
    t_clean = t_months[mask]

    if len(prod_clean) < 3:
        raise ValueError("Need at least 3 non-zero production months to fit.")

    # Convert monthly volumes to daily rates for fitting
    prod_daily = prod_clean / 30.4375

    # Time in days for petbox-dca
    t_days = t_clean * 30.4375

    qi_guess = float(prod_daily[0])

    if model_type == "exponential":
        def _rate_func(t, qi, di):
            m = dca.MH(qi=qi, Di=di, bi=0.0, Dterm=0.0)
            return m.rate(t)

        try:
            popt, _ = curve_fit(
                _rate_func, t_days, prod_daily,
                p0=[qi_guess, 0.5],
                bounds=([0.001, 0.001], [qi_guess * 5, 5.0]),
                maxfev=5000,
            )
        except RuntimeError as e:
            raise ValueError(f"Curve fit did not converge: {e}") from e

        qi_fit, di_fit = popt
        b_fit = 0.0

    elif model_type == "harmonic":
        def _rate_func(t, qi, di):
            m = dca.MH(qi=qi, Di=di, bi=1.0, Dterm=0.05)
            return m.rate(t)

        try:
            popt, _ = curve_fit(
                _rate_func, t_days, prod_daily,
                p0=[qi_guess, 0.5],
                bounds=([0.001, 0.001], [qi_guess * 5, 5.0]),
                maxfev=5000,
            )
        except RuntimeError as e:
            raise ValueError(f"Curve fit did not converge: {e}") from e

        qi_fit, di_fit = popt
        b_fit = 1.0

    else:  # hyperbolic
        def _rate_func(t, qi, di, b):
            m = dca.MH(qi=qi, Di=di, bi=b, Dterm=0.05)
            return m.rate(t)

        try:
            popt, _ = curve_fit(
                _rate_func, t_days, prod_daily,
                p0=[qi_guess, 0.5, 0.5],
                bounds=([0.001, 0.001, 0.001], [qi_guess * 5, 5.0, 2.0]),
                maxfev=5000,
            )
        except RuntimeError as e:
            raise ValueError(f"Curve fit did not converge: {e}") from e

        qi_fit, di_fit, b_fit = popt

    # Convert fitted qi back to monthly
    qi_monthly = qi_fit * 30.4375

    # Calculate fit quality
    model = dca.MH(qi=qi_fit, Di=di_fit, bi=b_fit, Dterm=0.05)
    predicted = model.rate(t_days)
    residuals = prod_daily - predicted
    ss_res = np.sum(residuals ** 2)
    ss_tot = np.sum((prod_daily - np.mean(prod_daily)) ** 2)
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
    rmse = float(np.sqrt(np.mean(residuals ** 2)))

    return {
        "decline_model": model_type,
        "qi": qi_monthly,
        "di": float(di_fit),
        "b": float(b_fit),
        "d_min": 0.05,
        "r_squared": float(r_squared),
        "rmse": rmse * 30.4375,  # convert back to monthly units
    }
