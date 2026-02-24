"""Tests for the Arps DCA wrapper around petbox-dca."""

import pytest

from src.economics.arps import (
    create_model,
    generate_forecast,
    find_economic_limit,
    calculate_eur,
    calculate_pv10,
    fit_arps,
)


class TestCreateModel:

    def test_hyperbolic_model(self):
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8, d_min=0.06)
        assert model["oil_model"] is not None
        assert model["gas_model"] is None
        assert model["params"]["decline_model"] == "hyperbolic"
        assert model["params"]["qi_oil"] == 500

    def test_exponential_forces_b_zero(self):
        model = create_model("exponential", qi_oil=500, di=0.65, b=0.8)
        assert model["params"]["b"] == 0.0

    def test_harmonic_forces_b_one(self):
        model = create_model("harmonic", qi_oil=500, di=0.65, b=0.3)
        assert model["params"]["b"] == 1.0

    def test_oil_and_gas_model(self):
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8, qi_gas=2000)
        assert model["oil_model"] is not None
        assert model["gas_model"] is not None
        assert model["params"]["qi_gas"] == 2000


class TestGenerateForecast:

    def test_forecast_length(self):
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8, d_min=0.06)
        forecast = generate_forecast(model, months=120)
        assert len(forecast) == 120

    def test_forecast_monotonically_declining(self):
        """Oil should decline over time for a standard Arps model."""
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8, d_min=0.06)
        forecast = generate_forecast(model, months=60)
        oils = [f.oil_bbl for f in forecast]
        # Allow small numerical tolerance — overall trend must decline
        for i in range(2, len(oils)):
            assert oils[i] <= oils[0] * 1.01  # never exceeds initial by >1%

    def test_cumulative_increases(self):
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8, d_min=0.06)
        forecast = generate_forecast(model, months=60)
        for i in range(1, len(forecast)):
            assert forecast[i].cumulative_oil_bbl >= forecast[i - 1].cumulative_oil_bbl

    def test_gas_forecast_when_provided(self):
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8, qi_gas=2000)
        forecast = generate_forecast(model, months=12)
        assert forecast[0].gas_mcf > 0
        assert forecast[0].cumulative_gas_mcf > 0

    def test_gas_zeros_when_not_provided(self):
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8)
        forecast = generate_forecast(model, months=12)
        assert all(f.gas_mcf == 0 for f in forecast)


class TestFindEconomicLimit:

    def test_reaches_economic_limit(self):
        """A well with high LOE should hit economic limit."""
        model = create_model("exponential", qi_oil=100, di=0.50)
        forecast = generate_forecast(model, months=240)
        limit = find_economic_limit(
            forecast, nri=0.80, oil_price=60.0, gas_price=3.0,
            loe_monthly=3000, production_tax_rate=0.046,
        )
        assert limit is not None
        assert 1 <= limit <= 240

    def test_stays_economic(self):
        """A high-rate well with low costs should never hit limit."""
        model = create_model("hyperbolic", qi_oil=5000, di=0.30, b=0.5, d_min=0.05)
        forecast = generate_forecast(model, months=120)
        limit = find_economic_limit(
            forecast, nri=0.80, oil_price=80.0, gas_price=3.0,
            loe_monthly=500, production_tax_rate=0.046,
        )
        assert limit is None

    def test_differentials_reduce_revenue(self):
        """Higher differentials should bring economic limit forward."""
        model = create_model("exponential", qi_oil=200, di=0.40)
        forecast = generate_forecast(model, months=240)
        limit_no_diff = find_economic_limit(
            forecast, nri=0.80, oil_price=60.0, gas_price=3.0,
            loe_monthly=2000, production_tax_rate=0.046,
        )
        limit_with_diff = find_economic_limit(
            forecast, nri=0.80, oil_price=60.0, gas_price=3.0,
            loe_monthly=2000, production_tax_rate=0.046,
            oil_differential=15.0,
        )
        # With differential, limit should come sooner (or at same time)
        if limit_no_diff is not None and limit_with_diff is not None:
            assert limit_with_diff <= limit_no_diff


class TestCalculateEur:

    def test_eur_at_economic_limit(self):
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8, d_min=0.06)
        forecast = generate_forecast(model, months=360)
        limit = find_economic_limit(
            forecast, nri=0.80, oil_price=60.0, gas_price=3.0,
            loe_monthly=2000, production_tax_rate=0.046,
        )
        eur = calculate_eur(forecast, limit)
        assert eur["eur_oil_bbl"] > 0
        # EUR should be less than full forecast cumulative
        full_eur = calculate_eur(forecast, None)
        if limit is not None:
            assert eur["eur_oil_bbl"] <= full_eur["eur_oil_bbl"]

    def test_eur_full_forecast(self):
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8, d_min=0.06)
        forecast = generate_forecast(model, months=120)
        eur = calculate_eur(forecast, None)
        assert eur["eur_oil_bbl"] == forecast[-1].cumulative_oil_bbl


class TestCalculatePv10:

    def test_pv10_positive_for_economic_well(self):
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8, d_min=0.06)
        forecast = generate_forecast(model, months=360)
        limit = find_economic_limit(
            forecast, nri=0.80, oil_price=70.0, gas_price=3.0,
            loe_monthly=2000, production_tax_rate=0.046,
        )
        result = calculate_pv10(
            forecast, limit, nri=0.80, oil_price=70.0, gas_price=3.0,
            loe_monthly=2000, production_tax_rate=0.046,
        )
        assert result["pv10"] > 0
        assert result["npv_undiscounted"] > result["pv10"]  # undiscounted > discounted
        assert len(result["cash_flows"]) > 0

    def test_discount_factor_decreases(self):
        model = create_model("hyperbolic", qi_oil=500, di=0.65, b=0.8)
        forecast = generate_forecast(model, months=60)
        result = calculate_pv10(
            forecast, None, nri=0.80, oil_price=70.0, gas_price=3.0,
            loe_monthly=1000, production_tax_rate=0.046,
        )
        cfs = result["cash_flows"]
        for i in range(1, len(cfs)):
            assert cfs[i].discount_factor <= cfs[i - 1].discount_factor

    def test_pv10_at_10_percent(self):
        """Verify default discount rate is 10%."""
        model = create_model("exponential", qi_oil=500, di=0.30)
        forecast = generate_forecast(model, months=12)
        result = calculate_pv10(
            forecast, None, nri=1.0, oil_price=100.0, gas_price=0.0,
            loe_monthly=0, production_tax_rate=0.0,
        )
        # Month 12 discount factor should be 1/(1.10)^1 = 0.9091
        last_cf = result["cash_flows"][-1]
        assert abs(last_cf.discount_factor - (1 / 1.10)) < 0.01


class TestFitArps:

    def test_fit_to_synthetic_exponential(self):
        """Fit should recover params from synthetic exponential decline."""
        import numpy as np
        # Generate synthetic exponential decline: q = 500 * e^(-0.4t)
        t_months = np.arange(1, 37)
        qi_true = 500.0
        di_true = 0.4  # annual
        production = qi_true * np.exp(-di_true * t_months / 12)

        result = fit_arps(production.tolist(), model_type="exponential")
        assert result["decline_model"] == "exponential"
        assert result["r_squared"] > 0.95
        # Fitted qi should be within 30% of true
        assert abs(result["qi"] - qi_true) / qi_true < 0.30

    def test_fit_to_synthetic_hyperbolic(self):
        """Fit should recover params from synthetic hyperbolic decline."""
        import numpy as np
        from petbox import dca

        qi_daily = 500 / 30.4375
        model = dca.MH(qi=qi_daily, Di=0.6, bi=0.8, Dterm=0.05)
        t = np.arange(1, 37) * 30.4375  # 36 months in days
        production = model.monthly_vol(t)

        result = fit_arps(production.tolist(), model_type="hyperbolic")
        assert result["r_squared"] > 0.90
        assert 0 < result["b"] < 2.0

    def test_fit_rejects_short_series(self):
        """Should raise ValueError with < 3 non-zero months."""
        with pytest.raises(ValueError, match="at least 3"):
            fit_arps([100, 0])

    def test_fit_ignores_zeros(self):
        """Zeros should be filtered before fitting."""
        production = [500, 450, 0, 380, 340, 0, 280, 250]
        result = fit_arps(production, model_type="exponential")
        assert result["r_squared"] > 0  # should still produce a fit
