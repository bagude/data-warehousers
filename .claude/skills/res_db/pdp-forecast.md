---
name: pdp-forecast
description: PDP well forecasting - decline fit, forecast, economics, reserves booking. Use when the user asks to forecast a well, evaluate PDP reserves, run decline analysis, calculate PV10, or assess well economics.
---

# PDP Well Forecasting

## When to Use

- User asks to forecast a well or evaluate its reserves
- User wants decline curve analysis on a producing well
- User asks for PV10, EUR, or economic limit calculations
- User wants to compare scenarios (base vs high price, etc.)

## Prerequisites

- `res-db` MCP server must be configured in `.mcp.json`
- Well must exist in the production warehouse (`production_monthly` table)
- `petbox-dca` and `scipy` must be installed

## Workflow

Follow these steps in order. Each step maps to one MCP tool call.

### Step 1: Register the Well

```
register_well(entity_key, nri, wi, loe_monthly, production_tax_rate, ad_valorem_annual)
```

Pull the well header from the warehouse into res_db. You MUST provide economic parameters. If unknown, ask the user or use these defaults:

| Parameter | Default | Notes |
|-----------|---------|-------|
| nri | 0.80 | Net revenue interest (80% = 1/8 royalty) |
| wi | 1.00 | Working interest |
| loe_monthly | 2000 | Lease operating expense $/month |
| production_tax_rate | 0.046 | TX default. See state table below. |
| ad_valorem_annual | 0 | Ad valorem tax $/year |

### Step 2: Fit Decline

```
fit_decline(entity_key, scenario_name="base", decline_model="hyperbolic")
```

This automatically:
1. Pulls production history from the warehouse
2. Detects the most recent significant peak (decline anchor)
3. Filters outliers (shut-ins, bad months)
4. Fits Arps decline via least-squares (qi, Di, b)
5. Creates a case in res_db

Returns: `case_id`, fitted params, R², RMSE.

### Step 3: Check Fit Quality

Review the R² and RMSE from Step 2.

| R² | Quality | Action |
|----|---------|--------|
| > 0.90 | Excellent | Proceed |
| 0.70 - 0.90 | Acceptable | Proceed with note |
| < 0.70 | Poor | Warn user. Try different decline_model or check for data quality issues. |

If the fit is poor, consider:
- Switching to `exponential` or `harmonic` model
- The well may have erratic production (workovers, artificial lift changes)
- Ask the user if they want to manually specify qi/Di/b

### Step 4: Set Price Deck

```
set_price_deck(price_deck_id, deck_type, prices)
```

**For SEC reserves reporting**: Use 12-month unweighted average of first-day-of-month prices. `deck_type = "sec"`. Flat price for all months.

**For internal evaluation**: Use strip pricing from NYMEX futures. `deck_type = "strip"`.

**For quick analysis**: Use current spot prices. `deck_type = "forecast"`.

`prices` is a JSON array:
```json
[{"forecast_month": "2026-01-01", "oil_price_bbl": 72.0, "gas_price_mcf": 3.10}]
```

For flat pricing, a single entry is sufficient — the forecast engine extrapolates the last available price.

### Step 5: Generate Forecast

```
generate_forecast(case_id, price_deck_id)
```

This:
1. Extrapolates the Arps decline into monthly volumes
2. Applies the price deck
3. Calculates monthly cash flows (revenue, taxes, opex, net)
4. Finds the economic limit (month where net cash flow <= 0)
5. Calculates EUR (cumulative to economic limit)
6. Calculates PV10 (10% annual discount)
7. Writes the full forecast series to res_db
8. Updates the case with EUR, PV10, economic limit date

### Step 6: Book Reserves

```
book_reserves(case_id, "PDP")
```

Sets the reserves category and calculates remaining reserves (EUR minus cumulative production to date).

**PDP criteria**: The well must be currently producing with existing equipment and operating methods.

| Category | When to Use |
|----------|-------------|
| PDP | Currently producing well |
| PDNP | Completed well, not yet producing (waiting on pipeline, etc.) |
| PUD | Undrilled location offsetting a producing well |
| PROBABLE | Incremental reserves beyond proved (2P - 1P) |
| POSSIBLE | Incremental reserves beyond probable (3P - 2P) |

### Step 7: Sensitivity Analysis (Optional)

```
clone_case(case_id, "high_price", "sensitivity", overrides)
generate_forecast(new_case_id, different_price_deck_id)
compare_cases("base_case_id,high_price_case_id")
```

Common scenarios:
- **High/low price**: Clone base case, apply different price deck
- **Aggressive decline**: Clone with `{"di": 0.8}` or `{"b_factor": 0.3}`
- **Conservative decline**: Clone with `{"di": 0.4}` or `{"b_factor": 1.2}`

## Domain Guidance

### Typical b-factors by Play

| Play/Basin | b-factor Range | Notes |
|-----------|---------------|-------|
| Permian (Wolfcamp, Bone Spring) | 1.0 - 1.5 | Shale, use d_min = 0.05-0.08 |
| Eagle Ford | 0.8 - 1.3 | Varies by window (oil vs gas) |
| Bakken | 1.0 - 1.4 | Tight oil |
| San Juan (conventional) | 0.3 - 0.8 | Conventional gas |
| Conventional vertical | 0.0 - 0.5 | Often exponential |
| Tight gas (Haynesville) | 0.8 - 1.2 | High initial decline |

### LOE Ranges

| Well Type | LOE $/month | LOE $/year |
|-----------|-------------|------------|
| Simple vertical | 800 - 1,500 | 10,000 - 18,000 |
| Horizontal shale | 1,500 - 3,000 | 18,000 - 36,000 |
| Artificial lift (rod pump) | 1,200 - 2,500 | 15,000 - 30,000 |
| ESP (electric submersible) | 2,000 - 4,000 | 24,000 - 48,000 |

### Production Tax Rates by State

| State | Oil Tax | Gas Tax |
|-------|---------|---------|
| TX | 4.6% | 7.5% |
| NM | 3.75% | 3.75% |
| OK | 7.0% | 7.0% |

### SEC Pricing Rule

For SEC reserves disclosures (PV10 in 10-K/10-Q):
- Use **unweighted arithmetic average** of first-day-of-month prices
- For the **12 months prior** to the report date
- Use the posted price for the field/area (not NYMEX)
- Apply appropriate differentials

## Quality Checks

After generating a forecast, verify:

| Check | Expected Range | If Outside Range |
|-------|---------------|------------------|
| R² | > 0.70 | Re-fit with different model or data window |
| EUR oil | Basin-appropriate | Compare against type curves or offset wells |
| Economic life | 5 - 40 years | Check LOE and pricing assumptions |
| PV10 | > 0 for PDP | If negative, well may be uneconomic — verify inputs |
| b-factor | 0 - 2.0 | If > 2.0, fit may be unstable — try exponential |
