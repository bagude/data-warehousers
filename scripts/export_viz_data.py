"""Regenerate viz_data.json and map_data.json from the gold DuckDB warehouse."""

import json
import duckdb
from src.utils.config import DUCKDB_PATH, PROJECT_ROOT

OUT_DIR = PROJECT_ROOT

con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

latest_year = con.execute(
    "select max(production_year) from monthly_production_by_county"
).fetchone()[0]
print(f"Latest production year: {latest_year}")

# =====================================================================
# viz_data.json
# =====================================================================

# 1. wellHistory: Top 5 wells by cumulative oil
top_wells = con.execute("""
    select entity_id, max(cumulative_oil_bbl) as cum_oil
    from well_production_history
    where entity_type = 'well' and cumulative_oil_bbl is not null
    group by entity_id
    order by cum_oil desc limit 5
""").fetchall()
top_ids = [r[0] for r in top_wells]

well_history = []
for eid in top_ids:
    rows = con.execute("""
        select entity_id, state, well_name, operator, county,
               production_date::text as production_date,
               months_on_production, total_oil_bbl, total_gas_mcf,
               cumulative_oil_bbl, cumulative_gas_mcf
        from well_production_history
        where entity_id = ? order by months_on_production
    """, [eid]).fetchall()
    cols = [
        "entity_id", "state", "well_name", "operator", "county",
        "production_date", "months_on_production", "total_oil_bbl",
        "total_gas_mcf", "cumulative_oil_bbl", "cumulative_gas_mcf",
    ]
    well_history.extend([dict(zip(cols, r)) for r in rows])
print(f"  wellHistory: {len(well_history)} records ({len(top_ids)} wells)")

# 2. county: Top 12 counties, last 4 years
top_counties = con.execute("""
    select county, state, sum(total_oil_bbl) as oil
    from monthly_production_by_county
    where production_year = ?
    group by county, state order by oil desc limit 12
""", [latest_year]).fetchall()

county_data = []
for (c, s, _) in top_counties:
    rows = con.execute("""
        select county, state, production_year,
               sum(total_oil_bbl) as oil, sum(total_gas_mcf) as gas,
               sum(total_water_bbl) as water, sum(entity_count) as entities
        from monthly_production_by_county
        where county = ? and state = ? and production_year >= ? - 3
        group by county, state, production_year
    """, [c, s, latest_year]).fetchall()
    cols = ["county", "state", "production_year", "oil", "gas", "water", "entities"]
    county_data.extend([dict(zip(cols, r)) for r in rows])
print(f"  county: {len(county_data)} records")

# 3. operator: Top 10 operators, last 8 years
top_ops = con.execute("""
    select operator, sum(total_oil_bbl) as oil
    from monthly_production_by_operator
    where production_year >= ? - 7
    group by operator order by oil desc limit 10
""", [latest_year]).fetchall()

op_data = []
for (op, _) in top_ops:
    rows = con.execute("""
        select operator, state, production_year,
               sum(total_oil_bbl) as oil, sum(total_gas_mcf) as gas,
               sum(entity_count) as entities
        from monthly_production_by_operator
        where operator = ? and production_year >= ? - 7
        group by operator, state, production_year
    """, [op, latest_year]).fetchall()
    cols = ["operator", "state", "production_year", "oil", "gas", "entities"]
    op_data.extend([dict(zip(cols, r)) for r in rows])
print(f"  operator: {len(op_data)} records")

# 4. field: Top 10 fields, last 8 years
top_fields = con.execute("""
    select field_name, sum(total_oil_bbl) as oil
    from monthly_production_by_field
    where production_year >= ? - 7
    group by field_name order by oil desc limit 10
""", [latest_year]).fetchall()

field_data = []
for (fld, _) in top_fields:
    rows = con.execute("""
        select field_name, basin, state, production_year,
               sum(total_oil_bbl) as oil, sum(total_gas_mcf) as gas,
               sum(entity_count) as entities
        from monthly_production_by_field
        where field_name = ? and production_year >= ? - 7
        group by field_name, basin, state, production_year
    """, [fld, latest_year]).fetchall()
    cols = ["field_name", "basin", "state", "production_year", "oil", "gas", "entities"]
    field_data.extend([dict(zip(cols, r)) for r in rows])
print(f"  field: {len(field_data)} records")

# 5. declineCurve: Top 5 wells by initial oil rate, first 60 months
top_dca = con.execute("""
    select api_number, state, max(initial_oil_rate) as init_rate
    from decline_curve_inputs
    where initial_oil_rate > 0
    group by api_number, state
    order by init_rate desc limit 5
""").fetchall()

dca_data = []
for (api, st, _) in top_dca:
    rows = con.execute("""
        select api_number, state, well_name, county, field_name,
               months_on_production, total_oil_bbl, total_gas_mcf,
               initial_oil_rate, oil_rate_pct_of_initial, cumulative_oil
        from decline_curve_inputs
        where api_number = ? and state = ? and months_on_production <= 60
        order by months_on_production
    """, [api, st]).fetchall()
    cols = [
        "api_number", "state", "well_name", "county", "field_name",
        "months_on_production", "total_oil_bbl", "total_gas_mcf",
        "initial_oil_rate", "oil_rate_pct_of_initial", "cumulative_oil",
    ]
    dca_data.extend([dict(zip(cols, r)) for r in rows])
print(f"  declineCurve: {len(dca_data)} records")

# 6. stateTotals
state_totals = con.execute("""
    select state, production_year,
           sum(total_oil_bbl) as oil, sum(total_gas_mcf) as gas
    from monthly_production_by_county
    group by state, production_year order by state, production_year
""").fetchall()
state_totals = [dict(zip(["state", "production_year", "oil", "gas"], r)) for r in state_totals]
print(f"  stateTotals: {len(state_totals)} records")

viz = {
    "wellHistory": well_history,
    "county": county_data,
    "operator": op_data,
    "field": field_data,
    "declineCurve": dca_data,
    "stateTotals": state_totals,
}
with open(OUT_DIR / "viz_data.json", "w") as f:
    json.dump(viz, f, default=str)
print("Wrote viz_data.json")

# =====================================================================
# map_data.json
# =====================================================================

# Counties: aggregate totals (last 3 years) with centroid from well coordinates
counties_map = con.execute("""
    with county_totals as (
        select county, state,
               sum(total_oil_bbl) as total_oil,
               sum(total_gas_mcf) as total_gas,
               sum(total_water_bbl) as total_water,
               sum(entity_count) as total_entities
        from monthly_production_by_county
        where production_year >= ? - 2
        group by county, state
    ),
    county_coords as (
        select county, state,
               avg(latitude) as lat, avg(longitude) as lng
        from well_production_history
        where latitude is not null and longitude is not null
        group by county, state
    )
    select ct.county, ct.state, ct.total_oil, ct.total_gas,
           ct.total_water, ct.total_entities,
           cc.lat, cc.lng
    from county_totals ct
    left join county_coords cc on ct.county = cc.county and ct.state = cc.state
""", [latest_year]).fetchall()

counties_list = [
    dict(zip(
        ["county", "state", "total_oil", "total_gas", "total_water",
         "total_entities", "lat", "lng"],
        r,
    ))
    for r in counties_map
]
n_with = sum(1 for c in counties_list if c["lat"] is not None)
print(f"  counties: {len(counties_list)} ({n_with} with coords)")

# Yearly county data for sparklines
yearly = con.execute("""
    select county, state, production_year,
           sum(total_oil_bbl) as oil, sum(total_gas_mcf) as gas,
           sum(entity_count) as entities
    from monthly_production_by_county
    group by county, state, production_year
    order by county, state, production_year
""").fetchall()
yearly_list = [
    dict(zip(["county", "state", "production_year", "oil", "gas", "entities"], r))
    for r in yearly
]
print(f"  yearly: {len(yearly_list)} records")

# State monthly
state_monthly = con.execute("""
    select state, production_date::text as production_date,
           sum(total_oil_bbl) as oil, sum(total_gas_mcf) as gas
    from monthly_production_by_county
    where production_year >= ? - 7
    group by state, production_date order by state, production_date
""", [latest_year]).fetchall()
state_monthly_list = [
    dict(zip(["state", "production_date", "oil", "gas"], r))
    for r in state_monthly
]
print(f"  stateMonthly: {len(state_monthly_list)} records")

# Individual wells: all with coords, columnar format for compact JSON
# Columnar layout eliminates repeated key names (~278K wells)
wells_map = con.execute("""
    with latest as (
        select entity_id, state, well_name, operator, county,
               round(latitude, 4) as latitude,
               round(longitude, 4) as longitude,
               well_type,
               round(coalesce(cumulative_oil_bbl, 0))::int as cum_oil,
               round(coalesce(cumulative_gas_mcf, 0))::int as cum_gas,
               round(coalesce(cumulative_water_bbl, 0))::int as cum_water,
               months_on_production,
               row_number() over (
                   partition by entity_id, state
                   order by production_date desc
               ) as rn
        from well_production_history
        where latitude is not null and longitude is not null
          and entity_type = 'well'
    )
    select entity_id, state,
           coalesce(left(well_name, 30), ''),
           coalesce(left(operator, 30), ''),
           coalesce(county, ''),
           latitude, longitude,
           coalesce(well_type, ''),
           cum_oil, cum_gas, cum_water,
           coalesce(months_on_production, 0)
    from latest where rn = 1
    order by cum_oil desc
""").fetchall()

# Build columnar format: {"cols": [...], "lat": [...], "lng": [...], ...}
wells_columnar = {
    "n": len(wells_map),
    "id":      [r[0] for r in wells_map],
    "state":   [r[1] for r in wells_map],
    "name":    [r[2] for r in wells_map],
    "op":      [r[3] for r in wells_map],
    "county":  [r[4] for r in wells_map],
    "lat":     [r[5] for r in wells_map],
    "lng":     [r[6] for r in wells_map],
    "type":    [r[7] for r in wells_map],
    "oil":     [r[8] for r in wells_map],
    "gas":     [r[9] for r in wells_map],
    "water":   [r[10] for r in wells_map],
    "months":  [r[11] for r in wells_map],
}
print(f"  wells: {len(wells_map)} wells with coordinates (columnar format)")

map_json = {
    "counties": counties_list,
    "yearly": yearly_list,
    "stateMonthly": state_monthly_list,
    "wells": wells_columnar,
}
with open(OUT_DIR / "map_data.json", "w") as f:
    json.dump(map_json, f, default=str)
print("Wrote map_data.json")

con.close()
print("Done!")
