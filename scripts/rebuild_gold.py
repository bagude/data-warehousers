"""One-shot gold rebuild script."""
from pathlib import Path
from orchestration.assets._gold_builder import build_gold

result = build_gold(
    duckdb_path=Path("data/warehouse.duckdb"),
    silver_parquet_glob="data/silver/production/**/*.parquet",
)

print(f"stg_rows: {result['stg_rows']}")
print(f"tests_passed: {result['tests_passed']}")
print(f"tests_total: {result['tests_total']}")
if result.get("test_failures"):
    for f in result["test_failures"]:
        print(f"  FAIL: {f}")
print("GOLD BUILD COMPLETE")
