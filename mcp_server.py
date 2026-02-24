#!/usr/bin/env python3
"""MCP server for querying the oil & gas production DuckDB warehouse.

Exposes read-only SQL access to gold-layer tables:
  - production_monthly    (wide fact table, the default for all queries)
  - decline_curve_inputs  (well-only subset for DCA)
  - schema_registry       (table-level metadata)
  - schema_registry_columns (column-level metadata)

Usage (Claude Code):
  Configured as an MCP server in .claude/settings.local.json.
  Runs via stdio transport.
"""

import re
from pathlib import Path

import duckdb
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
DUCKDB_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"

WRITE_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|MERGE|ATTACH|DETACH|COPY\s+.*\s+TO)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="data-warehouse",
    instructions=(
        "Query the oil & gas production data warehouse. "
        "Start with list_tables to discover available tables, then use query to run SQL. "
        "The main table is production_monthly — query it with GROUP BY for aggregations. "
        "All access is read-only."
    ),
)


def _connect() -> duckdb.DuckDBPyConnection:
    """Open a read-only connection to the warehouse."""
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


@mcp.tool()
def list_tables() -> str:
    """List all tables in the warehouse with their descriptions and row counts."""
    con = _connect()
    try:
        tables = con.execute(
            "SELECT table_name, description, primary_key, grain FROM schema_registry ORDER BY table_name"
        ).fetchall()
        rows = con.execute(
            "SELECT table_name, estimated_size FROM duckdb_tables() WHERE schema_name = 'main'"
        ).fetchall()
        row_counts = {r[0]: r[1] for r in rows}

        lines = []
        for name, desc, pk, grain in tables:
            count = row_counts.get(name, "?")
            lines.append(f"## {name}\n- Rows: {count}\n- PK: {pk}\n- Grain: {grain}\n- {desc}\n")
        return "\n".join(lines)
    finally:
        con.close()


@mcp.tool()
def describe_table(table_name: str) -> str:
    """Show column details for a specific table.

    Args:
        table_name: Name of the table (e.g. production_monthly, decline_curve_inputs).
    """
    con = _connect()
    try:
        cols = con.execute(
            "SELECT column_name, data_type, nullable, description "
            "FROM schema_registry_columns WHERE table_name = ? ORDER BY ordinal_position",
            [table_name],
        ).fetchall()
        if not cols:
            return f"Table '{table_name}' not found. Use list_tables to see available tables."
        lines = [f"| Column | Type | Nullable | Description |", f"|--------|------|----------|-------------|"]
        for name, dtype, nullable, desc in cols:
            lines.append(f"| {name} | {dtype} | {nullable} | {desc} |")
        return "\n".join(lines)
    finally:
        con.close()


@mcp.tool()
def query(sql: str) -> str:
    """Execute a read-only SQL query against the DuckDB warehouse.

    Args:
        sql: SQL SELECT query. Write operations (INSERT, UPDATE, DELETE, DROP, etc.) are blocked.
    """
    if WRITE_PATTERN.search(sql):
        return "Error: Write operations are not allowed. This server is read-only."

    con = _connect()
    try:
        result = con.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchmany(1000)
        if not rows:
            return "Query returned 0 rows."

        # Format as markdown table
        lines = ["| " + " | ".join(columns) + " |"]
        lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
        for row in rows:
            lines.append("| " + " | ".join(str(v) for v in row) + " |")

        total = len(rows)
        if total == 1000:
            lines.append(f"\n(Showing first 1,000 rows. Add LIMIT to your query for fewer results.)")
        else:
            lines.append(f"\n({total} rows)")
        return "\n".join(lines)
    except Exception as e:
        return f"Query error: {e}"
    finally:
        con.close()


if __name__ == "__main__":
    mcp.run(transport="stdio")
