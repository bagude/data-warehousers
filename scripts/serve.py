"""Dev server: static files + DuckDB API for well production history."""

import json
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import duckdb

from src.utils.config import DUCKDB_PATH, PROJECT_ROOT

con = duckdb.connect(str(DUCKDB_PATH), read_only=True)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(PROJECT_ROOT), **kwargs)

    def end_headers(self):
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/well":
            self.handle_well_api(parsed)
        else:
            super().do_GET()

    def handle_well_api(self, parsed):
        qs = parse_qs(parsed.query)
        entity_key = qs.get("id", [None])[0]
        if not entity_key:
            self._json_resp(400, {"error": "missing ?id= parameter"})
            return
        try:
            rows = con.execute("""
                select production_date::text,
                       coalesce(total_oil_bbl, 0),
                       coalesce(total_gas_mcf, 0),
                       coalesce(cumulative_oil_bbl, 0),
                       coalesce(cumulative_gas_mcf, 0),
                       coalesce(reported_month_index, 0)
                from production_monthly
                where entity_key = ? and entity_type = 'well'
                order by production_date
            """, [entity_key]).fetchall()

            data = {
                "n": len(rows),
                "dates":   [r[0] for r in rows],
                "oil":     [r[1] for r in rows],
                "gas":     [r[2] for r in rows],
                "cum_oil": [r[3] for r in rows],
                "cum_gas": [r[4] for r in rows],
                "months":  [r[5] for r in rows],
            }
            self._json_resp(200, data)
        except Exception as e:
            self._json_resp(500, {"error": str(e)})

    def _json_resp(self, code, data):
        body = json.dumps(data, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        # Quieter logging — only show API calls, not static files
        first = str(args[0]) if args else ""
        if "/api/" in first:
            super().log_message(fmt, *args)


if __name__ == "__main__":
    port = 8080
    print(f"DuckDB: {DUCKDB_PATH}")
    print(f"Serving {PROJECT_ROOT} on http://localhost:{port}")
    HTTPServer(("", port), Handler).serve_forever()
