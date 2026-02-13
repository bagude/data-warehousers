"""Canonical hashing for event amendment detection.

Spec:
- Scope: FACT_PRODUCTION_DETAIL measure columns only
- Column order: oil_bbl, gas_mcf, condensate_bbl, casinghead_gas_mcf, water_bbl, days_produced
- Floats: ROUND to 3 decimals, format as "{:.3f}"
- Integers: format as str, no decimals
- NULL: empty string ""
- Separator: pipe "|"
- Hash: SHA-256, lowercase hex, 64 chars
"""

from __future__ import annotations

import hashlib


def _fmt_float(val) -> str:
    if val is None:
        return ""
    return f"{round(float(val), 3):.3f}"


def _fmt_int(val) -> str:
    if val is None:
        return ""
    return str(int(val))


def compute_production_hash(
    oil_bbl,
    gas_mcf,
    condensate_bbl,
    casinghead_gas_mcf,
    water_bbl,
    days_produced,
) -> str:
    """Compute canonical SHA-256 hash for production event measures."""
    canonical = "|".join(
        [
            _fmt_float(oil_bbl),
            _fmt_float(gas_mcf),
            _fmt_float(condensate_bbl),
            _fmt_float(casinghead_gas_mcf),
            _fmt_float(water_bbl),
            _fmt_int(days_produced),
        ]
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
