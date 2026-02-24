"""Common production data schema for the silver layer.

This is the canonical schema that all state-specific parsers must conform to
when writing silver-layer Parquet files.  It defines the single, normalized
representation of monthly production records across Texas (RRC) and New Mexico
(OCD).

Design decisions (WO-006)
=========================

1. **Entity granularity — well vs. lease**

   Texas RRC reports oil production at *lease* level in the OG_LEASE_CYCLE
   table.  A single lease may cover multiple wells.  Gas wells are typically
   1:1 with leases, but oil leases are many-wells-to-one-lease.  New Mexico
   OCD reports production at *well* level (one row per API per month).

   Rather than force one granularity, the schema carries an ``entity_type``
   field ("well" or "lease") so downstream consumers know how to interpret
   each row.  The ``api_number`` field is populated when the entity is a
   well; ``lease_number`` and ``district`` are populated when the entity is
   a lease.

2. **Condensate — separate from oil**

   Texas RRC reports condensate (liquid hydrocarbons from gas wells)
   separately from crude oil.  New Mexico may combine them.  We keep
   ``condensate_bbl`` as a distinct field so that no information is lost
   at the silver layer.  The gold layer can combine oil + condensate when
   a unified "total liquids" metric is needed.

3. **Casinghead gas — separate from gas**

   Texas RRC reports casinghead gas (gas produced from oil wells)
   separately from gas-well gas.  We keep ``casinghead_gas_mcf`` as a
   distinct field for the same reason as condensate.

4. **NULL strategy**

   Fields that are not collected or not available for a given state are
   stored as NULL rather than zero.  NULL means "not reported / not
   applicable"; zero means "reported as zero production."

   Known NULLs by state:
   - TX: ``water_bbl`` (TX RRC does not collect water volumes),
     ``days_produced`` (not in standard PDQ tables).
   - NM: ``condensate_bbl`` (typically combined into oil),
     ``casinghead_gas_mcf`` (typically combined into gas),
     ``lease_number``, ``district`` (NM uses API, not lease/district).

5. **Units**

   All volume fields use the units standard in the U.S. oil & gas industry:
   - Oil / condensate / water: barrels (BBL)
   - Gas / casinghead gas: thousand cubic feet (MCF)
   These are the native units from both TX RRC and NM OCD, so no unit
   conversion is needed at the silver layer.

6. **production_date**

   Always the first day of the production month (e.g. 2025-01-01 for
   January 2025 production).  Constructed from CYCLE_YEAR + CYCLE_MONTH
   for TX, and from the month column in NM GO-TECH data.
"""

from __future__ import annotations

import pyarrow as pa

# ---------------------------------------------------------------------------
# Canonical silver-layer schema for monthly production records
# ---------------------------------------------------------------------------

PRODUCTION_SCHEMA = pa.schema(
    [
        # -- Identity & classification --
        pa.field("state", pa.string(), nullable=False, metadata={
            b"description": b"Two-letter state code: 'TX' or 'NM'",
        }),
        pa.field("entity_type", pa.string(), nullable=False, metadata={
            b"description": (
                b"Granularity of the record: 'well' (NM, TX gas wells) "
                b"or 'lease' (TX oil leases)"
            ),
        }),
        pa.field("api_number", pa.string(), metadata={
            b"description": (
                b"API well number in NN-NNN-NNNNN format. "
                b"Populated for well-level records. May be NULL for "
                b"TX lease-level records that span multiple wells."
            ),
        }),
        pa.field("lease_number", pa.string(), metadata={
            b"description": (
                b"TX RRC lease number. Populated for TX lease-level "
                b"records. NULL for NM well-level records."
            ),
        }),
        pa.field("district", pa.string(), metadata={
            b"description": (
                b"TX RRC district number (01-12, plus 6E, 7B, 7C, 8A, 9A). "
                b"NULL for NM records."
            ),
        }),

        # -- Descriptive attributes --
        pa.field("well_name", pa.string(), metadata={
            b"description": b"Well or lease name as reported by the operator.",
        }),
        pa.field("operator", pa.string(), metadata={
            b"description": b"Operator name (OPERATOR_NAME for TX, ogrid_name for NM).",
        }),
        pa.field("county", pa.string(), metadata={
            b"description": b"County name where the well or lease is located.",
        }),
        pa.field("field_name", pa.string(), metadata={
            b"description": (
                b"Oil/gas field name. FIELD_NAME for TX, pool/field for NM."
            ),
        }),
        pa.field("basin", pa.string(), metadata={
            b"description": (
                b"Basin name (e.g. Permian, San Juan). Derived from "
                b"county or field where possible. May be NULL."
            ),
        }),
        pa.field("well_type", pa.string(), metadata={
            b"description": (
                b"Well type classification: 'OIL', 'GAS', 'INJ', 'OTHER', etc. "
                b"Normalized from state-specific codes."
            ),
        }),
        pa.field("well_status", pa.string(), metadata={
            b"description": (
                b"Well operating status: 'ACTIVE', 'SHUT-IN', 'P&A', etc. "
                b"Normalized from state-specific codes."
            ),
        }),

        # -- Geographic coordinates --
        pa.field("latitude", pa.float64(), metadata={
            b"description": (
                b"Well/entity latitude in decimal degrees (WGS 84 / EPSG 4326). "
                b"Sourced from NM OCD ArcGIS well master or TX RRC drilling "
                b"permit data. NULL when coordinates are not available."
            ),
        }),
        pa.field("longitude", pa.float64(), metadata={
            b"description": (
                b"Well/entity longitude in decimal degrees (WGS 84 / EPSG 4326). "
                b"Sourced from NM OCD ArcGIS well master or TX RRC drilling "
                b"permit data. NULL when coordinates are not available."
            ),
        }),

        # -- Time --
        pa.field("production_date", pa.date32(), nullable=False, metadata={
            b"description": (
                b"First day of the production month (e.g. 2025-01-01 for "
                b"January 2025). Constructed from year+month fields."
            ),
        }),

        # -- Production volumes --
        pa.field("oil_bbl", pa.float64(), metadata={
            b"description": (
                b"Crude oil production in barrels. Does NOT include "
                b"condensate (see condensate_bbl). >= 0 or NULL."
            ),
        }),
        pa.field("gas_mcf", pa.float64(), metadata={
            b"description": (
                b"Gas well gas production in MCF (thousand cubic feet). "
                b"Does NOT include casinghead gas (see casinghead_gas_mcf). "
                b">= 0 or NULL."
            ),
        }),
        pa.field("condensate_bbl", pa.float64(), metadata={
            b"description": (
                b"Condensate production in barrels. Reported separately "
                b"by TX RRC. NULL for NM (typically included in oil_bbl). "
                b">= 0 or NULL."
            ),
        }),
        pa.field("casinghead_gas_mcf", pa.float64(), metadata={
            b"description": (
                b"Casinghead gas (gas from oil wells) in MCF. Reported "
                b"separately by TX RRC. NULL for NM (typically included "
                b"in gas_mcf). >= 0 or NULL."
            ),
        }),
        pa.field("water_bbl", pa.float64(), metadata={
            b"description": (
                b"Produced water in barrels. Available from NM OCD. "
                b"NULL for TX (TX RRC does not collect water volumes). "
                b">= 0 or NULL."
            ),
        }),

        # -- Operational --
        pa.field("days_produced", pa.int32(), metadata={
            b"description": (
                b"Number of days the well produced during the month (0-31). "
                b"Available from NM OCD. NULL for TX (not in PDQ tables)."
            ),
        }),

        # -- Metadata / lineage --
        pa.field("source_file", pa.string(), metadata={
            b"description": (
                b"Name or path of the bronze-layer file this record was "
                b"parsed from. Used for data lineage and debugging."
            ),
        }),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), metadata={
            b"description": (
                b"UTC timestamp when this record was ingested into the "
                b"silver layer."
            ),
        }),
    ]
)

# ---------------------------------------------------------------------------
# Convenience lists of column names by category
# ---------------------------------------------------------------------------

IDENTITY_COLUMNS: list[str] = [
    "state", "entity_type", "api_number", "lease_number", "district",
]

DESCRIPTIVE_COLUMNS: list[str] = [
    "well_name", "operator", "county", "field_name", "basin",
    "well_type", "well_status",
]

GEO_COLUMNS: list[str] = [
    "latitude", "longitude",
]

VOLUME_COLUMNS: list[str] = [
    "oil_bbl", "gas_mcf", "condensate_bbl", "casinghead_gas_mcf", "water_bbl",
]

ALL_COLUMNS: list[str] = [field.name for field in PRODUCTION_SCHEMA]
