"""Embeddings database infrastructure — schema and connection factory."""

import duckdb

from src.utils.config import EMBEDDINGS_DB_PATH


def ensure_embeddings_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create the node_embeddings table if it doesn't exist. Idempotent.

    Also runs migrations for:
    - schema_version, is_valid_node, validation_errors_json columns
    - make embed_text and embedding nullable (table swap)
    - backfill legacy rows with schema_version='1.0'
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS node_embeddings (
            node_id        VARCHAR PRIMARY KEY,
            node_type      VARCHAR NOT NULL,
            parent_doc_id  VARCHAR NOT NULL,
            embed_text     TEXT,
            embedding      FLOAT[3072],
            model_version  VARCHAR NOT NULL DEFAULT 'gemini-embedding-001',
            domain_tags    VARCHAR[],
            confidence     FLOAT,
            content_json   TEXT,
            embedded_at    TIMESTAMP DEFAULT now(),
            schema_version         VARCHAR NOT NULL DEFAULT '1.0',
            is_valid_node          INTEGER NOT NULL DEFAULT 1,
            validation_errors_json TEXT
        )
    """)

    # Migration guards — add columns that may be missing from older databases
    _embed_migrations = [
        ("node_embeddings", "schema_version", "VARCHAR DEFAULT '1.0'"),
        ("node_embeddings", "is_valid_node", "INTEGER DEFAULT 1"),
        ("node_embeddings", "validation_errors_json", "TEXT"),
    ]
    for table, col, col_type in _embed_migrations:
        try:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
        except duckdb.CatalogException:
            pass  # Column already exists

    # Table-swap migration: make embed_text and embedding nullable
    _migrate_node_embeddings_nullable(con)

    # Backfill legacy rows
    con.execute(
        "UPDATE node_embeddings SET schema_version = '1.0' "
        "WHERE schema_version IS NULL OR schema_version = ''"
    )


def _migrate_node_embeddings_nullable(con: duckdb.DuckDBPyConnection) -> None:
    """Make embed_text and embedding nullable via table swap if needed."""
    pragma_rows = con.execute("PRAGMA table_info('node_embeddings')").fetchall()

    # Check if embed_text and embedding are already nullable
    # PRAGMA returns: (cid, name, type, notnull, default_value, pk)
    nullable_targets = {"embed_text", "embedding"}
    needs_migration = False
    for row in pragma_rows:
        if row[1] in nullable_targets and row[3]:  # notnull is True
            needs_migration = True
            break

    if not needs_migration:
        return

    # Build CREATE TABLE dynamically, overriding notnull for embed_text/embedding
    col_defs = []
    for cid, name, dtype, notnull, default_val, pk in pragma_rows:
        parts = [name, dtype]
        # Override: make embed_text and embedding nullable
        if name in nullable_targets:
            pass  # Skip NOT NULL
        elif notnull and not pk:
            parts.append("NOT NULL")
        if default_val is not None:
            parts.append(f"DEFAULT {default_val}")
        if pk:
            parts.append("PRIMARY KEY")
        col_defs.append(" ".join(parts))

    create_sql = (
        "CREATE TABLE node_embeddings_new (\n    "
        + ",\n    ".join(col_defs)
        + "\n)"
    )

    con.execute(create_sql)
    con.execute("INSERT INTO node_embeddings_new SELECT * FROM node_embeddings")
    con.execute("DROP TABLE node_embeddings")
    con.execute("ALTER TABLE node_embeddings_new RENAME TO node_embeddings")


def connect_embeddings() -> duckdb.DuckDBPyConnection:
    """Open a read-write connection to the embeddings database."""
    EMBEDDINGS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(EMBEDDINGS_DB_PATH))
    ensure_embeddings_tables(con)
    return con
