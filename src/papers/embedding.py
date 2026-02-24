"""Paper embedding pipeline — compose text, call Gemini API, store vectors."""

import json
import logging
from pathlib import Path

from src.papers.validation import validate_node
from src.papers.versions import EMBEDDING_MODEL_VERSION, NODE_SCHEMA_VERSION

logger = logging.getLogger(__name__)

INVALID_EMBED_TEXT_MAX_LEN = 2000


def compose_embed_text(node: dict, parent_title: str = "") -> str:
    """Compose a text string from an atomic node for embedding.

    Each node type gets a template that maximizes semantic signal.
    Figures return empty string (no textual content to embed).
    Non-document nodes are prefixed with parent document title.
    """
    node_type = node.get("node_type", "")
    content = node.get("content", {})

    if node_type == "figure":
        return ""

    if node_type == "document":
        title = content.get("title", "")
        proposition = content.get("proposition", "")
        parts = [p for p in [title, proposition] if p]
        return ". ".join(parts)

    prefix = f"{parent_title} — " if parent_title else ""

    if node_type == "author":
        name = content.get("name", "")
        affiliation = content.get("affiliation", "")
        parts = [p for p in [name, affiliation] if p]
        return prefix + ", ".join(parts)

    if node_type == "equation":
        name = content.get("name", "")
        formula = content.get("formula_latex", "")
        variables = content.get("variables", {})
        constraints = content.get("constraints", {})
        var_str = ", ".join(f"{k}: {v}" for k, v in variables.items()) if variables else ""
        constraint_str = _format_constraints(constraints)
        parts = [f"Equation: {name}"]
        if formula:
            parts.append(f"Formula: {formula}")
        if var_str:
            parts.append(f"Variables: {var_str}")
        if constraint_str:
            parts.append(f"Constraints: {constraint_str}")
        return prefix + ". ".join(parts)

    if node_type == "method":
        name = content.get("name", "")
        role = content.get("role", "")
        context = content.get("context", "")
        constraints = content.get("constraints", {})
        constraint_str = _format_constraints(constraints)
        header = f"Method: {name} ({role})" if role else f"Method: {name}"
        parts = [header]
        if context:
            parts.append(context)
        if constraint_str:
            parts.append(f"Constraints: {constraint_str}")
        return prefix + ". ".join(parts)

    if node_type == "claim":
        text = content.get("text", "")
        claim_type = content.get("claim_type", "")
        section = content.get("section", "")
        suffix_parts = []
        if section:
            suffix_parts.append(f"Section: {section}")
        if claim_type:
            suffix_parts.append(f"Type: {claim_type}")
        suffix = ", ".join(suffix_parts)
        return prefix + (f"{text} [{suffix}]" if suffix else text)

    if node_type == "field_case":
        name = content.get("name", "")
        case_type = content.get("case_type", "")
        basin = content.get("basin")
        formation = content.get("formation")
        props = content.get("reservoir_properties", {})
        parts = [f"Field case: {name}"]
        if case_type:
            parts.append(f"Type: {case_type}")
        if basin:
            parts.append(f"Basin: {basin}")
        if formation:
            parts.append(f"Formation: {formation}")
        if props:
            prop_str = ", ".join(f"{k}: {v}" for k, v in props.items())
            parts.append(f"Properties: {prop_str}")
        return prefix + ". ".join(parts)

    return prefix + json.dumps(content)


def _format_constraints(constraints: dict) -> str:
    """Format a constraints dict into a readable string."""
    if not constraints:
        return ""
    parts = []
    for key in ("flow_regime", "fluid", "pressure_condition", "reservoir_type"):
        val = constraints.get(key)
        if val:
            if isinstance(val, list):
                parts.extend(val)
            else:
                parts.append(str(val))
    return ", ".join(parts)


def _embed_batch(texts: list[str], api_key: str, model: str) -> list[list[float]]:
    """Call Gemini embedding API for a batch of texts.

    Returns list of 768d float vectors.
    """
    from google import genai

    client = genai.Client(api_key=api_key)
    result = client.models.embed_content(
        model=model,
        contents=texts,
    )
    return [list(e.values) for e in result.embeddings]


def embed_papers(
    con,
    paper_data_dir: str,
    api_key: str,
    model: str = EMBEDDING_MODEL_VERSION,
    force: bool = False,
) -> dict:
    """Batch embed all nodes.jsonl files into the embeddings database.

    Validates every node via validate_node(). Valid non-figure nodes are
    embedded normally. Invalid non-figure nodes are stored with embedding=NULL
    and validation errors for QA diagnostics. Figures are counted but not stored.

    Args:
        con: DuckDB connection to embeddings database.
        paper_data_dir: Path to directory containing paper subdirectories with nodes.jsonl.
        api_key: Gemini API key.
        model: Embedding model name.
        force: If True, drop and recreate table before embedding.

    Returns:
        Dict with total_embedded, papers_processed, valid_count, invalid_count,
        skipped_figures, invalid_by_type.
    """
    from src.papers.db import ensure_embeddings_tables

    if force:
        con.execute("DROP TABLE IF EXISTS node_embeddings")
        ensure_embeddings_tables(con)

    base = Path(paper_data_dir)
    jsonl_files = sorted(base.glob("*/nodes.jsonl"))

    total_embedded = 0
    papers_processed = 0
    valid_count = 0
    invalid_count = 0
    skipped_figures = 0
    invalid_by_type: dict[str, int] = {}

    for jsonl_path in jsonl_files:
        nodes = []
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    nodes.append(json.loads(line))

        if not nodes:
            continue

        # Find document node by type (don't assume line order)
        parent_title = ""
        for node in nodes:
            if node.get("node_type") == "document":
                parent_title = node.get("content", {}).get("title", "")
                break

        # Validate and split nodes into categories
        valid_embeddable = []   # valid + non-figure
        invalid_storable = []   # invalid + non-figure

        for node in nodes:
            node_type = node.get("node_type", "")
            is_valid, errors = validate_node(node)

            if node_type == "figure":
                skipped_figures += 1
                continue

            if is_valid:
                text = compose_embed_text(node, parent_title=parent_title)
                if text:
                    valid_embeddable.append((node, text))
                    valid_count += 1
            else:
                invalid_storable.append((node, errors))
                invalid_count += 1
                invalid_by_type[node_type] = invalid_by_type.get(node_type, 0) + 1

        # Embed valid nodes in batches
        batch_nodes = []
        batch_texts = []
        batch_chars = 0
        CHAR_LIMIT = 10_000

        def _flush_batch():
            nonlocal total_embedded
            if not batch_texts:
                return
            vectors = _embed_batch(batch_texts, api_key, model)
            for (node, text), vector in zip(batch_nodes, vectors):
                con.execute("""
                    INSERT OR REPLACE INTO node_embeddings
                    (node_id, node_type, parent_doc_id, embed_text, embedding,
                     model_version, domain_tags, confidence, content_json,
                     schema_version, is_valid_node, validation_errors_json)
                    VALUES (?, ?, ?, ?, ?::FLOAT[3072], ?, ?, ?, ?, ?, ?, ?)
                """, [
                    node["node_id"],
                    node["node_type"],
                    node.get("parent_doc_id", ""),
                    text,
                    vector,
                    model,
                    node.get("domain_tags", []),
                    node.get("confidence"),
                    json.dumps(node.get("content", {})),
                    NODE_SCHEMA_VERSION,
                    1,
                    None,
                ])
                total_embedded += 1

        for node, text in valid_embeddable:
            if batch_chars + len(text) > CHAR_LIMIT and batch_texts:
                _flush_batch()
                batch_nodes = []
                batch_texts = []
                batch_chars = 0
            batch_nodes.append((node, text))
            batch_texts.append(text)
            batch_chars += len(text)

        _flush_batch()

        # Store invalid nodes with embedding=NULL
        for node, errors in invalid_storable:
            # Best-effort embed_text for QA diagnostics
            try:
                embed_text = compose_embed_text(node, parent_title=parent_title)
            except Exception:
                embed_text = ""
            if embed_text and len(embed_text) > INVALID_EMBED_TEXT_MAX_LEN:
                embed_text = embed_text[:INVALID_EMBED_TEXT_MAX_LEN]

            con.execute("""
                INSERT OR REPLACE INTO node_embeddings
                (node_id, node_type, parent_doc_id, embed_text, embedding,
                 model_version, domain_tags, confidence, content_json,
                 schema_version, is_valid_node, validation_errors_json)
                VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
            """, [
                node["node_id"],
                node["node_type"],
                node.get("parent_doc_id", ""),
                embed_text or None,
                model,
                node.get("domain_tags", []),
                node.get("confidence"),
                json.dumps(node.get("content", {})),
                NODE_SCHEMA_VERSION,
                0,
                json.dumps(errors),
            ])

        if valid_embeddable or invalid_storable:
            papers_processed += 1

    return {
        "total_embedded": total_embedded,
        "papers_processed": papers_processed,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "skipped_figures": skipped_figures,
        "invalid_by_type": invalid_by_type,
    }


def embedding_stats(con) -> dict:
    """Return summary statistics for the embeddings database.

    Returns:
        Dict with total_nodes, by_type, by_paper, model_versions, timestamps.
    """
    total = con.execute("SELECT COUNT(*) FROM node_embeddings").fetchone()[0]

    by_type = {}
    for row in con.execute(
        "SELECT node_type, COUNT(*) FROM node_embeddings GROUP BY node_type ORDER BY node_type"
    ).fetchall():
        by_type[row[0]] = row[1]

    by_paper = {}
    for row in con.execute(
        "SELECT parent_doc_id, COUNT(*) FROM node_embeddings GROUP BY parent_doc_id ORDER BY parent_doc_id"
    ).fetchall():
        by_paper[row[0]] = row[1]

    model_versions = [r[0] for r in con.execute(
        "SELECT DISTINCT model_version FROM node_embeddings"
    ).fetchall()]

    timestamps = con.execute(
        "SELECT MIN(embedded_at), MAX(embedded_at) FROM node_embeddings"
    ).fetchone()

    return {
        "total_nodes": total,
        "by_type": by_type,
        "by_paper": by_paper,
        "model_versions": model_versions,
        "oldest_embed": str(timestamps[0]) if timestamps[0] else None,
        "newest_embed": str(timestamps[1]) if timestamps[1] else None,
    }
