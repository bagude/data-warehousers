"""Tests for src/papers/embedding.py — producer embedding functions."""

import json

import duckdb
import pytest
from unittest.mock import patch
from pathlib import Path


def _mock_embed_batch(texts, api_key, model):
    return [[1.0] + [0.0] * 3071 for _ in texts]


class TestComposeEmbedText:
    def test_document_node(self):
        from src.papers.embedding import compose_embed_text
        node = {
            "node_type": "document",
            "content": {
                "title": "Decline Curve Analysis for HP/HT Gas Wells",
                "proposition": "Formalizes Arps D/b via semi-analytical relation."
            }
        }
        text = compose_embed_text(node)
        assert "Decline Curve Analysis for HP/HT Gas Wells" in text
        assert "Formalizes Arps D/b" in text

    def test_equation_node_with_parent_title(self):
        from src.papers.embedding import compose_embed_text
        node = {
            "node_type": "equation",
            "content": {
                "name": "Dimensionless D-function",
                "formula_latex": "D_D = P_{wD} \\frac{1}{t_{Dd}}",
                "variables": {"D_D": "decline rate [dimensionless]", "t_{Dd}": "decline time [dimensionless]"},
                "constraints": {"flow_regime": ["boundary_dominated"], "fluid": ["gas"]}
            }
        }
        text = compose_embed_text(node, parent_title="HP/HT Gas Wells")
        assert text.startswith("HP/HT Gas Wells")
        assert "Equation: Dimensionless D-function" in text
        assert "D_D = P_{wD}" in text
        assert "decline rate [dimensionless]" in text
        assert "boundary_dominated" in text

    def test_method_node(self):
        from src.papers.embedding import compose_embed_text
        node = {
            "node_type": "method",
            "content": {
                "name": "Semi-analytical",
                "role": "proposed",
                "context": "For tight gas",
                "constraints": {"fluid": ["gas"]}
            }
        }
        text = compose_embed_text(node)
        assert "Method: Semi-analytical (proposed)" in text
        assert "For tight gas" in text
        assert "gas" in text

    def test_claim_node(self):
        from src.papers.embedding import compose_embed_text
        node = {
            "node_type": "claim",
            "content": {"text": "b converges", "claim_type": "finding", "section": "Results"}
        }
        text = compose_embed_text(node)
        assert "b converges" in text
        assert "finding" in text

    def test_figure_returns_empty(self):
        from src.papers.embedding import compose_embed_text
        node = {"node_type": "figure", "content": {"caption": "Plot"}}
        assert compose_embed_text(node) == ""

    def test_field_case_node(self):
        from src.papers.embedding import compose_embed_text
        node = {
            "node_type": "field_case",
            "content": {
                "name": "Well A", "case_type": "field",
                "basin": "Permian", "formation": "Wolfcamp",
                "reservoir_properties": {"k_md": 0.01}
            }
        }
        text = compose_embed_text(node)
        assert "Well A" in text
        assert "Permian" in text
        assert "Wolfcamp" in text

    def test_author_node(self):
        from src.papers.embedding import compose_embed_text
        node = {"node_type": "author", "content": {"name": "D. Ilk", "affiliation": "Texas A&M"}}
        text = compose_embed_text(node)
        assert "D. Ilk" in text
        assert "Texas A&M" in text


class TestEmbedPapers:
    def test_embeds_valid_nodes(self, tmp_path):
        from src.papers.embedding import embed_papers
        from src.papers.db import ensure_embeddings_tables

        paper_dir = tmp_path / "paper_data" / "SPE_Test"
        paper_dir.mkdir(parents=True)
        nodes = [
            {"node_id": "doc_test", "node_type": "document", "parent_doc_id": "doc_test",
             "domain_tags": [], "page": None, "confidence": 1.0,
             "content": {"title": "Test", "proposition": "Test prop.", "paper_number": "SPE 1"},
             "edges": {}},
        ]
        with open(paper_dir / "nodes.jsonl", "w") as f:
            for n in nodes:
                f.write(json.dumps(n) + "\n")

        con = duckdb.connect(str(tmp_path / "embeddings.duckdb"))
        ensure_embeddings_tables(con)

        with patch("src.papers.embedding._embed_batch", side_effect=_mock_embed_batch):
            result = embed_papers(con, str(tmp_path / "paper_data"), "fake-key")

        assert result["total_embedded"] == 1
        assert result["papers_processed"] == 1
        assert result["valid_count"] == 1
        con.close()

    def test_stores_invalid_nodes_with_null_embedding(self, tmp_path):
        from src.papers.embedding import embed_papers
        from src.papers.db import ensure_embeddings_tables

        paper_dir = tmp_path / "paper_data" / "SPE_Test"
        paper_dir.mkdir(parents=True)
        nodes = [
            {"node_id": "doc_test", "node_type": "document", "parent_doc_id": "doc_test",
             "domain_tags": [], "page": None, "confidence": 1.0,
             "content": {"title": "Test", "proposition": "Test prop.", "paper_number": "SPE 1"},
             "edges": {}},
            {"node_id": "eq_bad", "node_type": "equation", "parent_doc_id": "doc_test",
             "domain_tags": [], "page": 3, "confidence": 0.8,
             "content": {"name": "Bad eq"},
             "edges": {}},
        ]
        with open(paper_dir / "nodes.jsonl", "w") as f:
            for n in nodes:
                f.write(json.dumps(n) + "\n")

        con = duckdb.connect(str(tmp_path / "embeddings.duckdb"))
        ensure_embeddings_tables(con)

        with patch("src.papers.embedding._embed_batch", side_effect=_mock_embed_batch):
            result = embed_papers(con, str(tmp_path / "paper_data"), "fake-key")

        assert result["invalid_count"] == 1
        assert result["valid_count"] == 1

        # Invalid node stored with NULL embedding
        row = con.execute(
            "SELECT embedding, is_valid_node FROM node_embeddings WHERE node_id = 'eq_bad'"
        ).fetchone()
        assert row[0] is None
        assert row[1] == 0
        con.close()


class TestEmbeddingStats:
    def test_stats_on_empty_db(self, tmp_path):
        from src.papers.embedding import embedding_stats
        from src.papers.db import ensure_embeddings_tables

        con = duckdb.connect(str(tmp_path / "embeddings.duckdb"))
        ensure_embeddings_tables(con)
        stats = embedding_stats(con)
        assert stats["total_nodes"] == 0
        assert stats["by_type"] == {}
        con.close()

    def test_stats_with_data(self, tmp_path):
        from src.papers.embedding import embedding_stats
        from src.papers.db import ensure_embeddings_tables

        con = duckdb.connect(str(tmp_path / "embeddings.duckdb"))
        ensure_embeddings_tables(con)
        con.execute("""
            INSERT INTO node_embeddings
            (node_id, node_type, parent_doc_id, embed_text, embedding, model_version, confidence, content_json)
            VALUES ('doc_1', 'document', 'doc_1', 'Test', ?::FLOAT[3072], 'gemini-embedding-001', 1.0, '{}')
        """, [[1.0] + [0.0] * 3071])

        stats = embedding_stats(con)
        assert stats["total_nodes"] == 1
        assert stats["by_type"]["document"] == 1
        con.close()
