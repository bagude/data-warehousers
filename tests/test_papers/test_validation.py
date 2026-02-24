"""Tests for src/papers/validation.py — node schema contracts."""

import pytest


class TestValidateNodeValidCases:
    """Valid nodes should pass validation."""

    def test_valid_document(self):
        from src.papers.validation import validate_node
        node = {"node_type": "document", "content": {
            "title": "Test Paper", "paper_number": "SPE 12345", "proposition": "A finding."
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is True
        assert errors == []

    def test_valid_equation_with_formula_latex(self):
        from src.papers.validation import validate_node
        node = {"node_type": "equation", "content": {
            "formula_latex": "D_D = ...", "variables": {"D_D": "decline rate"}
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is True

    def test_valid_equation_with_equation_text_alias(self):
        from src.papers.validation import validate_node
        node = {"node_type": "equation", "content": {
            "equation_text": "D_D equals decline rate", "symbol_defs": {"D_D": "decline rate"}
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is True

    def test_valid_document_with_abstract_alias(self):
        from src.papers.validation import validate_node
        node = {"node_type": "document", "content": {
            "title": "Test", "paper_number": "SPE 1", "abstract": "An abstract."
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is True

    def test_valid_author(self):
        from src.papers.validation import validate_node
        node = {"node_type": "author", "content": {"name": "D. Ilk"}}
        is_valid, errors = validate_node(node)
        assert is_valid is True

    def test_valid_method(self):
        from src.papers.validation import validate_node
        node = {"node_type": "method", "content": {
            "method_name": "Semi-analytical", "summary": "A method."
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is True

    def test_valid_claim(self):
        from src.papers.validation import validate_node
        node = {"node_type": "claim", "content": {
            "claim_text": "b converges to 0.5", "evidence_excerpt": "Fig 3 shows..."
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is True

    def test_valid_field_case(self):
        from src.papers.validation import validate_node
        node = {"node_type": "field_case", "content": {
            "formation_or_basin": "Permian", "summary": "Tight oil well."
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is True

    def test_valid_figure(self):
        from src.papers.validation import validate_node
        node = {"node_type": "figure", "content": {
            "caption": "Type curve plot", "figure_ref": "Fig. 3"
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is True


class TestValidateNodeInvalidCases:
    """Invalid nodes should fail with structured errors."""

    def test_unknown_node_type(self):
        from src.papers.validation import validate_node
        node = {"node_type": "unknown_type", "content": {"foo": "bar"}}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        assert any(e["code"] == "unknown_node_type" for e in errors)

    def test_missing_content(self):
        from src.papers.validation import validate_node
        node = {"node_type": "equation"}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        assert any(e["code"] == "missing_content" for e in errors)

    def test_empty_content(self):
        from src.papers.validation import validate_node
        node = {"node_type": "equation", "content": {}}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        assert any(e["code"] == "missing_content" for e in errors)

    def test_missing_required_field(self):
        from src.papers.validation import validate_node
        node = {"node_type": "equation", "content": {
            "formula_latex": "D_D = ...",
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        assert any(e["code"] == "missing_required" for e in errors)
        assert any("variables" in e["alias_group"] for e in errors)

    def test_all_aliases_missing(self):
        from src.papers.validation import validate_node
        node = {"node_type": "equation", "content": {
            "name": "Some equation",
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        missing_groups = [e["alias_group"] for e in errors if e["code"] == "missing_required"]
        assert "formula_latex|equation_text" in missing_groups
        assert "variables|symbol_defs" in missing_groups

    def test_document_missing_both_abstract_aliases(self):
        from src.papers.validation import validate_node
        node = {"node_type": "document", "content": {
            "title": "Test", "paper_number": "SPE 1",
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        assert any("abstract|proposition" in e.get("alias_group", "") for e in errors)


class TestValidateNodeEmptyValues:
    """Empty values should be caught."""

    def test_whitespace_string_is_empty(self):
        from src.papers.validation import validate_node
        node = {"node_type": "author", "content": {"name": "   "}}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        assert any(e["code"] == "empty_value" for e in errors)

    def test_empty_string_is_empty(self):
        from src.papers.validation import validate_node
        node = {"node_type": "author", "content": {"name": ""}}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        assert any(e["code"] == "empty_value" for e in errors)

    def test_none_value_is_empty(self):
        from src.papers.validation import validate_node
        node = {"node_type": "author", "content": {"name": None}}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        assert any(e["code"] == "empty_value" for e in errors)

    def test_empty_dict_is_empty(self):
        from src.papers.validation import validate_node
        node = {"node_type": "equation", "content": {
            "formula_latex": "x=1", "variables": {}
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        assert any(e["code"] == "empty_value" for e in errors)

    def test_empty_list_is_empty(self):
        from src.papers.validation import validate_node
        node = {"node_type": "equation", "content": {
            "formula_latex": "x=1", "variables": []
        }}
        is_valid, errors = validate_node(node)
        assert is_valid is False
        assert any(e["code"] == "empty_value" for e in errors)


class TestValidateNodeErrorFormat:
    """Structured error format must include field, alias_group, code."""

    def test_missing_required_error_has_alias_group(self):
        from src.papers.validation import validate_node
        node = {"node_type": "equation", "content": {"formula_latex": "x=1"}}
        _, errors = validate_node(node)
        err = next(e for e in errors if e["code"] == "missing_required")
        assert "alias_group" in err
        assert err["alias_group"] == "variables|symbol_defs"

    def test_empty_value_error_includes_found_field(self):
        from src.papers.validation import validate_node
        node = {"node_type": "author", "content": {"name": "  "}}
        _, errors = validate_node(node)
        err = next(e for e in errors if e["code"] == "empty_value")
        assert err["field"] == "name"
        assert "alias_group" in err

    def test_unknown_type_error_format(self):
        from src.papers.validation import validate_node
        node = {"node_type": "widget", "content": {"x": 1}}
        _, errors = validate_node(node)
        err = next(e for e in errors if e["code"] == "unknown_node_type")
        assert err["field"] == "node_type"

    def test_missing_content_error_format(self):
        from src.papers.validation import validate_node
        node = {"node_type": "author"}
        _, errors = validate_node(node)
        err = next(e for e in errors if e["code"] == "missing_content")
        assert err["field"] == "content"


class TestNodeSchemaVersion:
    def test_version_constant_exists(self):
        from src.papers.versions import NODE_SCHEMA_VERSION
        assert isinstance(NODE_SCHEMA_VERSION, str)
        assert len(NODE_SCHEMA_VERSION) > 0

    def test_required_fields_covers_all_types(self):
        from src.papers.validation import REQUIRED_FIELDS
        expected = {"document", "author", "equation", "method", "claim", "field_case", "figure"}
        assert set(REQUIRED_FIELDS.keys()) == expected
