"""Node-type schema contracts and validation.

Defines required fields per node_type with alias support.
Each alias group is a list of field names — at least one must
be present and non-empty in the node's content dict.
"""

from src.papers.versions import NODE_SCHEMA_VERSION

REQUIRED_FIELDS = {
    "document": [
        ["title"],
        ["paper_number"],
        ["abstract", "proposition"],
    ],
    "author": [["name"]],
    "equation": [
        ["formula_latex", "equation_text"],
        ["variables", "symbol_defs"],
    ],
    "method": [["method_name"], ["summary"]],
    "claim": [["claim_text"], ["evidence_excerpt"]],
    "field_case": [["formation_or_basin"], ["summary"]],
    "figure": [["caption"], ["figure_ref"]],
}


def _is_empty(value) -> bool:
    """Check if a value is considered empty.

    - Strings: None, "", or whitespace-only
    - Lists: [] or None
    - Dicts: {} or None
    - Any other falsy value
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return not value


def validate_node(node: dict) -> tuple[bool, list[dict]]:
    """Validate a node against its type's schema contract.

    Returns:
        (is_valid, errors) where errors is a list of structured dicts.
        Error format: {"field": str, "alias_group": str, "code": str}
        Codes: missing_required, empty_value, unknown_node_type, missing_content
    """
    errors = []
    node_type = node.get("node_type", "")

    if node_type not in REQUIRED_FIELDS:
        errors.append({"field": "node_type", "code": "unknown_node_type"})
        return False, errors

    content = node.get("content")
    if not content or not isinstance(content, dict) or len(content) == 0:
        errors.append({"field": "content", "code": "missing_content"})
        return False, errors

    for alias_group in REQUIRED_FIELDS[node_type]:
        alias_group_str = "|".join(alias_group)

        # Find the first alias that exists in content
        found_field = None
        for alias in alias_group:
            if alias in content:
                found_field = alias
                break

        if found_field is None:
            errors.append({
                "field": alias_group[0],
                "alias_group": alias_group_str,
                "code": "missing_required",
            })
        elif _is_empty(content[found_field]):
            errors.append({
                "field": found_field,
                "alias_group": alias_group_str,
                "code": "empty_value",
            })

    return (len(errors) == 0, errors)
