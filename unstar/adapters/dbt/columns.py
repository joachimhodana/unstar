from __future__ import annotations


def _collect_columns(sql: str) -> set[str]:
    try:
        import sqlglot
        from sqlglot import exp
    except Exception:
        return set()

    try:
        tree = sqlglot.parse_one(sql, error_level="ignore")
    except Exception:
        return set()

    if tree is None:
        return set()

    cols: set[str] = set()
    for col in tree.find_all(exp.Column):
        # Only bare identifiers (no star)
        if col.name and col.name != "*":
            cols.add(col.name)
    return cols


def infer_downstream_columns(sql_texts: list[str], target_aliases: set[str]) -> dict[str, set[str]]:
    # Heuristic: return union of all referenced column identifiers as unqualified scope
    union: set[str] = set()
    for s in sql_texts:
        union.update(_collect_columns(s))
    return {"": union} if union else {}
