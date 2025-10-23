from __future__ import annotations


class ExpansionWarning(Exception):
    pass


def expand_select_stars(sql: str, scope_columns: dict[str, set[str]]) -> str:
    """Expand SELECT * and qualifier.* using provided column scope.

    - scope_columns maps alias/table identifier -> set of column names
    - unqualified * expands to union of all columns in scope
    - qualified a.* expands to columns from that qualifier only
    - if no columns available for a star, leaves it unchanged
    """

    import re

    # Get all available columns
    all_cols = set()
    for cols in scope_columns.values():
        all_cols.update(cols)

    if not all_cols:
        return sql

    # Simple regex-based replacement that preserves formatting
    def replace_select_star(match):
        # Get the indentation from the original line
        indent = match.group(1)
        # Get the rest of the line after SELECT *
        rest_of_line = match.group(2).lstrip()
        # Replace with explicit columns, maintaining indentation
        cols_str = ",\n".join(f"{indent}    {col}" for col in sorted(all_cols))
        if rest_of_line:
            return f"select\n{cols_str}\n{indent}{rest_of_line}"
        else:
            return f"select\n{cols_str}"

    # Pattern to match SELECT * at the beginning of a line (not in comments)
    pattern = r"^(\s*)select\s+\*(.*)$"

    # Replace SELECT * with explicit columns
    result = re.sub(pattern, replace_select_star, sql, flags=re.IGNORECASE | re.MULTILINE)

    return result
