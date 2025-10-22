from __future__ import annotations

from typing import Dict, Set


class ExpansionWarning(Exception):
    pass


def expand_select_stars(sql: str, scope_columns: Dict[str, Set[str]]) -> str:
    """Expand SELECT * and qualifier.* using provided column scope.

    - scope_columns maps alias/table identifier -> set of column names
    - unqualified * expands to union of all columns in scope
    - qualified a.* expands to columns from that qualifier only
    - if no columns available for a star, leaves it unchanged
    """

    try:
        import sqlglot
        from sqlglot import exp
    except Exception:
        # sqlglot not installed; return original SQL
        return sql

    try:
        tree = sqlglot.parse_one(sql, error_level="ignore")
    except Exception:
        return sql

    if tree is None:
        return sql

    def columns_for_qualifier(qual: str) -> Set[str]:
        return set(scope_columns.get(qual, set()))

    def all_columns() -> Set[str]:
        cols: Set[str] = set()
        for s in scope_columns.values():
            cols.update(s)
        return cols

    changed = False

    for select in tree.find_all(exp.Select):
        new_expressions = []
        for proj in select.expressions:
            if isinstance(proj, exp.Star):
                cols = sorted(all_columns())
                if not cols:
                    new_expressions.append(proj)
                    continue
                changed = True
                new_expressions.extend([exp.to_identifier(c) for c in cols])
            elif isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
                    # qualified star: alias.*
                    qualifier = proj.table
                    if isinstance(qualifier, str):
                        qualifier_name = qualifier
                    elif hasattr(qualifier, 'name'):
                        qualifier_name = qualifier.name
                    else:
                        new_expressions.append(proj)
                        continue
                    
                    if not qualifier_name:
                        new_expressions.append(proj)
                        continue
                    cols = sorted(columns_for_qualifier(qualifier_name))
                    if not cols:
                        new_expressions.append(proj)
                        continue
                    changed = True
                    new_expressions.extend(
                        [exp.Column(this=exp.to_identifier(c), table=exp.to_identifier(qualifier_name)) for c in cols]
                    )
            else:
                new_expressions.append(proj)

        select.set("expressions", new_expressions)

    if not changed:
        return sql

    try:
        return tree.sql(dialect=None)
    except Exception:
        return sql


