from __future__ import annotations


def mask_jinja(sql: str) -> str:
    """Very light placeholder: returns original SQL.

    Later we can replace jinja blocks inside SELECT lists with stable tokens
    and unmask after rewriting.
    """

    return sql


def unmask_jinja(sql: str) -> str:
    return sql


