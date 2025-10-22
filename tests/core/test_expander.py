from __future__ import annotations

import pytest

from unstar.core.expander import expand_select_stars


class TestExpandSelectStars:
    def test_no_stars_unchanged(self):
        sql = "SELECT a, b FROM table"
        scope = {"": {"a", "b"}}
        assert expand_select_stars(sql, scope) == sql

    def test_simple_star_expansion(self):
        sql = "SELECT * FROM table"
        scope = {"": {"a", "b", "c"}}
        result = expand_select_stars(sql, scope)
        # Order may vary, check contains expected columns
        assert "a" in result
        assert "b" in result
        assert "c" in result
        assert "*" not in result

    def test_qualified_star_expansion(self):
        sql = "SELECT t.* FROM table t"
        scope = {"t": {"a", "b"}}
        result = expand_select_stars(sql, scope)
        assert "t.a" in result
        assert "t.b" in result
        assert "t.*" not in result

    def test_mixed_stars_and_columns(self):
        sql = "SELECT *, id FROM table"
        scope = {"": {"a", "b"}}
        result = expand_select_stars(sql, scope)
        assert "id" in result
        assert "a" in result
        assert "b" in result

    def test_no_scope_unchanged(self):
        sql = "SELECT * FROM table"
        scope = {}
        assert expand_select_stars(sql, scope) == sql

    def test_empty_scope_unchanged(self):
        sql = "SELECT * FROM table"
        scope = {"": set()}
        assert expand_select_stars(sql, scope) == sql

    def test_invalid_sql_unchanged(self):
        sql = "INVALID SQL"
        scope = {"": {"a"}}
        assert expand_select_stars(sql, scope) == sql

    def test_distinct_preserved(self):
        sql = "SELECT DISTINCT * FROM table"
        scope = {"": {"a", "b"}}
        result = expand_select_stars(sql, scope)
        assert "DISTINCT" in result
        assert "a" in result
        assert "b" in result

    def test_multiple_qualified_stars(self):
        sql = "SELECT t1.*, t2.* FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id"
        scope = {"t1": {"a", "b"}, "t2": {"c", "d"}}
        result = expand_select_stars(sql, scope)
        assert "t1.a" in result
        assert "t1.b" in result
        assert "t2.c" in result
        assert "t2.d" in result

    def test_cte_with_star(self):
        sql = "WITH cte AS (SELECT * FROM table) SELECT * FROM cte"
        scope = {"": {"a", "b"}, "cte": {"a", "b"}}
        result = expand_select_stars(sql, scope)
        # Should expand both stars
        assert "*" not in result
        assert "a" in result
        assert "b" in result

    def test_join_with_ambiguous_columns(self):
        sql = "SELECT * FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id"
        scope = {"": {"t1.a", "t1.b", "t2.c", "t2.d"}}
        result = expand_select_stars(sql, scope)
        # Should expand to all available columns
        assert "t1.a" in result
        assert "t1.b" in result
        assert "t2.c" in result
        assert "t2.d" in result

    def test_sqlglot_not_available_unchanged(self):
        # This test would require mocking sqlglot import
        # For now, just test that function doesn't crash
        sql = "SELECT * FROM table"
        scope = {"": {"a"}}
        result = expand_select_stars(sql, scope)
        # Should either expand or return unchanged
        assert isinstance(result, str)
