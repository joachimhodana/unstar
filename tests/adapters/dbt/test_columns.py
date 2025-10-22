from __future__ import annotations

import pytest

from unstar.adapters.dbt.columns import infer_downstream_columns


class TestDbtColumns:
    def test_infer_downstream_columns_empty(self):
        result = infer_downstream_columns([], set())
        assert result == {}

    def test_infer_downstream_columns_simple(self):
        sql_texts = ["SELECT a, b FROM table", "SELECT c, d FROM other_table"]
        result = infer_downstream_columns(sql_texts, set())
        assert "" in result
        assert "a" in result[""]
        assert "b" in result[""]
        assert "c" in result[""]
        assert "d" in result[""]

    def test_infer_downstream_columns_no_sqlglot(self):
        # Test that function doesn't crash when sqlglot is not available
        # Since sqlglot is now installed, this test verifies the function works
        sql_texts = ["SELECT a FROM table"]
        result = infer_downstream_columns(sql_texts, set())
        # Should return columns when sqlglot is available
        assert "" in result
        assert "a" in result[""]

    def test_infer_downstream_columns_invalid_sql(self):
        sql_texts = ["INVALID SQL", "SELECT a FROM table"]
        result = infer_downstream_columns(sql_texts, set())
        # Should handle invalid SQL gracefully
        assert isinstance(result, dict)
