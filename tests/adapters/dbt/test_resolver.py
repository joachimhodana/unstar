from __future__ import annotations

from unstar.adapters.dbt.artifacts import DbtArtifacts, DbtModel
from unstar.adapters.dbt.resolver import find_models_by_names, find_models_by_path


class TestDbtResolver:
    def test_find_models_by_names(self):
        artifacts = DbtArtifacts(
            project_dir="/test",
            models_by_name={
                "model_a": DbtModel("model_a", "/path/a.sql", [], "id1", None, None),
                "model_b": DbtModel("model_b", "/path/b.sql", [], "id2", None, None),
            },
        )

        result = list(find_models_by_names(artifacts, ["model_a", "model_c"]))
        assert len(result) == 1
        assert result[0].name == "model_a"

    def test_find_models_by_names_empty(self):
        artifacts = DbtArtifacts("/test", {})
        result = list(find_models_by_names(artifacts, None))
        assert len(result) == 0

    def test_find_models_by_path(self):
        artifacts = DbtArtifacts(
            project_dir="/test",
            models_by_name={
                "model_a": DbtModel("model_a", "/test/models/staging/a.sql", [], "id1", None, None),
                "model_b": DbtModel("model_b", "/test/models/marts/b.sql", [], "id2", None, None),
            },
        )

        result = list(find_models_by_path(artifacts, "models/staging"))
        assert len(result) == 1
        assert result[0].name == "model_a"

    def test_find_models_by_path_none(self):
        artifacts = DbtArtifacts("/test", {})
        result = list(find_models_by_path(artifacts, None))
        assert len(result) == 0
