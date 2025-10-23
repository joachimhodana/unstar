from __future__ import annotations

import json
import tempfile
from pathlib import Path

from unstar.adapters.dbt.artifacts import DbtModel, load_artifacts


class TestDbtArtifacts:
    def test_load_artifacts_no_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = load_artifacts(tmpdir)
            assert result is None

    def test_load_artifacts_raw_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create target directory and manifest
            target_dir = Path(tmpdir) / "target"
            target_dir.mkdir()

            manifest_data = {
                "nodes": {
                    "model.test.model_a": {
                        "resource_type": "model",
                        "name": "model_a",
                        "path": "models/model_a.sql",
                        "depends_on": {"nodes": ["model.test.model_b"]},
                        "raw_sql": "SELECT * FROM {{ ref('model_b') }}",
                        "compiled_sql": "SELECT * FROM model_b",
                    },
                    "model.test.model_b": {
                        "resource_type": "model",
                        "name": "model_b",
                        "path": "models/model_b.sql",
                        "depends_on": {"nodes": []},
                        "raw_sql": "SELECT a, b FROM source_table",
                        "compiled_sql": "SELECT a, b FROM source_table",
                    },
                }
            }

            manifest_path = target_dir / "manifest.json"
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)

            result = load_artifacts(tmpdir)
            assert result is not None
            assert len(result.models_by_name) == 2
            assert "model_a" in result.models_by_name
            assert "model_b" in result.models_by_name

            model_a = result.models_by_name["model_a"]
            assert model_a.name == "model_a"
            assert model_a.depends_on == ["model.test.model_b"]
            assert model_a.raw_sql == "SELECT * FROM {{ ref('model_b') }}"
            assert model_a.compiled_sql == "SELECT * FROM model_b"

    def test_load_artifacts_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir) / "target"
            target_dir.mkdir()

            manifest_path = target_dir / "manifest.json"
            with open(manifest_path, "w") as f:
                f.write("invalid json")

            result = load_artifacts(tmpdir)
            assert result is not None
            assert len(result.models_by_name) == 0

    def test_dbt_model_creation(self):
        model = DbtModel(
            name="test_model",
            path="/path/to/model.sql",
            depends_on=["dep1", "dep2"],
            node_id="model.test.test_model",
            raw_sql="SELECT * FROM table",
            compiled_sql="SELECT * FROM table",
        )

        assert model.name == "test_model"
        assert model.path == "/path/to/model.sql"
        assert model.depends_on == ["dep1", "dep2"]
        assert model.node_id == "model.test.test_model"
        assert model.raw_sql == "SELECT * FROM table"
        assert model.compiled_sql == "SELECT * FROM table"
