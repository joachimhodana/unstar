from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from unstar.cli import main


class TestCliIntegration:
    def test_cli_help(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["--help"])
        assert exc_info.value.code == 0

    def test_cli_version(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["--version"])
        assert exc_info.value.code == 0

    def test_help(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["--help"])
        assert exc_info.value.code == 0

    def test_no_models(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create minimal dbt project structure
            (Path(tmpdir) / "dbt_project.yml").write_text("name: test")
            (Path(tmpdir) / "target").mkdir()
            
            # Create empty manifest
            manifest_path = Path(tmpdir) / "target" / "manifest.json"
            with open(manifest_path, "w") as f:
                json.dump({"nodes": {}}, f)
            
            result = main([
                "--project-dir", tmpdir,
                "--dry-run"
            ])
            assert result == 0

    def test_with_models(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create minimal dbt project structure
            (Path(tmpdir) / "dbt_project.yml").write_text("name: test")
            (Path(tmpdir) / "target").mkdir()
            
            # Create manifest with one model
            manifest_data = {
                "nodes": {
                    "model.test.model_a": {
                        "resource_type": "model",
                        "name": "model_a",
                        "path": "models/model_a.sql",
                        "depends_on": {"nodes": []},
                        "raw_sql": "SELECT * FROM source_table",
                        "compiled_sql": "SELECT * FROM source_table"
                    }
                }
            }
            
            manifest_path = Path(tmpdir) / "target" / "manifest.json"
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f)
            
            # Create the actual model file
            models_dir = Path(tmpdir) / "models"
            models_dir.mkdir()
            model_file = models_dir / "model_a.sql"
            model_file.write_text("SELECT * FROM source_table")
            
            result = main([
                "--project-dir", tmpdir,
                "--models", "model_a",
                "--dry-run"
            ])
            assert result == 0

    def test_invalid_adapter(self):
        with pytest.raises(SystemExit) as exc_info:
            main([
                "--adapter", "invalid",
                "--dry-run"
            ])
        assert exc_info.value.code == 2

    def test_mutually_exclusive_modes(self):
        with pytest.raises(SystemExit) as exc_info:
            main([
                "--write",
                "--dry-run",
                "--output", "/tmp"
            ])
        # Should fail due to mutually exclusive options
        assert exc_info.value.code == 2
