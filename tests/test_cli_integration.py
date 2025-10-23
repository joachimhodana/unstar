from __future__ import annotations

import json
import tempfile
from pathlib import Path

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
        # Test with dbt adapter - should work if in dbt project
        result = main(["--adapter", "dbt", "--dry-run"])
        # May return 0 or 2 depending on if we're in a dbt project
        assert result in [0, 2]

    def test_project_dir_argument(self):
        # Test with custom project directory
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create minimal dbt project structure
            (Path(tmpdir) / "dbt_project.yml").write_text("name: test")
            (Path(tmpdir) / "target").mkdir()

            # Create empty manifest
            manifest_path = Path(tmpdir) / "target" / "manifest.json"
            with open(manifest_path, "w") as f:
                json.dump({"nodes": {}}, f)

            result = main(["--adapter", "dbt", "--project-dir", tmpdir, "--dry-run"])
            assert result == 0

    def test_with_models(self):
        # Test with dbt adapter
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create minimal dbt project structure
            (Path(tmpdir) / "dbt_project.yml").write_text("name: test")
            (Path(tmpdir) / "target").mkdir()
            
            # Create empty manifest
            manifest_path = Path(tmpdir) / "target" / "manifest.json"
            with open(manifest_path, "w") as f:
                json.dump({"nodes": {}}, f)

            result = main(["--adapter", "dbt", "--project-dir", tmpdir, "--dry-run"])
            assert result == 0

    def test_invalid_adapter(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["--adapter", "sql", "--dry-run"])
        assert exc_info.value.code == 2

    def test_mutually_exclusive_modes(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["--write", "--dry-run", "--output", "/tmp"])
        # Should fail due to mutually exclusive options
        assert exc_info.value.code == 2

    def test_reporter_formats(self):
        """Test different reporter formats"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create minimal dbt project structure
            (Path(tmpdir) / "dbt_project.yml").write_text("name: test")
            (Path(tmpdir) / "target").mkdir()
            
            # Create empty manifest
            manifest_path = Path(tmpdir) / "target" / "manifest.json"
            with open(manifest_path, "w") as f:
                json.dump({"nodes": {}}, f)

            # Test human reporter (default)
            result = main(
                ["--adapter", "dbt", "--project-dir", tmpdir, "--dry-run", "--reporter", "human"]
            )
            assert result == 0  # No changes detected in this case

            # Test diff reporter
            result = main(
                ["--adapter", "dbt", "--project-dir", tmpdir, "--dry-run", "--reporter", "diff"]
            )
            assert result == 0

            # Test github reporter
            result = main(
                ["--adapter", "dbt", "--project-dir", tmpdir, "--dry-run", "--reporter", "github"]
            )
            assert result == 0
