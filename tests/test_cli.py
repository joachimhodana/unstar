from __future__ import annotations

import subprocess
import sys


def test_cli_runs():
    # Test help command which should work without dbt project
    result = subprocess.run([sys.executable, "-m", "unstar", "--help"], capture_output=True, text=True)
    assert result.returncode == 0
    assert "unstar" in result.stdout
