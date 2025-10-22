from __future__ import annotations

import subprocess
import sys


def test_cli_runs():
    result = subprocess.run([sys.executable, "-m", "unstar"], capture_output=True, text=True)
    assert result.returncode == 0
    assert "unstar" in result.stdout
