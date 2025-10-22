from __future__ import annotations

import difflib
import os
from pathlib import Path
from typing import Tuple


def read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def write_text(path: str, content: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(content, encoding="utf-8", newline="\n")


def ensure_backup(path: str) -> None:
    bak = f"{path}.bak"
    if not os.path.exists(bak) and os.path.exists(path):
        Path(bak).write_text(Path(path).read_text(encoding="utf-8"), encoding="utf-8")


def unified_diff(a_path: str, a_text: str, b_path: str, b_text: str) -> str:
    return "\n".join(
        difflib.unified_diff(
            a_text.splitlines(), b_text.splitlines(), fromfile=a_path, tofile=b_path, lineterm=""
        )
    )


