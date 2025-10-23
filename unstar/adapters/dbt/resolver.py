from __future__ import annotations

import os
from collections.abc import Iterable

from .artifacts import DbtArtifacts, DbtModel


def find_models_by_names(artifacts: DbtArtifacts, names: Iterable[str] | None) -> list[DbtModel]:
    if not names:
        return []
    result: list[DbtModel] = []
    for name in names:
        model = artifacts.models_by_name.get(name)
        if model:
            result.append(model)
    return result


def find_models_by_path(artifacts: DbtArtifacts, base_path: str | None) -> list[DbtModel]:
    if not base_path:
        return []
    abs_base = os.path.abspath(os.path.join(artifacts.project_dir, base_path))
    out: list[DbtModel] = []
    for m in artifacts.models_by_name.values():
        try:
            m_dir = os.path.dirname(m.path)
            if os.path.commonpath([abs_base, m_dir]) == abs_base:
                out.append(m)
        except Exception:
            continue
    return out
