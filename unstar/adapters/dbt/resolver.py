from __future__ import annotations

import os
from typing import Iterable, List, Optional

from .artifacts import DbtArtifacts, DbtModel


def find_models_by_names(artifacts: DbtArtifacts, names: Optional[Iterable[str]]) -> List[DbtModel]:
    if not names:
        return []
    result: List[DbtModel] = []
    for name in names:
        model = artifacts.models_by_name.get(name)
        if model:
            result.append(model)
    return result


def find_models_by_path(artifacts: DbtArtifacts, base_path: Optional[str]) -> List[DbtModel]:
    if not base_path:
        return []
    abs_base = os.path.abspath(os.path.join(artifacts.project_dir, base_path))
    out: List[DbtModel] = []
    for m in artifacts.models_by_name.values():
        try:
            m_dir = os.path.dirname(m.path)
            if os.path.commonpath([abs_base, m_dir]) == abs_base:
                out.append(m)
        except Exception:
            continue
    return out


