from __future__ import annotations

import os
from typing import Iterable, Optional, Sequence

from ...core.adapters import Adapter, ModelTarget, register_adapter
from .artifacts import DbtArtifacts, DbtModel, load_artifacts
from .resolver import find_models_by_names, find_models_by_path
from .columns import infer_downstream_columns


class DbtAdapter(Adapter):
    """Minimal dbt adapter placeholder.

    Full implementation will rely on dbt-artifacts-parser and sqlglot.
    """

    def detect(self, project_dir: str) -> bool:  # pragma: no cover - simple stub
        return os.path.exists(os.path.join(project_dir, "dbt_project.yml"))

    def list_models(
        self, project_dir: str, models: Optional[Sequence[str]], path: Optional[str]
    ) -> Iterable[ModelTarget]:  # type: ignore[override]
        artifacts = load_artifacts(project_dir)
        if artifacts is None:
            return []

        selected: list[DbtModel] = []
        selected.extend(find_models_by_names(artifacts, models))
        selected.extend(find_models_by_path(artifacts, path))

        # de-duplicate by name
        seen: set[str] = set()
        out: list[ModelTarget] = []
        for m in selected or list(artifacts.models_by_name.values()):
            if m.name in seen:
                continue
            seen.add(m.name)
            out.append(ModelTarget(name=m.name, path=m.path))
        return out

    def get_downstream_columns(self, project_dir, target):  # type: ignore[override]
        artifacts = load_artifacts(project_dir)
        if artifacts is None:
            return {}

        # Collect SQL from simple downstream dependents (by name match in depends_on)
        sql_texts: list[str] = []
        target_models = [m for m in artifacts.models_by_name.values() if m.name == target.name]
        target_ids = {m.node_id for m in target_models}
        for m in artifacts.models_by_name.values():
            if any(dep in target_ids for dep in m.depends_on):
                if m.compiled_sql:
                    sql_texts.append(m.compiled_sql)
                elif m.raw_sql:
                    sql_texts.append(m.raw_sql)

        # Alias heuristics: allow empty alias set for now
        cols = infer_downstream_columns(sql_texts, set())
        # Map empty key to unqualified scope expected by expander
        if "" in cols:
            return {"": set(cols[""])}
        return cols

    def read_sql(self, target: ModelTarget) -> str:  # pragma: no cover - placeholder
        from ...core.io import read_text

        return read_text(target.path)

    def write_sql(self, target: ModelTarget, sql: str) -> None:  # pragma: no cover
        from ...core.io import write_text

        write_text(target.path, sql)


register_adapter("dbt", DbtAdapter())


