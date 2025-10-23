from __future__ import annotations

import os
from collections.abc import Iterable, Sequence

from ...core.adapters import Adapter, ModelTarget, register_adapter
from .artifacts import DbtModel, load_artifacts
from .columns import infer_downstream_columns
from .resolver import find_models_by_names, find_models_by_path


class DbtAdapter(Adapter):
    """Minimal dbt adapter placeholder.

    Full implementation will rely on dbt-artifacts-parser and sqlglot.
    """

    def __init__(self):
        self._project_dir = None

    def detect(self, project_dir: str) -> bool:  # pragma: no cover - simple stub
        return os.path.exists(os.path.join(project_dir, "dbt_project.yml"))

    def list_models(
        self,
        project_dir: str,
        models: Sequence[str] | None,
        path: str | None,
        manifest_path: str | None = None,
    ) -> Iterable[ModelTarget]:  # type: ignore[override]
        # Store project_dir for use in read_sql
        self._project_dir = project_dir
        # Use custom manifest path if provided
        artifacts = load_artifacts(project_dir, manifest_path)
        if artifacts is None:
            return []

        selected: list[DbtModel] = []
        selected.extend(find_models_by_names(artifacts, models))
        selected.extend(find_models_by_path(artifacts, path))

        # If no specific selection, return all models
        if not selected:
            selected = list(artifacts.models_by_name.values())

        # de-duplicate by name
        seen: set[str] = set()
        out: list[ModelTarget] = []
        for m in selected:
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
        # For dbt, read the raw SQL file (with Jinja templates)
        from ...core.io import read_text

        # Check if file exists before trying to read it
        if not os.path.exists(target.path):
            raise FileNotFoundError(
                f"Model file not found: {target.path}\n"
                f"This usually means the dbt manifest is out of sync with the actual files.\n"
                f"Try running 'dbt compile' to regenerate the manifest."
            )

        return read_text(target.path)

    def write_sql(self, target: ModelTarget, sql: str) -> None:  # pragma: no cover
        from ...core.io import write_text

        write_text(target.path, sql)


register_adapter("dbt", DbtAdapter())
