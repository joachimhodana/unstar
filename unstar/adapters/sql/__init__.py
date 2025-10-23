from __future__ import annotations

import os
from collections.abc import Iterable, Sequence

from ...core.adapters import Adapter, ModelTarget, register_adapter


class SqlAdapter(Adapter):
    """Simple adapter for processing individual SQL files."""

    def detect(self, project_dir: str) -> bool:
        # Always available for direct file processing
        return True

    def list_models(
        self, project_dir: str, models: Sequence[str] | None, path: str | None
    ) -> Iterable[ModelTarget]:
        # Handle direct file specification
        if models:
            for model in models:
                if os.path.exists(model):
                    yield ModelTarget(name=os.path.basename(model), path=os.path.abspath(model))

        # Handle path-based selection
        if path:
            import glob

            sql_files = glob.glob(os.path.join(path, "**/*.sql"), recursive=True)
            for sql_file in sql_files:
                yield ModelTarget(name=os.path.basename(sql_file), path=os.path.abspath(sql_file))

    def get_downstream_columns(self, project_dir: str, target: ModelTarget) -> dict[str, set[str]]:
        # For simple SQL files, we can't infer downstream columns
        # Return empty scope - stars will be left unchanged
        return {}

    def read_sql(self, target: ModelTarget) -> str:
        from ...core.io import read_text

        return read_text(target.path)

    def write_sql(self, target: ModelTarget, sql: str) -> None:
        from ...core.io import write_text

        write_text(target.path, sql)


register_adapter("sql", SqlAdapter())
