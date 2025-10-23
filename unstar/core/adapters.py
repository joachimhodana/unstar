from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass


@dataclass
class ModelTarget:
    """Represents a single model file to operate on."""

    name: str
    path: str  # absolute file path


class Adapter:
    """Adapter interface for different ecosystems (dbt first).

    Concrete adapters must implement all abstract methods below.
    """

    def detect(self, project_dir: str) -> bool:
        raise NotImplementedError

    def list_models(
        self, project_dir: str, models: Sequence[str] | None, path: str | None
    ) -> Iterable[ModelTarget]:
        raise NotImplementedError

    def get_downstream_columns(self, project_dir: str, target: ModelTarget) -> dict[str, set[str]]:
        """Return mapping alias/table -> set of referenced columns in downstream nodes."""

        raise NotImplementedError

    def read_sql(self, target: ModelTarget) -> str:
        raise NotImplementedError

    def write_sql(self, target: ModelTarget, sql: str) -> None:
        raise NotImplementedError


_ADAPTERS: dict[str, Adapter] = {}


def register_adapter(name: str, adapter: Adapter) -> None:
    _ADAPTERS[name] = adapter


def get_adapter(name: str) -> Adapter:
    try:
        return _ADAPTERS[name]
    except KeyError as exc:
        available = ", ".join(sorted(_ADAPTERS))
        raise KeyError(f"Unknown adapter '{name}'. Available: {available}") from exc
