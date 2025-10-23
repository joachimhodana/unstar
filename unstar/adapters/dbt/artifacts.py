from __future__ import annotations

import json
import os
from dataclasses import dataclass


@dataclass
class DbtModel:
    name: str
    path: str  # absolute path to .sql file
    depends_on: list[str]
    node_id: str
    raw_sql: str | None
    compiled_sql: str | None


@dataclass
class DbtArtifacts:
    project_dir: str
    models_by_name: dict[str, DbtModel]


def _load_with_parser(
    manifest_path: str, project_dir: str
) -> DbtArtifacts | None:  # pragma: no cover
    try:
        from dbt_artifacts_parser.parser import parse
    except Exception:
        return None

    try:
        manifest = parse(manifest_path)
    except Exception:
        return None

    models: dict[str, DbtModel] = {}
    for node in manifest.nodes.values():
        if getattr(node, "resource_type", None) != "model":
            continue
        name = node.name
        # file_path relative to project root - prefer original_file_path as it's more reliable
        rel_path = getattr(node, "original_file_path", None) or getattr(node, "path", None) or ""
        abs_path = os.path.abspath(os.path.join(project_dir, rel_path))

        # Debug: log path resolution for troubleshooting
        if not os.path.exists(abs_path):
            print(f"Warning: Model file not found: {abs_path} (from rel_path: {rel_path})")
        depends = list(getattr(node, "depends_on", {}).get("nodes", []))
        models[name] = DbtModel(
            name=name,
            path=abs_path,
            depends_on=depends,
            node_id=getattr(node, "unique_id", ""),
            raw_sql=(getattr(node, "raw_code", None) or getattr(node, "raw_sql", None)),
            compiled_sql=(
                getattr(node, "compiled_code", None) or getattr(node, "compiled_sql", None)
            ),
        )

    return DbtArtifacts(project_dir=project_dir, models_by_name=models)


def _load_raw_json(manifest_path: str, project_dir: str) -> DbtArtifacts:
    try:
        with open(manifest_path, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return DbtArtifacts(project_dir=project_dir, models_by_name={})

    nodes = data.get("nodes", {})
    models: dict[str, DbtModel] = {}
    for node_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        name = node.get("name")
        rel_path = node.get("original_file_path") or node.get("path") or ""
        abs_path = os.path.abspath(os.path.join(project_dir, rel_path))

        # Debug: log path resolution for troubleshooting
        if not os.path.exists(abs_path):
            print(f"Warning: Model file not found: {abs_path} (from rel_path: {rel_path})")
        depends = list(node.get("depends_on", {}).get("nodes", []))
        if name:
            models[name] = DbtModel(
                name=name,
                path=abs_path,
                depends_on=depends,
                node_id=node_id,
                raw_sql=node.get("raw_sql") or node.get("raw_code"),
                compiled_sql=node.get("compiled_sql") or node.get("compiled_code"),
            )

    return DbtArtifacts(project_dir=project_dir, models_by_name=models)


def load_artifacts(project_dir: str, manifest_path: str | None = None) -> DbtArtifacts | None:
    if manifest_path is None:
        manifest_path = os.path.join(project_dir, "target", "manifest.json")

    if not os.path.exists(manifest_path):
        return None

    # Try parser first, then fallback to raw JSON
    parsed = _load_with_parser(manifest_path, project_dir)
    if parsed is not None:
        return parsed
    return _load_raw_json(manifest_path, project_dir)
