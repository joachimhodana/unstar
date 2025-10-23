from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

from . import __version__
from .core.adapters import get_adapter
from .core.expander import expand_select_stars
from .core.io import ensure_backup, unified_diff, write_text


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="unstar", description="Expand SELECT * safely")
    parser.add_argument("-V", "--version", action="version", version=__version__)

    # Simplified selection - just use --select like dbt
    parser.add_argument("--select", nargs="*", help="Models/files to process")

    # Adapter selection
    parser.add_argument(
        "--adapter", choices=["dbt"], default="dbt", help="Adapter to use (default: dbt)"
    )

    # Project directory
    parser.add_argument("--project-dir", default=".", help="Project root directory (default: .)")

    # dbt-specific options
    parser.add_argument("--manifest", help="Custom path to dbt manifest.json (dbt adapter only)")

    # Output modes
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--write", action="store_true", help="Edit files in place")
    modes.add_argument(
        "--dry-run", action="store_true", help="Show changes without applying (default)"
    )
    modes.add_argument("--output", help="Write updated files to directory")

    # Reporter options for dry-run
    parser.add_argument(
        "--reporter",
        choices=["human", "diff", "github"],
        default="human",
        help="Output format for dry-run (default: human)",
    )

    # Other options
    parser.add_argument(
        "--backup", action="store_true", default=False, help="Create .bak files when writing"
    )
    parser.add_argument(
        "--verbose", action="store_true", default=False, help="Show detailed output"
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    parser = _build_parser()
    args = parser.parse_args(list(argv))

    # Ensure adapters are registered
    if args.adapter == "dbt":
        from .adapters import dbt as _  # noqa: F401

    adapter = get_adapter(args.adapter)

    # Use project directory from args
    project_dir = args.project_dir
    if args.adapter == "dbt":
        # Check if dbt_project.yml exists in the specified directory
        dbt_project_path = os.path.join(project_dir, "dbt_project.yml")
        if not os.path.exists(dbt_project_path):
            print(f"unstar: no dbt project found at {project_dir}")
            print("unstar: specify --project-dir or run from dbt project root")
            return 2

    # Handle --select argument (dbt models)
    if args.select:
        targets = list(adapter.list_models(project_dir, args.select, None, args.manifest))
    else:
        # No selection = process all models
        targets = list(adapter.list_models(project_dir, None, None, args.manifest))

    if not targets:
        print("unstar: no models selected")
        return 0

    exit_code = 0
    changes_detected = False

    for t in targets:
        original_sql = adapter.read_sql(t)

        scope = adapter.get_downstream_columns(project_dir, t)
        new_sql = expand_select_stars(original_sql, scope)

        if new_sql == original_sql:
            continue

        changes_detected = True

        if args.dry_run:
            if args.reporter == "diff":
                diff = unified_diff(t.path, original_sql, t.path, new_sql)
                print(diff)
            elif args.reporter == "github":
                # GitHub Actions format
                print(f"::warning file={t.path}::SELECT * can be expanded to explicit columns")
            elif args.reporter == "human":
                # Human-readable format - simple one line
                columns = sorted(scope.get("", set()))
                if columns:
                    print(f"Model {t.name}: SELECT * → {', '.join(columns)}")
                else:
                    print(f"Model {t.name}: No downstream columns found")
            continue

        if args.output:
            rel = os.path.relpath(t.path, start=project_dir)
            dest = os.path.join(args.output, rel)
            write_text(dest, new_sql)
            continue

        if args.write:
            if args.backup:
                ensure_backup(t.path)
            adapter.write_sql(t, new_sql)
            continue

        # Default when no mode provided: dry-run
        diff = unified_diff(t.path, original_sql, t.path, new_sql)
        print(diff)

    # Return 1 if changes were detected in dry-run mode (for CI/linting)
    if args.dry_run and changes_detected:
        return 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
