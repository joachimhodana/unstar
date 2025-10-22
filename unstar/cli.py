from __future__ import annotations

import argparse
import sys
from typing import Optional, Sequence

from . import __version__
from .core.adapters import get_adapter
from .core.io import ensure_backup, read_text, unified_diff, write_text
from .core.expander import expand_select_stars


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="unstar", description="Expand SELECT * safely")
    parser.add_argument("-V", "--version", action="version", version=__version__)
    parser.add_argument("--adapter", choices=["dbt", "sql"], default="dbt")
    parser.add_argument("--project-dir", default=".")
    parser.add_argument("--models", nargs="*")
    parser.add_argument("--path")
    parser.add_argument("--files", nargs="*", help="Specific SQL files to process")
    parser.add_argument("--manifest", help="Custom path to manifest.json (for dbt)")

    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--write", action="store_true", help="Edit files in place")
    modes.add_argument("--dry-run", action="store_true", help="Print diff and exit")
    modes.add_argument("--output", help="Write updated files to directory")

    parser.add_argument("--backup", action="store_true", default=False)
    parser.add_argument("--fail-on-warn", action="store_true", default=False)
    parser.add_argument("--verbose", action="store_true", default=False)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    parser = _build_parser()
    args = parser.parse_args(list(argv))

    # Ensure adapters are registered
    if args.adapter == "dbt":
        from .adapters import dbt as _  # noqa: F401
    elif args.adapter == "sql":
        from .adapters import sql as _  # noqa: F401

    adapter = get_adapter(args.adapter)

    # Determine targets
    targets = list(adapter.list_models(args.project_dir, args.models, args.path))
    if not targets:
        print("unstar: no models selected")
        return 0

    exit_code = 0

    for t in targets:
        original_sql = adapter.read_sql(t)

        # TODO: obtain real downstream columns from adapter
        scope = adapter.get_downstream_columns(args.project_dir, t)
        new_sql = expand_select_stars(original_sql, scope)

        if new_sql == original_sql:
            continue

        if args.dry_run:
            diff = unified_diff(t.path, original_sql, t.path, new_sql)
            print(diff)
            continue

        if args.output:
            import os

            rel = os.path.relpath(t.path, start=args.project_dir)
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
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
