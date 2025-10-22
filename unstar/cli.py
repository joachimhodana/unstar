from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    """Entry point for the unstar CLI."""
    if argv is None:
        argv = sys.argv[1:]

    if "--version" in argv or "-V" in argv:
        from . import __version__

        print(__version__)
        return 0

    print("unstar: hello world")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
