"""CLI entry point for tval."""

from __future__ import annotations

import argparse
import logging

from . import __version__
from .logger import configure_log_level


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="tval",
        description="Table data validator",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # init
    init_parser = subparsers.add_parser("init", help="Generate project skeleton")
    init_parser.add_argument(
        "--dir", default="./tval", help="Target directory (default: ./tval)"
    )

    # run
    run_parser = subparsers.add_parser("run", help="Run validation")
    run_parser.add_argument("--config", default=None, help="Path to config.yaml")
    run_parser.add_argument(
        "--export",
        action="store_true",
        help="Export to Parquet if all validations pass",
    )
    run_parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress all output except errors",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and schemas without loading data",
    )

    args = parser.parse_args()

    if args.subcommand == "init":
        from .init import run_init

        run_init(args.dir)
    elif args.subcommand == "run":
        if args.quiet:
            configure_log_level(logging.ERROR)

        from .main import run

        run(args.config, export=args.export, dry_run=args.dry_run)
