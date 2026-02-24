"""CLI entry point for tval."""

from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="tval",
        description="Table data validator",
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

    args = parser.parse_args()

    if args.subcommand == "init":
        from .init import run_init

        run_init(args.dir)
    elif args.subcommand == "run":
        from .main import run

        run(args.config, export=args.export)
