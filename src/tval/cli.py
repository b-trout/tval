from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="tval",
        description="Table data validator",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # init
    init_parser = subparsers.add_parser("init", help="プロジェクトスケルトンを生成する")
    init_parser.add_argument(
        "--dir",
        default="./tval",
        help="生成先ディレクトリ（デフォルト: ./tval）",
    )

    # run
    run_parser = subparsers.add_parser("run", help="バリデーションを実行する")
    run_parser.add_argument("--config", default=None, help="config.yamlのパス")
    run_parser.add_argument(
        "--export",
        action="store_true",
        help="バリデーション全成功時にParquetを書き出す",
    )

    args = parser.parse_args()

    if args.subcommand == "init":
        from .init import run_init

        run_init(args.dir)
    elif args.subcommand == "run":
        from .main import run

        run(args.config, export=args.export)


if __name__ == "__main__":
    main()
