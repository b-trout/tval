"""Scaffold a new tval project directory.

Creates the standard directory structure (schema/, data/, output/), a default
config.yaml, and appends tval-specific entries to .gitignore.
"""

from __future__ import annotations

from pathlib import Path

CONFIG_TEMPLATE = """\
database_path: ./tval/work.duckdb
schema_dir: ./tval/schema
output_path: ./tval/output/report.html
encoding_confidence_threshold: 0.8
"""

GITIGNORE_ENTRIES = [
    "# tval",
    "tval/data/",
    "tval/output/",
]


def run_init(target_dir: str = "./tval") -> None:
    """Generate the tval project skeleton under the given directory.

    Creates subdirectories, config.yaml, .gitkeep files, and updates
    .gitignore. Exits with code 1 if the target directory already exists.
    """
    target = Path(target_dir)

    if target.exists():
        print(f"エラー: {target} はすでに存在します。上書きは行いません。")  # noqa: T201
        raise SystemExit(1)

    # ディレクトリ作成
    target.mkdir(parents=True)
    (target / "schema").mkdir()
    (target / "data").mkdir()
    (target / "output").mkdir()

    # .gitkeep配置
    for subdir in ["schema", "data", "output"]:
        (target / subdir / ".gitkeep").touch()

    # config.yaml生成
    (target / "config.yaml").write_text(CONFIG_TEMPLATE, encoding="utf-8")

    print(f"✅ {target}/ を作成しました")  # noqa: T201

    # .gitignore追記
    gitignore_path = Path(".gitignore")
    existing_lines: set[str] = set()
    if gitignore_path.exists():
        existing_lines = set(gitignore_path.read_text(encoding="utf-8").splitlines())

    entries_to_add: list[str] = []
    for entry in GITIGNORE_ENTRIES:
        if entry not in existing_lines:
            entries_to_add.append(entry)

    if entries_to_add:
        with open(gitignore_path, "a", encoding="utf-8") as f:
            f.write("\n" + "\n".join(entries_to_add) + "\n")
        print("✅ .gitignore に tval/data/, tval/output/ を追記しました")  # noqa: T201

    print(  # noqa: T201
        "\n次のステップ:\n"
        "  1. tval/schema/ にテーブル定義YAMLを追加してください\n"
        "  2. tval/data/ に受領データを配置してください\n"
        "  3. tval run で バリデーションを実行してください"
    )
