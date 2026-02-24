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
        print(f"Error: {target} already exists. Will not overwrite.")  # noqa: T201
        raise SystemExit(1)

    # Create directories
    target.mkdir(parents=True)
    (target / "schema").mkdir()
    (target / "data").mkdir()
    (target / "output").mkdir()

    # Place .gitkeep files
    for subdir in ["schema", "data", "output"]:
        (target / subdir / ".gitkeep").touch()

    # Generate config.yaml
    (target / "config.yaml").write_text(CONFIG_TEMPLATE, encoding="utf-8")

    print(f"Created {target}/")  # noqa: T201

    # Append to .gitignore
    gitignore_path = Path(".gitignore")
    existing_lines: set[str] = set()
    if gitignore_path.exists():
        existing_lines = set(gitignore_path.read_text(encoding="utf-8").splitlines())

    entries_to_add = [e for e in GITIGNORE_ENTRIES if e not in existing_lines]

    if entries_to_add:
        with open(gitignore_path, "a", encoding="utf-8") as f:
            f.write("\n" + "\n".join(entries_to_add) + "\n")
        print("Appended tval/data/, tval/output/ to .gitignore")  # noqa: T201

    print(  # noqa: T201
        "\nNext steps:\n"
        "  1. Add table definition YAML files to tval/schema/\n"
        "  2. Place your data files in tval/data/\n"
        "  3. Run validation with: tval run"
    )
