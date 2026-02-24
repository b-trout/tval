from __future__ import annotations

import shutil
from pathlib import Path

import yaml

from tval.main import run

FIXTURES = Path(__file__).parent / "fixtures"


def _setup_project(tmp_path: Path) -> Path:
    """テスト用のプロジェクト構造をtmp_pathにコピーし、パスを絶対パスに書き換える。"""
    tval_dir = tmp_path / "tval"
    tval_dir.mkdir()

    # schema/
    schema_dir = tval_dir / "schema"
    shutil.copytree(FIXTURES / "schema", schema_dir)

    # data/
    data_dir = tval_dir / "data"
    shutil.copytree(FIXTURES / "data", data_dir)

    # output/
    (tval_dir / "output").mkdir()

    # source_dirをschema YAML内で絶対パスに書き換え
    for yaml_file in schema_dir.glob("*.yaml"):
        with open(yaml_file, encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        src_rel = doc["table"]["source_dir"]
        doc["table"]["source_dir"] = str((tval_dir / src_rel).resolve())
        with open(yaml_file, "w", encoding="utf-8") as f:
            yaml.dump(doc, f, allow_unicode=True)

    # config.yaml を絶対パスで生成
    config = {
        "database_path": str(tval_dir / "work.duckdb"),
        "schema_dir": str(schema_dir),
        "output_path": str(tval_dir / "output" / "report.html"),
        "encoding_confidence_threshold": 0.8,
    }
    config_path = tval_dir / "config.yaml"
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, allow_unicode=True)

    return config_path


class TestIntegration:
    def test_run_generates_report(self, tmp_path: Path) -> None:
        config_path = _setup_project(tmp_path)
        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        assert report.exists()
        content = report.read_text(encoding="utf-8")
        assert "users" in content
        assert "orders" in content

    def test_run_with_export(self, tmp_path: Path) -> None:
        config_path = _setup_project(tmp_path)
        run(str(config_path), export=True)

        report = tmp_path / "tval" / "output" / "report.html"
        assert report.exists()

        parquet_dir = tmp_path / "tval" / "output" / "parquet"
        orders_parquet = parquet_dir / "orders" / "orders.parquet"
        assert orders_parquet.exists()
