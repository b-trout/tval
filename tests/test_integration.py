"""End-to-end integration tests for the tval validation pipeline."""

from __future__ import annotations

import shutil
from pathlib import Path

import yaml

from tval.main import run

FIXTURES = Path(__file__).parent / "fixtures"


def _setup_project(tmp_path: Path) -> Path:
    """Copy test fixtures into tmp_path and build a project structure."""
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

    # Rewrite source_dir in schema YAMLs to absolute paths
    for yaml_file in schema_dir.glob("*.yaml"):
        with open(yaml_file, encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        src = doc["table"]["source_dir"]
        doc["table"]["source_dir"] = str((tval_dir / src).resolve())
        with open(yaml_file, "w", encoding="utf-8") as f:
            yaml.dump(doc, f, allow_unicode=True)

    # Generate config.yaml with absolute paths
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
    """Integration tests that exercise the full run() pipeline."""

    def test_run_generates_report(self, tmp_path: Path) -> None:
        """Running tval should produce an HTML report containing table names."""
        config_path = _setup_project(tmp_path)
        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        assert report.exists()
        content = report.read_text(encoding="utf-8")
        assert "users" in content
        assert "orders" in content

    def test_run_with_export(self, tmp_path: Path) -> None:
        """Running tval with export=True should produce Parquet output files."""
        config_path = _setup_project(tmp_path)
        run(str(config_path), export=True)

        report = tmp_path / "tval" / "output" / "report.html"
        assert report.exists()

        parquet_dir = tmp_path / "tval" / "output" / "parquet"
        orders_parquet = parquet_dir / "orders" / "orders.parquet"
        assert orders_parquet.exists()

    def test_run_with_type_mismatch(self, tmp_path: Path) -> None:
        """Type mismatch data should produce an NG report."""
        config_path = _setup_project(tmp_path)
        # Overwrite orders.csv with type-mismatch data (string in integer column)
        orders_csv = tmp_path / "tval" / "data" / "orders" / "orders.csv"
        orders_csv.write_text(
            "order_id,user_id,amount,status\nnot_an_int,1,100.0,pending\n",
            encoding="utf-8",
        )
        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        content = report.read_text(encoding="utf-8")
        assert "NG" in content or "ERROR" in content

    def test_run_with_null_violation(self, tmp_path: Path) -> None:
        """NOT NULL violation should produce an NG report."""
        config_path = _setup_project(tmp_path)
        users_csv = tmp_path / "tval" / "data" / "users" / "users.csv"
        users_csv.write_text(
            "user_id,name,email\n,Alice,alice@example.com\n",
            encoding="utf-8",
        )
        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        content = report.read_text(encoding="utf-8")
        assert "NG" in content or "ERROR" in content

    def test_run_with_fk_violation(self, tmp_path: Path) -> None:
        """Foreign key violation should produce an NG report."""
        config_path = _setup_project(tmp_path)
        orders_csv = tmp_path / "tval" / "data" / "orders" / "orders.csv"
        orders_csv.write_text(
            "order_id,user_id,amount,status\n1,999,100.0,pending\n",
            encoding="utf-8",
        )
        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        content = report.read_text(encoding="utf-8")
        assert "NG" in content or "ERROR" in content

    def test_run_with_duplicate_pk(self, tmp_path: Path) -> None:
        """Duplicate primary key should produce an NG report."""
        config_path = _setup_project(tmp_path)
        users_csv = tmp_path / "tval" / "data" / "users" / "users.csv"
        users_csv.write_text(
            "user_id,name,email\n1,Alice,alice@example.com\n1,Bob,bob@example.com\n",
            encoding="utf-8",
        )
        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        content = report.read_text(encoding="utf-8")
        assert "NG" in content or "ERROR" in content

    def test_run_with_allowed_values_violation(self, tmp_path: Path) -> None:
        """Allowed values violation should result in check NG."""
        config_path = _setup_project(tmp_path)
        orders_csv = tmp_path / "tval" / "data" / "orders" / "orders.csv"
        orders_csv.write_text(
            "order_id,user_id,amount,status\n1,1,100.0,invalid_status\n",
            encoding="utf-8",
        )
        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        content = report.read_text(encoding="utf-8")
        assert "NG" in content
