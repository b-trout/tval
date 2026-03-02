"""End-to-end integration tests for the tval validation pipeline."""

from __future__ import annotations

import shutil
from itertools import chain
from pathlib import Path

import duckdb
import yaml

from tval.builder import build_load_order, create_tables
from tval.loader import load_files
from tval.main import _build_table_reports, run
from tval.parser import ProjectConfig, load_table_definitions
from tval.status import CheckStatus

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

    def test_run_with_range_violation(self, tmp_path: Path) -> None:
        """Negative amount with min:0 should produce an NG report."""
        config_path = _setup_project(tmp_path)
        orders_csv = tmp_path / "tval" / "data" / "orders" / "orders.csv"
        orders_csv.write_text(
            "order_id,user_id,amount,status\n1,1,-50.0,pending\n",
            encoding="utf-8",
        )
        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        content = report.read_text(encoding="utf-8")
        assert "NG" in content

    def test_run_with_row_condition_violation(self, tmp_path: Path) -> None:
        """row_conditions violation should produce an NG report."""
        config_path = _setup_project(tmp_path)
        # Add row_conditions to orders schema
        schema_path = tmp_path / "tval" / "schema" / "orders.yaml"
        with open(schema_path, encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        doc["table_constraints"]["row_conditions"] = [
            {
                "description": "amount must be less than 10000",
                "condition": "amount < 10000",
            }
        ]
        with open(schema_path, "w", encoding="utf-8") as f:
            yaml.dump(doc, f, allow_unicode=True)
        # Write data that violates the condition
        orders_csv = tmp_path / "tval" / "data" / "orders" / "orders.csv"
        orders_csv.write_text(
            "order_id,user_id,amount,status\n1,1,99999.0,pending\n",
            encoding="utf-8",
        )
        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        content = report.read_text(encoding="utf-8")
        assert "NG" in content

    def test_run_with_relations(self, tmp_path: Path) -> None:
        """Running tval with relations.yaml should validate cardinalities."""
        config_path = _setup_project(tmp_path)
        relations_data = {
            "relations": [
                {
                    "name": "users-orders (1:N)",
                    "cardinality": "1:N",
                    "from": {"table": "users", "columns": ["user_id"]},
                    "to": {"table": "orders", "columns": ["user_id"]},
                }
            ]
        }
        relations_path = tmp_path / "tval" / "relations.yaml"
        relations_path.write_text(
            yaml.dump(relations_data, allow_unicode=True),
            encoding="utf-8",
        )
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        config["relations_path"] = str(relations_path)
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, allow_unicode=True)

        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        assert report.exists()
        content = report.read_text(encoding="utf-8")
        assert "Relation Cardinality Validation" in content

    def test_run_with_check_ng_skips_profiling(self, tmp_path: Path) -> None:
        """Tables with check NG should have empty profiling results."""
        config_path = _setup_project(tmp_path)
        # Write data that violates allowed_values (status check) -> NG
        orders_csv = tmp_path / "tval" / "data" / "orders" / "orders.csv"
        orders_csv.write_text(
            "order_id,user_id,amount,status\n1,1,100.0,invalid_status\n",
            encoding="utf-8",
        )
        # Load and run checks/profiling manually to inspect table_reports
        with open(config_path, encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)

        config = ProjectConfig.model_validate(raw_config)
        project_root = Path(config_path).resolve().parent
        db_path = project_root / config.database_path
        schema_dir = project_root / config.schema_dir

        table_defs = load_table_definitions(str(schema_dir), project_root=project_root)
        ordered_defs = build_load_order(table_defs)

        if db_path.exists():
            db_path.unlink()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(db_path)) as conn_rw:
            create_tables(conn_rw, ordered_defs)
            all_load_errors = {}
            for tdef in ordered_defs:
                all_load_errors[tdef.table.name] = load_files(conn_rw, tdef)

        with duckdb.connect(str(db_path), read_only=True) as conn_ro:
            table_reports = _build_table_reports(conn_ro, ordered_defs, all_load_errors)

        # Orders should have NG checks and empty profiles
        orders_report = next(
            r for r in table_reports if r.table_def.table.name == "orders"
        )
        has_ng = any(
            cr.status == CheckStatus.NG
            for cr in chain(
                orders_report.check_results,
                orders_report.agg_check_results,
            )
        )
        assert has_ng
        assert orders_report.profiles == []

        # Users should still have profiles (no check failures)
        users_report = next(
            r for r in table_reports if r.table_def.table.name == "users"
        )
        assert users_report.profiles != []

    def test_run_with_check_ng_skips_relation(self, tmp_path: Path) -> None:
        """Relation checks should be SKIPPED when a related table has check NG."""
        config_path = _setup_project(tmp_path)
        # Write data that violates allowed_values -> check NG on orders
        orders_csv = tmp_path / "tval" / "data" / "orders" / "orders.csv"
        orders_csv.write_text(
            "order_id,user_id,amount,status\n1,1,100.0,invalid_status\n",
            encoding="utf-8",
        )
        # Add relations config
        relations_data = {
            "relations": [
                {
                    "name": "users-orders (1:N)",
                    "cardinality": "1:N",
                    "from": {"table": "users", "columns": ["user_id"]},
                    "to": {"table": "orders", "columns": ["user_id"]},
                }
            ]
        }
        relations_path = tmp_path / "tval" / "relations.yaml"
        relations_path.write_text(
            yaml.dump(relations_data, allow_unicode=True),
            encoding="utf-8",
        )
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        config["relations_path"] = str(relations_path)
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, allow_unicode=True)

        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        content = report.read_text(encoding="utf-8")
        assert "SKIPPED" in content

    def test_run_with_extra_columns_csv(self, tmp_path: Path) -> None:
        """CSV with extra columns should produce EXTRA_COLUMNS error in report."""
        config_path = _setup_project(tmp_path)
        orders_csv = tmp_path / "tval" / "data" / "orders" / "orders.csv"
        orders_csv.write_text(
            "order_id,user_id,amount,status,bonus\n1,1,100.0,pending,999\n",
            encoding="utf-8",
        )
        run(str(config_path))
        report = tmp_path / "tval" / "output" / "report.html"
        content = report.read_text(encoding="utf-8")
        assert "EXTRA_COLUMNS" in content
