"""Tests for the relation module's cardinality validation checks."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
import yaml
from pydantic import ValidationError

from tval.loader import LoadError
from tval.parser import TableDef
from tval.relation import (
    RelationDef,
    load_relations,
    run_relation_checks,
    validate_relation_refs,
)
from tval.status import CheckStatus


def _make_relation(
    cardinality: str,
    from_table: str,
    from_cols: list[str],
    to_table: str,
    to_cols: list[str],
    name: str = "test-relation",
) -> RelationDef:
    """Create a RelationDef for testing."""
    return RelationDef.model_validate(
        {
            "name": name,
            "cardinality": cardinality,
            "from_": {"table": from_table, "columns": from_cols},
            "to": {"table": to_table, "columns": to_cols},
        }
    )


def _make_tdef(tmp_path: Path, name: str, col_names: list[str]) -> TableDef:
    """Create a minimal TableDef for relation tests."""
    d = tmp_path / "data" / name
    d.mkdir(parents=True, exist_ok=True)
    return TableDef.model_validate(
        {
            "table": {
                "name": name,
                "description": f"{name} table",
                "source_dir": str(d),
            },
            "columns": [
                {
                    "name": c,
                    "logical_name": c,
                    "type": "INTEGER",
                    "not_null": False,
                }
                for c in col_names
            ],
            "table_constraints": {
                "primary_key": [],
                "unique": [],
                "foreign_keys": [],
                "checks": [],
                "aggregation_checks": [],
            },
        },
        context={"project_root": str(tmp_path)},
    )


class TestLoadRelations:
    """Tests for YAML parsing of relations."""

    def test_load_valid_relations(self, tmp_path: Path) -> None:
        """Valid relations.yaml should parse successfully."""
        yaml_content = {
            "relations": [
                {
                    "name": "users-orders",
                    "cardinality": "1:N",
                    "from": {"table": "users", "columns": ["user_id"]},
                    "to": {"table": "orders", "columns": ["user_id"]},
                }
            ]
        }
        path = tmp_path / "relations.yaml"
        path.write_text(yaml.dump(yaml_content), encoding="utf-8")
        relations = load_relations(path)
        assert len(relations) == 1
        assert relations[0].cardinality == "1:N"
        assert relations[0].from_.table == "users"
        assert relations[0].to.table == "orders"

    def test_invalid_cardinality_raises(self, tmp_path: Path) -> None:
        """Invalid cardinality value should raise ValidationError."""
        yaml_content = {
            "relations": [
                {
                    "name": "bad",
                    "cardinality": "2:3",
                    "from": {"table": "a", "columns": ["id"]},
                    "to": {"table": "b", "columns": ["id"]},
                }
            ]
        }
        path = tmp_path / "relations.yaml"
        path.write_text(yaml.dump(yaml_content), encoding="utf-8")
        with pytest.raises(ValidationError):
            load_relations(path)


class TestValidateRelationRefs:
    """Tests for relation reference validation."""

    def test_undefined_table_raises(self, tmp_path: Path) -> None:
        """Referencing a table not in table_defs should raise ValueError."""
        tdefs = [_make_tdef(tmp_path, "users", ["user_id"])]
        rel = _make_relation("1:N", "users", ["user_id"], "nonexistent", ["id"])
        with pytest.raises(ValueError, match="undefined table"):
            validate_relation_refs([rel], tdefs)

    def test_undefined_column_raises(self, tmp_path: Path) -> None:
        """Referencing a column not in the table should raise ValueError."""
        tdefs = [
            _make_tdef(tmp_path, "users", ["user_id"]),
            _make_tdef(tmp_path, "orders", ["order_id"]),
        ]
        rel = _make_relation("1:N", "users", ["bad_col"], "orders", ["order_id"])
        with pytest.raises(ValueError, match="undefined column"):
            validate_relation_refs([rel], tdefs)

    def test_valid_refs_no_error(self, tmp_path: Path) -> None:
        """Valid references should not raise."""
        tdefs = [
            _make_tdef(tmp_path, "users", ["user_id"]),
            _make_tdef(tmp_path, "orders", ["user_id"]),
        ]
        rel = _make_relation("1:N", "users", ["user_id"], "orders", ["user_id"])
        validate_relation_refs([rel], tdefs)


class TestRunRelationChecks:
    """Tests for relation check execution."""

    def test_one_to_one_all_ok(self) -> None:
        """Valid 1:1 relationship should return all OK."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "profiles" (user_id INTEGER)')
        conn.execute('INSERT INTO "users" VALUES (1), (2)')
        conn.execute('INSERT INTO "profiles" VALUES (1), (2)')
        rel = _make_relation("1:1", "users", ["user_id"], "profiles", ["user_id"])
        results = run_relation_checks(conn, [rel], {})
        assert len(results) == 4
        assert all(r.status == CheckStatus.OK for r in results)

    def test_one_to_one_duplicate_ng(self) -> None:
        """Duplicate on one side of 1:1 should return NG."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "profiles" (user_id INTEGER)')
        conn.execute('INSERT INTO "users" VALUES (1), (2)')
        conn.execute('INSERT INTO "profiles" VALUES (1), (1)')
        rel = _make_relation("1:1", "users", ["user_id"], "profiles", ["user_id"])
        results = run_relation_checks(conn, [rel], {})
        statuses = [r.status for r in results]
        assert CheckStatus.NG in statuses

    def test_one_to_n_ok(self) -> None:
        """Valid 1:N relationship should return all OK."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        conn.execute('INSERT INTO "users" VALUES (1), (2)')
        conn.execute('INSERT INTO "orders" VALUES (1), (1), (2)')
        rel = _make_relation("1:N", "users", ["user_id"], "orders", ["user_id"])
        results = run_relation_checks(conn, [rel], {})
        assert len(results) == 2
        assert all(r.status == CheckStatus.OK for r in results)

    def test_one_to_n_orphan_ng(self) -> None:
        """Orphan FK in N side should return NG."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        conn.execute('INSERT INTO "users" VALUES (1)')
        conn.execute('INSERT INTO "orders" VALUES (1), (99)')
        rel = _make_relation("1:N", "users", ["user_id"], "orders", ["user_id"])
        results = run_relation_checks(conn, [rel], {})
        statuses = [r.status for r in results]
        assert CheckStatus.NG in statuses

    def test_n_to_one_ok(self) -> None:
        """Valid N:1 relationship should return all OK."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('INSERT INTO "orders" VALUES (1), (1), (2)')
        conn.execute('INSERT INTO "users" VALUES (1), (2)')
        rel = _make_relation("N:1", "orders", ["user_id"], "users", ["user_id"])
        results = run_relation_checks(conn, [rel], {})
        assert len(results) == 2
        assert all(r.status == CheckStatus.OK for r in results)

    def test_skipped_on_load_errors(self) -> None:
        """Relation checks should be SKIPPED when either table has errors."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        rel = _make_relation("1:N", "users", ["user_id"], "orders", ["user_id"])
        load_errors: dict[str, list[LoadError]] = {
            "users": [
                LoadError(
                    file_path="f.csv",
                    error_type="UNKNOWN",
                    column=None,
                    row=None,
                    raw_message="err",
                )
            ]
        }
        results = run_relation_checks(conn, [rel], load_errors)
        assert all(r.status == CheckStatus.SKIPPED for r in results)

    def test_null_values_excluded(self) -> None:
        """NULL FK values should not count as referential violations."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        conn.execute('INSERT INTO "users" VALUES (1)')
        conn.execute('INSERT INTO "orders" VALUES (1), (NULL)')
        rel = _make_relation("1:N", "users", ["user_id"], "orders", ["user_id"])
        results = run_relation_checks(conn, [rel], {})
        assert all(r.status == CheckStatus.OK for r in results)

    def test_skipped_on_check_failures(self) -> None:
        """Relation checks should be SKIPPED when a table has check failures."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        conn.execute('INSERT INTO "users" VALUES (1)')
        conn.execute('INSERT INTO "orders" VALUES (1)')
        rel = _make_relation("1:N", "users", ["user_id"], "orders", ["user_id"])
        results = run_relation_checks(
            conn, [rel], {}, check_failed_tables={"users"}
        )
        assert all(r.status == CheckStatus.SKIPPED for r in results)
        assert "check failed" in results[0].message
