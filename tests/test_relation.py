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
    CrossCheckDef,
    RelationDef,
    load_relations,
    run_cross_checks,
    run_relation_checks,
    validate_cross_check_refs,
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
        config = load_relations(path)
        assert len(config.relations) == 1
        assert config.relations[0].cardinality == "1:N"
        assert config.relations[0].from_.table == "users"
        assert config.relations[0].to.table == "orders"

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

    def test_load_cross_checks(self, tmp_path: Path) -> None:
        """YAML with both relations and cross_checks should parse successfully."""
        yaml_content = {
            "relations": [
                {
                    "name": "users-orders",
                    "cardinality": "1:N",
                    "from": {"table": "users", "columns": ["user_id"]},
                    "to": {"table": "orders", "columns": ["user_id"]},
                }
            ],
            "cross_checks": [
                {
                    "name": "All order users must have email",
                    "tables": ["users", "orders"],
                    "query": (
                        'SELECT COUNT(*) FROM "orders" o '
                        'JOIN "users" u ON o."user_id" = u."user_id" '
                        'WHERE u."email" IS NULL'
                    ),
                    "expect_zero": True,
                }
            ],
        }
        path = tmp_path / "relations.yaml"
        path.write_text(yaml.dump(yaml_content), encoding="utf-8")
        config = load_relations(path)
        assert len(config.relations) == 1
        assert len(config.cross_checks) == 1
        assert config.cross_checks[0].name == "All order users must have email"
        assert config.cross_checks[0].tables == ["users", "orders"]

    def test_load_cross_checks_only(self, tmp_path: Path) -> None:
        """YAML with only cross_checks (no relations) should be valid."""
        yaml_content = {
            "cross_checks": [
                {
                    "name": "check1",
                    "tables": ["a", "b"],
                    "query": "SELECT 0",
                }
            ],
        }
        path = tmp_path / "relations.yaml"
        path.write_text(yaml.dump(yaml_content), encoding="utf-8")
        config = load_relations(path)
        assert config.relations == []
        assert len(config.cross_checks) == 1

    def test_load_relations_backward_compat(self, tmp_path: Path) -> None:
        """Existing relations-only YAML (no cross_checks) should still work."""
        yaml_content = {
            "relations": [
                {
                    "name": "r1",
                    "cardinality": "1:N",
                    "from": {"table": "a", "columns": ["id"]},
                    "to": {"table": "b", "columns": ["id"]},
                }
            ]
        }
        path = tmp_path / "relations.yaml"
        path.write_text(yaml.dump(yaml_content), encoding="utf-8")
        config = load_relations(path)
        assert len(config.relations) == 1
        assert config.cross_checks == []

    def test_cross_check_single_table_raises(self) -> None:
        """cross_checks with only 1 table should raise ValidationError."""
        with pytest.raises(ValidationError, match="at least 2 tables"):
            CrossCheckDef(
                name="bad",
                tables=["only_one"],
                query="SELECT 0",
            )


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
        results = run_relation_checks(conn, [rel], {}, check_failed_tables={"users"})
        assert all(r.status == CheckStatus.SKIPPED for r in results)
        assert "check failed" in results[0].message


class TestValidateCrossCheckRefs:
    """Tests for cross-check reference validation."""

    def test_validate_cross_check_undefined_table(self, tmp_path: Path) -> None:
        """Referencing an undefined table should raise ValueError."""
        tdefs = [_make_tdef(tmp_path, "users", ["user_id"])]
        cc = CrossCheckDef(
            name="bad ref",
            tables=["users", "nonexistent"],
            query="SELECT 0",
        )
        with pytest.raises(ValueError, match="undefined table"):
            validate_cross_check_refs([cc], tdefs)

    def test_valid_refs_no_error(self, tmp_path: Path) -> None:
        """Valid table references should not raise."""
        tdefs = [
            _make_tdef(tmp_path, "users", ["user_id"]),
            _make_tdef(tmp_path, "orders", ["user_id"]),
        ]
        cc = CrossCheckDef(
            name="ok",
            tables=["users", "orders"],
            query="SELECT 0",
        )
        validate_cross_check_refs([cc], tdefs)


class TestRunCrossChecks:
    """Tests for cross-check execution."""

    def test_run_cross_check_ok(self) -> None:
        """COUNT=0 with expect_zero=True should return OK."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER, email TEXT)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        conn.execute("INSERT INTO \"users\" VALUES (1, 'a@b.com')")
        conn.execute('INSERT INTO "orders" VALUES (1)')
        cc = CrossCheckDef(
            name="email check",
            tables=["users", "orders"],
            query=(
                'SELECT COUNT(*) FROM "orders" o '
                'JOIN "users" u ON o."user_id" = u."user_id" '
                'WHERE u."email" IS NULL'
            ),
            expect_zero=True,
        )
        results = run_cross_checks(conn, [cc], {})
        assert len(results) == 1
        assert results[0].status == CheckStatus.OK

    def test_run_cross_check_ng(self) -> None:
        """COUNT>0 with expect_zero=True should return NG."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER, email TEXT)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        conn.execute('INSERT INTO "users" VALUES (1, NULL)')
        conn.execute('INSERT INTO "orders" VALUES (1)')
        cc = CrossCheckDef(
            name="email check",
            tables=["users", "orders"],
            query=(
                'SELECT COUNT(*) FROM "orders" o '
                'JOIN "users" u ON o."user_id" = u."user_id" '
                'WHERE u."email" IS NULL'
            ),
            expect_zero=True,
        )
        results = run_cross_checks(conn, [cc], {})
        assert len(results) == 1
        assert results[0].status == CheckStatus.NG

    def test_run_cross_check_expect_nonzero(self) -> None:
        """COUNT>0 with expect_zero=False should return OK."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER, amount DOUBLE)')
        conn.execute('INSERT INTO "users" VALUES (1)')
        conn.execute('INSERT INTO "orders" VALUES (1, 100.0)')
        cc = CrossCheckDef(
            name="revenue check",
            tables=["users", "orders"],
            query=(
                'SELECT SUM("amount") FROM "orders" o '
                'JOIN "users" u ON o."user_id" = u."user_id"'
            ),
            expect_zero=False,
        )
        results = run_cross_checks(conn, [cc], {})
        assert len(results) == 1
        assert results[0].status == CheckStatus.OK

    def test_run_cross_check_skipped_on_load_errors(self) -> None:
        """Cross-check should be SKIPPED when a referenced table has load errors."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        cc = CrossCheckDef(
            name="check",
            tables=["users", "orders"],
            query="SELECT 0",
        )
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
        results = run_cross_checks(conn, [cc], load_errors)
        assert len(results) == 1
        assert results[0].status == CheckStatus.SKIPPED

    def test_run_cross_check_skipped_on_check_failures(self) -> None:
        """Cross-check should be SKIPPED when a referenced table has check failures."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        cc = CrossCheckDef(
            name="check",
            tables=["users", "orders"],
            query="SELECT 0",
        )
        results = run_cross_checks(conn, [cc], {}, check_failed_tables={"orders"})
        assert len(results) == 1
        assert results[0].status == CheckStatus.SKIPPED
        assert "check failed" in results[0].message

    def test_run_cross_check_sql_error(self) -> None:
        """Invalid SQL should return ERROR status."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "users" (user_id INTEGER)')
        conn.execute('CREATE TABLE "orders" (user_id INTEGER)')
        cc = CrossCheckDef(
            name="bad sql",
            tables=["users", "orders"],
            query="SELECT * FROM nonexistent_table",
        )
        results = run_cross_checks(conn, [cc], {})
        assert len(results) == 1
        assert results[0].status == CheckStatus.ERROR
