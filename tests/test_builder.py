"""Tests for the builder module's load order resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from tval.builder import (
    build_create_table_sql,
    build_load_order,
    quote_identifier,
    validate_identifier,
)
from tval.parser import TableDef


def _make_tdef(
    tmp_path: Path,
    name: str,
    fk_refs: list[dict[str, object]] | None = None,
) -> TableDef:
    """Create a minimal TableDef for testing with optional FK references."""
    data_dir = tmp_path / "data" / name
    data_dir.mkdir(parents=True, exist_ok=True)
    fks = fk_refs or []
    data = {
        "table": {
            "name": name,
            "description": f"{name} table",
            "source_dir": str(data_dir),
        },
        "columns": [
            {
                "name": f"{name}_id",
                "logical_name": f"{name} ID",
                "type": "INTEGER",
                "not_null": True,
            },
        ],
        "table_constraints": {
            "primary_key": {"columns": [f"{name}_id"]},
            "unique": [],
            "foreign_keys": fks,
            "checks": [],
            "aggregation_checks": [],
        },
    }
    return TableDef.model_validate(data, context={"project_root": tmp_path})


class TestBuildLoadOrder:
    """Tests for build_load_order topological sorting."""

    def test_fk_dependency_order(self, tmp_path: Path) -> None:
        """Referenced tables should appear before dependent tables."""
        users = _make_tdef(tmp_path, "users")
        orders = _make_tdef(
            tmp_path,
            "orders",
            fk_refs=[
                {
                    "columns": ["orders_id"],
                    "references": {"table": "users", "columns": ["users_id"]},
                }
            ],
        )
        result = build_load_order([orders, users])
        names = [t.table.name for t in result]
        assert names.index("users") < names.index("orders")

    def test_circular_dependency_raises(self, tmp_path: Path) -> None:
        """Circular FK dependencies should raise ValueError."""
        a = _make_tdef(
            tmp_path,
            "a",
            fk_refs=[
                {
                    "columns": ["a_id"],
                    "references": {"table": "b", "columns": ["b_id"]},
                }
            ],
        )
        b = _make_tdef(
            tmp_path,
            "b",
            fk_refs=[
                {
                    "columns": ["b_id"],
                    "references": {"table": "a", "columns": ["a_id"]},
                }
            ],
        )
        with pytest.raises(ValueError, match="Circular dependency"):
            build_load_order([a, b])

    def test_undefined_fk_reference_raises(self, tmp_path: Path) -> None:
        """FK referencing an undefined table should raise ValueError."""
        orders = _make_tdef(
            tmp_path,
            "orders",
            fk_refs=[
                {
                    "columns": ["orders_id"],
                    "references": {
                        "table": "nonexistent",
                        "columns": ["id"],
                    },
                }
            ],
        )
        with pytest.raises(ValueError, match="FK reference table is not defined"):
            build_load_order([orders])


class TestValidateIdentifier:
    """Tests for validate_identifier."""

    def test_validate_identifier_valid(self) -> None:
        """Normal identifiers should pass validation."""
        assert validate_identifier("users") == "users"
        assert validate_identifier("_private") == "_private"
        assert validate_identifier("col_1") == "col_1"

    def test_validate_identifier_invalid_start_with_number(self) -> None:
        """Identifiers starting with a number should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("1col")

    def test_validate_identifier_invalid_special_chars(self) -> None:
        """Identifiers with special characters should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("col-name")
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("col name")


class TestQuoteIdentifier:
    """Tests for quote_identifier."""

    def test_quote_identifier(self) -> None:
        """Identifiers should be wrapped in double quotes."""
        assert quote_identifier("users") == '"users"'
        assert quote_identifier("_t1") == '"_t1"'


class TestBuildCreateTableSql:
    """Tests for build_create_table_sql."""

    def test_build_create_table_sql_with_all_constraints(self, tmp_path: Path) -> None:
        """SQL should include PK, FK, UNIQUE, and NOT NULL constraints."""
        _make_tdef(tmp_path, "parent")
        child_data = {
            "table": {
                "name": "child",
                "description": "child table",
                "source_dir": str(tmp_path / "data" / "child"),
            },
            "columns": [
                {
                    "name": "child_id",
                    "logical_name": "Child ID",
                    "type": "INTEGER",
                    "not_null": True,
                },
                {
                    "name": "parent_id",
                    "logical_name": "Parent ID",
                    "type": "INTEGER",
                    "not_null": False,
                },
            ],
            "table_constraints": {
                "primary_key": {"columns": ["child_id"]},
                "unique": [{"columns": ["child_id"]}],
                "foreign_keys": [
                    {
                        "columns": ["parent_id"],
                        "references": {
                            "table": "parent",
                            "columns": ["parent_id"],
                        },
                    }
                ],
                "checks": [],
                "aggregation_checks": [],
            },
        }
        (tmp_path / "data" / "child").mkdir(parents=True, exist_ok=True)
        child = TableDef.model_validate(child_data, context={"project_root": tmp_path})
        sql = build_create_table_sql(child)
        assert "NOT NULL" in sql
        assert "PRIMARY KEY" in sql
        assert "UNIQUE" in sql
        assert "FOREIGN KEY" in sql
        assert "REFERENCES" in sql

    def test_build_create_table_sql_minimal(self, tmp_path: Path) -> None:
        """SQL with no constraints should just have column definitions."""
        d = tmp_path / "data" / "simple"
        d.mkdir(parents=True, exist_ok=True)
        tdef = TableDef.model_validate(
            {
                "table": {
                    "name": "simple",
                    "description": "simple table",
                    "source_dir": str(d),
                },
                "columns": [
                    {
                        "name": "col_a",
                        "logical_name": "Column A",
                        "type": "VARCHAR",
                        "not_null": False,
                    }
                ],
                "table_constraints": {
                    "primary_key": [],
                    "unique": [],
                    "foreign_keys": [],
                    "checks": [],
                    "aggregation_checks": [],
                },
            },
            context={"project_root": tmp_path},
        )
        sql = build_create_table_sql(tdef)
        assert "CREATE TABLE" in sql
        assert "PRIMARY KEY" not in sql
        assert "FOREIGN KEY" not in sql

    def test_single_table_no_dependencies(self, tmp_path: Path) -> None:
        """A single table with no FKs should be returned as-is."""
        t = _make_tdef(tmp_path, "solo")
        result = build_load_order([t])
        assert len(result) == 1
        assert result[0].table.name == "solo"
