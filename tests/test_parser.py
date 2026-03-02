"""Tests for the YAML schema parser and validation logic."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from tval.parser import load_table_definition


@pytest.fixture()
def project_root(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture()
def data_dir(project_root: Path) -> Path:
    d = project_root / "data" / "users"
    d.mkdir(parents=True)
    return d


def _make_yaml(tmp_path: Path, data: dict[str, object]) -> Path:
    """Write a dict as a YAML file and return its path."""
    p = tmp_path / "test.yaml"
    p.write_text(yaml.dump(data, allow_unicode=True), encoding="utf-8")
    return p


def _valid_data(data_dir: Path) -> dict[str, object]:
    """Return a minimal valid table definition dict for testing."""
    return {
        "table": {
            "name": "users",
            "description": "Users table",
            "source_dir": str(data_dir),
        },
        "columns": [
            {
                "name": "user_id",
                "logical_name": "User ID",
                "type": "INTEGER",
                "not_null": True,
            },
            {
                "name": "name",
                "logical_name": "Name",
                "type": "VARCHAR",
                "not_null": True,
            },
        ],
        "table_constraints": {
            "primary_key": {"columns": ["user_id"]},
            "unique": [],
            "foreign_keys": [],
            "checks": [],
            "aggregation_checks": [],
        },
    }


class TestParserValid:
    """Tests for valid schema definitions that should parse successfully."""

    def test_valid_yaml(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """A well-formed YAML should parse into a correct TableDef."""
        data = _valid_data(data_dir)
        path = _make_yaml(tmp_path, data)
        tdef = load_table_definition(path, project_root=project_root)
        assert tdef.table.name == "users"
        assert len(tdef.columns) == 2
        assert tdef.columns[0].type == "INTEGER"
        assert len(tdef.table_constraints.primary_key) == 1

    def test_allowed_values_parsed(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """allowed_values should be correctly parsed into the ColumnDef."""
        data = _valid_data(data_dir)
        data["columns"][1] = {  # type: ignore[index]
            "name": "status",
            "logical_name": "Status",
            "type": "VARCHAR",
            "not_null": True,
            "allowed_values": ["active", "inactive"],
        }
        data["table_constraints"]["primary_key"] = {"columns": ["user_id"]}  # type: ignore[index]
        path = _make_yaml(tmp_path, data)
        tdef = load_table_definition(path, project_root=project_root)
        status_col = [c for c in tdef.columns if c.name == "status"][0]
        assert status_col.allowed_values == ["active", "inactive"]

    def test_min_max_parsed_on_numeric_column(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """min/max should be correctly parsed on a numeric column."""
        data = _valid_data(data_dir)
        data["columns"][0] = {  # type: ignore[index]
            "name": "user_id",
            "logical_name": "User ID",
            "type": "INTEGER",
            "not_null": True,
            "min": 0,
            "max": 1000,
        }
        path = _make_yaml(tmp_path, data)
        tdef = load_table_definition(path, project_root=project_root)
        col = [c for c in tdef.columns if c.name == "user_id"][0]
        assert col.min == 0
        assert col.max == 1000

    def test_min_only_parsed(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """min-only without max should be accepted."""
        data = _valid_data(data_dir)
        data["columns"][0] = {  # type: ignore[index]
            "name": "user_id",
            "logical_name": "User ID",
            "type": "INTEGER",
            "not_null": True,
            "min": 0,
        }
        path = _make_yaml(tmp_path, data)
        tdef = load_table_definition(path, project_root=project_root)
        col = [c for c in tdef.columns if c.name == "user_id"][0]
        assert col.min == 0
        assert col.max is None

    def test_row_conditions_parsed(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """row_conditions should be correctly parsed into TableConstraints."""
        data = _valid_data(data_dir)
        data["table_constraints"]["row_conditions"] = [  # type: ignore[index]
            {
                "description": "user_id must be positive",
                "condition": "user_id > 0",
            }
        ]
        path = _make_yaml(tmp_path, data)
        tdef = load_table_definition(path, project_root=project_root)
        assert len(tdef.table_constraints.row_conditions) == 1
        rc = tdef.table_constraints.row_conditions[0]
        assert rc.description == "user_id must be positive"
        assert rc.condition == "user_id > 0"

    def test_row_conditions_defaults_to_empty(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """row_conditions should default to empty list when omitted."""
        data = _valid_data(data_dir)
        path = _make_yaml(tmp_path, data)
        tdef = load_table_definition(path, project_root=project_root)
        assert tdef.table_constraints.row_conditions == []


class TestParserInvalid:
    """Tests for invalid schema definitions that should raise ValidationError."""

    def test_missing_table_constraints(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """Missing table_constraints should raise ValidationError."""
        data = _valid_data(data_dir)
        del data["table_constraints"]
        path = _make_yaml(tmp_path, data)
        with pytest.raises(ValidationError):
            load_table_definition(path, project_root=project_root)

    def test_nonexistent_source_dir(self, tmp_path: Path, project_root: Path) -> None:
        """A nonexistent source_dir should raise ValidationError."""
        data = _valid_data(project_root / "data" / "users")
        data["table"]["source_dir"] = str(  # type: ignore[index]
            project_root / "nonexistent"
        )
        path = _make_yaml(tmp_path, data)
        with pytest.raises(ValidationError, match="source_dir does not exist"):
            load_table_definition(path, project_root=project_root)

    def test_source_dir_outside_project(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """A source_dir outside the project root should raise ValidationError."""
        outside = tmp_path.parent / "outside"
        outside.mkdir(exist_ok=True)
        data = _valid_data(data_dir)
        data["table"]["source_dir"] = str(outside)  # type: ignore[index]
        path = _make_yaml(tmp_path, data)
        with pytest.raises(
            ValidationError, match="source_dir must be under the project root"
        ):
            load_table_definition(path, project_root=project_root)

    def test_pk_nonexistent_column(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """PK with nonexistent column raises ValidationError."""
        data = _valid_data(data_dir)
        data["table_constraints"]["primary_key"] = {  # type: ignore[index]
            "columns": ["nonexistent"]
        }
        path = _make_yaml(tmp_path, data)
        with pytest.raises(ValidationError, match="Column not found in primary_key"):
            load_table_definition(path, project_root=project_root)

    def test_export_partition_by_nonexistent_column(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """partition_by with nonexistent column raises ValidationError."""
        data = _valid_data(data_dir)
        data["export"] = {"partition_by": ["nonexistent"]}
        path = _make_yaml(tmp_path, data)
        with pytest.raises(
            ValidationError, match="Column not found in export.partition_by"
        ):
            load_table_definition(path, project_root=project_root)

    def test_min_on_non_numeric_type(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """Using min on a non-numeric type should raise ValidationError."""
        data = _valid_data(data_dir)
        data["columns"][1] = {  # type: ignore[index]
            "name": "name",
            "logical_name": "Name",
            "type": "VARCHAR",
            "not_null": True,
            "min": 0,
        }
        path = _make_yaml(tmp_path, data)
        with pytest.raises(
            ValidationError, match="min/max is only valid for numeric types"
        ):
            load_table_definition(path, project_root=project_root)

    def test_min_greater_than_max(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """min > max should raise ValidationError."""
        data = _valid_data(data_dir)
        data["columns"][0] = {  # type: ignore[index]
            "name": "user_id",
            "logical_name": "User ID",
            "type": "INTEGER",
            "not_null": True,
            "min": 100,
            "max": 10,
        }
        path = _make_yaml(tmp_path, data)
        with pytest.raises(ValidationError, match="min .* must be <= max"):
            load_table_definition(path, project_root=project_root)

    def test_format_on_non_datetime_type(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """Using format on a non-datetime type should raise ValidationError."""
        data = _valid_data(data_dir)
        data["columns"][0] = {  # type: ignore[index]
            "name": "user_id",
            "logical_name": "User ID",
            "type": "INTEGER",
            "not_null": True,
            "format": "%Y-%m-%d",
        }
        path = _make_yaml(tmp_path, data)
        with pytest.raises(
            ValidationError, match="format is only valid for DATE/TIMESTAMP/TIME types"
        ):
            load_table_definition(path, project_root=project_root)

    def test_format_on_date_type_is_valid(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        """Using format on a DATE type should be accepted."""
        data = _valid_data(data_dir)
        data["columns"].append(  # type: ignore[union-attr]
            {
                "name": "created_at",
                "logical_name": "Created date",
                "type": "DATE",
                "not_null": False,
                "format": "%Y/%m/%d",
            }
        )
        path = _make_yaml(tmp_path, data)
        tdef = load_table_definition(path, project_root=project_root)
        created_col = [c for c in tdef.columns if c.name == "created_at"][0]
        assert created_col.format == "%Y/%m/%d"
