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
    p = tmp_path / "test.yaml"
    p.write_text(yaml.dump(data, allow_unicode=True), encoding="utf-8")
    return p


def _valid_data(data_dir: Path) -> dict[str, object]:
    return {
        "table": {
            "name": "users",
            "description": "ユーザーテーブル",
            "source_dir": str(data_dir),
        },
        "columns": [
            {
                "name": "user_id",
                "logical_name": "ユーザーID",
                "type": "INTEGER",
                "not_null": True,
            },
            {
                "name": "name",
                "logical_name": "名前",
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
    def test_valid_yaml(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
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
        data = _valid_data(data_dir)
        data["columns"][1] = {  # type: ignore[index]
            "name": "status",
            "logical_name": "ステータス",
            "type": "VARCHAR",
            "not_null": True,
            "allowed_values": ["active", "inactive"],
        }
        data["table_constraints"]["primary_key"] = {"columns": ["user_id"]}  # type: ignore[index]
        path = _make_yaml(tmp_path, data)
        tdef = load_table_definition(path, project_root=project_root)
        status_col = [c for c in tdef.columns if c.name == "status"][0]
        assert status_col.allowed_values == ["active", "inactive"]


class TestParserInvalid:
    def test_missing_table_constraints(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        data = _valid_data(data_dir)
        del data["table_constraints"]
        path = _make_yaml(tmp_path, data)
        with pytest.raises(ValidationError):
            load_table_definition(path, project_root=project_root)

    def test_nonexistent_source_dir(self, tmp_path: Path, project_root: Path) -> None:
        data = _valid_data(project_root / "data" / "users")
        data["table"]["source_dir"] = str(  # type: ignore[index]
            project_root / "nonexistent"
        )
        path = _make_yaml(tmp_path, data)
        with pytest.raises(ValidationError, match="source_dir が存在しません"):
            load_table_definition(path, project_root=project_root)

    def test_source_dir_outside_project(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        outside = tmp_path.parent / "outside"
        outside.mkdir(exist_ok=True)
        data = _valid_data(data_dir)
        data["table"]["source_dir"] = str(outside)  # type: ignore[index]
        path = _make_yaml(tmp_path, data)
        with pytest.raises(
            ValidationError, match="source_dir はプロジェクトルート以下"
        ):
            load_table_definition(path, project_root=project_root)

    def test_pk_nonexistent_column(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        data = _valid_data(data_dir)
        data["table_constraints"]["primary_key"] = {  # type: ignore[index]
            "columns": ["nonexistent"]
        }
        path = _make_yaml(tmp_path, data)
        with pytest.raises(ValidationError, match="primary_key に存在しないカラム"):
            load_table_definition(path, project_root=project_root)

    def test_export_partition_by_nonexistent_column(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        data = _valid_data(data_dir)
        data["export"] = {"partition_by": ["nonexistent"]}
        path = _make_yaml(tmp_path, data)
        with pytest.raises(
            ValidationError, match="export.partition_by に存在しないカラム"
        ):
            load_table_definition(path, project_root=project_root)

    def test_format_on_non_datetime_type(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        data = _valid_data(data_dir)
        data["columns"][0] = {  # type: ignore[index]
            "name": "user_id",
            "logical_name": "ユーザーID",
            "type": "INTEGER",
            "not_null": True,
            "format": "%Y-%m-%d",
        }
        path = _make_yaml(tmp_path, data)
        with pytest.raises(
            ValidationError, match="format は DATE/TIMESTAMP/TIME 型のみ有効"
        ):
            load_table_definition(path, project_root=project_root)

    def test_format_on_date_type_is_valid(
        self, tmp_path: Path, data_dir: Path, project_root: Path
    ) -> None:
        data = _valid_data(data_dir)
        data["columns"].append(  # type: ignore[union-attr]
            {
                "name": "created_at",
                "logical_name": "作成日",
                "type": "DATE",
                "not_null": False,
                "format": "%Y/%m/%d",
            }
        )
        path = _make_yaml(tmp_path, data)
        tdef = load_table_definition(path, project_root=project_root)
        created_col = [c for c in tdef.columns if c.name == "created_at"][0]
        assert created_col.format == "%Y/%m/%d"
