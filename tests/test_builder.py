from __future__ import annotations

from pathlib import Path

import pytest

from tval.builder import build_load_order
from tval.parser import TableDef


def _make_tdef(
    tmp_path: Path,
    name: str,
    fk_refs: list[dict[str, object]] | None = None,
) -> TableDef:
    """テスト用のTableDefを生成する。"""
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
    def test_fk_dependency_order(self, tmp_path: Path) -> None:
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
        with pytest.raises(ValueError, match="循環依存"):
            build_load_order([a, b])

    def test_undefined_fk_reference_raises(self, tmp_path: Path) -> None:
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
        with pytest.raises(ValueError, match="FK参照先テーブルが未定義"):
            build_load_order([orders])
