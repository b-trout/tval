"""Tests for the exporter module's Parquet export functionality."""

from __future__ import annotations

from pathlib import Path

import duckdb

from tval.exporter import _escape_string_literal, export_table
from tval.parser import TableDef


def _make_tdef(
    tmp_path: Path,
    *,
    partition_by: list[str] | None = None,
) -> TableDef:
    """Create a minimal TableDef for exporter tests."""
    d = tmp_path / "data" / "t"
    d.mkdir(parents=True, exist_ok=True)
    return TableDef.model_validate(
        {
            "table": {
                "name": "t",
                "description": "test table",
                "source_dir": str(d),
            },
            "columns": [
                {
                    "name": "id",
                    "logical_name": "ID",
                    "type": "INTEGER",
                    "not_null": True,
                },
                {
                    "name": "cat",
                    "logical_name": "Category",
                    "type": "VARCHAR",
                    "not_null": False,
                },
            ],
            "table_constraints": {
                "primary_key": [],
                "unique": [],
                "foreign_keys": [],
                "checks": [],
                "aggregation_checks": [],
            },
            "export": {"partition_by": partition_by or []},
        },
        context={"project_root": str(tmp_path)},
    )


class TestExporter:
    """Tests for export_table and helpers."""

    def test_export_simple_table(self, tmp_path: Path) -> None:
        """Basic export should produce a Parquet file with OK status."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER, cat VARCHAR)')
        conn.execute("INSERT INTO \"t\" VALUES (1, 'a'), (2, 'b')")
        tdef = _make_tdef(tmp_path)
        output_dir = tmp_path / "out"
        result = export_table(conn, tdef, output_dir)
        assert result.status == "OK"
        assert Path(result.output_path).exists()

    def test_export_with_partition(self, tmp_path: Path) -> None:
        """Partitioned export should produce directory structure with OK status."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER, cat VARCHAR)')
        conn.execute("INSERT INTO \"t\" VALUES (1, 'a'), (2, 'b')")
        tdef = _make_tdef(tmp_path, partition_by=["cat"])
        output_dir = tmp_path / "out"
        result = export_table(conn, tdef, output_dir)
        assert result.status == "OK"
        assert Path(result.output_path).exists()

    def test_export_creates_directory(self, tmp_path: Path) -> None:
        """Export should create the output directory if it does not exist."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER, cat VARCHAR)')
        conn.execute("INSERT INTO \"t\" VALUES (1, 'a')")
        tdef = _make_tdef(tmp_path)
        output_dir = tmp_path / "nested" / "deep" / "out"
        assert not output_dir.exists()
        result = export_table(conn, tdef, output_dir)
        assert result.status == "OK"
        assert output_dir.exists()

    def test_export_error_returns_error_result(self, tmp_path: Path) -> None:
        """Export of a non-existent table should return ERROR status."""
        conn = duckdb.connect()
        # Table "t" does not exist — should produce an error
        tdef = _make_tdef(tmp_path)
        output_dir = tmp_path / "out"
        result = export_table(conn, tdef, output_dir)
        assert result.status == "ERROR"
        assert result.message != ""

    def test_escape_string_literal(self) -> None:
        """Single quotes should be doubled."""
        assert _escape_string_literal("it's") == "it''s"
        assert _escape_string_literal("no quotes") == "no quotes"
        assert _escape_string_literal("a''b") == "a''''b"
