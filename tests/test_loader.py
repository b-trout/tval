"""Tests for the loader module's error parsing and encoding detection."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from tval.loader import (
    EncodingDetectionError,
    _check_extra_columns,
    _col_expr,
    _resolve_csv_path,
    load_files,
    parse_duckdb_error,
)
from tval.parser import ColumnDef, TableDef


class TestParseDuckdbError:
    """Tests for parse_duckdb_error pattern matching."""

    def test_type_mismatch(self) -> None:
        """TYPE_MISMATCH errors should extract column name and row number."""
        msg = 'Could not convert string "abc" to INT64 in column "user_id", at Row: 3'
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "TYPE_MISMATCH"
        assert err.column == "user_id"
        assert err.row == 3
        assert err.raw_message == msg

    def test_not_null(self) -> None:
        """NOT_NULL errors should extract the column name."""
        msg = "NOT NULL constraint failed: users.email"
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "NOT_NULL"
        assert err.column == "email"
        assert err.raw_message == msg

    def test_column_mismatch(self) -> None:
        """COLUMN_MISMATCH errors should be recognized."""
        msg = "table users has 3 columns but 5 values were supplied"
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "COLUMN_MISMATCH"
        assert err.raw_message == msg

    def test_fk_violation(self) -> None:
        """FK_VIOLATION errors should be recognized."""
        msg = (
            "Violates foreign key constraint because key "
            '"user_id: 999" does not exist in table "users"'
        )
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "FK_VIOLATION"
        assert err.raw_message == msg

    def test_unique_violation(self) -> None:
        """UNIQUE_VIOLATION with 'primary key' should be recognized."""
        msg = 'Duplicate key "user_id: 1" violates primary key constraint'
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "UNIQUE_VIOLATION"
        assert err.raw_message == msg

    def test_unique_constraint_variation(self) -> None:
        """UNIQUE_VIOLATION with 'unique constraint' should also be recognized."""
        msg = 'Duplicate key "email: x@y.com" violates unique constraint'
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "UNIQUE_VIOLATION"
        assert err.raw_message == msg

    def test_unknown_error(self) -> None:
        """Unrecognized errors should be classified as UNKNOWN."""
        msg = "Some completely unknown error message"
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "UNKNOWN"
        assert err.raw_message == msg
        assert err.column is None
        assert err.row is None


class TestResolveCsvPath:
    """Tests for CSV encoding detection and threshold validation."""

    def test_encoding_below_threshold_raises(self, tmp_path: Path) -> None:
        """Low-confidence encoding detection should raise EncodingDetectionError."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_bytes(b"\x80\x81\x82\x83\x84\x85")
        with pytest.raises(
            EncodingDetectionError, match="confidence is below threshold"
        ):
            _resolve_csv_path(str(csv_file), confidence_threshold=0.99)


def _make_tdef(tmp_path: Path, source_dir: str | None = None) -> TableDef:
    """Create a minimal TableDef for loader tests."""
    d = Path(source_dir) if source_dir else (tmp_path / "data" / "t")
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
                    "name": "name",
                    "logical_name": "Name",
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
        },
        context={"project_root": str(tmp_path)},
    )


class TestLoadFiles:
    """Tests for load_files with actual DuckDB connections."""

    def test_load_csv_file(self, tmp_path: Path) -> None:
        """CSV files should be loaded into the table."""
        data_dir = tmp_path / "data" / "t"
        data_dir.mkdir(parents=True, exist_ok=True)
        csv_file = data_dir / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

        tdef = _make_tdef(tmp_path, source_dir=str(data_dir))
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER NOT NULL, name VARCHAR)')
        errors = load_files(conn, tdef)
        assert errors == []
        row = conn.execute('SELECT COUNT(*) FROM "t"').fetchone()
        assert row is not None
        assert row[0] == 2

    def test_load_empty_directory(self, tmp_path: Path) -> None:
        """Empty source directory should return NO_FILES error."""
        data_dir = tmp_path / "data" / "t"
        data_dir.mkdir(parents=True, exist_ok=True)
        tdef = _make_tdef(tmp_path, source_dir=str(data_dir))
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER NOT NULL, name VARCHAR)')
        errors = load_files(conn, tdef)
        assert len(errors) == 1
        assert errors[0].error_type == "NO_FILES"

    def test_load_xls_returns_unsupported(self, tmp_path: Path) -> None:
        """.xls files should return UNSUPPORTED_FORMAT error."""
        data_dir = tmp_path / "data" / "t"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "test.xls").write_bytes(b"fake xls")
        tdef = _make_tdef(tmp_path, source_dir=str(data_dir))
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER NOT NULL, name VARCHAR)')
        errors = load_files(conn, tdef)
        assert len(errors) == 1
        assert errors[0].error_type == "UNSUPPORTED_FORMAT"

    def test_load_unsupported_extension_skipped(self, tmp_path: Path) -> None:
        """.json files should be silently skipped (no error)."""
        data_dir = tmp_path / "data" / "t"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "test.json").write_text("{}", encoding="utf-8")
        (data_dir / "test.csv").write_text("id,name\n1,Alice\n", encoding="utf-8")
        tdef = _make_tdef(tmp_path, source_dir=str(data_dir))
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER NOT NULL, name VARCHAR)')
        errors = load_files(conn, tdef)
        assert errors == []

    def test_load_sjis_csv_file(self, tmp_path: Path) -> None:
        """SJIS-encoded CSV files should be loaded correctly."""
        data_dir = tmp_path / "data" / "t"
        data_dir.mkdir(parents=True, exist_ok=True)
        csv_file = data_dir / "test.csv"
        # Use enough rows for chardet to detect the encoding reliably
        rows = "\n".join(f"{i},テスト名前{i}" for i in range(1, 51))
        content = f"id,name\n{rows}\n"
        csv_file.write_bytes(content.encode("cp932"))

        tdef = _make_tdef(tmp_path, source_dir=str(data_dir))
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER NOT NULL, name VARCHAR)')
        errors = load_files(conn, tdef)
        assert errors == []
        row = conn.execute('SELECT COUNT(*) FROM "t"').fetchone()
        assert row is not None
        assert row[0] == 50
        name = conn.execute('SELECT "name" FROM "t" WHERE "id" = 1').fetchone()
        assert name is not None
        assert name[0] == "テスト名前1"


class TestResolveCsvPathUtf8:
    """Tests for _resolve_csv_path with UTF-8 content."""

    def test_resolve_csv_path_utf8_returns_original(self, tmp_path: Path) -> None:
        """UTF-8 CSV should be returned as-is without a temp file."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n", encoding="utf-8")
        resolved, is_tmp = _resolve_csv_path(str(csv_file), confidence_threshold=0.5)
        assert resolved == str(csv_file)
        assert is_tmp is False

    def test_resolve_csv_path_sjis_creates_tmp(self, tmp_path: Path) -> None:
        """SJIS CSV should be converted to a temporary UTF-8 file."""
        csv_file = tmp_path / "test.csv"
        content = "id,名前\n1,太郎\n2,花子\n"
        csv_file.write_bytes(content.encode("cp932"))
        resolved, is_tmp = _resolve_csv_path(str(csv_file), confidence_threshold=0.5)
        assert resolved != str(csv_file)
        assert is_tmp is True
        resolved_content = Path(resolved).read_text(encoding="utf-8")
        assert "名前" in resolved_content
        assert "太郎" in resolved_content
        Path(resolved).unlink(missing_ok=True)


class TestColExpr:
    """Tests for _col_expr column expression building."""

    def test_col_expr_without_format(self) -> None:
        """Columns without format should return just the quoted name."""
        col = ColumnDef(
            name="id",
            logical_name="ID",
            type="INTEGER",
            not_null=True,
        )
        assert _col_expr(col) == '"id"'

    def test_col_expr_with_format(self) -> None:
        """Columns with format should return STRPTIME-wrapped expression."""
        col = ColumnDef(
            name="created_at",
            logical_name="Created",
            type="DATE",
            not_null=False,
            format="%Y-%m-%d",
        )
        expr = _col_expr(col)
        assert "STRPTIME" in expr
        assert "%Y-%m-%d" in expr
        assert "created_at" in expr


class TestCheckExtraColumns:
    """Tests for _check_extra_columns detection."""

    def test_csv_with_extra_columns(self, tmp_path: Path) -> None:
        """CSV with columns not in schema should return EXTRA_COLUMNS error."""
        data_dir = tmp_path / "data" / "t"
        data_dir.mkdir(parents=True, exist_ok=True)
        csv_file = data_dir / "test.csv"
        csv_file.write_text("id,name,extra_col\n1,Alice,foo\n", encoding="utf-8")

        tdef = _make_tdef(tmp_path, source_dir=str(data_dir))
        conn = duckdb.connect()
        error = _check_extra_columns(conn, tdef, str(csv_file), ".csv")
        assert error is not None
        assert error.error_type == "EXTRA_COLUMNS"
        assert "extra_col" in error.raw_message

    def test_csv_without_extra_columns(self, tmp_path: Path) -> None:
        """CSV with only schema columns should return None."""
        data_dir = tmp_path / "data" / "t"
        data_dir.mkdir(parents=True, exist_ok=True)
        csv_file = data_dir / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n", encoding="utf-8")

        tdef = _make_tdef(tmp_path, source_dir=str(data_dir))
        conn = duckdb.connect()
        error = _check_extra_columns(conn, tdef, str(csv_file), ".csv")
        assert error is None

    def test_load_csv_with_extra_columns_returns_error(self, tmp_path: Path) -> None:
        """load_files should return EXTRA_COLUMNS when CSV has extra columns."""
        data_dir = tmp_path / "data" / "t"
        data_dir.mkdir(parents=True, exist_ok=True)
        csv_file = data_dir / "test.csv"
        csv_file.write_text("id,name,bonus\n1,Alice,100\n", encoding="utf-8")

        tdef = _make_tdef(tmp_path, source_dir=str(data_dir))
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER NOT NULL, name VARCHAR)')
        errors = load_files(conn, tdef)
        assert len(errors) == 1
        assert errors[0].error_type == "EXTRA_COLUMNS"
        assert "bonus" in errors[0].raw_message
