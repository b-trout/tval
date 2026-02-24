from __future__ import annotations

from pathlib import Path

import pytest

from tval.loader import EncodingDetectionError, _resolve_csv_path, parse_duckdb_error


class TestParseDuckdbError:
    def test_type_mismatch(self) -> None:
        msg = 'Could not convert string "abc" to INT64 in column "user_id", at Row: 3'
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "TYPE_MISMATCH"
        assert err.column == "user_id"
        assert err.row == 3
        assert err.raw_message == msg

    def test_not_null(self) -> None:
        msg = "NOT NULL constraint failed: users.email"
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "NOT_NULL"
        assert err.column == "email"
        assert err.raw_message == msg

    def test_column_mismatch(self) -> None:
        msg = "table users has 3 columns but 5 values were supplied"
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "COLUMN_MISMATCH"
        assert err.raw_message == msg

    def test_fk_violation(self) -> None:
        msg = (
            "Violates foreign key constraint because key "
            '"user_id: 999" does not exist in table "users"'
        )
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "FK_VIOLATION"
        assert err.raw_message == msg

    def test_unique_violation(self) -> None:
        msg = 'Duplicate key "user_id: 1" violates primary key constraint'
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "UNIQUE_VIOLATION"
        assert err.raw_message == msg

    def test_unique_constraint_variation(self) -> None:
        msg = 'Duplicate key "email: x@y.com" violates unique constraint'
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "UNIQUE_VIOLATION"
        assert err.raw_message == msg

    def test_unknown_error(self) -> None:
        msg = "Some completely unknown error message"
        err = parse_duckdb_error("test.csv", msg)
        assert err.error_type == "UNKNOWN"
        assert err.raw_message == msg
        assert err.column is None
        assert err.row is None


class TestResolveCsvPath:
    def test_encoding_below_threshold_raises(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "test.csv"
        csv_file.write_bytes(b"\x80\x81\x82\x83\x84\x85")
        with pytest.raises(EncodingDetectionError, match="信頼度が閾値未満"):
            _resolve_csv_path(str(csv_file), confidence_threshold=0.99)
