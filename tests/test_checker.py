"""Tests for the checker module's validation check execution."""

from __future__ import annotations

import duckdb

from tval.checker import run_checks
from tval.loader import LoadError
from tval.parser import CheckDef, ColumnDef, RowConditionDef, TableDef
from tval.status import CheckStatus


def _make_tdef(
    tmp_path: object,
    *,
    columns: list[ColumnDef] | None = None,
    checks: list[CheckDef] | None = None,
    aggregation_checks: list[CheckDef] | None = None,
    row_conditions: list[RowConditionDef] | None = None,
    source_dir: str | None = None,
) -> TableDef:
    """Create a minimal TableDef for checker tests."""
    if source_dir is None:
        from pathlib import Path

        d = Path(str(tmp_path)) / "data" / "t"
        d.mkdir(parents=True, exist_ok=True)
        source_dir = str(d)
    return TableDef.model_validate(
        {
            "table": {
                "name": "t",
                "description": "test table",
                "source_dir": source_dir,
            },
            "columns": [c.model_dump() for c in columns]
            if columns
            else [
                {
                    "name": "id",
                    "logical_name": "ID",
                    "type": "INTEGER",
                    "not_null": True,
                }
            ],
            "table_constraints": {
                "primary_key": [],
                "unique": [],
                "foreign_keys": [],
                "checks": [c.model_dump() for c in checks] if checks else [],
                "aggregation_checks": [c.model_dump() for c in aggregation_checks]
                if aggregation_checks
                else [],
                "row_conditions": [rc.model_dump() for rc in row_conditions]
                if row_conditions
                else [],
            },
        },
        context={"project_root": str(tmp_path)},
    )


class TestChecker:
    """Tests for run_checks and _execute_check."""

    def test_allowed_values_check_ok(self, tmp_path: object) -> None:
        """Allowed values within range should return OK."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (status VARCHAR)')
        conn.execute("INSERT INTO \"t\" VALUES ('a'), ('b')")
        col = ColumnDef(
            name="status",
            logical_name="Status",
            type="VARCHAR",
            not_null=False,
            allowed_values=["a", "b", "c"],
        )
        tdef = _make_tdef(tmp_path, columns=[col])
        results, _ = run_checks(conn, tdef, [])
        assert len(results) == 1
        assert results[0].status == CheckStatus.OK

    def test_allowed_values_check_ng(self, tmp_path: object) -> None:
        """Values outside allowed range should return NG."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (status VARCHAR)')
        conn.execute("INSERT INTO \"t\" VALUES ('a'), ('x')")
        col = ColumnDef(
            name="status",
            logical_name="Status",
            type="VARCHAR",
            not_null=False,
            allowed_values=["a", "b"],
        )
        tdef = _make_tdef(tmp_path, columns=[col])
        results, _ = run_checks(conn, tdef, [])
        assert len(results) == 1
        assert results[0].status == CheckStatus.NG

    def test_user_defined_check_expect_zero_ok(self, tmp_path: object) -> None:
        """expect_zero=True with count=0 should return OK."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (1), (2)')
        check = CheckDef(
            description="no negatives",
            query="SELECT COUNT(*) FROM {table} WHERE val < 0",
            expect_zero=True,
        )
        tdef = _make_tdef(tmp_path, checks=[check])
        results, _ = run_checks(conn, tdef, [])
        assert results[0].status == CheckStatus.OK

    def test_user_defined_check_expect_zero_ng(self, tmp_path: object) -> None:
        """expect_zero=True with count>0 should return NG."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (-1), (2)')
        check = CheckDef(
            description="no negatives",
            query="SELECT COUNT(*) FROM {table} WHERE val < 0",
            expect_zero=True,
        )
        tdef = _make_tdef(tmp_path, checks=[check])
        results, _ = run_checks(conn, tdef, [])
        assert results[0].status == CheckStatus.NG

    def test_user_defined_check_expect_non_zero_ok(self, tmp_path: object) -> None:
        """expect_zero=False with count>0 should return OK."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (1), (2)')
        check = CheckDef(
            description="has rows",
            query="SELECT COUNT(*) FROM {table}",
            expect_zero=False,
        )
        tdef = _make_tdef(tmp_path, checks=[check])
        results, _ = run_checks(conn, tdef, [])
        assert results[0].status == CheckStatus.OK

    def test_user_defined_check_expect_non_zero_ng(self, tmp_path: object) -> None:
        """expect_zero=False with count=0 should return NG."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        check = CheckDef(
            description="has rows",
            query="SELECT COUNT(*) FROM {table}",
            expect_zero=False,
        )
        tdef = _make_tdef(tmp_path, checks=[check])
        results, _ = run_checks(conn, tdef, [])
        assert results[0].status == CheckStatus.NG

    def test_check_with_invalid_sql_returns_error(self, tmp_path: object) -> None:
        """Broken SQL should return ERROR status instead of SKIPPED."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER)')
        check = CheckDef(
            description="bad sql",
            query="SELCT BROKEN FROM {table}",
            expect_zero=True,
        )
        tdef = _make_tdef(tmp_path, checks=[check])
        results, _ = run_checks(conn, tdef, [])
        assert results[0].status == CheckStatus.ERROR
        assert results[0].message != ""

    def test_checks_skipped_on_load_errors(self, tmp_path: object) -> None:
        """All checks should be SKIPPED when load errors exist."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        check = CheckDef(
            description="some check",
            query="SELECT COUNT(*) FROM {table}",
            expect_zero=True,
        )
        tdef = _make_tdef(tmp_path, checks=[check])
        load_errors = [
            LoadError(
                file_path="test.csv",
                error_type="UNKNOWN",
                column=None,
                row=None,
                raw_message="error",
            )
        ]
        results, _ = run_checks(conn, tdef, load_errors)
        assert all(r.status == CheckStatus.SKIPPED for r in results)

    def test_aggregation_checks_skipped_on_load_errors(self, tmp_path: object) -> None:
        """Aggregation checks should be SKIPPED when load errors exist."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        agg_check = CheckDef(
            description="agg check",
            query="SELECT COUNT(*) FROM {table}",
            expect_zero=False,
        )
        tdef = _make_tdef(tmp_path, aggregation_checks=[agg_check])
        load_errors = [
            LoadError(
                file_path="test.csv",
                error_type="UNKNOWN",
                column=None,
                row=None,
                raw_message="error",
            )
        ]
        _, agg_results = run_checks(conn, tdef, load_errors)
        assert all(r.status == CheckStatus.SKIPPED for r in agg_results)

    def test_allowed_values_with_null_values_ignored(self, tmp_path: object) -> None:
        """NULL values should not count as allowed_values violations."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (status VARCHAR)')
        conn.execute("INSERT INTO \"t\" VALUES ('a'), (NULL)")
        col = ColumnDef(
            name="status",
            logical_name="Status",
            type="VARCHAR",
            not_null=False,
            allowed_values=["a"],
        )
        tdef = _make_tdef(tmp_path, columns=[col])
        results, _ = run_checks(conn, tdef, [])
        assert results[0].status == CheckStatus.OK

    def test_range_check_ok(self, tmp_path: object) -> None:
        """Values within range should return OK."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (5), (10), (50)')
        col = ColumnDef(
            name="val",
            logical_name="Value",
            type="INTEGER",
            not_null=True,
            min=0,
            max=100,
        )
        tdef = _make_tdef(tmp_path, columns=[col])
        results, _ = run_checks(conn, tdef, [])
        assert len(results) == 1
        assert results[0].status == CheckStatus.OK

    def test_range_check_ng_below_min(self, tmp_path: object) -> None:
        """Value below min should return NG."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (-1), (10)')
        col = ColumnDef(
            name="val",
            logical_name="Value",
            type="INTEGER",
            not_null=True,
            min=0,
            max=100,
        )
        tdef = _make_tdef(tmp_path, columns=[col])
        results, _ = run_checks(conn, tdef, [])
        assert results[0].status == CheckStatus.NG

    def test_range_check_ng_above_max(self, tmp_path: object) -> None:
        """Value above max should return NG."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (10), (200)')
        col = ColumnDef(
            name="val",
            logical_name="Value",
            type="INTEGER",
            not_null=True,
            min=0,
            max=100,
        )
        tdef = _make_tdef(tmp_path, columns=[col])
        results, _ = run_checks(conn, tdef, [])
        assert results[0].status == CheckStatus.NG

    def test_range_check_null_ignored(self, tmp_path: object) -> None:
        """NULL values should not count as range violations."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (5), (NULL)')
        col = ColumnDef(
            name="val",
            logical_name="Value",
            type="INTEGER",
            not_null=False,
            min=0,
            max=100,
        )
        tdef = _make_tdef(tmp_path, columns=[col])
        results, _ = run_checks(conn, tdef, [])
        assert results[0].status == CheckStatus.OK

    def test_range_check_skipped_on_load_errors(self, tmp_path: object) -> None:
        """Range checks should be SKIPPED when load errors exist."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        col = ColumnDef(
            name="val",
            logical_name="Value",
            type="INTEGER",
            not_null=True,
            min=0,
            max=100,
        )
        tdef = _make_tdef(tmp_path, columns=[col])
        load_errors = [
            LoadError(
                file_path="test.csv",
                error_type="UNKNOWN",
                column=None,
                row=None,
                raw_message="error",
            )
        ]
        results, _ = run_checks(conn, tdef, load_errors)
        assert all(r.status == CheckStatus.SKIPPED for r in results)

    def test_row_condition_ok(self, tmp_path: object) -> None:
        """Rows satisfying condition should return OK."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER, val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (1, 10), (2, 20)')
        col_id = ColumnDef(
            name="id",
            logical_name="ID",
            type="INTEGER",
            not_null=True,
        )
        col_val = ColumnDef(
            name="val",
            logical_name="Value",
            type="INTEGER",
            not_null=True,
        )
        rc = RowConditionDef(
            description="val must be positive",
            condition="val > 0",
        )
        tdef = _make_tdef(tmp_path, columns=[col_id, col_val], row_conditions=[rc])
        results, _ = run_checks(conn, tdef, [])
        rc_results = [r for r in results if r.description == "val must be positive"]
        assert len(rc_results) == 1
        assert rc_results[0].status == CheckStatus.OK

    def test_row_condition_ng(self, tmp_path: object) -> None:
        """Rows violating condition should return NG."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER, val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (1, -5), (2, 20)')
        col_id = ColumnDef(
            name="id",
            logical_name="ID",
            type="INTEGER",
            not_null=True,
        )
        col_val = ColumnDef(
            name="val",
            logical_name="Value",
            type="INTEGER",
            not_null=True,
        )
        rc = RowConditionDef(
            description="val must be positive",
            condition="val > 0",
        )
        tdef = _make_tdef(tmp_path, columns=[col_id, col_val], row_conditions=[rc])
        results, _ = run_checks(conn, tdef, [])
        rc_results = [r for r in results if r.description == "val must be positive"]
        assert len(rc_results) == 1
        assert rc_results[0].status == CheckStatus.NG

    def test_row_condition_skipped_on_load_errors(self, tmp_path: object) -> None:
        """Row condition checks should be SKIPPED when load errors exist."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (id INTEGER)')
        rc = RowConditionDef(
            description="always true",
            condition="1 = 1",
        )
        tdef = _make_tdef(tmp_path, row_conditions=[rc])
        load_errors = [
            LoadError(
                file_path="test.csv",
                error_type="UNKNOWN",
                column=None,
                row=None,
                raw_message="error",
            )
        ]
        results, _ = run_checks(conn, tdef, load_errors)
        rc_results = [r for r in results if r.description == "always true"]
        assert len(rc_results) == 1
        assert rc_results[0].status == CheckStatus.SKIPPED
