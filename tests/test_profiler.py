"""Tests for the profiler module's descriptive statistics computation."""

from __future__ import annotations

from pathlib import Path

import duckdb

from tval.loader import LoadError
from tval.parser import TableDef
from tval.profiler import NUMERIC_TYPES, _is_numeric, profile_table


def _make_tdef(
    tmp_path: object,
    columns: list[dict[str, object]],
) -> TableDef:
    """Create a minimal TableDef for profiler tests."""
    d = Path(str(tmp_path)) / "data" / "t"
    d.mkdir(parents=True, exist_ok=True)
    return TableDef.model_validate(
        {
            "table": {
                "name": "t",
                "description": "test table",
                "source_dir": str(d),
            },
            "columns": columns,
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


class TestProfiler:
    """Tests for profile_table and helpers."""

    def test_profile_numeric_column(self, tmp_path: object) -> None:
        """Numeric columns should have mean, std, min, max, etc."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (10), (20), (30)')
        tdef = _make_tdef(
            tmp_path,
            [
                {
                    "name": "val",
                    "logical_name": "Value",
                    "type": "INTEGER",
                    "not_null": True,
                },
            ],
        )
        profiles = profile_table(conn, tdef, [])
        assert len(profiles) == 1
        p = profiles[0]
        assert p.is_numeric is True
        assert p.count == 3
        assert p.not_null_count == 3
        assert p.mean is not None
        assert p.min is not None
        assert p.max is not None

    def test_profile_non_numeric_column(self, tmp_path: object) -> None:
        """Non-numeric columns should have count/not_null/unique but no stats."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (name VARCHAR)')
        conn.execute("INSERT INTO \"t\" VALUES ('a'), ('b'), ('a')")
        tdef = _make_tdef(
            tmp_path,
            [
                {
                    "name": "name",
                    "logical_name": "Name",
                    "type": "VARCHAR",
                    "not_null": True,
                },
            ],
        )
        profiles = profile_table(conn, tdef, [])
        assert len(profiles) == 1
        p = profiles[0]
        assert p.is_numeric is False
        assert p.count == 3
        assert p.unique_count == 2
        assert p.mean is None

    def test_profile_empty_table(self, tmp_path: object) -> None:
        """Empty tables should return an empty profile list."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        tdef = _make_tdef(
            tmp_path,
            [
                {
                    "name": "val",
                    "logical_name": "Value",
                    "type": "INTEGER",
                    "not_null": True,
                },
            ],
        )
        profiles = profile_table(conn, tdef, [])
        assert profiles == []

    def test_profile_skipped_on_load_errors(self, tmp_path: object) -> None:
        """Load errors should result in an empty profile list."""
        conn = duckdb.connect()
        conn.execute('CREATE TABLE "t" (val INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (1)')
        tdef = _make_tdef(
            tmp_path,
            [
                {
                    "name": "val",
                    "logical_name": "Value",
                    "type": "INTEGER",
                    "not_null": True,
                },
            ],
        )
        load_errors = [
            LoadError(
                file_path="f.csv",
                error_type="UNKNOWN",
                column=None,
                row=None,
                raw_message="err",
            )
        ]
        profiles = profile_table(conn, tdef, load_errors)
        assert profiles == []

    def test_is_numeric_types(self) -> None:
        """Each type in NUMERIC_TYPES should be recognized as numeric."""
        for t in NUMERIC_TYPES:
            assert _is_numeric(t) is True
        assert _is_numeric("VARCHAR") is False
        assert _is_numeric("DATE") is False

    def test_is_numeric_with_precision(self) -> None:
        """DECIMAL(10,2) should be recognized as numeric."""
        assert _is_numeric("DECIMAL(10,2)") is True
        assert _is_numeric("NUMERIC(5)") is True

    def test_profile_error_includes_error_field(self, tmp_path: object) -> None:
        """Profiling failures should include the error field."""
        conn = duckdb.connect()
        # Create table with "real_col" but define tdef with "missing_col"
        # so the per-column SQL will fail (column does not exist),
        # while the initial COUNT(*) on the table succeeds.
        conn.execute('CREATE TABLE "t" ("real_col" INTEGER)')
        conn.execute('INSERT INTO "t" VALUES (1)')
        tdef = _make_tdef(
            tmp_path,
            [
                {
                    "name": "missing_col",
                    "logical_name": "Missing",
                    "type": "INTEGER",
                    "not_null": True,
                },
            ],
        )
        profiles = profile_table(conn, tdef, [])
        assert len(profiles) == 1
        assert profiles[0].error is not None
        assert profiles[0].count == 0
