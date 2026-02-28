"""Tests for the reporter module's overall status logic and report generation."""

from __future__ import annotations

from pathlib import Path

from tval.checker import CheckResult
from tval.loader import LoadError
from tval.parser import TableDef
from tval.reporter import TableReport, generate_report
from tval.status import CheckStatus


def _make_tdef(tmp_path: Path) -> TableDef:
    """Create a minimal TableDef for reporter tests."""
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


def _make_check_result(status: CheckStatus) -> CheckResult:
    return CheckResult(
        description="test check",
        query="SELECT 1",
        status=status,
        result_count=0,
        message="",
    )


class TestReporter:
    """Tests for TableReport.overall_status and generate_report."""

    def test_overall_status_ok(self, tmp_path: Path) -> None:
        """No errors should yield OK."""
        report = TableReport(
            table_def=_make_tdef(tmp_path),
            load_errors=[],
            check_results=[_make_check_result(CheckStatus.OK)],
            agg_check_results=[],
            profiles=[],
            export_result=None,
        )
        assert report.overall_status == CheckStatus.OK

    def test_overall_status_ng_on_load_errors(self, tmp_path: Path) -> None:
        """Load errors should yield NG."""
        report = TableReport(
            table_def=_make_tdef(tmp_path),
            load_errors=[
                LoadError(
                    file_path="f.csv",
                    error_type="UNKNOWN",
                    column=None,
                    row=None,
                    raw_message="err",
                )
            ],
            check_results=[],
            agg_check_results=[],
            profiles=[],
            export_result=None,
        )
        assert report.overall_status == CheckStatus.NG

    def test_overall_status_ng_on_check_failure(self, tmp_path: Path) -> None:
        """NG check result should yield NG."""
        report = TableReport(
            table_def=_make_tdef(tmp_path),
            load_errors=[],
            check_results=[_make_check_result(CheckStatus.NG)],
            agg_check_results=[],
            profiles=[],
            export_result=None,
        )
        assert report.overall_status == CheckStatus.NG

    def test_overall_status_ng_on_check_error(self, tmp_path: Path) -> None:
        """ERROR check result should yield NG overall."""
        report = TableReport(
            table_def=_make_tdef(tmp_path),
            load_errors=[],
            check_results=[_make_check_result(CheckStatus.ERROR)],
            agg_check_results=[],
            profiles=[],
            export_result=None,
        )
        assert report.overall_status == CheckStatus.NG

    def test_generate_report_creates_file(self, tmp_path: Path) -> None:
        """generate_report should create an HTML file."""
        report = TableReport(
            table_def=_make_tdef(tmp_path),
            load_errors=[],
            check_results=[_make_check_result(CheckStatus.OK)],
            agg_check_results=[],
            profiles=[],
            export_result=None,
        )
        output_path = str(tmp_path / "report.html")
        generate_report(
            table_reports=[report],
            output_path=output_path,
            db_path="test.duckdb",
            executed_at="2024-01-01T00:00:00",
        )
        result = Path(output_path)
        assert result.exists()
        content = result.read_text(encoding="utf-8")
        assert "tval Validation Report" in content
        assert "t" in content
