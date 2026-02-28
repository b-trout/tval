"""Generate HTML validation reports using Jinja2 templates.

Aggregates per-table load errors, check results, profiling data, and export
results into a single HTML report.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from .checker import CheckResult
from .exporter import ExportResult
from .loader import LoadError
from .parser import TableDef
from .profiler import ColumnProfile


@dataclass
class TableReport:
    """Aggregated validation results for a single table."""

    table_def: TableDef
    load_errors: list[LoadError]
    check_results: list[CheckResult]
    agg_check_results: list[CheckResult]
    profiles: list[ColumnProfile]
    export_result: ExportResult | None

    @property
    def overall_status(self) -> str:
        """Return 'NG' if any load errors or check failures exist, otherwise 'OK'."""
        if self.load_errors:
            return "NG"
        for cr in self.check_results:
            if cr.status in ("NG", "ERROR"):
                return "NG"
        for cr in self.agg_check_results:
            if cr.status in ("NG", "ERROR"):
                return "NG"
        return "OK"


def generate_report(
    table_reports: list[TableReport],
    output_path: str,
    db_path: str,
    executed_at: str,
    relation_check_results: list[CheckResult] | None = None,
) -> None:
    """Render the HTML report from Jinja2 template and write to output_path."""
    template_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=True,
    )
    template = env.get_template("report.html.j2")

    summary = {
        "total": len(table_reports),
        "ok": sum(1 for r in table_reports if r.overall_status == "OK"),
        "ng": sum(1 for r in table_reports if r.overall_status == "NG"),
    }

    rel_results = relation_check_results or []
    relation_summary = {
        "total": len(rel_results),
        "ok": sum(1 for r in rel_results if r.status == "OK"),
        "ng": sum(1 for r in rel_results if r.status in ("NG", "ERROR")),
        "skipped": sum(1 for r in rel_results if r.status == "SKIPPED"),
    }

    html = template.render(
        executed_at=executed_at,
        db_path=db_path,
        table_reports=table_reports,
        summary=summary,
        relation_check_results=rel_results,
        relation_summary=relation_summary,
    )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(html, encoding="utf-8")
