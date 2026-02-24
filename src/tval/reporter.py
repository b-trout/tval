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
    table_def: TableDef
    load_errors: list[LoadError]
    check_results: list[CheckResult]
    agg_check_results: list[CheckResult]
    profiles: list[ColumnProfile]
    export_result: ExportResult | None

    @property
    def overall_status(self) -> str:
        if self.load_errors:
            return "NG"
        for cr in self.check_results:
            if cr.status == "NG":
                return "NG"
        for cr in self.agg_check_results:
            if cr.status == "NG":
                return "NG"
        return "OK"


def generate_report(
    table_reports: list[TableReport],
    output_path: str,
    db_path: str,
    executed_at: str,
) -> None:
    """Jinja2でHTMLを生成しoutput_pathに書き出す。"""
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

    html = template.render(
        executed_at=executed_at,
        db_path=db_path,
        table_reports=table_reports,
        summary=summary,
    )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(html, encoding="utf-8")
