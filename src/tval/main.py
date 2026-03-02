"""Main orchestration module for tval validation pipeline.

Coordinates the full workflow: config loading, schema parsing, table creation,
data loading, validation checks, profiling, optional Parquet export, and HTML
report generation.
"""

from __future__ import annotations

from datetime import datetime
from itertools import chain
from pathlib import Path

import duckdb
import yaml

from .builder import build_load_order, create_tables
from .checker import CheckResult, run_checks
from .exporter import ExportResult, export_table
from .loader import LoadError, load_files
from .logger import get_logger
from .parser import ProjectConfig, TableDef, load_table_definitions
from .profiler import profile_table
from .relation import (
    RelationDef,
    load_relations,
    run_relation_checks,
    validate_relation_refs,
)
from .reporter import TableReport, generate_report
from .status import CheckStatus, ExportStatus

logger = get_logger(__name__)


def _discover_config_path(config_path: str | None) -> str:
    """Auto-discover config.yaml if not specified."""
    if config_path is not None:
        return config_path
    for candidate in ["./tval/config.yaml", "./config.yaml"]:
        if Path(candidate).exists():
            return candidate
    raise FileNotFoundError(
        "config.yaml not found. Specify with --config or create ./tval/config.yaml."
    )


def _load_data(
    conn: duckdb.DuckDBPyConnection,
    ordered_defs: list[TableDef],
    confidence_threshold: float,
) -> dict[str, list[LoadError]]:
    """Create tables and load all files, returning errors by table name."""
    create_tables(conn, ordered_defs)
    all_load_errors: dict[str, list[LoadError]] = {}
    for tdef in ordered_defs:
        load_errors = load_files(conn, tdef, confidence_threshold=confidence_threshold)
        all_load_errors[tdef.table.name] = load_errors
    return all_load_errors


def _build_table_reports(
    conn: duckdb.DuckDBPyConnection,
    ordered_defs: list[TableDef],
    all_load_errors: dict[str, list[LoadError]],
) -> list[TableReport]:
    """Run checks and profiling for each table, returning reports."""
    table_reports: list[TableReport] = []
    for tdef in ordered_defs:
        load_errors = all_load_errors[tdef.table.name]
        check_results, agg_check_results = run_checks(conn, tdef, load_errors)

        # Early termination: skip profiling if any check failed
        has_check_failure = any(
            cr.status in (CheckStatus.NG, CheckStatus.ERROR)
            for cr in chain(check_results, agg_check_results)
        )
        if has_check_failure:
            profiles = []
        else:
            profiles = profile_table(conn, tdef, load_errors)

        table_reports.append(
            TableReport(
                table_def=tdef,
                load_errors=load_errors,
                check_results=check_results,
                agg_check_results=agg_check_results,
                profiles=profiles,
                export_result=None,
            )
        )
    return table_reports


def run(config_path: str | None = None, export: bool = False) -> None:
    """Run the full validation pipeline and generate an HTML report.

    Loads config, parses schemas, creates DuckDB tables, loads data files,
    runs validation checks and profiling, optionally exports to Parquet,
    and writes an HTML report.

    Args:
        config_path: Path to config.yaml. Auto-discovered if None.
        export: If True, export validated tables to Parquet files.

    Raises:
        FileNotFoundError: If config.yaml or schema files are not found.
        ValueError: If database_path does not have a .duckdb extension.
    """
    logger.info("tval execution started")

    resolved_path = _discover_config_path(config_path)

    with open(resolved_path, encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)
    config = ProjectConfig.model_validate(raw_config)

    # Resolve paths relative to config.yaml's parent directory
    project_root = Path(resolved_path).resolve().parent
    db_path = project_root / config.database_path
    schema_dir = project_root / config.schema_dir
    output_path_cfg = project_root / config.output_path

    # Load schema YAML files
    table_defs = load_table_definitions(str(schema_dir), project_root=project_root)

    # Load relations (optional)
    relations: list[RelationDef] = []
    if config.relations_path:
        relations_file = project_root / config.relations_path
        relations = load_relations(str(relations_file))
        validate_relation_refs(relations, table_defs)

    # Determine load order via DAG
    ordered_defs = build_load_order(table_defs)

    # Connect to DuckDB (delete and recreate existing file)
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn_rw:
        all_load_errors = _load_data(
            conn_rw, ordered_defs, config.encoding_confidence_threshold
        )

    # Run checks/profiler on a read-only connection
    relation_check_results: list[CheckResult] = []
    with duckdb.connect(str(db_path), read_only=True) as conn_ro:
        table_reports = _build_table_reports(conn_ro, ordered_defs, all_load_errors)

        # Collect tables with check failures for relation skip
        check_failed_tables = {
            r.table_def.table.name
            for r in table_reports
            if any(
                cr.status in (CheckStatus.NG, CheckStatus.ERROR)
                for cr in chain(r.check_results, r.agg_check_results)
            )
        }

        if relations:
            relation_check_results = run_relation_checks(
                conn_ro, relations, all_load_errors, check_failed_tables
            )

    # Export
    if export:
        tables_ok = all(r.overall_status == CheckStatus.OK for r in table_reports)
        relations_ok = all(
            r.status in (CheckStatus.OK, CheckStatus.SKIPPED)
            for r in relation_check_results
        )
        all_ok = tables_ok and relations_ok
        output_base_dir = output_path_cfg.parent / "parquet"
        with duckdb.connect(str(db_path), read_only=True) as conn_ro:
            for report, tdef in zip(table_reports, ordered_defs, strict=True):
                if not all_ok:
                    report.export_result = ExportResult(
                        table_name=tdef.table.name,
                        status=ExportStatus.SKIPPED,
                        output_path="",
                        message="Skipped because tables with validation failures exist",
                    )
                else:
                    report.export_result = export_table(conn_ro, tdef, output_base_dir)

    # Generate report
    output_path_cfg.parent.mkdir(parents=True, exist_ok=True)
    generate_report(
        table_reports=table_reports,
        output_path=str(output_path_cfg),
        db_path=str(db_path),
        executed_at=datetime.now().isoformat(),
        relation_check_results=relation_check_results,
    )

    logger.info("tval execution completed")
