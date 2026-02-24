"""Main orchestration module for tval validation pipeline.

Coordinates the full workflow: config loading, schema parsing, table creation,
data loading, validation checks, profiling, optional Parquet export, and HTML
report generation.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import yaml

from .builder import build_load_order, create_tables
from .checker import run_checks
from .exporter import ExportResult, export_table
from .loader import LoadError, load_files
from .logger import get_logger
from .parser import load_table_definitions
from .profiler import profile_table
from .reporter import TableReport, generate_report

logger = get_logger(__name__)


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

    # 1. Discover config
    if config_path is None:
        for candidate in ["./tval/config.yaml", "./config.yaml"]:
            if Path(candidate).exists():
                config_path = candidate
                break
        else:
            raise FileNotFoundError(
                "config.yaml not found. "
                "Specify with --config or create ./tval/config.yaml."
            )

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # 2. Resolve paths relative to config.yaml's parent directory
    project_root = Path(config_path).resolve().parent
    db_path = project_root / config["database_path"]
    schema_dir = project_root / config["schema_dir"]
    output_path_cfg = project_root / config["output_path"]

    # Validate database_path extension
    if db_path.suffix != ".duckdb":
        raise ValueError(f"database_path must have .duckdb extension: {db_path}")

    # 3. Load schema YAML files
    table_defs = load_table_definitions(str(schema_dir), project_root=project_root)

    # 4. Determine load order via DAG
    ordered_defs = build_load_order(table_defs)

    # 5. Connect to DuckDB (delete and recreate existing file)
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn_rw = duckdb.connect(str(db_path))

    # 6. Create tables and load files
    create_tables(conn_rw, ordered_defs)
    confidence_threshold: float = config.get("encoding_confidence_threshold", 0.8)

    all_load_errors: dict[str, list[LoadError]] = {}
    for tdef in ordered_defs:
        load_errors = load_files(
            conn_rw, tdef, confidence_threshold=confidence_threshold
        )
        all_load_errors[tdef.table.name] = load_errors

    conn_rw.close()

    # 7. Run checks/profiler on a read-only connection
    conn_ro = duckdb.connect(str(db_path), read_only=True)
    table_reports: list[TableReport] = []
    for tdef in ordered_defs:
        load_errors = all_load_errors[tdef.table.name]
        check_results, agg_check_results = run_checks(conn_ro, tdef, load_errors)
        profiles = profile_table(conn_ro, tdef, load_errors)

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
    conn_ro.close()

    # 8. Export
    if export:
        all_ok = all(r.overall_status == "OK" for r in table_reports)
        output_base_dir = output_path_cfg.parent / "parquet"
        conn_ro = duckdb.connect(str(db_path), read_only=True)
        for report, tdef in zip(table_reports, ordered_defs):
            if not all_ok:
                report.export_result = ExportResult(
                    table_name=tdef.table.name,
                    status="SKIPPED",
                    output_path="",
                    message="Skipped because tables with validation failures exist",
                )
            else:
                report.export_result = export_table(conn_ro, tdef, output_base_dir)
        conn_ro.close()

    # 9. Generate report
    output_path_cfg.parent.mkdir(parents=True, exist_ok=True)
    generate_report(
        table_reports=table_reports,
        output_path=str(output_path_cfg),
        db_path=str(db_path),
        executed_at=datetime.now().isoformat(),
    )

    logger.info("tval execution completed")
