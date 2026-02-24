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
    """バリデーションを実行しHTMLレポートを生成する。"""
    logger.info("tval 実行開始")

    # 1. config探索
    if config_path is None:
        for candidate in ["./tval/config.yaml", "./config.yaml"]:
            if Path(candidate).exists():
                config_path = candidate
                break
        else:
            raise FileNotFoundError(
                "config.yaml が見つかりません。"
                "--config で明示指定するか、./tval/config.yaml を作成してください。"
            )

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # 2. project_root（セキュリティ用: source_dirのパストラバーサル防止）
    project_root = Path(config_path).resolve().parent

    # config内のパスはそのまま使用（CLIのCWDまたは絶対パス基準）
    db_path = Path(config["database_path"])
    schema_dir = config["schema_dir"]
    output_path = config["output_path"]

    # database_pathの拡張子検証
    if db_path.suffix != ".duckdb":
        raise ValueError(
            f"database_path の拡張子は .duckdb である必要があります: {db_path}"
        )

    # 3. スキーマYAML読み込み
    table_defs = load_table_definitions(schema_dir, project_root=project_root)

    # 4. DAGによるロード順決定
    ordered_defs = build_load_order(table_defs)

    # 5. DuckDB接続（既存ファイルは削除して再作成）
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn_rw = duckdb.connect(str(db_path))

    # 6. テーブル作成・ファイルロード
    create_tables(conn_rw, ordered_defs)
    confidence_threshold: float = config.get("encoding_confidence_threshold", 0.8)

    all_load_errors: dict[str, list[LoadError]] = {}
    for tdef in ordered_defs:
        load_errors = load_files(
            conn_rw, tdef, confidence_threshold=confidence_threshold
        )
        all_load_errors[tdef.table.name] = load_errors

    conn_rw.close()

    # 7. checks/profilerはread_only接続で実行
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

    # 8. エクスポート
    if export:
        all_ok = all(r.overall_status == "OK" for r in table_reports)
        output_base_dir = Path(output_path).parent / "parquet"
        conn_ro = duckdb.connect(str(db_path), read_only=True)
        for report, tdef in zip(table_reports, ordered_defs):
            if not all_ok:
                report.export_result = ExportResult(
                    table_name=tdef.table.name,
                    status="SKIPPED",
                    output_path="",
                    message="バリデーションNGのテーブルが存在するためスキップしました",
                )
            else:
                report.export_result = export_table(conn_ro, tdef, output_base_dir)
        conn_ro.close()

    # 9. レポート生成
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    generate_report(
        table_reports=table_reports,
        output_path=output_path,
        db_path=str(db_path),
        executed_at=datetime.now().isoformat(),
    )

    logger.info("tval 実行完了")
