from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .builder import quote_identifier
from .loader import LoadError
from .logger import get_logger
from .parser import CheckDef, ColumnDef, TableDef

logger = get_logger(__name__)


@dataclass
class CheckResult:
    description: str
    query: str
    status: str  # "OK" | "NG" | "SKIPPED"
    result_count: int | None
    message: str


def _build_allowed_values_check(table_name: str, col: ColumnDef) -> CheckDef:
    """allowed_valuesから自動SQLを生成する。"""
    escaped = ", ".join(
        f"'{v.replace(chr(39), chr(39) * 2)}'" for v in col.allowed_values
    )
    qcol = quote_identifier(col.name)
    return CheckDef(
        description=f"{col.logical_name}（{col.name}）の許容値チェック",
        query=(
            f"SELECT COUNT(*) FROM {{table}} "
            f"WHERE {qcol} NOT IN ({escaped}) AND {qcol} IS NOT NULL"
        ),
        expect_zero=True,
    )


def _execute_check(
    conn: duckdb.DuckDBPyConnection,
    check: CheckDef,
    table_name: str,
) -> CheckResult:
    """1件のチェックを実行する。"""
    query = check.query.replace("{table}", quote_identifier(table_name))
    try:
        result = conn.execute(query).fetchone()
        count = int(result[0]) if result else 0
        if check.expect_zero:
            status = "OK" if count == 0 else "NG"
        else:
            status = "OK" if count > 0 else "NG"
        message = "" if status == "OK" else f"結果件数: {count}"
        if status == "NG":
            logger.error(
                "チェックNG",
                extra={
                    "table": table_name,
                    "check_description": check.description,
                },
            )
        return CheckResult(
            description=check.description,
            query=query,
            status=status,
            result_count=count,
            message=message,
        )
    except Exception as e:
        logger.warning(
            "チェックSKIPPED",
            extra={
                "table": table_name,
                "check_description": check.description,
            },
        )
        return CheckResult(
            description=check.description,
            query=query,
            status="SKIPPED",
            result_count=None,
            message=str(e),
        )


def run_checks(
    conn: duckdb.DuckDBPyConnection,
    tdef: TableDef,
    load_errors: list[LoadError],
) -> tuple[list[CheckResult], list[CheckResult]]:
    """checks と aggregation_checks を実行する。"""
    table_name = tdef.table.name
    logger.info("チェック実行開始", extra={"table": table_name})

    # ロードエラーがあれば全SKIPPED
    if load_errors:
        checks_results: list[CheckResult] = []
        agg_results: list[CheckResult] = []

        all_checks: list[CheckDef] = []
        for col in tdef.columns:
            if col.allowed_values:
                all_checks.append(_build_allowed_values_check(table_name, col))
        all_checks.extend(tdef.table_constraints.checks)

        for check in all_checks:
            query = check.query.replace("{table}", quote_identifier(table_name))
            logger.warning(
                "チェックSKIPPED",
                extra={
                    "table": table_name,
                    "check_description": check.description,
                },
            )
            checks_results.append(
                CheckResult(
                    description=check.description,
                    query=query,
                    status="SKIPPED",
                    result_count=None,
                    message="ロードエラーのためスキップ",
                )
            )

        for check in tdef.table_constraints.aggregation_checks:
            query = check.query.replace("{table}", quote_identifier(table_name))
            logger.warning(
                "チェックSKIPPED",
                extra={
                    "table": table_name,
                    "check_description": check.description,
                },
            )
            agg_results.append(
                CheckResult(
                    description=check.description,
                    query=query,
                    status="SKIPPED",
                    result_count=None,
                    message="ロードエラーのためスキップ",
                )
            )

        logger.info("チェック実行完了", extra={"table": table_name})
        return checks_results, agg_results

    # 正常ケース: チェック実行
    checks_results = []

    # 1. allowed_values チェック
    for col in tdef.columns:
        if col.allowed_values:
            check = _build_allowed_values_check(table_name, col)
            checks_results.append(_execute_check(conn, check, table_name))

    # 2. checks
    for check in tdef.table_constraints.checks:
        checks_results.append(_execute_check(conn, check, table_name))

    # 3. aggregation_checks
    agg_results = []
    for check in tdef.table_constraints.aggregation_checks:
        agg_results.append(_execute_check(conn, check, table_name))

    logger.info("チェック実行完了", extra={"table": table_name})
    return checks_results, agg_results
