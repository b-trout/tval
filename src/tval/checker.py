"""Execute validation checks against loaded DuckDB tables.

Runs allowed-value checks, user-defined checks, and aggregation checks for each
table, returning structured results with OK / NG / SKIPPED status.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import chain

import duckdb

from .builder import quote_identifier
from .loader import LoadError
from .logger import get_logger
from .parser import CheckDef, ColumnDef, TableDef

logger = get_logger(__name__)


@dataclass
class CheckResult:
    """Result of a single validation check execution."""

    description: str
    query: str
    status: str  # "OK" | "NG" | "SKIPPED"
    result_count: int | None
    message: str


def _build_allowed_values_check(table_name: str, col: ColumnDef) -> CheckDef:
    """Build a CheckDef that verifies column values are within the allowed set."""
    qcol = quote_identifier(col.name)
    return CheckDef(
        description=f"{col.logical_name}（{col.name}）の許容値チェック",
        query=(
            f"SELECT COUNT(*) FROM {{table}} "
            f"WHERE {qcol} NOT IN (SELECT UNNEST(?::VARCHAR[])) "
            f"AND {qcol} IS NOT NULL"
        ),
        expect_zero=True,
        params=[col.allowed_values],
    )


def _execute_check(
    conn: duckdb.DuckDBPyConnection,
    check: CheckDef,
    table_name: str,
) -> CheckResult:
    """Execute a single check query and return a CheckResult."""
    query = check.query.replace("{table}", quote_identifier(table_name))
    try:
        result = conn.execute(query, check.params or None).fetchone()
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
    """Run all checks and aggregation checks for a table.

    If load errors exist for the table, all checks are marked as SKIPPED.
    Returns a tuple of (check_results, aggregation_check_results).
    """
    table_name = tdef.table.name
    logger.info("チェック実行開始", extra={"table": table_name})

    # ロードエラーがあれば全SKIPPED
    if load_errors:
        checks_results: list[CheckResult] = []
        agg_results: list[CheckResult] = []

        all_checks = chain(
            (
                _build_allowed_values_check(table_name, col)
                for col in tdef.columns
                if col.allowed_values
            ),
            tdef.table_constraints.checks,
        )

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
    checks_results = [
        _execute_check(conn, _build_allowed_values_check(table_name, col), table_name)
        for col in tdef.columns
        if col.allowed_values
    ] + [
        _execute_check(conn, check, table_name)
        for check in tdef.table_constraints.checks
    ]

    # 3. aggregation_checks
    agg_results = [
        _execute_check(conn, check, table_name)
        for check in tdef.table_constraints.aggregation_checks
    ]

    logger.info("チェック実行完了", extra={"table": table_name})
    return checks_results, agg_results
