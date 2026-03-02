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
from .parser import CheckDef, ColumnDef, RowConditionDef, TableDef
from .status import CheckStatus

logger = get_logger(__name__)


@dataclass
class CheckResult:
    """Result of a single validation check execution."""

    description: str
    query: str
    status: CheckStatus
    result_count: int | None
    message: str


def make_skipped_result(check: CheckDef, table_name: str, message: str) -> CheckResult:
    """Create a SKIPPED CheckResult for a check that cannot run."""
    query = check.query
    if "{table}" in query:
        query = query.replace("{table}", quote_identifier(table_name))
    logger.warning(
        "Check skipped",
        extra={
            "table": table_name,
            "check_description": check.description,
        },
    )
    return CheckResult(
        description=check.description,
        query=query,
        status=CheckStatus.SKIPPED,
        result_count=None,
        message=message,
    )


def _build_allowed_values_check(table_name: str, col: ColumnDef) -> CheckDef:
    """Build a CheckDef that verifies column values are within the allowed set."""
    qcol = quote_identifier(col.name)
    return CheckDef(
        description=f"{col.logical_name}({col.name}) allowed values check",
        query=(
            f"SELECT COUNT(*) FROM {{table}} "
            f"WHERE {qcol} NOT IN (SELECT UNNEST(?::VARCHAR[])) "
            f"AND {qcol} IS NOT NULL"
        ),
        expect_zero=True,
        params=[col.allowed_values],
    )


def _build_range_check(table_name: str, col: ColumnDef) -> CheckDef:
    """Build a CheckDef that verifies column values are within the min/max range."""
    qcol = quote_identifier(col.name)
    conditions: list[str] = []
    params: list[float] = []
    if col.min is not None:
        conditions.append(f"{qcol} < ?")
        params.append(col.min)
    if col.max is not None:
        conditions.append(f"{qcol} > ?")
        params.append(col.max)
    where_clause = " OR ".join(conditions)
    range_parts: list[str] = []
    if col.min is not None:
        range_parts.append(f"min={col.min}")
    if col.max is not None:
        range_parts.append(f"max={col.max}")
    return CheckDef(
        description=(
            f"{col.logical_name}({col.name}) range check ({', '.join(range_parts)})"
        ),
        query=(
            f"SELECT COUNT(*) FROM {{table}} "
            f"WHERE ({where_clause}) AND {qcol} IS NOT NULL"
        ),
        expect_zero=True,
        params=params,
    )


def _build_row_condition_check(condition: RowConditionDef) -> CheckDef:
    """Build a CheckDef from a declarative row-level condition."""
    return CheckDef(
        description=condition.description,
        query=f"SELECT COUNT(*) FROM {{table}} WHERE NOT ({condition.condition})",
        expect_zero=True,
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
            status = CheckStatus.OK if count == 0 else CheckStatus.NG
        else:
            status = CheckStatus.OK if count > 0 else CheckStatus.NG
        message = "" if status == CheckStatus.OK else f"Result count: {count}"
        if status == CheckStatus.NG:
            logger.error(
                "Check failed",
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
        logger.error(
            "Check execution error",
            extra={
                "table": table_name,
                "check_description": check.description,
                "error": str(e),
            },
        )
        return CheckResult(
            description=check.description,
            query=query,
            status=CheckStatus.ERROR,
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
    logger.info("Starting checks", extra={"table": table_name})

    # Skip all checks if load errors exist
    if load_errors:
        skip_msg = "Skipped due to load error"
        all_checks = chain(
            (
                _build_allowed_values_check(table_name, col)
                for col in tdef.columns
                if col.allowed_values
            ),
            (
                _build_range_check(table_name, col)
                for col in tdef.columns
                if col.min is not None or col.max is not None
            ),
            (
                _build_row_condition_check(rc)
                for rc in tdef.table_constraints.row_conditions
            ),
            tdef.table_constraints.checks,
        )
        checks_results: list[CheckResult] = [
            make_skipped_result(check, table_name, skip_msg) for check in all_checks
        ]
        agg_results: list[CheckResult] = [
            make_skipped_result(check, table_name, skip_msg)
            for check in tdef.table_constraints.aggregation_checks
        ]
        logger.info("Checks completed", extra={"table": table_name})
        return checks_results, agg_results

    # Normal case: execute checks
    checks_results = (
        [
            _execute_check(
                conn, _build_allowed_values_check(table_name, col), table_name
            )
            for col in tdef.columns
            if col.allowed_values
        ]
        + [
            _execute_check(conn, _build_range_check(table_name, col), table_name)
            for col in tdef.columns
            if col.min is not None or col.max is not None
        ]
        + [
            _execute_check(conn, _build_row_condition_check(rc), table_name)
            for rc in tdef.table_constraints.row_conditions
        ]
        + [
            _execute_check(conn, check, table_name)
            for check in tdef.table_constraints.checks
        ]
    )

    # 3. aggregation_checks
    agg_results = [
        _execute_check(conn, check, table_name)
        for check in tdef.table_constraints.aggregation_checks
    ]

    logger.info("Checks completed", extra={"table": table_name})
    return checks_results, agg_results
