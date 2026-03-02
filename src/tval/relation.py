"""Validate inter-table relationship cardinalities.

Parses relations.yaml, generates SQL validation queries for each defined
relationship (1:1, 1:N, N:1, N:N), and returns structured CheckResult instances.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import duckdb
import yaml
from pydantic import BaseModel, field_validator

from .builder import quote_identifier
from .checker import CheckResult, make_skipped_result
from .loader import LoadError
from .logger import get_logger
from .parser import CheckDef, TableDef
from .status import CheckStatus

logger = get_logger(__name__)


class RelationEndpoint(BaseModel):
    """One side of a relation (table + columns)."""

    table: str
    columns: list[str]


class RelationDef(BaseModel):
    """A single relationship definition between two tables."""

    name: str
    cardinality: Literal["1:1", "1:N", "N:1", "N:N"]
    from_: RelationEndpoint
    to: RelationEndpoint


class CrossCheckDef(BaseModel):
    """A user-defined SQL check that spans multiple tables."""

    name: str
    tables: list[str]
    query: str
    expect_zero: bool = True

    @field_validator("tables")
    @classmethod
    def validate_tables_min_length(cls, v: list[str]) -> list[str]:
        if len(v) < 2:
            raise ValueError("cross_checks must reference at least 2 tables")
        return v


class RelationsConfig(BaseModel):
    """Top-level relations.yaml model."""

    relations: list[RelationDef] = []
    cross_checks: list[CrossCheckDef] = []


def load_relations(path: str | Path) -> RelationsConfig:
    """Load relations.yaml and return validated RelationsConfig."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    for rel in data.get("relations", []):
        if "from" in rel:
            rel["from_"] = rel.pop("from")
    return RelationsConfig.model_validate(data)


def validate_relation_refs(
    relations: list[RelationDef],
    table_defs: list[TableDef],
) -> None:
    """Validate that all tables and columns referenced in relations exist.

    Raises:
        ValueError: If a referenced table or column is not defined.
    """
    table_map: dict[str, set[str]] = {}
    for tdef in table_defs:
        table_map[tdef.table.name] = {col.name for col in tdef.columns}

    for rel in relations:
        for endpoint, label in [(rel.from_, "from"), (rel.to, "to")]:
            if endpoint.table not in table_map:
                raise ValueError(
                    f"Relation '{rel.name}' references undefined table "
                    f"in {label}: {endpoint.table}"
                )
            for col in endpoint.columns:
                if col not in table_map[endpoint.table]:
                    raise ValueError(
                        f"Relation '{rel.name}' references undefined column "
                        f"'{col}' in {label}.{endpoint.table}"
                    )


def validate_cross_check_refs(
    cross_checks: list[CrossCheckDef],
    table_defs: list[TableDef],
) -> None:
    """Validate that all tables referenced in cross_checks exist in schema.

    Raises:
        ValueError: If a referenced table is not defined.
    """
    table_names = {tdef.table.name for tdef in table_defs}
    for cc in cross_checks:
        for table in cc.tables:
            if table not in table_names:
                raise ValueError(
                    f"Cross-check '{cc.name}' references undefined table: {table}"
                )


def _build_uniqueness_sql(table: str, cols: list[str]) -> str:
    """Build SQL that returns the count of duplicate key combinations.

    Returns 0 if all key combinations are unique (check passes).
    """
    col_list = ", ".join(cols)
    return (
        f"SELECT COUNT(*) FROM ("
        f"SELECT {col_list} FROM {table} "
        f"GROUP BY {col_list} HAVING COUNT(*) > 1"
        f")"
    )


def _build_referential_sql(
    source_table: str,
    source_cols: list[str],
    target_table: str,
    target_cols: list[str],
) -> str:
    """Build SQL that returns the count of orphan rows.

    Finds rows in source_table whose column values have no match in target_table.
    Returns 0 if referential integrity holds (check passes).
    NULLs are excluded (consistent with SQL FK semantics).
    """
    join_cond = " AND ".join(
        f"s.{sc} = t.{tc}" for sc, tc in zip(source_cols, target_cols, strict=True)
    )
    null_filter = " AND ".join(f"s.{sc} IS NOT NULL" for sc in source_cols)
    target_null = " AND ".join(f"t.{tc} IS NULL" for tc in target_cols)

    return (
        f"SELECT COUNT(*) FROM {source_table} s "
        f"LEFT JOIN {target_table} t ON {join_cond} "
        f"WHERE {null_filter} AND {target_null}"
    )


def _build_relation_checks(rel: RelationDef) -> list[tuple[str, str]]:
    """Build (description, sql) pairs for a relation's cardinality checks.

    Each query returns a COUNT(*) that should be zero for the check to pass.
    """
    from_table = quote_identifier(rel.from_.table)
    to_table = quote_identifier(rel.to.table)
    from_cols = [quote_identifier(c) for c in rel.from_.columns]
    to_cols = [quote_identifier(c) for c in rel.to.columns]

    from_col_names = ", ".join(rel.from_.columns)
    to_col_names = ", ".join(rel.to.columns)

    checks: list[tuple[str, str]] = []

    if rel.cardinality == "1:1":
        checks.append(
            (
                f"[{rel.name}] {rel.from_.table}({from_col_names}) uniqueness",
                _build_uniqueness_sql(from_table, from_cols),
            )
        )
        checks.append(
            (
                f"[{rel.name}] {rel.to.table}({to_col_names}) uniqueness",
                _build_uniqueness_sql(to_table, to_cols),
            )
        )
        checks.append(
            (
                f"[{rel.name}] {rel.from_.table} -> {rel.to.table} "
                f"referential integrity",
                _build_referential_sql(from_table, from_cols, to_table, to_cols),
            )
        )
        checks.append(
            (
                f"[{rel.name}] {rel.to.table} -> {rel.from_.table} "
                f"referential integrity",
                _build_referential_sql(to_table, to_cols, from_table, from_cols),
            )
        )

    elif rel.cardinality == "1:N":
        checks.append(
            (
                f"[{rel.name}] {rel.from_.table}({from_col_names}) uniqueness (1-side)",
                _build_uniqueness_sql(from_table, from_cols),
            )
        )
        checks.append(
            (
                f"[{rel.name}] {rel.to.table} -> {rel.from_.table} "
                f"referential integrity",
                _build_referential_sql(to_table, to_cols, from_table, from_cols),
            )
        )

    elif rel.cardinality == "N:1":
        checks.append(
            (
                f"[{rel.name}] {rel.to.table}({to_col_names}) uniqueness (1-side)",
                _build_uniqueness_sql(to_table, to_cols),
            )
        )
        checks.append(
            (
                f"[{rel.name}] {rel.from_.table} -> {rel.to.table} "
                f"referential integrity",
                _build_referential_sql(from_table, from_cols, to_table, to_cols),
            )
        )

    elif rel.cardinality == "N:N":
        checks.append(
            (
                f"[{rel.name}] {rel.from_.table} -> {rel.to.table} "
                f"referential integrity",
                _build_referential_sql(from_table, from_cols, to_table, to_cols),
            )
        )
        checks.append(
            (
                f"[{rel.name}] {rel.to.table} -> {rel.from_.table} "
                f"referential integrity",
                _build_referential_sql(to_table, to_cols, from_table, from_cols),
            )
        )

    return checks


def run_relation_checks(
    conn: duckdb.DuckDBPyConnection,
    relations: list[RelationDef],
    all_load_errors: dict[str, list[LoadError]],
    check_failed_tables: set[str] | None = None,
) -> list[CheckResult]:
    """Run cardinality validation checks for all defined relations.

    If either table in a relation has load errors or check failures,
    all checks for that relation are SKIPPED. Returns a flat list of
    CheckResult.
    """
    logger.info("Starting relation checks")
    check_failed = check_failed_tables or set()
    results: list[CheckResult] = []

    for rel in relations:
        from_errors = all_load_errors.get(rel.from_.table, [])
        to_errors = all_load_errors.get(rel.to.table, [])
        from_check_failed = rel.from_.table in check_failed
        to_check_failed = rel.to.table in check_failed
        check_pairs = _build_relation_checks(rel)

        if from_errors or to_errors or from_check_failed or to_check_failed:
            skipped_tables = []
            if from_errors:
                skipped_tables.append(rel.from_.table)
            if to_errors:
                skipped_tables.append(rel.to.table)
            if from_check_failed:
                skipped_tables.append(f"{rel.from_.table} (check failed)")
            if to_check_failed:
                skipped_tables.append(f"{rel.to.table} (check failed)")
            skip_msg = f"Skipped due to errors in: {', '.join(skipped_tables)}"
            for desc, query in check_pairs:
                check_def = CheckDef(description=desc, query=query)
                results.append(make_skipped_result(check_def, rel.name, skip_msg))
            continue

        for desc, query in check_pairs:
            try:
                row = conn.execute(query).fetchone()
                count = int(row[0]) if row else 0
                status = CheckStatus.OK if count == 0 else CheckStatus.NG
                message = "" if status == CheckStatus.OK else f"Result count: {count}"
                if status == CheckStatus.NG:
                    logger.error(
                        "Relation check failed",
                        extra={
                            "relation": rel.name,
                            "check_description": desc,
                        },
                    )
                results.append(
                    CheckResult(
                        description=desc,
                        query=query,
                        status=status,
                        result_count=count,
                        message=message,
                    )
                )
            except Exception as e:
                logger.error(
                    "Relation check execution error",
                    extra={
                        "relation": rel.name,
                        "check_description": desc,
                        "error": str(e),
                    },
                )
                results.append(
                    CheckResult(
                        description=desc,
                        query=query,
                        status=CheckStatus.ERROR,
                        result_count=None,
                        message=str(e),
                    )
                )

    logger.info("Relation checks completed")
    return results


def run_cross_checks(
    conn: duckdb.DuckDBPyConnection,
    cross_checks: list[CrossCheckDef],
    all_load_errors: dict[str, list[LoadError]],
    check_failed_tables: set[str] | None = None,
) -> list[CheckResult]:
    """Run user-defined SQL checks that span multiple tables.

    Each cross-check query must return a single scalar integer.
    If expect_zero is True, result == 0 means OK; otherwise result > 0 means OK.
    If any referenced table has load errors or check failures, the check is SKIPPED.
    """
    logger.info("Starting cross-table checks")
    check_failed = check_failed_tables or set()
    results: list[CheckResult] = []

    for cc in cross_checks:
        # Skip if any referenced table has errors
        has_errors = any(all_load_errors.get(t, []) for t in cc.tables)
        has_check_failures = any(t in check_failed for t in cc.tables)

        if has_errors or has_check_failures:
            skipped_tables = []
            for t in cc.tables:
                if all_load_errors.get(t, []):
                    skipped_tables.append(t)
                if t in check_failed:
                    skipped_tables.append(f"{t} (check failed)")
            skip_msg = f"Skipped due to errors in: {', '.join(skipped_tables)}"
            check_def = CheckDef(description=f"[cross] {cc.name}", query=cc.query)
            results.append(make_skipped_result(check_def, cc.tables[0], skip_msg))
            continue

        try:
            row = conn.execute(cc.query).fetchone()
            value = int(row[0]) if row else 0
            if cc.expect_zero:
                status = CheckStatus.OK if value == 0 else CheckStatus.NG
            else:
                status = CheckStatus.OK if value > 0 else CheckStatus.NG
            message = "" if status == CheckStatus.OK else f"Result: {value}"
            if status == CheckStatus.NG:
                logger.error(
                    "Cross-check failed",
                    extra={
                        "cross_check": cc.name,
                        "result": value,
                    },
                )
            results.append(
                CheckResult(
                    description=f"[cross] {cc.name}",
                    query=cc.query,
                    status=status,
                    result_count=value,
                    message=message,
                )
            )
        except Exception as e:
            logger.error(
                "Cross-check execution error",
                extra={
                    "cross_check": cc.name,
                    "error": str(e),
                },
            )
            results.append(
                CheckResult(
                    description=f"[cross] {cc.name}",
                    query=cc.query,
                    status=CheckStatus.ERROR,
                    result_count=None,
                    message=str(e),
                )
            )

    logger.info("Cross-table checks completed")
    return results
