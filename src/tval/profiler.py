"""Profile table columns to compute descriptive statistics.

Collects count, null count, unique count, and (for numeric columns) mean,
standard deviation, skewness, kurtosis, min, percentiles, and max.
For date/time columns, collects min and max only.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .builder import quote_identifier
from .loader import LoadError
from .logger import get_logger
from .parser import DATETIME_TYPES, NUMERIC_TYPES, TableDef

logger = get_logger(__name__)


def _is_numeric(col_type: str) -> bool:
    """Check whether a column type is numeric based on its base type name."""
    base_type = col_type.split("(")[0].strip()
    return base_type in NUMERIC_TYPES


def _is_temporal(col_type: str) -> bool:
    """Check whether a column type is a date/time type based on its base type name."""
    base_type = col_type.split("(")[0].strip()
    return base_type in DATETIME_TYPES


@dataclass
class ColumnProfile:
    """Descriptive statistics for a single table column."""

    column_name: str
    logical_name: str
    column_type: str
    is_numeric: bool
    is_temporal: bool
    count: int
    not_null_count: int
    unique_count: int
    mean: float | None
    std: float | None
    skewness: float | None
    kurtosis: float | None
    min: float | str | None
    p25: float | None
    median: float | None
    p75: float | None
    max: float | str | None
    error: str | None = None


def profile_table(
    conn: duckdb.DuckDBPyConnection,
    tdef: TableDef,
    load_errors: list[LoadError],
) -> list[ColumnProfile]:
    """Compute descriptive statistics for all columns in a table.

    Returns an empty list if there are load errors or the table is empty.
    For numeric columns, includes mean, std, skewness, kurtosis, and percentiles.
    """
    table_name = tdef.table.name
    logger.info("Profiling started", extra={"table": table_name})

    if load_errors:
        logger.info("Profiling completed", extra={"table": table_name})
        return []

    qtable = quote_identifier(table_name)

    # Check if table is empty
    row_count_result = conn.execute(f"SELECT COUNT(*) FROM {qtable}").fetchone()
    if not row_count_result or row_count_result[0] == 0:
        logger.info("Profiling completed", extra={"table": table_name})
        return []

    profiles: list[ColumnProfile] = []

    for col in tdef.columns:
        qcol = quote_identifier(col.name)
        numeric = _is_numeric(col.type)
        temporal = _is_temporal(col.type)

        try:
            # Common statistics
            common_sql = (
                f"SELECT COUNT(*) AS count, "
                f"COUNT({qcol}) AS not_null_count, "
                f"COUNT(DISTINCT {qcol}) AS unique_count "
                f"FROM {qtable}"
            )
            common_row = conn.execute(common_sql).fetchone()
            if not common_row:
                continue
            count = int(common_row[0])
            not_null_count = int(common_row[1])
            unique_count = int(common_row[2])

            # Numeric / temporal additional statistics
            mean: float | None = None
            std: float | None = None
            skewness: float | None = None
            kurtosis: float | None = None
            min_val: float | str | None = None
            p25: float | None = None
            median_val: float | None = None
            p75: float | None = None
            max_val: float | str | None = None

            if numeric:
                num_sql = (
                    f"SELECT "
                    f"AVG({qcol}) AS mean, "
                    f"STDDEV_SAMP({qcol}) AS std, "
                    f"SKEWNESS({qcol}) AS skewness, "
                    f"KURTOSIS({qcol}) AS kurtosis, "
                    f"MIN({qcol}) AS min, "
                    f"PERCENTILE_CONT(0.25) WITHIN GROUP "
                    f"(ORDER BY {qcol}) AS p25, "
                    f"PERCENTILE_CONT(0.50) WITHIN GROUP "
                    f"(ORDER BY {qcol}) AS median, "
                    f"PERCENTILE_CONT(0.75) WITHIN GROUP "
                    f"(ORDER BY {qcol}) AS p75, "
                    f"MAX({qcol}) AS max "
                    f"FROM {qtable}"
                )
                num_row = conn.execute(num_sql).fetchone()
                if num_row:
                    mean = _to_float(num_row[0])
                    std = _to_float(num_row[1])
                    skewness = _to_float(num_row[2])
                    kurtosis = _to_float(num_row[3])
                    min_val = _to_float(num_row[4])
                    p25 = _to_float(num_row[5])
                    median_val = _to_float(num_row[6])
                    p75 = _to_float(num_row[7])
                    max_val = _to_float(num_row[8])
            elif temporal:
                temp_sql = f"SELECT MIN({qcol}), MAX({qcol}) FROM {qtable}"
                temp_row = conn.execute(temp_sql).fetchone()
                if temp_row:
                    min_val = str(temp_row[0]) if temp_row[0] is not None else None
                    max_val = str(temp_row[1]) if temp_row[1] is not None else None

            profiles.append(
                ColumnProfile(
                    column_name=col.name,
                    logical_name=col.logical_name,
                    column_type=col.type,
                    is_numeric=numeric,
                    is_temporal=temporal,
                    count=count,
                    not_null_count=not_null_count,
                    unique_count=unique_count,
                    mean=mean,
                    std=std,
                    skewness=skewness,
                    kurtosis=kurtosis,
                    min=min_val,
                    p25=p25,
                    median=median_val,
                    p75=p75,
                    max=max_val,
                )
            )
        except Exception as e:
            logger.error(
                "Profiling failed",
                extra={"table": table_name, "column": col.name},
                exc_info=True,
            )
            profiles.append(
                ColumnProfile(
                    column_name=col.name,
                    logical_name=col.logical_name,
                    column_type=col.type,
                    is_numeric=numeric,
                    is_temporal=temporal,
                    count=0,
                    not_null_count=0,
                    unique_count=0,
                    mean=None,
                    std=None,
                    skewness=None,
                    kurtosis=None,
                    min=None,
                    p25=None,
                    median=None,
                    p75=None,
                    max=None,
                    error=str(e),
                )
            )

    logger.info("Profiling completed", extra={"table": table_name})
    return profiles


def _to_float(value: object) -> float | None:
    """Convert a value to float, returning None if the input is None."""
    if value is None:
        return None
    return float(value)  # type: ignore[arg-type]
