"""Export validated DuckDB tables to Parquet files.

Supports optional partitioning by specified columns using DuckDB's COPY
statement.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from .builder import quote_identifier
from .logger import get_logger
from .parser import TableDef
from .status import ExportStatus

logger = get_logger(__name__)


def _escape_string_literal(value: str) -> str:
    """Escape single quotes for use in a SQL string literal."""
    return value.replace("'", "''")


@dataclass
class ExportResult:
    """Result of a single table export operation."""

    table_name: str
    status: ExportStatus
    output_path: str
    message: str = ""


def export_table(
    conn: duckdb.DuckDBPyConnection,
    tdef: TableDef,
    output_base_dir: str | Path,
) -> ExportResult:
    """Export a table to Parquet format, optionally partitioned by columns."""
    table_name = tdef.table.name
    output_dir = Path(output_base_dir) / table_name
    output_dir.mkdir(parents=True, exist_ok=True)

    qtable = quote_identifier(table_name)
    partition_by = tdef.export.partition_by

    try:
        if partition_by:
            partition_cols = ", ".join(quote_identifier(c) for c in partition_by)
            output_path = str(output_dir.resolve())
            sql = (
                f"COPY {qtable} TO '{_escape_string_literal(output_path)}' "
                f"(FORMAT parquet, PARTITION_BY ({partition_cols}), "
                f"OVERWRITE_OR_IGNORE)"
            )
            conn.execute(sql)
        else:
            output_file = output_dir / f"{table_name}.parquet"
            output_path = str(output_file.resolve())
            escaped_path = _escape_string_literal(output_path)
            sql = f"COPY {qtable} TO '{escaped_path}' (FORMAT parquet)"
            conn.execute(sql)

        return ExportResult(
            table_name=table_name,
            status=ExportStatus.OK,
            output_path=output_path,
        )
    except Exception as e:
        return ExportResult(
            table_name=table_name,
            status=ExportStatus.ERROR,
            output_path=str(output_dir),
            message=str(e),
        )
