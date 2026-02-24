"""Load data files (CSV, XLSX, Parquet) into DuckDB tables.

Handles character encoding detection for CSV files, datetime format conversion,
and structured error reporting for load failures.
"""

from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

import chardet
import duckdb

from .builder import quote_identifier
from .logger import get_logger
from .parser import DATETIME_TYPES as DATETIME_TYPES
from .parser import ColumnDef, TableDef

logger = get_logger(__name__)

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".parquet"}


class EncodingDetectionError(Exception):
    """Raised when chardet's confidence is below the configured threshold."""


@dataclass
class LoadError:
    """Structured representation of a file load error."""

    file_path: str
    error_type: str
    column: str | None
    row: int | None
    raw_message: str


def _resolve_csv_path(
    file_path: str,
    confidence_threshold: float,
) -> tuple[str, bool]:
    """Detect CSV encoding and convert to UTF-8 if needed.

    Returns a tuple of (resolved_path, is_temporary). If the file is already
    UTF-8/ASCII, returns the original path. Otherwise, creates a temporary
    UTF-8 copy.

    Raises:
        EncodingDetectionError: If detection confidence is below the threshold.
    """
    with open(file_path, "rb") as f:
        raw = f.read()

    detected = chardet.detect(raw)
    encoding: str = detected.get("encoding") or "utf-8"
    confidence: float = detected.get("confidence") or 0.0

    if confidence < confidence_threshold:
        raise EncodingDetectionError(
            f"文字コード検出の信頼度が閾値未満です "
            f"(detected={encoding}, confidence={confidence:.2f}, "
            f"threshold={confidence_threshold})"
        )

    if encoding.lower().replace("-", "") in ("utf8", "ascii"):
        return file_path, False

    logger.warning(
        "CSV文字コードをUTF-8に変換しました",
        extra={
            "file": file_path,
            "detected_encoding": encoding,
            "confidence": detected.get("confidence"),
        },
    )
    text = raw.decode(encoding, errors="replace")
    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".csv",
        delete=False,
    )
    tmp.write(text)
    tmp.close()
    return tmp.name, True


def _col_expr(col: ColumnDef) -> str:
    """Return a SELECT-clause expression for a single column.

    Columns with a format specifier are wrapped in STRPTIME for parsing.
    """
    qname = quote_identifier(col.name)
    if col.format:
        fmt = col.format.replace("'", "''")
        return f"STRPTIME({qname}, '{fmt}')::{col.type} AS {qname}"
    return qname


def _build_insert_select(tdef: TableDef) -> str:
    """Build a SELECT clause with STRPTIME conversions for format-specified columns."""
    format_cols = {col.name: col for col in tdef.columns if col.format}
    if not format_cols:
        return "SELECT *"

    return "SELECT " + ", ".join(_col_expr(c) for c in tdef.columns)


def _build_columns_override(tdef: TableDef) -> str:
    """Build a columns/types override string for read_csv_auto or read_xlsx.

    Columns with a format specifier are overridden to VARCHAR so that
    STRPTIME can parse them in the SELECT clause.
    """
    format_cols = {col.name for col in tdef.columns if col.format}
    if not format_cols:
        return ""

    return (
        "{"
        + ", ".join(
            f"'{col.name}': '{'VARCHAR' if col.name in format_cols else col.type}'"
            for col in tdef.columns
        )
        + "}"
    )


def parse_duckdb_error(file_path: str, message: str) -> LoadError:
    """Parse a DuckDB error message into a structured LoadError.

    Recognizes TYPE_MISMATCH, NOT_NULL, COLUMN_MISMATCH, FK_VIOLATION,
    and UNIQUE_VIOLATION patterns. Unrecognized errors are classified as
    UNKNOWN.
    """
    # TYPE_MISMATCH
    m = re.search(
        r"Could not convert .+ to (\w+) in column \"(\w+)\".+Row: (\d+)",
        message,
        re.DOTALL,
    )
    if m:
        return LoadError(
            file_path=file_path,
            error_type="TYPE_MISMATCH",
            column=m.group(2),
            row=int(m.group(3)),
            raw_message=message,
        )

    # NOT_NULL
    m = re.search(r"NOT NULL constraint failed: \w+\.(\w+)", message)
    if m:
        return LoadError(
            file_path=file_path,
            error_type="NOT_NULL",
            column=m.group(1),
            row=None,
            raw_message=message,
        )

    # COLUMN_MISMATCH
    m = re.search(r"has (\d+) columns but (\d+) values", message)
    if m:
        return LoadError(
            file_path=file_path,
            error_type="COLUMN_MISMATCH",
            column=None,
            row=None,
            raw_message=message,
        )

    # FK_VIOLATION
    m = re.search(
        r"Violates foreign key constraint because key .+ does not exist",
        message,
    )
    if m:
        return LoadError(
            file_path=file_path,
            error_type="FK_VIOLATION",
            column=None,
            row=None,
            raw_message=message,
        )

    # UNIQUE_VIOLATION
    m = re.search(
        r"Duplicate key .+ violates (primary key|unique) constraint",
        message,
    )
    if m:
        return LoadError(
            file_path=file_path,
            error_type="UNIQUE_VIOLATION",
            column=None,
            row=None,
            raw_message=message,
        )

    return LoadError(
        file_path=file_path,
        error_type="UNKNOWN",
        column=None,
        row=None,
        raw_message=message,
    )


def _insert_file(
    conn: duckdb.DuckDBPyConnection,
    tdef: TableDef,
    file_path: str,
    ext: str,
    confidence_threshold: float,
) -> LoadError | None:
    """Insert a single data file into the corresponding DuckDB table."""
    table_name = quote_identifier(tdef.table.name)
    select_clause = _build_insert_select(tdef)
    columns_override = _build_columns_override(tdef)
    resolved_path: str | None = None
    is_tmp = False

    try:
        if ext == ".csv":
            resolved_path, is_tmp = _resolve_csv_path(file_path, confidence_threshold)
            if columns_override:
                sql = (
                    f"INSERT INTO {table_name} {select_clause} "
                    f"FROM read_csv_auto(?, header=true, "
                    f"columns={columns_override})"
                )
            else:
                sql = (
                    f"INSERT INTO {table_name} {select_clause} "
                    f"FROM read_csv_auto(?, header=true)"
                )
            conn.execute(sql, [resolved_path])
        elif ext == ".parquet":
            if columns_override:
                sql = f"INSERT INTO {table_name} {select_clause} FROM read_parquet(?)"
            else:
                sql = f"INSERT INTO {table_name} {select_clause} FROM read_parquet(?)"
            conn.execute(sql, [file_path])
        elif ext == ".xlsx":
            if columns_override:
                sql = (
                    f"INSERT INTO {table_name} {select_clause} "
                    f"FROM read_xlsx(?, header=true, "
                    f"types={columns_override})"
                )
            else:
                sql = (
                    f"INSERT INTO {table_name} {select_clause} "
                    f"FROM read_xlsx(?, header=true)"
                )
            conn.execute(sql, [file_path])
        return None
    except EncodingDetectionError as e:
        logger.error(
            "文字コード検出の信頼度が閾値未満です",
            extra={
                "file": file_path,
                "detected_encoding": "unknown",
                "confidence": 0.0,
                "threshold": confidence_threshold,
            },
        )
        return LoadError(
            file_path=file_path,
            error_type="ENCODING_DETECTION_FAILED",
            column=None,
            row=None,
            raw_message=str(e),
        )
    except Exception as e:
        error = parse_duckdb_error(file_path, str(e))
        logger.error(
            "ファイルロードエラー",
            extra={
                "table": tdef.table.name,
                "file": file_path,
                "error_type": error.error_type,
            },
        )
        return error
    finally:
        if is_tmp and resolved_path:
            Path(resolved_path).unlink(missing_ok=True)


def load_files(
    conn: duckdb.DuckDBPyConnection,
    tdef: TableDef,
    confidence_threshold: float = 0.8,
) -> list[LoadError]:
    """Load all data files from the table's source directory.

    Iterates over files in source_dir, inserting each supported file into
    the table. Returns a list of LoadError instances for any failures.
    """
    source_dir = Path(tdef.table.source_dir)
    errors: list[LoadError] = []

    files = sorted(
        f for f in source_dir.iterdir() if f.is_file() and not f.name.startswith(".")
    )

    if not files:
        return [
            LoadError(
                file_path=str(source_dir),
                error_type="NO_FILES",
                column=None,
                row=None,
                raw_message=f"ファイルが見つかりません: {source_dir}",
            )
        ]

    for file in files:
        ext = file.suffix.lower()
        file_str = str(file)

        if ext == ".xls":
            errors.append(
                LoadError(
                    file_path=file_str,
                    error_type="UNSUPPORTED_FORMAT",
                    column=None,
                    row=None,
                    raw_message=f".xls形式は非対応です: {file_str}",
                )
            )
            continue

        if ext not in SUPPORTED_EXTENSIONS:
            logger.warning("非対応拡張子をスキップ", extra={"file": file_str})
            continue

        logger.info(
            "ファイルロード開始",
            extra={"table": tdef.table.name, "file": file_str},
        )
        error = _insert_file(conn, tdef, file_str, ext, confidence_threshold)
        if error:
            errors.append(error)
        else:
            logger.info(
                "ファイルロード完了",
                extra={"table": tdef.table.name, "file": file_str},
            )

    return errors
