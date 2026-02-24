from __future__ import annotations

import re
from graphlib import CycleError, TopologicalSorter
from itertools import chain

import duckdb

from .logger import get_logger
from .parser import TableDef

logger = get_logger(__name__)


def validate_identifier(name: str) -> str:
    """英字またはアンダースコア始まり、英数字・アンダースコアのみ許可。"""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


def quote_identifier(name: str) -> str:
    """識別子をダブルクォートでエスケープする。"""
    return f'"{validate_identifier(name)}"'


def build_load_order(table_defs: list[TableDef]) -> list[TableDef]:
    """外部キー依存関係からDAGを構築し、トポロジカルソート順で返す。"""
    name_to_def: dict[str, TableDef] = {}
    for tdef in table_defs:
        name_to_def[tdef.table.name] = tdef

    graph: dict[str, set[str]] = {tdef.table.name: set() for tdef in table_defs}
    for tdef in table_defs:
        for fk in tdef.table_constraints.foreign_keys:
            ref_table = fk.references.table
            if ref_table not in name_to_def:
                raise ValueError(
                    f"FK参照先テーブルが未定義です: {tdef.table.name} -> {ref_table}"
                )
            graph[tdef.table.name].add(ref_table)

    sorter: TopologicalSorter[str] = TopologicalSorter(graph)
    try:
        ordered_names = list(sorter.static_order())
    except CycleError as e:
        raise ValueError(f"循環依存が検出されました: {e.args[1]}") from e

    return [name_to_def[name] for name in ordered_names]


def build_create_table_sql(tdef: TableDef) -> str:
    """TableDefからCREATE TABLE文字列を生成する。"""
    table_name = quote_identifier(tdef.table.name)
    col_defs = (
        f"    {quote_identifier(col.name)} {col.type}"
        + (" NOT NULL" if col.not_null else "")
        for col in tdef.columns
    )
    pk_defs = (
        f"    PRIMARY KEY ({', '.join(quote_identifier(c) for c in pk.columns)})"
        for pk in tdef.table_constraints.primary_key
    )
    uq_defs = (
        f"    UNIQUE ({', '.join(quote_identifier(c) for c in uq.columns)})"
        for uq in tdef.table_constraints.unique
    )
    fk_defs = (
        f"    FOREIGN KEY ({', '.join(quote_identifier(c) for c in fk.columns)})"
        f" REFERENCES {quote_identifier(fk.references.table)}"
        f" ({', '.join(quote_identifier(c) for c in fk.references.columns)})"
        for fk in tdef.table_constraints.foreign_keys
    )
    body = ",\n".join(chain(col_defs, pk_defs, uq_defs, fk_defs))
    return f"CREATE TABLE {table_name} (\n{body}\n)"


def create_tables(conn: duckdb.DuckDBPyConnection, table_defs: list[TableDef]) -> None:
    """build_load_order順でCREATE TABLEを実行する。"""
    ordered = build_load_order(table_defs)
    for tdef in ordered:
        sql = build_create_table_sql(tdef)
        conn.execute(sql)
        logger.info("テーブル作成", extra={"table": tdef.table.name})
