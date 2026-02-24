"""Parse and validate YAML schema definitions into Pydantic models.

Defines the data models (ColumnDef, TableDef, etc.) for table schema
definitions and provides functions to load them from YAML files.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ValidationInfo, field_validator, model_validator

DATETIME_TYPES = {"DATE", "TIMESTAMP", "TIME"}


class ColumnDef(BaseModel):
    """Definition of a single table column with type and constraint metadata."""

    name: str
    logical_name: str
    type: str
    not_null: bool
    description: str = ""
    allowed_values: list[str] = []
    format: str | None = None

    @field_validator("type")
    @classmethod
    def upper_type(cls, v: str) -> str:
        """Normalize the column type to uppercase."""
        return v.upper()

    @field_validator("format")
    @classmethod
    def validate_format_pattern(cls, v: str | None) -> str | None:
        """Restrict format to safe strptime-style patterns only."""
        if v is None:
            return v
        if not re.fullmatch(r"[%A-Za-z0-9\-/.: ]+", v):
            raise ValueError(f"Invalid format pattern: {v!r}")
        return v

    @model_validator(mode="after")
    def validate_format_type(self) -> ColumnDef:
        """Ensure the format field is only used with DATE/TIMESTAMP/TIME types."""
        if self.format is not None:
            base_type = self.type.split("(")[0].strip()
            if base_type not in DATETIME_TYPES:
                raise ValueError(
                    f"format は DATE/TIMESTAMP/TIME 型のみ有効です: type={self.type}"
                )
        return self


class PrimaryKeyDef(BaseModel):
    """Primary key constraint definition."""

    columns: list[str]


class UniqueDef(BaseModel):
    """Unique constraint definition."""

    columns: list[str]


class FKReference(BaseModel):
    """Foreign key reference target (table and columns)."""

    table: str
    columns: list[str]


class ForeignKeyDef(BaseModel):
    """Foreign key constraint definition."""

    columns: list[str]
    references: FKReference


class CheckDef(BaseModel):
    """User-defined SQL check with an expected outcome."""

    description: str
    query: str
    expect_zero: bool = True
    params: list[Any] = []


class TableConstraints(BaseModel):
    """Collection of all table-level constraints."""

    primary_key: list[PrimaryKeyDef]
    foreign_keys: list[ForeignKeyDef]
    unique: list[UniqueDef]
    checks: list[CheckDef]
    aggregation_checks: list[CheckDef]

    @field_validator("primary_key", mode="before")
    @classmethod
    def wrap_single_pk(cls, v: Any) -> Any:
        """Allow a single primary key dict to be passed without wrapping in a list."""
        if isinstance(v, dict):
            return [v]
        return v


class ExportDef(BaseModel):
    """Export configuration for a table (e.g. Parquet partitioning)."""

    partition_by: list[str] = []


class TableMeta(BaseModel):
    """Table-level metadata: name, description, and data source directory."""

    name: str
    description: str
    source_dir: str


class TableDef(BaseModel):
    """Complete table definition including columns, constraints, and export config."""

    table: TableMeta
    columns: list[ColumnDef]
    table_constraints: TableConstraints
    export: ExportDef = ExportDef()

    @model_validator(mode="wrap")
    @classmethod
    def validate_all(cls, values: Any, handler: Any, info: ValidationInfo) -> TableDef:
        """Validate cross-field constraints."""
        obj: TableDef = handler(values)

        if len(obj.columns) == 0:
            raise ValueError("columns は1件以上必要です")

        col_names = {c.name for c in obj.columns}

        # Validate source_dir existence
        source_dir = Path(obj.table.source_dir)
        if not source_dir.exists():
            raise ValueError(f"source_dir が存在しません: {obj.table.source_dir}")

        # Validate source_dir is within project root
        context = info.context or {}
        project_root = context.get("project_root")
        if project_root is not None:
            resolved = source_dir.resolve()
            root_resolved = Path(project_root).resolve()
            try:
                resolved.relative_to(root_resolved)
            except ValueError:
                raise ValueError(
                    f"source_dir はプロジェクトルート以下である必要があります: "
                    f"{obj.table.source_dir}"
                )

        # Validate PK columns exist
        for pk in obj.table_constraints.primary_key:
            for col in pk.columns:
                if col not in col_names:
                    raise ValueError(
                        f"primary_key に存在しないカラムが指定されています: {col}"
                    )

        # Validate UNIQUE columns exist
        for uq in obj.table_constraints.unique:
            for col in uq.columns:
                if col not in col_names:
                    raise ValueError(
                        f"unique に存在しないカラムが指定されています: {col}"
                    )

        # Validate FK source columns exist
        for fk in obj.table_constraints.foreign_keys:
            for col in fk.columns:
                if col not in col_names:
                    raise ValueError(
                        f"foreign_keys に存在しないカラムが指定されています: {col}"
                    )

        # Validate export.partition_by columns exist
        for col in obj.export.partition_by:
            if col not in col_names:
                raise ValueError(
                    f"export.partition_by に存在しないカラムが指定されています: {col}"
                )

        return obj


def load_table_definition(
    path: str | Path, project_root: str | Path | None = None
) -> TableDef:
    """Load a single YAML schema file and return a validated TableDef."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    context: dict[str, Any] = {}
    if project_root is not None:
        context["project_root"] = project_root
    return TableDef.model_validate(data, context=context)


def load_table_definitions(
    schema_dir: str | Path, project_root: str | Path | None = None
) -> list[TableDef]:
    """Load all YAML schema files from a directory and return a list of TableDefs."""
    schema_path = Path(schema_dir)
    yaml_files = sorted(schema_path.glob("*.yaml"))
    if not yaml_files:
        raise FileNotFoundError(
            f"schema_dir にYAMLファイルが見つかりません: {schema_dir}"
        )
    return [load_table_definition(f, project_root=project_root) for f in yaml_files]
