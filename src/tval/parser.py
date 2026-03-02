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

NUMERIC_TYPES = {
    "INTEGER",
    "INT",
    "INT4",
    "INT32",
    "BIGINT",
    "INT8",
    "INT64",
    "SMALLINT",
    "INT2",
    "INT16",
    "TINYINT",
    "INT1",
    "HUGEINT",
    "FLOAT",
    "FLOAT4",
    "REAL",
    "DOUBLE",
    "FLOAT8",
    "DECIMAL",
    "NUMERIC",
}


class ProjectConfig(BaseModel):
    """Project configuration loaded from config.yaml."""

    database_path: str
    schema_dir: str
    output_path: str
    encoding_confidence_threshold: float = 0.8
    relations_path: str | None = None

    @field_validator("database_path")
    @classmethod
    def validate_db_extension(cls, v: str) -> str:
        """Ensure database_path has a .duckdb extension."""
        if not v.endswith(".duckdb"):
            raise ValueError(f"database_path must have .duckdb extension: {v}")
        return v


class ColumnDef(BaseModel):
    """Definition of a single table column with type and constraint metadata."""

    name: str
    logical_name: str
    type: str
    not_null: bool
    description: str = ""
    allowed_values: list[str] = []
    format: str | None = None
    min: float | None = None
    max: float | None = None

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
                    "format is only valid for DATE/TIMESTAMP/TIME types: "
                    f"type={self.type}"
                )
        return self

    @model_validator(mode="after")
    def validate_min_max_type(self) -> ColumnDef:
        """Ensure min/max fields are only used with numeric types."""
        if self.min is not None or self.max is not None:
            base_type = self.type.split("(")[0].strip()
            if base_type not in NUMERIC_TYPES:
                raise ValueError(
                    f"min/max is only valid for numeric types: type={self.type}"
                )
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError(f"min ({self.min}) must be <= max ({self.max})")
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


class RowConditionDef(BaseModel):
    """Declarative row-level condition expressed as a SQL boolean expression."""

    description: str
    condition: str


class TableConstraints(BaseModel):
    """Collection of all table-level constraints."""

    primary_key: list[PrimaryKeyDef]
    foreign_keys: list[ForeignKeyDef]
    unique: list[UniqueDef]
    checks: list[CheckDef]
    aggregation_checks: list[CheckDef]
    row_conditions: list[RowConditionDef] = []

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


def _validate_source_dir(source_dir_str: str, project_root: str | None) -> None:
    """Validate that source_dir exists and is within project root."""
    source_dir = Path(source_dir_str)
    if not source_dir.exists():
        raise ValueError(f"source_dir does not exist: {source_dir_str}")
    if project_root is not None:
        resolved = source_dir.resolve()
        root_resolved = Path(project_root).resolve()
        try:
            resolved.relative_to(root_resolved)
        except ValueError:
            raise ValueError(
                f"source_dir must be under the project root: {source_dir_str}"
            ) from None


def _validate_constraint_columns(
    col_names: set[str], constraints: "TableConstraints", export: ExportDef
) -> None:
    """Validate that all constraint-referenced columns exist in the table."""
    for pk in constraints.primary_key:
        for col in pk.columns:
            if col not in col_names:
                raise ValueError(f"Column not found in primary_key: {col}")
    for uq in constraints.unique:
        for col in uq.columns:
            if col not in col_names:
                raise ValueError(f"Column not found in unique: {col}")
    for fk in constraints.foreign_keys:
        for col in fk.columns:
            if col not in col_names:
                raise ValueError(f"Column not found in foreign_keys: {col}")
    for col in export.partition_by:
        if col not in col_names:
            raise ValueError(f"Column not found in export.partition_by: {col}")


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
            raise ValueError("At least one column is required")

        col_names = {c.name for c in obj.columns}
        if len(col_names) != len(obj.columns):
            seen: set[str] = set()
            for c in obj.columns:
                if c.name in seen:
                    raise ValueError(f"Duplicate column name: {c.name}")
                seen.add(c.name)

        context = info.context or {}
        project_root: str | None = context.get("project_root")
        _validate_source_dir(obj.table.source_dir, project_root)
        _validate_constraint_columns(col_names, obj.table_constraints, obj.export)

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
        raise FileNotFoundError(f"No YAML files found in schema_dir: {schema_dir}")
    return [load_table_definition(f, project_root=project_root) for f in yaml_files]
