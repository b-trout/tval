# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.3.2] - 2026-03-02

### Added

- `cross_checks` section in `relations.yaml` for user-defined cross-table SQL checks
- `CrossCheckDef` model with `tables` (min 2), `query`, `expect_zero` fields
- `validate_cross_check_refs()` and `run_cross_checks()` in relation module
- Cross-Table Validation section in HTML report
- `--quiet`, `--dry-run` flags and `--version` option in CLI
- DuckDB connection error handling (`SystemExit(1)`)
- Output directory write permission validation (`SystemExit(1)`)
- `_log_dry_run_summary()`, `_validate_output_dirs()`, `_connect_duckdb()` helpers in main module
- `tval init` rollback on partial failure

### Changed

- `load_relations()` now returns `RelationsConfig` instead of `list[RelationDef]`
- Export gate now considers cross-check results (tables + relations + cross-checks must all pass)
- `generate_report()` accepts `cross_check_results` parameter
- `run()` accepts `dry_run` parameter

## [0.3.1] - 2026-03-02

### Added

- Extra columns detection: files with columns not defined in the schema now produce an `EXTRA_COLUMNS` load error before INSERT
- Early termination: skip profiling when any check result is NG or ERROR (per-table scope)
- Early termination: skip relation checks when either related table has check failures (`check_failed_tables` parameter in `run_relation_checks()`)

### Changed

- Refactored `_insert_file()` in loader to lift CSV encoding resolution and temp file cleanup to the caller level, simplifying `_insert_csv()`

## [0.3.0] - 2026-03-02

### Added

- Declarative `min`/`max` constraints for numeric columns — auto-generates range check SQL in checker
- Declarative `row_conditions` for table-level row validation — each condition is a SQL boolean expression auto-converted to a check query
- MIN/MAX profiling for DATE, TIMESTAMP, and TIME columns
- `NUMERIC_TYPES` constant in parser for numeric type validation
- `RowConditionDef` Pydantic model for row-level condition definitions

### Changed

- Updated DESIGN.md with min/max and row_conditions specifications
- Updated README.md with min/max and row_conditions documentation, YAML examples, and reference tables

## [0.2.1] - 2026-02-28

### Changed

- Pin all runtime and dev dependency versions to `>=current,<next_major` constraints

## [0.2.0] - 2026-02-28

### Added

- Inter-table relation cardinality validation (1:1, 1:N, N:1, N:N) via `relations.yaml`
- `relations_path` option in `config.yaml` for optional relation definitions
- Uniqueness and referential integrity checks based on cardinality type
- Relation Cardinality Validation section in HTML report
- Export gate now considers both table and relation check results
- `ERROR` status for CheckResult (in addition to OK, NG, SKIPPED)
- `error` field on ColumnProfile for per-column profiling error capture
- Duplicate column name validation in schema parser
- Unit tests for checker, profiler, exporter, reporter, and relation modules

### Changed

- CSV encoding detection now samples only first 8KB instead of reading the full file
- UTF-8/ASCII CSV files are passed directly to DuckDB without creating a temp copy
- Non-UTF-8 CSV conversion uses streaming I/O (`shutil.copyfileobj`) to reduce peak memory
- Replaced string literals with `CheckStatus` / `ExportStatus` enums (`status.py`) — eliminates typo risk and enables IDE autocompletion across checker, exporter, relation, and reporter modules
- Split monolithic `run()` in `main.py` into `_discover_config_path()`, `_load_data()`, `_build_table_reports()` — reduces function complexity for readability and testability
- Split `_insert_file()` in `loader.py` into `_insert_csv()`, `_insert_xlsx()`, `_insert_parquet()` — isolates format-specific logic while keeping centralized error handling
- Added `ProjectConfig` Pydantic model in `parser.py` — validates `config.yaml` with type safety instead of raw dict access
- Enabled ruff rules B (flake8-bugbear) and C90 (mccabe, max-complexity=15) — catches common bugs (`zip()` without `strict`, duplicate set items) and enforces complexity limits
- Simplified README.md for data analysts — replaced DDL jargon (PRIMARY KEY, FOREIGN KEY) with plain-language explanations, added cardinality examples, expanded SKIPPED/ERROR guidance
- Reduced DESIGN.md from 1967 to 718 lines — removed inline code snippets, kept only spec tables, YAML examples, and architecture diagrams

### Fixed

- Checker now reports `ERROR` status instead of silent `SKIPPED` on SQL execution failures
- Profiler now captures per-column errors instead of silently skipping failed columns
- Reporter correctly treats `ERROR` status as NG in overall status
- Removed dead code branch in Parquet loader

## [0.1.0] - Unreleased

### Added

- CLI entry point with `tval init` and `tval run` commands
- YAML-based table schema definitions validated by Pydantic
- CSV, Excel (.xlsx), and Parquet file loading into DuckDB
- Automatic CSV encoding detection via chardet
- Allowed-value checks for enumerated columns
- User-defined SQL checks with parameterized queries
- Aggregation checks reported in a dedicated section
- Foreign key dependency resolution via topological sort
- Column profiling (count, nulls, unique, mean, std, min, max, percentiles)
- Parquet export with optional Hive-style partitioning
- Self-contained HTML report generation via Jinja2
- Structured JSON logging
- PEP 561 type information marker (py.typed)
