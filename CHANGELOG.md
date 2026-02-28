# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.2.0] - Unreleased

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
