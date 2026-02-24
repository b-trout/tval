# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
