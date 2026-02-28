# tval

**Table data schema validator** - Validate CSV, Excel, and Parquet files against YAML-defined schemas using DuckDB.

[![PyPI version](https://img.shields.io/pypi/v/tval-cli)](https://pypi.org/project/tval-cli/)
![Python >= 3.10](https://img.shields.io/badge/Python-%3E%3D3.10-blue)
![License: MIT](https://img.shields.io/badge/License-MIT-green)
![CI](https://img.shields.io/badge/CI-passing-brightgreen)

## Quick Start

```bash
pip install tval-cli           # Install tval
tval init                     # Scaffold project skeleton
# Place YAML schemas in tval/schema/ and data files in tval/data/
tval run                      # Run validation and generate report
```

Open `tval/output/report.html` in your browser to review the results.

---

## Table of Contents

- [1. Purpose](#1-purpose)
  - [1.1 What tval Does](#11-what-tval-does)
  - [1.2 Problems It Solves](#12-problems-it-solves)
  - [1.3 How It Works](#13-how-it-works)
  - [1.4 Supported Formats](#14-supported-formats)
  - [1.5 Design Principles](#15-design-principles)
- [2. Directory Structure](#2-directory-structure)
  - [2.1 Repository Layout](#21-repository-layout)
  - [2.2 Project Layout after tval init](#22-project-layout-after-tval-init)
  - [2.3 Module Responsibilities](#23-module-responsibilities)
- [3. User Guide](#3-user-guide)
  - [3.1 Prerequisites](#31-prerequisites)
  - [3.2 Installation](#32-installation)
  - [3.3 Initialize a Project](#33-initialize-a-project)
  - [3.4 Define Table Schemas](#34-define-table-schemas)
  - [3.5 Configure config.yaml](#35-configure-configyaml)
  - [3.6 Run Validation](#36-run-validation)
  - [3.7 Define Relations (Optional)](#37-define-relations-optional)
  - [3.8 Understanding the HTML Report](#38-understanding-the-html-report)
  - [3.9 Parquet Export](#39-parquet-export)
  - [3.10 Docker Usage](#310-docker-usage)
- [4. Developer Guide](#4-developer-guide)
  - [4.1 Development Setup](#41-development-setup)
  - [4.2 Code Quality](#42-code-quality)
  - [4.3 Running Tests](#43-running-tests)
  - [4.4 CI Pipeline](#44-ci-pipeline)
  - [4.5 Architecture Overview](#45-architecture-overview)
- [License](#license)

---

## 1. Purpose

### 1.1 What tval Does

tval is a CLI tool that validates tabular data files (CSV, Excel, Parquet) against YAML schema definitions. It leverages DuckDB as an in-process analytical database to execute constraint checks, compute column statistics, and optionally export validated data to Parquet format. Results are collected into a single self-contained HTML report.

### 1.2 Problems It Solves

1. **Inconsistent validation quality** - When multiple analysts manually inspect data, coverage and rigor vary. tval enforces a single set of declarative rules.
2. **Manual validation effort** - Hand-checking row counts, allowed values, foreign keys, and aggregation totals is tedious and error-prone. tval automates the entire pipeline.

### 1.3 How It Works

```
config.yaml          schema/*.yaml
     |                     |
     v                     v
  Load config       Pydantic validation
     |                     |
     +--------+------------+
              |
              v
     Build dependency DAG
              |
              v
     CREATE TABLE (DuckDB)
              |
              v
     INSERT data (CSV / XLSX / Parquet)
              |
              v
     Run checks (allowed values, user-defined, aggregation)
              |
              v
     Run relation checks (optional, from relations.yaml)
              |
              v
     Profile columns (statistics)
              |
              v
     Export to Parquet (optional, gated by table + relation results)
              |
              v
     Generate HTML report
```

### 1.4 Supported Formats

| Format  | Extensions   | Notes                                          |
|---------|--------------|-------------------------------------------------|
| CSV     | `.csv`       | Character encoding auto-detected via `chardet`  |
| Excel   | `.xlsx`      | `.xls` is **not** supported                     |
| Parquet | `.parquet`   | Read directly by DuckDB                         |

### 1.5 Design Principles

- **Fail-fast** - Schema YAML is validated by Pydantic before any data is loaded. Invalid definitions stop execution immediately.
- **One file = one transaction** - Each source file is loaded in its own INSERT; a single bad file does not prevent other files from loading.
- **Database delegation** - Constraint checking, profiling, and export are all executed as SQL against DuckDB, keeping Python code thin.
- **Check skipping** - If a table has load errors, all downstream checks are marked `SKIPPED` rather than producing misleading results.

---

## 2. Directory Structure

### 2.1 Repository Layout

```
tval/
├── src/tval/
│   ├── __init__.py          # Package marker
│   ├── cli.py               # CLI entry point (argparse)
│   ├── init.py              # tval init scaffolding
│   ├── main.py              # Pipeline orchestration
│   ├── parser.py            # YAML schema → Pydantic models
│   ├── builder.py           # DDL generation + topological sort
│   ├── loader.py            # CSV/XLSX/Parquet → DuckDB INSERT
│   ├── checker.py           # Validation check execution
│   ├── relation.py          # Inter-table relation cardinality validation
│   ├── profiler.py          # Column statistics computation
│   ├── exporter.py          # Parquet export with partitioning
│   ├── reporter.py          # HTML report generation
│   ├── logger.py            # Structured JSON logging
│   └── templates/
│       └── report.html.j2   # Jinja2 HTML report template
├── tests/
│   ├── test_parser.py       # Schema parsing and validation
│   ├── test_builder.py      # DDL generation and dependency ordering
│   ├── test_loader.py       # Data loading into DuckDB
│   ├── test_checker.py      # Validation check execution
│   ├── test_profiler.py     # Column profiling
│   ├── test_exporter.py     # Parquet export
│   ├── test_reporter.py     # HTML report generation
│   ├── test_relation.py     # Relation cardinality validation
│   └── test_integration.py  # End-to-end pipeline tests
├── docs/
│   ├── DESIGN.md            # Architecture and design decisions
│   ├── Background.md        # Motivation and design rationale
│   └── CLAUDE.md            # AI assistant context
├── pyproject.toml           # Build config, dependencies, tool settings
├── Dockerfile               # Development container
├── .pre-commit-config.yaml  # Pre-commit hook configuration
└── .github/workflows/
    └── ci.yml               # GitHub Actions CI pipeline
```

### 2.2 Project Layout after `tval init`

Running `tval init` creates the following directory structure:

```
tval/
├── config.yaml       # Validation configuration
├── schema/           # Table definition YAML files
│   └── .gitkeep
├── data/             # Source data files (CSV, XLSX, Parquet)
│   └── .gitkeep
└── output/           # Generated reports and exports
    └── .gitkeep
```

### 2.3 Module Responsibilities

| Module       | Responsibility                                                      |
|--------------|---------------------------------------------------------------------|
| `cli.py`     | Parse CLI arguments, dispatch to `init` or `run`                    |
| `init.py`    | Scaffold project directories, config, and `.gitignore` entries      |
| `main.py`    | Orchestrate the full validation pipeline end-to-end                 |
| `parser.py`  | Define Pydantic models and load/validate YAML schema files          |
| `builder.py` | Generate CREATE TABLE SQL, resolve FK dependency order (topo sort)  |
| `loader.py`  | Load CSV/XLSX/Parquet files into DuckDB with encoding detection     |
| `checker.py` | Execute allowed-value, user-defined, and aggregation checks         |
| `relation.py`| Validate inter-table relationship cardinalities (1:1, 1:N, N:1, N:N) |
| `profiler.py`| Compute column statistics (count, nulls, unique, mean, percentiles) |
| `exporter.py`| Export tables to Parquet with optional Hive partitioning            |
| `reporter.py`| Render HTML report from Jinja2 template                             |
| `logger.py`  | Provide structured JSON logging                                     |

---

## 3. User Guide

### 3.1 Prerequisites

- **Python >= 3.10**
- **pip** or [**uv**](https://github.com/astral-sh/uv) (recommended)

### 3.2 Installation

Using uv (recommended):

```bash
uv pip install tval-cli
```

Using pip:

```bash
pip install tval-cli
```

For development (editable install):

```bash
git clone https://github.com/b-trout/tval.git
cd tval
pip install -e ".[dev]"
```

### 3.3 Initialize a Project

```bash
tval init
```

Output:

```
Created tval/
Appended tval/data/, tval/output/ to .gitignore

Next steps:
  1. Add table definition YAML files to tval/schema/
  2. Place your data files in tval/data/
  3. Run validation with: tval run
```

To specify a different target directory:

```bash
tval init --dir ./my-project
```

### 3.4 Define Table Schemas

Each table is defined by a YAML file in the `schema/` directory. Below is a fully annotated example:

```yaml
# --- Table metadata ---
table:
  name: orders                    # DuckDB table name
  description: Orders table       # Human-readable description
  source_dir: ./data/orders/      # Directory containing data files (relative to config.yaml)

# --- Column definitions ---
columns:
  - name: order_id                # Column name (must match data file headers)
    logical_name: Order ID        # Display name for reports
    type: INTEGER                 # DuckDB column type
    not_null: true                # NOT NULL constraint

  - name: user_id
    logical_name: User ID
    type: INTEGER
    not_null: true

  - name: amount
    logical_name: Amount
    type: DOUBLE
    not_null: true

  - name: status
    logical_name: Status
    type: VARCHAR
    not_null: true
    allowed_values:               # Enumerated allowed values (checked at validation)
      - pending
      - shipped
      - cancelled

  - name: order_date
    logical_name: Order Date
    type: DATE
    not_null: true
    format: "%Y-%m-%d"           # strptime format (DATE/TIMESTAMP/TIME only)

# --- Table-level constraints ---
table_constraints:
  primary_key:
    columns: [order_id]           # Primary key (single or composite)

  unique:
    - columns: [order_id, user_id]  # Unique constraint

  foreign_keys:
    - columns: [user_id]           # FK source columns
      references:
        table: users               # Referenced table (must be defined in another YAML)
        columns: [user_id]         # Referenced columns

  # User-defined SQL checks
  checks:
    - description: Amount must be non-negative
      query: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
      expect_zero: true            # Pass if query returns 0

  # Aggregation checks (reported separately in the HTML report)
  aggregation_checks: []

# --- Export configuration ---
export:
  partition_by: []                 # Columns for Hive-style Parquet partitioning
```

#### Column Definition Reference

| Field            | Type       | Required | Description                                             |
|------------------|------------|----------|---------------------------------------------------------|
| `name`           | `string`   | Yes      | Column name matching data file headers                  |
| `logical_name`   | `string`   | Yes      | Human-readable name for reports                         |
| `type`           | `string`   | Yes      | DuckDB type (e.g. `INTEGER`, `VARCHAR`, `DOUBLE`, `DATE`) |
| `not_null`       | `bool`     | Yes      | Whether the column has a NOT NULL constraint            |
| `description`    | `string`   | No       | Optional description                                    |
| `allowed_values` | `string[]` | No       | Enumerated values; rows not matching are flagged NG     |
| `format`         | `string`   | No       | strptime pattern for DATE/TIMESTAMP/TIME columns        |

#### Table Constraints Reference

| Constraint      | Field          | Description                                                 |
|-----------------|----------------|-------------------------------------------------------------|
| `primary_key`   | `columns`      | One or more columns forming the primary key                 |
| `unique`        | `columns`      | One or more columns that must be unique together            |
| `foreign_keys`  | `columns`, `references.table`, `references.columns` | FK relationship to another table |
| `checks`        | `description`, `query`, `expect_zero`, `params` | User-defined SQL check         |
| `aggregation_checks` | (same as `checks`) | Aggregation checks reported in a separate section    |

#### User-Defined Checks

Use `{table}` as a placeholder for the quoted table name in queries:

```yaml
# Check that amount is non-negative
checks:
  - description: Amount must be non-negative
    query: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
    expect_zero: true

# Check that at least one row exists
  - description: Table must not be empty
    query: "SELECT COUNT(*) FROM {table}"
    expect_zero: false

# Check with parameterized allowed values
  - description: Status must be valid
    query: "SELECT COUNT(*) FROM {table} WHERE status NOT IN (SELECT UNNEST(?::VARCHAR[]))"
    expect_zero: true
    params: [["pending", "shipped", "cancelled"]]
```

#### Export Configuration

| Field          | Type       | Default | Description                                       |
|----------------|------------|---------|---------------------------------------------------|
| `partition_by` | `string[]` | `[]`    | Column names for Hive-style Parquet partitioning   |

### 3.5 Configure config.yaml

The `config.yaml` file controls the validation pipeline. All paths are resolved **relative to the directory containing config.yaml**.

```yaml
database_path: ./tval/work.duckdb           # DuckDB file path (must end in .duckdb)
schema_dir: ./tval/schema                    # Directory containing schema YAML files
output_path: ./tval/output/report.html       # HTML report output path
encoding_confidence_threshold: 0.8           # Minimum confidence for CSV encoding detection (0.0-1.0)
# relations_path: ./tval/relations.yaml      # Optional: inter-table relation definitions
```

| Field                          | Type    | Default | Description                                             |
|--------------------------------|---------|---------|---------------------------------------------------------|
| `database_path`                | `string`| -       | Path to the DuckDB database file (`.duckdb` extension required) |
| `schema_dir`                   | `string`| -       | Directory containing table schema YAML files            |
| `output_path`                  | `string`| -       | Output path for the generated HTML report               |
| `encoding_confidence_threshold`| `float` | `0.8`   | Minimum confidence from `chardet` to trust detected CSV encoding |
| `relations_path`               | `string`| -       | Optional path to `relations.yaml` for cardinality validation |

### 3.6 Run Validation

```bash
# Auto-discover config (searches ./tval/config.yaml then ./config.yaml)
tval run

# Specify config path explicitly
tval run --config path/to/config.yaml

# Run validation and export to Parquet
tval run --export
```

#### CLI Reference

| Command    | Option      | Default            | Description                                  |
|------------|-------------|--------------------|----------------------------------------------|
| `tval init`| `--dir`     | `./tval`           | Target directory for project skeleton        |
| `tval run` | `--config`  | Auto-discover      | Path to `config.yaml`                        |
| `tval run` | `--export`  | Disabled           | Export to Parquet if all validations pass     |

### 3.7 Define Relations (Optional)

To validate inter-table relationship cardinalities, create a `relations.yaml` file and reference it in `config.yaml`:

```yaml
# config.yaml
relations_path: ./tval/relations.yaml
```

```yaml
# relations.yaml
relations:
  - name: users-orders
    cardinality: "1:N"
    from:
      table: users
      columns: [user_id]
    to:
      table: orders
      columns: [user_id]

  - name: orders-order_details
    cardinality: "1:N"
    from:
      table: orders
      columns: [order_id]
    to:
      table: order_details
      columns: [order_id]
```

#### Supported Cardinalities

| Cardinality | Checks Performed | Count |
|-------------|------------------|-------|
| `1:1` | from uniqueness + to uniqueness + bidirectional referential integrity | 4 |
| `1:N` | from (1-side) uniqueness + to→from referential integrity | 2 |
| `N:1` | to (1-side) uniqueness + from→to referential integrity | 2 |
| `N:N` | bidirectional referential integrity | 2 |

#### Relation Definition Reference

| Field         | Type       | Required | Description                                           |
|---------------|------------|----------|-------------------------------------------------------|
| `name`        | `string`   | Yes      | Human-readable name for the relation                  |
| `cardinality` | `string`   | Yes      | One of `1:1`, `1:N`, `N:1`, `N:N`                    |
| `from.table`  | `string`   | Yes      | Table name (must match a schema YAML definition)      |
| `from.columns`| `string[]` | Yes      | Column(s) on the from-side of the relation            |
| `to.table`    | `string`   | Yes      | Table name (must match a schema YAML definition)      |
| `to.columns`  | `string[]` | Yes      | Column(s) on the to-side of the relation              |

Relation checks follow the same skip logic as table checks: if either table has load errors, all checks for that relation are marked `SKIPPED`. NULL values are excluded from referential integrity checks (consistent with SQL FK semantics).

### 3.8 Understanding the HTML Report

The generated HTML report contains the following sections:

| Section                            | Description                                                         |
|------------------------------------|---------------------------------------------------------------------|
| **Summary**                        | Total tables, OK count, NG count                                    |
| **Load Results** (per table)       | Per-file load status; errors are displayed with messages             |
| **Logic Validation** (per table)   | Results of allowed-value checks and user-defined `checks`           |
| **Aggregation** (per table)        | Results of `aggregation_checks` (reported separately)               |
| **Statistics** (per table)         | Column profiles: count, nulls, unique, mean, std, min, max, percentiles |
| **Export** (per table)             | Parquet export status (only when `--export` is used)                |
| **Relation Cardinality Validation**| Cross-table relation check results (only when `relations_path` is configured) |

#### Status Definitions

| Status      | Meaning                                                            |
|-------------|------------------------------------------------------------------- |
| **OK**      | Check passed (or table has no validation failures)                 |
| **NG**      | Check failed (e.g. unexpected values found, constraint violated)   |
| **ERROR**   | Check execution failed (e.g. SQL error)                            |
| **SKIPPED** | Check was skipped due to upstream load errors                      |

### 3.9 Parquet Export

Parquet export is triggered by the `--export` flag and follows an **all-or-nothing** rule:

- If **all tables** have `OK` status **and** all relation checks pass (or are skipped), every table is exported to Parquet.
- If **any table** has `NG` status or any relation check fails, all exports are marked `SKIPPED`.

Export output is written to `<output_path_parent>/parquet/<table_name>/`.

When `partition_by` is set in the schema's `export` section, DuckDB writes Hive-style partitioned Parquet files:

```yaml
export:
  partition_by: [region, year]
```

This produces a directory structure like:

```
parquet/orders/region=US/year=2024/data_0.parquet
parquet/orders/region=JP/year=2024/data_0.parquet
```

### 3.10 Docker Usage

A development container is provided via the `Dockerfile`:

```bash
docker build -t tval-dev .
docker run -it -v "$(pwd)":/home/dev/workspace tval-dev
```

The container includes Python 3.12, uv, all project dependencies, and pre-commit hooks pre-installed.

---

## 4. Developer Guide

### 4.1 Development Setup

```bash
git clone https://github.com/b-trout/tval.git
cd tval
uv sync --extra dev
uv run pre-commit install
```

### 4.2 Code Quality

All tool configuration is centralized in `pyproject.toml`.

```bash
uv run ruff check src/ tests/     # Lint (pycodestyle, pyflakes, isort)
uv run ruff format --check src/ tests/  # Format check
uv run mypy src/                   # Type check (strict mode)
```

Pre-commit hooks run `ruff check`, `ruff format`, and `mypy` automatically on each commit.

### 4.3 Running Tests

```bash
uv run pytest tests/ -v
```

Tests use **real DuckDB instances and real data files** (no mocking):

| Test File              | Scope                                               |
|------------------------|-----------------------------------------------------|
| `test_parser.py`       | YAML schema parsing and Pydantic validation          |
| `test_builder.py`      | DDL generation and FK dependency ordering            |
| `test_loader.py`       | CSV/XLSX/Parquet file loading into DuckDB            |
| `test_checker.py`      | Validation check execution and error handling        |
| `test_profiler.py`     | Column statistics computation and error handling     |
| `test_exporter.py`     | Parquet export with partitioning                     |
| `test_reporter.py`     | HTML report generation and status aggregation        |
| `test_relation.py`     | Relation cardinality validation (1:1, 1:N, N:1, N:N)|
| `test_integration.py`  | End-to-end pipeline validation                       |

### 4.4 CI Pipeline

GitHub Actions runs on every pull request to `main`, testing against a Python version matrix:

| Step            | Command                              |
|-----------------|--------------------------------------|
| Ruff check      | `uv run ruff check src/ tests/`      |
| Ruff format     | `uv run ruff format --check src/ tests/` |
| Mypy            | `uv run mypy src/`                   |
| Pytest          | `uv run pytest tests/ -v`            |

Matrix: **Python 3.10** and **Python 3.12** on `ubuntu-latest`.

### 4.5 Architecture Overview

```
                   cli.py
                  /      \
            init.py      main.py
                         /  |  \  \  \  \  \  \
                parser.py   |   |  |  |  |  |  reporter.py
                   builder.py   |  |  |  |  |
                      loader.py |  |  |  |
                        checker.py |  |  |
                          relation.py |  |
                            profiler.py |
                              exporter.py
                                  |
                   All modules --> logger.py
```

Key design decisions:

- **SQL injection prevention** - All identifiers pass through `quote_identifier()` in `builder.py`, which validates against a strict regex and wraps in double quotes.
- **Connection separation** - Data loading uses a read-write connection (`conn_rw`); checks, profiling, and export use a read-only connection (`conn_ro`).
- **Dependency ordering** - Foreign key relationships are resolved via topological sort before table creation.

For detailed design documentation, see [docs/DESIGN.md](docs/DESIGN.md).

---

## License

MIT
