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
- [2. Project Layout](#2-project-layout)
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

tval is a CLI tool that validates tabular data files (CSV, Excel, Parquet) against YAML schema definitions. It checks data types, missing values, allowed values, custom rules, and cross-table relationships — then generates a single HTML report with all results.

### 1.2 Problems It Solves

- **Inconsistent validation** — When multiple analysts manually inspect data, coverage varies. tval enforces a single set of declarative rules.
- **Manual effort** — Hand-checking row counts, allowed values, and aggregation totals is tedious. tval automates the entire process.

### 1.3 How It Works

```
config.yaml          schema/*.yaml       relations.yaml (optional)
     |                     |                     |
     v                     v                     |
  Load config       Validate schemas             |
     |                     |                     |
     +--------+------------+                     |
              |                                  |
              v                                  |
     Load data files (CSV / XLSX / Parquet)       |
              |                                  |
              v                                  |
     Run validation checks                       |
              |                                  |
              v                                  |
     Run relation checks  <----------------------+
              |
              v
     Compute column statistics
              |
              v
     Export to Parquet (optional)
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

---

## 2. Project Layout

After running `tval init`, your project will have this structure:

```
your-project/
├── tval/
│   ├── config.yaml       # Validation configuration
│   ├── schema/           # Table definition YAML files (one per table)
│   ├── data/             # Source data files (CSV, XLSX, Parquet)
│   └── output/           # Generated reports and exports
├── ...                   # Your other project files
└── .gitignore            # tval/data/ and tval/output/ are auto-added
```

For the repository source layout and module details, see [Section 4.5 Architecture Overview](#45-architecture-overview).

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
  name: orders                    # Table name used internally
  description: Orders table       # Human-readable description
  source_dir: ./data/orders/      # Directory containing data files (relative to config.yaml)

# --- Column definitions ---
columns:
  - name: order_id                # Column name (must match data file headers)
    logical_name: Order ID        # Display name for reports
    type: INTEGER                 # Data type (INTEGER, VARCHAR, DOUBLE, DATE, etc.)
    not_null: true                # true = blank/missing values are not allowed

  - name: user_id
    logical_name: User ID
    type: INTEGER
    not_null: true

  - name: amount
    logical_name: Amount
    type: DOUBLE
    not_null: true
    min: 0                            # Minimum allowed value (numeric columns only)
    max: 1000000                      # Maximum allowed value (numeric columns only)

  - name: status
    logical_name: Status
    type: VARCHAR
    not_null: true
    allowed_values:               # Only these values are accepted; anything else is flagged
      - pending
      - shipped
      - cancelled

  - name: order_date
    logical_name: Order Date
    type: DATE
    not_null: true
    format: "%Y-%m-%d"           # Expected date format (for DATE, TIMESTAMP, TIME columns)

# --- Table-level rules ---
table_constraints:
  # Columns that uniquely identify each row (no duplicates allowed)
  primary_key:
    columns: [order_id]

  # Column combinations where duplicate values are not allowed
  unique:
    - columns: [order_id, user_id]

  # Columns whose values must exist in another table
  foreign_keys:
    - columns: [user_id]           # Column in this table
      references:
        table: users               # The other table (must have its own YAML)
        columns: [user_id]         # Matching column in the other table

  # Custom validation rules written in SQL
  checks:
    - description: Amount must be non-negative
      query: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
      expect_zero: true            # true = pass when no rows match (i.e., no violations found)

  # Declarative row-level conditions (auto-generates SQL checks)
  row_conditions:
    - description: Amount must not exceed 10x order_id
      condition: "amount <= order_id * 10"

  # Aggregation-level validation rules (shown in a separate report section)
  aggregation_checks: []

# --- Export settings (optional) ---
export:
  partition_by: []                 # Split exported Parquet files by these columns
```

#### Column Definition Reference

| Field            | Type       | Required | Description                                             |
|------------------|------------|----------|---------------------------------------------------------|
| `name`           | `string`   | Yes      | Column name matching data file headers                  |
| `logical_name`   | `string`   | Yes      | Human-readable name for reports                         |
| `type`           | `string`   | Yes      | Data type (e.g. `INTEGER`, `VARCHAR`, `DOUBLE`, `DATE`) |
| `not_null`       | `bool`     | Yes      | `true` = blank/missing values are not allowed           |
| `description`    | `string`   | No       | Optional description                                    |
| `allowed_values` | `string[]` | No       | List of accepted values; other values are flagged as NG |
| `min`            | `number`   | No       | Minimum allowed value (numeric columns only). NULL values are excluded from check |
| `max`            | `number`   | No       | Maximum allowed value (numeric columns only). Must be >= `min` if both specified |
| `format`         | `string`   | No       | Expected date/time format (e.g. `"%Y-%m-%d"`) for DATE/TIMESTAMP/TIME columns |

#### Table Constraints Reference

| Rule                 | What it does                                                               |
|----------------------|----------------------------------------------------------------------------|
| `primary_key`        | Ensures each row is uniquely identified — no duplicate values allowed in these columns |
| `unique`             | Ensures a combination of columns has no duplicate values                   |
| `foreign_keys`       | Ensures values in these columns exist in the referenced table (e.g. every `user_id` in orders must exist in the users table) |
| `checks`             | Custom SQL queries to validate data (see below)                           |
| `row_conditions`     | Declarative row-level conditions — each `condition` is a SQL boolean expression that must be true for every row |
| `aggregation_checks` | Same as `checks`, but results appear in a separate section of the report  |

#### User-Defined Checks

Write custom validation rules using SQL. Use `{table}` as a placeholder for the table name:

```yaml
checks:
  # Flag rows where amount is negative
  - description: Amount must be non-negative
    query: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
    expect_zero: true     # true  → pass when the query returns 0 (no violations)
                          # false → pass when the query returns non-zero

  # Ensure the table has at least one row
  - description: Table must not be empty
    query: "SELECT COUNT(*) FROM {table}"
    expect_zero: false

  # Check with parameterized allowed values
  - description: Status must be valid
    query: "SELECT COUNT(*) FROM {table} WHERE status NOT IN (SELECT UNNEST(?::VARCHAR[]))"
    expect_zero: true
    params: [["pending", "shipped", "cancelled"]]
```

> **Tip:** `expect_zero: true` means "this query counts violations — pass when zero violations are found."
> `expect_zero: false` means "this query counts expected rows — pass when at least one row is found."

#### Export Configuration

| Field          | Type       | Default | Description                                       |
|----------------|------------|---------|---------------------------------------------------|
| `partition_by` | `string[]` | `[]`    | Column names to split Parquet output files by      |

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

Relations let you verify how tables are connected. For example, you can check that every `user_id` in the orders table actually exists in the users table.

To enable relation validation, create a `relations.yaml` file and reference it in `config.yaml`:

```yaml
# config.yaml
relations_path: ./tval/relations.yaml
```

```yaml
# relations.yaml
relations:
  # "One user can have many orders"
  - name: users-orders
    cardinality: "1:N"
    from:
      table: users
      columns: [user_id]
    to:
      table: orders
      columns: [user_id]

  # "One order can have many line items"
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

| Cardinality | Meaning | What is checked | Count |
|-------------|---------|-----------------|-------|
| `1:1` | One row in each table matches exactly one row in the other (e.g. user ↔ profile) | No duplicates on either side + every value exists in both tables | 4 |
| `1:N` | One row on the from-side can match many rows on the to-side (e.g. user → orders) | No duplicates on the from-side + every to-side value exists in the from table | 2 |
| `N:1` | Many rows on the from-side match one row on the to-side (e.g. orders → user) | No duplicates on the to-side + every from-side value exists in the to table | 2 |
| `N:N` | Many-to-many (e.g. students ↔ courses) | Every value on each side exists in the other table | 2 |

#### Relation Definition Reference

| Field         | Type       | Required | Description                                           |
|---------------|------------|----------|-------------------------------------------------------|
| `name`        | `string`   | Yes      | Human-readable name for the relation                  |
| `cardinality` | `string`   | Yes      | One of `1:1`, `1:N`, `N:1`, `N:N`                    |
| `from.table`  | `string`   | Yes      | Table name (must match a schema YAML definition)      |
| `from.columns`| `string[]` | Yes      | Column(s) on the from-side of the relation            |
| `to.table`    | `string`   | Yes      | Table name (must match a schema YAML definition)      |
| `to.columns`  | `string[]` | Yes      | Column(s) on the to-side of the relation              |

> **Note:** If either table has data loading errors or validation check failures, all checks for that relation are marked `SKIPPED` (since the data is incomplete). Blank (NULL) values are excluded from cross-table existence checks.

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

| Status      | Icon | Meaning                                                          |
|-------------|------|------------------------------------------------------------------|
| **OK**      | ✅   | Check passed — no issues found                                  |
| **NG**      | ❌   | Check failed — data does not meet the expected rule (e.g. duplicates found, invalid values, constraint violation) |
| **ERROR**   | ❌   | Check could not run — typically caused by a bug in the SQL query or an internal error. Review the error message and fix the query |
| **SKIPPED** | ⚠️   | Check was not executed — this happens when data files failed to load or when validation checks failed (profiling and relation checks are skipped for tables with check failures). Fix the underlying errors first, then re-run |

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

#### Repository Layout

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
├── docs/
├── pyproject.toml           # Build config, dependencies, tool settings
├── Dockerfile               # Development container
├── .pre-commit-config.yaml  # Pre-commit hook configuration
└── .github/workflows/
    └── ci.yml               # GitHub Actions CI pipeline
```

#### Module Responsibilities

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

#### Pipeline Flow

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

#### Key Design Decisions

- **SQL injection prevention** - All identifiers pass through `quote_identifier()` in `builder.py`, which validates against a strict regex and wraps in double quotes.
- **Connection separation** - Data loading uses a read-write connection (`conn_rw`); checks, profiling, and export use a read-only connection (`conn_ro`).
- **Dependency ordering** - Foreign key relationships are resolved via topological sort before table creation.

For detailed design documentation, see [docs/DESIGN.md](docs/DESIGN.md).

---

## License

MIT
