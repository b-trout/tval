# Background & Motivation

## Why tval Exists

In data-driven projects, the quality of received data directly determines the cost of the entire pipeline. A defect that slips through intake validation does not surface until modeling or insight generation — at which point the cost of remediation is an order of magnitude higher than catching it at the door.

At the same time, clients reasonably expect validation to be a fast, low-friction step. The business value they are paying for lies in analysis and model development, not in the mechanics of checking whether a column is nullable. The implicit expectation is always: *get through validation quickly and move on.*

Resolving this tension requires shifting data quality checks from ad-hoc scripting to **declarative, automated, and reproducible validation** — a process that is both rigorous and fast.

---

## The Problem with Existing Approaches

After observing how data validation is handled across multiple projects, three dominant patterns emerge — each with meaningful drawbacks.

### Pattern 1 — Full-Scratch Python (pandas / polars)

The most common approach. Teams write bespoke validation scripts using pandas or, more recently, polars. Libraries like pandera have improved the situation, but the fundamental issues remain.

**Drawbacks:**
- Validation logic varies by engineer, making coverage inconsistent and review difficult
- Memory-bound processing becomes a bottleneck on datasets exceeding a few hundred MB
- High degrees of freedom lead to systematic omissions — checks that *should* be written but are not

### Pattern 2 — Dedicated Validation Frameworks (great-expectations, frictionless, etc.)

Purpose-built tools that support declarative schema definitions. Conceptually aligned with what is needed, but operationally costly.

**Drawbacks:**
- Steep learning curve; onboarding a new team member to great-expectations is a non-trivial investment
- Framework-specific configuration syntax locks teams into a particular abstraction
- Flexibility is intentionally constrained, making project-specific checks difficult to express

### Pattern 3 — DWH + SQL (BigQuery, Redshift, Snowflake, etc.)

Loading data into a cloud data warehouse and validating it via SQL is a scalable approach, but it carries a subtle and often overlooked risk.

**Drawbacks:**
- **Distributed cloud DWHs do not enforce PRIMARY KEY, FOREIGN KEY, or CHECK constraints.** Constraints can be declared, but violations are silently accepted. Teams unaware of this behavior ship pipelines with undetected data integrity issues.
- Writing validation SQL from scratch for every project is tedious and error-prone
- The choice of warehouse engine materially affects which constraints are enforced — see the comparison below

---

## Constraint Enforcement Across Major Engines

The table below summarizes constraint enforcement behavior across commonly used databases and data warehouses.

| Symbol | Meaning |
|:---:|---|
| ✅ | Supported and enforced at write time |
| △ | Can be declared, but **not enforced** at write time — violations are silently accepted |
| ✗ | Not supported |

| Constraint | SQLite | DuckDB | PostgreSQL | BigQuery | Redshift | Snowflake | Synapse | Spark |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Type enforcement | ✗ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| NOT NULL | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| PRIMARY KEY | ✅ | ✅ | ✅ | △ | △ | △ | △ | △ |
| FOREIGN KEY | ✅ | ✅ | ✅ | △ | △ | △ | △ | ✗ |
| CHECK constraint | ✅ | ✅ | ✅ | △ | △ | △ | △ | ✗ |

> **Key insight:** Every major cloud-native distributed DWH sacrifices constraint enforcement in exchange for horizontal scalability. This is a deliberate architectural trade-off — not a bug — but it means that using these engines as a validation layer produces a false sense of security. The △ entries are especially dangerous: the schema *appears* valid, yet invalid data passes through undetected.

**Sources — official documentation for each engine:**

| Engine | Reference |
|---|---|
| SQLite | [Datatypes In SQLite (Type Affinity)](https://sqlite.org/datatype3.html) · [SQLite Foreign Key Support](https://sqlite.org/foreignkeys.html) |
| DuckDB | [Constraints – DuckDB](https://duckdb.org/docs/stable/sql/constraints) |
| PostgreSQL | [Constraints – PostgreSQL Documentation](https://www.postgresql.org/docs/current/ddl-constraints.html) |
| BigQuery | [Use primary and foreign keys – BigQuery](https://cloud.google.com/bigquery/docs/primary-foreign-keys) |
| Redshift | [Table constraints – Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html) |
| Snowflake | [Overview of Constraints – Snowflake](https://docs.snowflake.com/en/sql-reference/constraints-overview) |
| Synapse | [Primary, foreign, and unique keys – Azure Synapse Analytics](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-table-constraints) |
| Spark (Databricks) | [Constraints on Databricks](https://docs.databricks.com/aws/en/tables/constraints) |

---

## Why DuckDB

The constraint comparison above points toward a clear requirement: a validation engine that **enforces constraints strictly** while remaining **fast enough for file-level data at rest**.

DuckDB satisfies both conditions.

It is an in-process OLAP database with columnar storage, capable of reading CSV, Excel, and Parquet files directly without a separate server or ETL step. Unlike PostgreSQL — an OLTP engine that is operationally heavy for ephemeral validation workloads — DuckDB is designed for analytical queries over local files and embeds naturally into a CLI tool.

Critically, DuckDB enforces type constraints, NOT NULL, PRIMARY KEY, FOREIGN KEY, and CHECK constraints at insert time, matching the behavior of PostgreSQL rather than the permissive behavior of cloud DWHs.

| Criterion | DuckDB | PostgreSQL | Cloud DWH |
|---|:---:|:---:|:---:|
| Constraint enforcement | ✅ Full | ✅ Full | ✗ Partial |
| File-native reads (CSV, Parquet) | ✅ | ✗ | ✅ |
| Serverless / embeddable | ✅ | ✗ | ✗ |
| OLAP / columnar storage | ✅ | ✗ | ✅ |
| Suitable for ephemeral validation | ✅ | ✗ | ✗ |

---

## The tval Approach

tval is built on a single principle: **delegate constraint enforcement to DuckDB, and cover the remainder with user-defined SQL.**

This translates to the following design commitments:

**Maximize use of DuckDB's native constraints.** Type checking, NOT NULL, PRIMARY KEY, FOREIGN KEY — these are expressed once in YAML and enforced by the database engine. There is no reimplementation of constraint logic in Python.

**Extend with user-defined SQL where constraints fall short.** Business rules that cannot be expressed as database constraints — value range checks, cross-column conditions, aggregation invariants — are covered by arbitrary SQL queries defined in the schema file.

**Standardize through declarative YAML definitions.** Validation logic is expressed as data, not code. This makes schemas reviewable, diffable, version-controllable, and transferable between engineers without requiring them to understand an implementation.

**Eliminate reporting overhead.** Results are rendered as a self-contained HTML report. Communicating validation outcomes to stakeholders requires no additional tooling.

The result is a workflow that is fast enough to satisfy the expectation that validation is a pre-analysis step, rigorous enough to catch the defects that cause downstream rework, and standardized enough to be consistent across engineers and projects.
