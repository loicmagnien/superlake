# SuperLake: Unified Data Lakehouse Management for Apache Spark & Delta Lake

**SuperLake** is a powerful Python framework for building, managing, and monitoring modern data lakehouse architectures on Apache Spark and Delta Lake. Designed for data engineers and analytics teams, SuperLake streamlines ETL pipeline orchestration, Delta table management, and operational monitoring—all in one extensible package.

**Main SuperLake Classes**

- **SuperSpark**: Unified SparkSession manager for Delta Lake. Handles Spark initialization, Delta Lake integration, warehouse/external paths, and catalog configuration (classic Spark, Databricks, Unity Catalog). Ensures consistent Spark setup for all pipelines and environments.

- **SuperDeltaTable**: Advanced Delta table abstraction. Supports managed/external tables, schema evolution (Merge, Overwrite, Keep), SCD2 (Slowly Changing Dimension) logic, partitioning, z-order, compression, and table properties. Provides robust methods for create, read, write, merge, SCD2 merge, delete, drop, optimize, vacuum, and schema alignment. Works seamlessly across Spark, Databricks, and Unity Catalog.

- **SuperPipeline**: Orchestrates end-to-end ETL pipelines (bronze → silver). Manages idempotent ingestion, CDC (Change Data Capture), transformation, and deletion logic. Integrates with SuperTracer for run tracking and supports force_cdc, force_caching, and robust error handling. Designed for medallion architecture and production-grade reliability.

- **SuperSimplePipeline / SuperGoldPipeline**: Simplified pipeline for gold-layer aggregations or single-table jobs. Runs a function (e.g., aggregation, modeling) and saves results to a Delta table, with full logging, tracing, and error handling.

- **SuperDataframe**: Utility class for DataFrame cleaning, transformation, and schema management. Features include column name/value cleaning, type casting, dropping/renaming columns, null handling, deduplication, distributed pivot, surrogate key generation, and schema-aligned union across DataFrames.

- **SuperLogger**: Unified logging and metrics for all pipeline operations. Supports contextual logging, metrics collection, and optional Azure Application Insights integration. Enables info, warning, error, and metric logging with sub-pipeline context.

- **SuperTracer**: Pipeline run trace manager. Persists run metadata (e.g., bronze/silver/gold updates, skips, deletions) in a Delta table for full auditability and idempotency. Enables robust recovery and monitoring of pipeline execution state.

- **SuperOrchestrator**: (For advanced users) Dependency-aware pipeline orchestrator. Discovers, groups, and executes pipelines based on dependency graphs. Supports parallelization, cycle detection, partial graph execution, and robust error handling for complex lakehouse projects.

- **MetricsCollector**: (Monitoring) Collects and aggregates table, data quality, performance, and storage metrics. Supports custom metric definitions and saving metrics to Delta tables for monitoring and alerting.

- **AlertManager**: (Monitoring) Flexible alerting engine. Supports custom alert rules, severity levels, and handlers (email, Slack, Teams, etc.) for real-time notifications based on metrics or pipeline events.

## Features

- **Delta Table Management**
  - Managed and external Delta tables (classic Spark, Databricks, Unity Catalog)
  - Schema evolution: Merge, Overwrite, Keep (add/drop/modify columns)
  - SCD2 (Slowly Changing Dimension) support with automatic history tracking
  - Partitioning, z-order, compression, and generated columns
  - Table properties, descriptions, and catalog registration
  - Optimize and vacuum operations for performance and storage

- **ETL Pipeline Orchestration**
  - Medallion architecture: bronze (raw), silver (cleaned), gold (aggregated)
  - Idempotent, traceable pipeline execution (SuperTracer)
  - Change Data Capture (CDC) and deletion logic
  - Force CDC and force caching for robust reruns and testing
  - Custom transformation and deletion functions
  - Full support for test, dev, and production environments

- **DataFrame Utilities**
  - Column name/value cleaning and normalization
  - Type casting and schema alignment
  - Drop, rename, and deduplicate columns/rows
  - Null value handling and replacement
  - Distributed pivot and schema-aligned union (type promotion)
  - Surrogate key generation (SHA-256 hash of fields)

- **Monitoring & Logging**
  - Unified logging (SuperLogger) with contextual sub-pipeline names
  - Metrics collection (row counts, durations, custom metrics)
  - Optional Azure Application Insights integration for enterprise observability
  - Pipeline run tracing (SuperTracer) for full auditability

- **Alerting & Notifications**
  - Custom alert rules and severity levels (info, warning, error, critical)
  - Handlers for email, Slack, Teams, and custom integrations
  - Real-time notifications based on metrics or pipeline events

- **Orchestration (Advanced)**
  - Dependency graph analysis and cycle detection
  - Group-based orchestration (roots-to-leaves or leaves-to-roots)
  - Parallel or serial execution of pipeline groups
  - Thread-safe status tracking and contextual logging
  - Partial graph execution and cascading skips on failure

- **Metrics & Data Quality**
  - Table, data quality, performance, and storage metrics
  - Null counts, distinct counts, basic statistics, and version history
  - Save metrics to Delta tables for monitoring and alerting

- **Extensibility & Modularity**
  - Modular design: use only what you need (core, monitoring, orchestration)
  - Easy to add new data sources, models, and custom pipeline logic
  - Open source, MIT-licensed, and community-driven

## Why SuperLake?

- **Accelerate Data Engineering**: Focus on business logic, not boilerplate.
- **Production-Ready**: Built-in monitoring, error handling, and alerting for reliable data operations.
- **Extensible & Modular**: Use only what you need—core data management, monitoring, or both.
- **Open Source**: MIT-licensed and community-driven.

## Installation

```bash
pip install superlake
```

## Quick Start

> **Best way to get started:**  
> Check out the [superlake-lakehouse](https://github.com/loicmagnien/superlake-lakehouse) repository for a full example project and ready-to-use templates.
