# SuperLake: Unified Data Lakehouse Management for Apache Spark & Delta Lake

**SuperLake** is a powerful Python framework for building, managing, and monitoring modern data lakehouse architectures on Apache Spark and Delta Lake. Designed for data engineers and analytics teams, SuperLake streamlines ETL pipeline orchestration, Delta table management, and operational monitoring—all in one extensible package.

## Table of Contents

- [Why SuperLake?](#why-superlake)
  - [Delta Table Management](#delta-table-management)
  - [ETL Pipeline](#etl-pipeline)
  - [Orchestration](#orchestration)
  - [DataFrame Utilities](#dataframe-utilities)
  - [Monitoring & Logging](#monitoring--logging)
  - [Data Quality & Monitoring](#data-quality--monitoring)
  - [Alerting & Notifications](#alerting--notifications)
  - [Metrics & Data Quality](#metrics--data-quality)
  - [Extensibility & Modularity](#extensibility--modularity)
  - [Installation](#installation)
  - [Quick Start](#quick-start)
- [SuperLake Classes](#superlake-classes)
  - [SuperSpark](#superspark)
  - [SuperDeltaTable](#superdeltatable)
  - [SuperPipeline](#superpipeline)
  - [SuperSimplePipeline](#supersimplepipeline)
  - [SuperOrchestrator](#superorchestrator)
  - [SuperCataloguer](#supercataloguer)
  - [SuperCatalogQualityTable](#supercatalogqualitytable)
  - [SuperDataframe](#superdataframe)
  - [SuperLogger](#superlogger)
  - [SuperTracer](#supertracer)

<br/><br/><br/>

# Why SuperLake?

- **Build Data Lakehouses at Lightning Speed:**
  Skip the boilerplate and focus on what matters: your data and business logic. SuperLake automates the hard parts of Spark, Delta Lake, and catalog management.

- **Enterprise-Grade Reliability:**
  Run production pipelines with confidence. SuperLake bakes in robust error handling, idempotency, and monitoring so you can sleep at night.

- **Effortless Modularity & Extensibility:**
  Use only the components you need. SuperLake's modular design lets you plug in advanced monitoring, orchestration, or data quality when and where you want it.

- **Unified Data Quality & Observability:**
  Track, audit, and resolve data quality issues across your entire lakehouse with a single, consistent framework.

- **Modern, Open, and Extensible:**
  Built for the modern data stack—open source, cloud-ready, and designed to extended easily.


## Delta Table Management
- Managed and external Delta tables (classic Spark, Databricks, Unity Catalog)
- Schema evolution: Merge, Overwrite, Keep (add/drop/modify columns)
- SCD2 (Slowly Changing Dimension) support with automatic history tracking
- Partitioning, z-order, compression, and generated columns
- Table properties, descriptions, and catalog registration
- Optimize and vacuum operations for performance and storage
- NOT NULL constraint enforcement for primary key creation

## ETL Pipeline
- Medallion architecture: bronze (raw), silver (cleaned), gold (modelled)
- Idempotent, traceable pipeline execution (SuperTracer)
- Change Data Capture (CDC) and deletion logic
- Force CDC and force caching for robust reruns and testing
- Custom transformation and deletion functions
- Full support for test, dev, and production environments

## Orchestration
- Dependency graph analysis and cycle detection
- Group-based orchestration (roots-to-leaves or leaves-to-roots)
- Parallel or serial execution of pipeline groups
- Thread-safe status tracking and contextual logging
- Partial graph execution and cascading skips on failure

## DataFrame Utilities
- Column name/value cleaning and normalization
- Type casting and schema alignment
- Drop, rename, and deduplicate columns/rows
- Null value handling and replacement
- Distributed pivot and schema-aligned union (type promotion)
- Surrogate key generation (SHA-256 hash of fields)

## Monitoring & Logging
- Unified logging (SuperLogger) with contextual sub-pipeline names
- Metrics collection (row counts, durations, custom metrics)
- Optional Azure Application Insights integration for enterprise observability
- Pipeline run tracing (SuperTracer) for full auditability

## Data Quality & Monitoring
- Unified DQ Issue Schema: All DQ issues are tracked with: 
  - `table_name`, `column_name`, `check_key`, `check_value`, `check_dt`
- Descriptive DQ Messages: Each issue includes a clear, actionable message.
- Batch DQ Persistence: DQ issues from all tables are collected and written in a single batch for performance.
- Cleaner, Consistent DQ Table: Easier to query, join, and monitor.

## Alerting & Notifications
- Custom alert rules and severity levels (info, warning, error, critical)
- Handlers for email, Slack, Teams, and custom integrations (in progress)
- Real-time notifications based on metrics or pipeline events

## Metrics & Data Quality
- Table, data quality, performance, and storage metrics
- Null counts, distinct counts, basic statistics, and version history
- Save metrics to Delta tables for monitoring and alerting

## Extensibility & Modularity
- Modular design: use only what you need (core, monitoring, orchestration)
- Easy to add new data sources, models, and custom pipeline logic

## Installation

```bash
pip install superlake
```

## Quick Start

> **Best way to get started:**  
> Check out the [superlake-lakehouse](https://github.com/loicmagnien/superlake-lakehouse) repository for a full example project and ready-to-use templates.

<br/><br/><br/>

# SuperLake Classes

## SuperSpark 

The `SuperSpark` class manages your Spark session. It is an unified SparkSession manager for Delta Lake. Handles Spark initialization, Delta Lake integration, warehouse/external paths, and catalog configuration (classic Spark, Databricks, Unity Catalog). Ensures consistent Spark setup for all pipelines and environments.

### Key Features
- **Unified Spark Session Management**: Instantiates a SparkSession with all required Delta Lake and catalog configurations for any environment (local, Databricks, Unity Catalog).
- **Delta Lake Support**: Automatically attaches the Delta Lake library, enables Delta SQL extensions, and configures the Delta catalog for ACID transactions and advanced table features.
- **Flexible Catalog and Warehouse Configuration**: Supports custom catalogs (Unity Catalog, legacy Hive, Databricks), configurable warehouse directories for managed tables, and external paths for external tables.
- **Environment Agnostic**: Works seamlessly across local Spark, Databricks (with or without Unity Catalog), and cloud environments (Azure, AWS, GCP) with correct path and catalog handling.
- **Consistent Python Environment**: Ensures both driver and executors use the same Python interpreter, preventing environment mismatches in distributed jobs.
- **Session Naming for Traceability**: Allows naming the Spark session for easier identification in logs and the Spark UI.
- **Minimal Boilerplate**: Instantiates and configures Spark in a single line, exposing all best practices and options as parameters.


### Configuration

**Key parameters:**
- `session_name`: Name of the Spark session (for logs, Spark UI, etc.).
- `warehouse_dir`: Path to the Spark SQL warehouse directory (where managed Delta tables are stored).
- `external_path`: Root path for external tables (for data stored outside the warehouse directory).
- `catalog_name`: Name of the catalog to use (e.g., for Unity Catalog, Databricks, or legacy Hive Metastore).

**Example:**
```python
from superlake.core.spark import SuperSpark

super_spark = SuperSpark(
    session_name="SuperSpark for SuperLake",
    warehouse_dir="./data/spark-warehouse",
    external_path="./data/external-table/",
    catalog_name="spark_catalog"
)
```

- For Unity Catalog, set `warehouse_dir` and `external_path` to your cloud storage URIs and `catalog_name` to your Unity Catalog name.
- For local or legacy Spark, use local paths and the default catalog name.

All these parameters are set in `lakehouse/action/config.py` and used automatically by the main scripts.

## SuperDeltaTable

The `SuperDeltaTable` class provides a unified interface for managing Delta tables across Spark, Databricks, and Unity Catalog environments. You typically configure and instantiate `SuperDeltaTable` in your pipeline/model scripts. It is an advanced Delta table abstraction. Supports managed/external tables, schema evolution (Merge, Overwrite, Keep), SCD2 (Slowly Changing Dimension) logic, partitioning, z-order, compression, and table properties. Provides robust methods for create, read, write, merge, SCD2 merge, delete, drop, optimize, vacuum, and schema alignment. Works seamlessly across Spark, Databricks, and Unity Catalog.

### Key Features
   - **Unified Delta Table Management**: Handles Delta tables across Spark, Databricks, and Unity Catalog environments.
   - **Managed & External Table Support**: Supports both managed and external Delta tables, with correct path and catalog handling for each environment.
   - **Schema Evolution**: Automatically evolves table schemas to match DataFrames, with options for merge, overwrite, or keep strategies.
   - **SCD2 (Slowly Changing Dimension) Support**: Built-in support for SCD2 merge operations, including automatic management of SCD columns.
   - **Primary & Foreign Key Management**: Creates, drops, and manages primary and foreign key constraints, with full Unity Catalog support and constraint validation.
   - **Catalog Registration**: Registers tables in the Spark/Unity Catalog, ensuring discoverability and correct metadata.
   - **Optimization & Vacuum**: Supports Delta Lake OPTIMIZE (with ZORDER) and VACUUM commands for performance and storage management.
   - **Merge/Upsert & Delete**: Provides robust merge (upsert) and delete operations, including SCD2 logic and DataFrame alignment.
   - **Idempotent & Safe Operations**: All operations are designed to be idempotent and safe to re-run, with checks for table/schema existence and schema compatibility.
   - **Robust Error Handling**: Logs informative warnings and errors, skips unsupported operations, and provides detailed feedback for catalog and data quality issues.
   - **Unity Catalog Logic**: Detects Unity Catalog environments and applies catalog-specific logic for table creation, constraints, and metadata.

### Configuration

**All parameters:**
- `super_spark` (**required**): The `SuperSpark` instance managing your Spark session.
- `catalog_name` (**optional**): Name of the catalog (e.g., 'spark_catalog', 'main', 'my_unity_catalog'). If not set, uses the catalog from `super_spark`.
- `schema_name` (**required**): Name of the schema/database.
- `table_name` (**required**): Name of the table.
- `table_schema` (**required**): Spark `StructType` defining the table schema.
- `table_save_mode` (**required**): Save mode (`TableSaveMode.Append`, `TableSaveMode.Overwrite`, `TableSaveMode.Merge`, `TableSaveMode.MergeSCD`).
- `primary_keys` (**required**): List of primary key column names.
- `partition_cols` (optional): List of partition columns.
- `pruning_partition_cols` (optional, default=True): Whether to prune partition columns.
- `pruning_primary_keys` (optional, default=False): Whether to prune primary keys.
- `optimize_table` (optional, default=False): Whether to optimize the table (Delta OPTIMIZE).
- `optimize_zorder_cols` (optional): List of columns for ZORDER optimization.
- `optimize_target_file_size` (optional): Target file size for optimization (bytes).
- `compression_codec` (optional): Compression codec to use (e.g., 'snappy').
- `schema_evolution_option` (optional): Schema evolution option (`SchemaEvolution.Merge`, `SchemaEvolution.Overwrite`, `SchemaEvolution.Keep`).
- `logger` (optional): Custom logger (defaults to `SuperLogger`).
- `managed` (optional, default=False): Whether the table is managed (True) or external (False).
- `scd_change_cols` (optional): Columns that trigger SCD2 changes (not including PKs).
- `table_path` (optional): For external tables, the storage path (defaults to external_path/schema_name/table_name).
- `generated_columns` (optional): Dict of generated columns and their formulas, e.g. `{"year_col": "YEAR(date_col)"}`.
- `delta_properties` (optional): Dict of Delta table properties to set.
- `table_description` (optional): Table description for catalog metadata.
- `foreign_keys` (optional): List of dicts for foreign key constraints, each with keys:
    - `fk_columns`: list of local column names
    - `ref_table`: fully qualified referenced table name
    - `ref_columns`: list of referenced column names
    - `fk_name`: (optional) constraint name, auto-generated if not provided

**Example:**
```python
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution
import pyspark.sql.types as T

super_delta_table = SuperDeltaTable(
    super_spark=super_spark,
    catalog_name="my_unity_catalog",  # Unity Catalog example
    schema_name="01_bronze",
    table_name="erp_sales_transactions",
    table_schema=T.StructType([
        T.StructField("order_id", T.LongType(), False, {"description": "Unique ID of the order"}),
        T.StructField("order_line", T.IntegerType(), False, {"description": "Unique ID of the line within the order"}),
        # ... other fields ...
    ]),
    table_save_mode=TableSaveMode.Append,
    primary_keys=["order_id", "order_line"],
    partition_cols=["superlake_dt"],
    pruning_partition_cols=True,
    pruning_primary_keys=False,
    optimize_table=True,
    optimize_zorder_cols=["order_id"],
    optimize_target_file_size=134217728,  # 128MB
    compression_codec="snappy",
    schema_evolution_option=SchemaEvolution.Merge,
    managed=False,  # Set to True for managed tables
    scd_change_cols=["customer_id", "product_id"],
    table_path="/mnt/data/external-table/01_bronze/erp_sales_transactions",  # for external tables
    generated_columns={"year_col": "YEAR(order_date)"},
    delta_properties={"delta.autoOptimize.optimizeWrite": "true"},
    table_description="Raw ERP sales transactions (generated data)",
    foreign_keys=[
        {
            "fk_columns": ["customer_id"],
            "ref_table": "main.01_bronze.customers",
            "ref_columns": ["customer_id"]
        }
    ]
)
```
- For **Unity Catalog**, set `catalog_name` to your Unity Catalog name, and use cloud storage URIs for external tables.
- For **managed tables**, set `managed=True` (data is stored in the warehouse directory and deleted on DROP TABLE).
- For **external tables**, set `managed=False` (data is stored in the external path and not deleted on DROP TABLE).

All these parameters are typically set in your pipeline/model scripts, and are used by the ingestion, modeling, and catalog management logic.



## SuperPipeline

The `SuperPipeline` class manages a full medallion pipeline (bronze → silver), including CDC ingestion, transformation, deletion, and idempotency logic. It is designed for robust, idempotent, and traceable ETL workflows. It orchestrates end-to-end ETL pipelines (bronze → silver). Manages idempotent ingestion, CDC (Change Data Capture), transformation, and deletion logic. Integrates with SuperTracer for run tracking and supports force_cdc, force_caching, and robust error handling. Designed for medallion architecture and production-grade reliability.

### Key Features
  - **Medallion Architecture Support**: Manages full bronze → silver data pipelines, including CDC ingestion, transformation, and deletion logic.
  - **Idempotent and Safe**: Ensures all operations are idempotent, so reruns are safe and do not duplicate or corrupt data.
  - **CDC (Change Data Capture) Integration**: Supports custom CDC functions to ingest only new or changed data.
  - **Transformation Pipeline**: Allows custom transformation functions to be applied to ingested data before loading into silver tables.
  - **Automated Deletion Handling**: Optionally supports deletion logic to remove records from silver tables that are no longer present in the source.
  - **Traceability and Logging**: Integrates with SuperTracer for detailed run tracking, idempotency, and pipeline status logging.
  - **Flexible Execution Control**: Supports force CDC, force caching, and environment-based debug/test modes.
  - **Automatic Deduplication**: Handles duplicate source rows during merge/upsert operations.
  - **Performance Optimization**: Optionally caches intermediate DataFrames for large data processing.
  - **Debug and Test Friendly**: Provides debug/test modes for easier development and troubleshooting.
  - **Micro-batching Support**: Supports both batch and micro-batch execution modes for different use cases.

### Congiguration

**Parameters:**
- `logger`: Logger instance (e.g., `SuperLogger`) for contextual logging and metrics.
- `super_spark`: The `SuperSpark` instance managing your Spark session.
- `super_tracer`: The `SuperTracer` instance for idempotency and trace logging.
- `superlake_dt`: The pipeline run datetime (typically a `datetime` object).
- `pipeline_name`: Name of the pipeline (used for logging, tracing, and catalog).
- `bronze_table`: The `SuperDeltaTable` instance for the bronze (raw) table.
- `silver_table`: The `SuperDeltaTable` instance for the silver (transformed) table.
- `cdc_function`: Function to extract CDC data. Signature: `cdc_function(spark: SparkSession) -> DataFrame`.
- `tra_function`: Transformation function to apply to DataFrames. Signature: `tra_function(df: DataFrame) -> DataFrame`.
- `del_function` (optional): Function to return all current source rows (for deletion logic). Signature: `del_function(spark: SparkSession) -> DataFrame`. If not provided, deletion is skipped.
- `force_cdc` (optional, default=False): If `True`, always run the CDC function and force pipeline execution, even if already traced.
- `force_caching` (optional, default=False): If `True`, cache intermediate DataFrames for performance (useful for large data).
- `environment` (optional, default="dev"): Environment string, e.g., `"dev"`, `"test"`, `"debug"`. Enables extra logging and DataFrame `.show()` in debug/test.

**Example:**
```python
from superlake.core.pipeline import SuperPipeline

pipeline = SuperPipeline(
    logger=logger,
    super_spark=super_spark,
    super_tracer=super_tracer,
    superlake_dt=superlake_dt,
    pipeline_name="fact_sales",
    bronze_table=bronze_table,
    silver_table=silver_table,
    cdc_function=cdc_function,
    tra_function=tra_function,
    del_function=del_function,  # optional
    force_cdc=False,
    force_caching=True,
    environment="dev"
)
pipeline.execute()
```

The `pipeline.execute()` method takes a `pipeline_mode` argument: 
- `batch`: processes the pipeline in batch mode using `execute_batch()`
- `micro_batch`: processes the pipeline as a stream of micro-batches using `execute_micro_batch()`

## SuperSimplePipeline

The `SuperSimplePipeline` class is a minimal pipeline for running a single function and saving its result to a table (no CDC, no bronze/silver split, no deletion logic). It is ideal for simple dimension tables or one-off loads. It is a simplified pipeline for gold-layer aggregations or single-table jobs. Runs a function (e.g., aggregation, modeling) and saves results to a Delta table, with full logging, tracing, and error handling.

### Key Features
   - **Minimal Pipeline for Simple Loads**: Designed for simple, one-step data loads (e.g., dimension tables, one-off ETL jobs).
   - **Function-Based Data Generation**: Runs a user-defined function to generate a DataFrame and saves it to a Delta table.
   - **Idempotent and Safe**: Ensures safe, repeatable loads with built-in traceability.
   - **Traceability and Logging**: Integrates with SuperTracer for run tracking and logging.
   - **Automatic Deduplication**: Handles duplicate source rows during merge/upsert operations.
   - **Debug and Test Friendly**: Supports debug/test modes for easier development and troubleshooting.

### Configuration

**Parameters:**
- `logger`: Logger instance (e.g., `SuperLogger`).
- `super_spark`: The `SuperSpark` instance managing your Spark session.
- `super_tracer`: The `SuperTracer` instance for idempotency and trace logging.
- `superlake_dt`: The pipeline run datetime (typically a `datetime` object).
- `pipeline_name`: Name of the pipeline (for logging/tracing).
- `function`: Function to generate the DataFrame. Signature: `function(super_spark, superlake_dt) -> DataFrame`.
- `table`: The `SuperDeltaTable` instance to save the result to.
- `environment` (optional): Environment string, e.g., `"dev"`, `"test"`, `"debug"`. Enables extra logging and DataFrame `.show()` in debug/test.

**Example:**
```python
from superlake.core.pipeline import SuperSimplePipeline

simple_pipeline = SuperSimplePipeline(
    logger=logger,
    super_spark=super_spark,
    super_tracer=super_tracer,
    superlake_dt=superlake_dt,
    pipeline_name="dim_date",
    function=generate_date_dim,
    table=date_dim_table,
    environment="dev"
)
simple_pipeline.execute()
```

## SuperOrchestrator

The `SuperOrchestrator` class provides robust orchestration for your data pipelines, handling dependency management, parallelization, cycle detection, and execution order. It is designed to discover, analyze, and execute ingestion and modeling pipelines in the correct order, supporting both serial and parallel execution, and advanced dependency graph operations.. It is a dependency-aware pipeline orchestrator. Discovers, groups, and executes pipelines based on dependency graphs. Supports parallelization, cycle detection, partial graph execution, and robust error handling for complex lakehouse projects.

### Key Features
- **Dependency graph analysis:** Automatically discovers dependencies between pipeline files.
- **Cycle detection:** Detects and logs cyclic dependencies in the pipeline graph.
- **Group-based orchestration:** Pipelines are grouped by dependency level and processed in order (roots to leaves or vice versa).
- **Thread-safe status tracking:** Tracks the status (success, failed, skipped) of each pipeline.
- **Contextual logging:** Logs show which pipeline or orchestrator step is being executed.
- **Partial graph execution:** Orchestrate only a subset of the full pipeline graph by specifying targets and direction.
- **Cascading skips:** If a pipeline is skipped due to all upstreams failing/skipped, its downstreams will also be skipped in cascade.
- **Parallel execution:** Optionally run pipeline groups in parallel threads for faster orchestration.
- **Loop Execution**: Can run pipelines in loops with configurable intervals and duration limits.


### Configuration

**Parameters:**
- `super_spark`: The `SuperSpark` instance managing your Spark session.
- `catalog_name`: Name of the catalog to use (e.g., for Unity Catalog, Databricks, or legacy Hive Metastore).
- `logger`: Logger instance (e.g., `SuperLogger`).
- `managed`: Boolean indicating if tables are managed.
- `superlake_dt`: The pipeline run datetime (typically a `datetime` object).
- `super_tracer`: The `SuperTracer` instance for idempotency and trace logging.
- `environment`: Environment string, e.g., `"dev"`, `"test"`, `"debug"`.
- `project_root`: Path to the project root directory.
- `ingestion_folder` (optional): Name of the ingestion folder (default: 'ingestion').
- `modelisation_folder` (optional): Name of the modeling folder (default: 'modelisation').

You can modify parameters in this file and in the respective methods:
- `warehouse_dir`, `external_path`, `catalog_name`, `managed`, `environment`
- `target_pipelines`: List of pipelines to run (e.g., `['fact_bike_status', 'dim_bike_station']`)
- `loading_mode`: How to discover and load pipeline files (default: `'file'`).
- `orchestration_mode`: Order of processing pipeline groups:
    - `'process_first'`: Roots to leaves (upstream to downstream).
    - `'process_last'`: Leaves to roots (downstream to upstream).
- `direction`: Which part of the dependency graph to process relative to targets:
    - `'upstream'`: Only dependencies of the targets (and the targets themselves).
    - `'downstream'`: Only dependents of the targets (and the targets themselves).
    - `'all'`: Both upstream and downstream pipelines (full subgraph).
    - `'none'`: Only the specified target_pipelines, with no dependencies.
- `parallelize_groups`: If `True`, pipelines within each group are run in parallel threads. If `False`, run serially.
- `fail_fast`: If `True`, stop execution as soon as any pipeline fails. If `False`, log errors and continue.
- `skip_downstream_on_failure`: If `True`, a pipeline will be skipped if all of its upstream dependencies have failed or been skipped.


**Example:**
```python
from superlake.core.orchestration import SuperOrchestrator

orchestrator = SuperOrchestrator(
    super_spark=super_spark,
    catalog_name="my_unity_catalog",
    logger=logger,
    managed=True,
    superlake_dt=superlake_dt,
    super_tracer=super_tracer,
    environment="dev",
    project_root="/path/to/project/root"
)

orchestrator.orchestrate(
    loading_mode='file',
    orchestration_mode='process_last',
    target_pipelines=["fact_sales.py", "dim_date.py"],  # or [] for all
    direction='all',
    parallelize_groups=True,
    fail_fast=True,
    skip_downstream_on_failure=True
)
```


## SuperCataloguer

The `SuperCataloguer` class provides utilities to discover, register, and manage all model and ingestion tables in a SuperLake lakehouse project. It automates catalog operations such as ensuring table existence, applying comments, and managing primary/foreign keys, especially for Unity Catalog environments. The `SuperCatalogQualityTable` class allows you to persist and track catalog/data quality issues in a Delta table. Utility for discovering, registering, and managing all model and ingestion tables in a SuperLake project. Automates table discovery, catalog registration, schema and comment application, and batch data quality issues persistence within the lakehouse. Supports both modelisation and ingestion folders, and provides robust helpers for orchestrating catalog and DQ operations on all tables.

### Key Features

  - **Table Existence Management**: Ensures all required tables exist in the catalog before operations begin.
  - **Table Comments**: Applies and manages table-level comments, with full support for Unity Catalog tables.
  - **Column Comments**: Applies and manages column-level comments, supporting Unity Catalog metadata.
  - **Primary Key Management**: Creates, drops, and manages primary keys for tables, with Unity Catalog support.
  - **Foreign Key Management**: Creates, drops, and manages foreign keys for tables, with Unity Catalog support.
  - **Catalog Quality Tracking**: Maintains a catalog quality table to track metadata completeness and quality across the lakehouse.
  - **Error Handling**: Skips unsupported operations gracefully (e.g., on non-Unity Catalog tables) and logs informative messages.

### Configuration

**Parameters:**
- `project_root`: Path to the project root directory.
- `modelisation_folder` (optional): Name of the modeling folder (default: 'modelisation').
- `ingestion_folder` (optional): Name of the ingestion folder (default: 'ingestion').


## SuperCatalogQualityTable

### Key Features
- **Persist DQ issues:** Saves catalog/data quality issues as Delta table records.
- **Schema:**
  - `table_name`, `column_name`, `check_key`, `check_value`, `check_dt`
- **Easy integration:** Used by `SuperCataloguer` to track and persist issues found during catalog operations.


### Configuration

**Parameters:**
- `super_spark`: The `SuperSpark` instance managing your Spark session.
- `catalog_name`: Name of the catalog.
- `schema_name`: Name of the schema/database.
- `table_name`: Name of the DQ table.
- `managed`: Boolean indicating if the DQ table is managed.
- `logger`: Logger instance.

**Example:**
```python
from superlake.core.catalog import SuperCataloguer, SuperCatalogQualityTable

# Set up the cataloguer
cataloguer = SuperCataloguer(
    project_root="/path/to/project/root",
    modelisation_folder="modelisation",
    ingestion_folder="ingestion"
)

# Set up the catalog quality table (optional, for DQ tracking)
quality_table = SuperCatalogQualityTable(
    super_spark=super_spark,
    catalog_name="my_unity_catalog",
    schema_name="00_catalog_quality",
    table_name="catalog_quality_issues",
    managed=True,
    logger=logger
)

# Example: Apply table comments and persist catalog quality issues
cataloguer.apply_table_comment(
    super_spark=super_spark,
    catalog_name="my_unity_catalog",
    logger=logger,
    managed=True,
    superlake_dt=superlake_dt,
    persist_catalog_quality=True,
    super_catalog_quality_table=quality_table
)

# Other operations: ensure_tables_exist, register_tables, apply_columns_comments, create/drop keys, etc.
```

## SuperDataframe

The `SuperDataframe` class provides a rich set of utility methods for cleaning, transforming, and engineering features on PySpark DataFrames. It is designed to simplify common data preparation and modeling tasks in the Lakehouse, including column cleaning, type casting, deduplication, surrogate key generation, and distributed pivoting. Utility class for DataFrame cleaning, transformation, and schema management. Features include column name/value cleaning, type casting, dropping/renaming columns, null handling, deduplication, distributed pivot, surrogate key generation, and schema-aligned union across DataFrames.

### Key Features
- **Column cleaning:**
  - `clean_columns_names()`: Replace invalid characters in column names.
  - `clean_column_values(columns)`: Clean values in specified columns.
- **Type and value operations:**
  - `cast_columns(schema)`: Cast columns to match a target schema.
  - `drop_columns(columns)`: Drop specified columns.
  - `rename_columns(columns_dict)`: Rename columns using a mapping.
  - `replace_null_values(column_value_dict)`: Replace nulls in specified columns.
  - `drop_duplicates(columns)`: Drop duplicate rows based on columns.
  - `drop_null_values(columns)`: Drop rows with nulls in specified columns.
- **Advanced transformations:**
  - `distributed_pivot(...)`: Distributed pivot operation for wide-to-long reshaping.
  - `generate_surrogate_key(field_list, key_column_name)`: Add a surrogate key column using SHA-256 hash of fields.
  - `super_union_by_name(dfs)`: Static method to union DataFrames with different schemas, aligning columns and promoting types.

### Configuration

**Parameters:**
- `df`: The PySpark `DataFrame` to wrap.
- `logger` (optional): Logger instance for metrics and info (default: None).


## SuperLogger

The `SuperLogger` class provides unified, contextual logging and metrics for all SuperLake pipeline operations. It supports info, warning, and error logging, as well as custom metric tracking and integration with Azure Application Insights for enterprise observability.

### Key Features
- **Contextual Logging:** Supports sub-pipeline and operation context, making logs easy to trace across complex workflows.
- **Metrics Collection:** Log and retrieve custom metrics (e.g., row counts, durations) for any operation.
- **Application Insights Integration:** Optionally send logs and metrics to Azure Application Insights for centralized monitoring.
- **Execution Tracking:** Context manager for timing and logging the duration and status of operations, with automatic success/failure metrics.
- **Thread-Safe Context:** Maintains contextual information (e.g., pipeline name) even in parallel or multi-threaded execution.
- **Flush and Reset:** Methods to flush logs/metrics and reset metrics storage.

**Example:**
```python
from superlake.monitoring import SuperLogger

logger = SuperLogger(name="MyPipelineLogger")
logger.info("Pipeline started")
logger.metric("rows_loaded", 12345)
logger.warning("This is a warning")
logger.error("This is an error")
```

## SuperTracer

The `SuperTracer` class manages pipeline run traces for full auditability and idempotency in SuperLake. It persists run metadata (e.g., bronze/silver/gold updates, skips, deletions) in a Delta table, enabling robust recovery, monitoring, and idempotent pipeline execution.

### Key Features
- **Run Trace Persistence:** Saves pipeline run events (bronze/silver/gold updates, skips, deletions) with timestamps and keys in a Delta table.
- **Idempotency Management:** Ensures pipelines can be safely retried or resumed without duplicating or corrupting data.
- **Flexible Trace Schema:** Tracks `superlake_dt`, `trace_dt`, `trace_key`, `trace_value`, and `trace_year` for each event.
- **Trace Querying:** Retrieve and filter traces for any pipeline run, enabling conditional logic and recovery.
- **Integration with Pipelines:** Used by all SuperLake pipelines to record and check run status, supporting robust orchestration and error handling.

**Example:**
```python
from superlake.core.pipeline import SuperTracer

tracer = SuperTracer(
    super_spark=super_spark,
    catalog_name="my_catalog",
    schema_name="00_superlake__tracing",
    table_name="pipeline_traces",
    managed=True,
    logger=logger
)

# Add a trace for a pipeline run
tracer.add_trace(superlake_dt, "pipeline_name", "bronze_updated")

# Check if a trace exists
if tracer.has_trace(superlake_dt, "pipeline_name", "bronze_updated"):
    print("Bronze table already updated for this run.")
```
