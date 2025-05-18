# SuperLake: Unified Data Lakehouse Management for Apache Spark & Delta Lake

**SuperLake** is a powerful Python framework for building, managing, and monitoring modern data lakehouse architectures on Apache Spark and Delta Lake. Designed for data engineers and analytics teams, SuperLake streamlines ETL pipeline orchestration, Delta table management, and operational monitoring—all in one extensible package.

**Main SuperLake Classes**
- **SuperSpark**: Instantiates a SparkSession with Delta Lake support.
- **SuperDeltaTable**: Manages Delta tables (create, read, write, 
optimize, vacuum, SCD2, schema evolution, etc.).
- **SuperPipeline**: Orchestrates data pipelines from source to bronze 
and silver layers, including CDC and transformation logic.
- **SuperGoldPipeline**: Manages gold-layer aggregations and writes 
results to gold tables.
- **SuperDataframe**: Utility class for DataFrame cleaning, casting, 
and manipulation.
- **SuperLogger**: Logging and metrics for pipeline operations.

## Features

- **Delta Table Management**: Effortlessly create, update, and optimize Delta tables with support for schema evolution, SCD2, partitioning, and advanced save modes.
- **ETL Pipeline Orchestration**: Build robust, multi-stage ETL pipelines (bronze, silver, gold) with reusable pipeline classes and seamless Spark integration.
- **DataFrame Utilities**: Clean, transform, and align Spark DataFrames with intuitive helper methods.
- **Comprehensive Monitoring**: Track pipeline health with unified logging, custom metrics, and real-time alerts.
- **Alerting & Notifications**: Set up threshold-based or custom alert rules with notifications via email, Slack, or Microsoft Teams.
- **Azure Application Insights Integration**: Optional telemetry for enterprise-grade observability.

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

```python
# superlake Library
from superlake.core import (
    SuperSpark, 
    SuperDeltaTable, 
    SuperTracer, 
    SuperPipeline, 
    SuperGoldPipeline, 
    TableSaveMode, 
    SchemaEvolution
)
from superlake.monitoring import SuperLogger

# Standard Library
import pyspark.sql.types as T
import pyspark.sql.functions as F
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
import sys
import time

# User Guidance:
# If running on classic Spark or Databricks without Unity Catalog:
# do not set catalog_name (or set it to None or "spark_catalog").
# Only set catalog_name to a real Unity Catalog name
# if your Spark session is configured for it.

# Initialize SuperSpark for SuperDeltaTable
super_spark = SuperSpark(
    session_name="SuperSpark for SuperLake",
    warehouse_dir="./data/spark-warehouse",   # path to the spark-warehouse
    external_path="./data/external-table/",   # path to the external table
    catalog_name="spark_catalog"              # default catalog name is spark_catalog
)

# Initialize classic SparkSession
spark = super_spark.spark
logger = SuperLogger(name="SuperLake")
superlake_dt = datetime.now()

# initialize SuperTracer and generate trace table
super_tracer = SuperTracer(
    super_spark=super_spark,
    catalog_name="spark_catalog",
    schema_name="00_superlake",
    table_name="super_trace",
    managed=False,
    logger=logger
)


# ------------------------------------------------------------------------------------------------
#                     Bronze and silver tables, cdc and transformation functions   
# ------------------------------------------------------------------------------------------------

# Bronze Customer Table (managed)
bronze_customer = SuperDeltaTable(
    super_spark=super_spark,
    catalog_name="spark_catalog",
    schema_name="01_bronze",
    table_name="customer",
    table_schema=T.StructType([
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("signup_date", T.DateType(), True),
        T.StructField("superlake_dt", T.TimestampType(), True)
    ]),
    table_save_mode=TableSaveMode.Append,
    primary_keys=["customer_id"],
    partition_cols=["superlake_dt"],
    pruning_partition_cols=True,
    pruning_primary_keys=False,
    optimize_table=False,
    optimize_zorder_cols=[],
    optimize_target_file_size=100000000,
    compression_codec="snappy",
    schema_evolution_option=SchemaEvolution.Merge,
    logger=logger,
    managed=True  # Managed table (in spark-warehouse)
)

# Silver Customer Table (external)
silver_customer = SuperDeltaTable(
    super_spark=super_spark,
    catalog_name="spark_catalog",
    schema_name="02_silver",
    table_name="customer",
    table_schema=T.StructType([
        T.StructField("customer_id", T.IntegerType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("signup_date", T.DateType(), True),
        T.StructField("superlake_dt", T.TimestampType(), True)
    ]),
    table_save_mode=TableSaveMode.MergeSCD,
    primary_keys=["customer_id"],
    partition_cols=["scd_is_current"],
    pruning_partition_cols=True,
    pruning_primary_keys=False,
    optimize_table=True,
    optimize_zorder_cols=["country"],
    optimize_target_file_size=100000000,
    compression_codec="snappy",
    schema_evolution_option=SchemaEvolution.Merge,
    logger=logger,
    scd_change_cols=["name", "email", "country"],
    managed=False  # External table (custom path)
)

PIPELINE_RUN_NUMBER = 1

def get_source_customer_data_for_pipeline_run(spark, pipeline_run_number):
    """Mockup function to return source customer data for a pipeline run."""

    # original data, 3 rows and no phone number
    if pipeline_run_number == 1:
        customer_source_schema = T.StructType([
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("name", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("signup_date", T.DateType(), True)
        ])
        customer_source_data = [
            ("1", "John Doe", "john.doe@example.com", "US", date(2022, 1, 15)),
            ("2", "Jane Smith", "jane.smith@example.com", "FR", date(2022, 2, 20)),
            ("3", "Pedro Alvarez", "pedro.alvarez@example.com", "EN", date(2022, 3, 10)),
        ]
        customer_source_df = spark.createDataFrame(customer_source_data, schema=customer_source_schema)
        return customer_source_df
    
    # changed data, 5 rows with phone number
    # customer_id 1 deleted
    # customer_id 2 changed name and country
    # customer_id 3 changed country
    # customer_id 4 changed country
    # customer_id 5 no changes
    if pipeline_run_number == 2:
        customer_source_schema = T.StructType([
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("phone_number", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("signup_date", T.DateType(), True)
        ])
        customer_source_data = [
            ("2", "0923623624","Jane changed", "jane.smith@example.com", "CH", date(2022, 2, 20)),
            ("3", "0923623625","Pedro Alvarez", "pedro.alvarez@example.com", "CH", date(2022, 3, 10)),
            ("4", "0923623626","Anna Müller", "anna.mueller@example.com", "CH", date(2022, 4, 5)),
            ("5", "0923623627","Li Wei", "li.wei@example.com", "DE", date(2022, 5, 12))
        ]
        customer_source_df = spark.createDataFrame(customer_source_data, schema=customer_source_schema)

    # deleted data
    # customer_id 1 and 2 deleted
    if pipeline_run_number == 3:
        customer_source_schema = T.StructType([
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("phone_number", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("signup_date", T.DateType(), True)
        ])
        customer_source_data = [
            ("3", "0923623625","Pedro Alvarez", "pedro.alvarez@example.com", "CH", date(2022, 3, 10)),
            ("4", "0923623626","Anna Müller", "anna.mueller@example.com", "CH", date(2022, 4, 5)),
            ("5", "0923623627","Li Wei", "li.wei@example.com", "DE", date(2022, 5, 12))
        ]
        customer_source_df = spark.createDataFrame(customer_source_data, schema=customer_source_schema)
        
    return customer_source_df

# Change Data Capture Function
def customer_cdc(spark):
    if silver_customer.table_exists(spark):
        max_customer_id = silver_customer.read(spark).select(F.max("customer_id")).collect()[0][0]
        # testing fixtures
        if PIPELINE_RUN_NUMBER == 2:
            max_customer_id = 0
        if PIPELINE_RUN_NUMBER == 3:
            max_customer_id = 4
    else:
        max_customer_id = 0 
    customer_source_df = get_source_customer_data_for_pipeline_run(spark, PIPELINE_RUN_NUMBER)
    customer_source_df = customer_source_df.filter(F.col("customer_id") > max_customer_id)
    logger.info(f"CDC max customer id: {max_customer_id}")
    return customer_source_df

# Transformation Function
def customer_tra(df: DataFrame):
    """Clean and transform customer data."""
    df = (
        df
        .withColumn("email", F.lower(F.col("email")))
        .withColumn("name", F.lower(F.col("name")))
        .withColumn("country", F.upper(F.col("country")))
    )
    return df

# Deletion Function
def customer_del(spark):
    source_df = get_source_customer_data_for_pipeline_run(spark, PIPELINE_RUN_NUMBER)
    return source_df

# ------------------------------------------------------------------------------------------------
#                                  Gold table and gold function
# ------------------------------------------------------------------------------------------------

# Gold Customer Agg Function
def gold_customer_agg_function(spark, superlake_dt):
    # aggregate customer count by country for current superlake_dt
    df = silver_customer.read(spark).filter(F.col("scd_is_current") == True)
    df = df.groupBy("country").agg(F.count("*").alias("customer_count"))
    df = df.withColumn("superlake_dt", F.lit(superlake_dt))
    return df

# Gold Customer Agg Table
gold_customer_agg = SuperDeltaTable(
    super_spark=super_spark,
    catalog_name="spark_catalog",
    schema_name="03_gold",
    table_name="customer_agg",
    table_schema=T.StructType([
        T.StructField("country", T.StringType(), True),
        T.StructField("customer_count", T.LongType(), True),
        T.StructField("superlake_dt", T.TimestampType(), True)
    ]),
    table_save_mode=TableSaveMode.Merge,
    primary_keys=["country"],
    partition_cols=[],
    pruning_partition_cols=True,
    pruning_primary_keys=False,
    optimize_table=True,
    optimize_zorder_cols=["country"],
    optimize_target_file_size=100000000,
    compression_codec="snappy",
    schema_evolution_option=SchemaEvolution.Merge,
    logger=logger,
    managed=True
)


# ------------------------------------------------------------------------------------------------
#                 Customer Data Pipeline from Source > Bronze > Silver > Gold
# ------------------------------------------------------------------------------------------------

print("################################################################################################")
super_tracer.generate_trace_table()

print("------------------------ drop tables -----------------------")
bronze_customer.drop(spark)
silver_customer.drop(spark)
gold_customer_agg.drop(spark)
print("------------------------ pipeline 1 ------------------------")

PIPELINE_RUN_NUMBER = 1

# set superlake_dt
superlake_dt = datetime.now()

# source > bronze > silver pipeline
customer_pipeline = SuperPipeline(
    logger=logger,
    super_spark=super_spark,
    super_tracer=super_tracer,
    superlake_dt=superlake_dt,
    pipeline_name="customer_pipeline_silver",
    bronze_table=bronze_customer,
    silver_table=silver_customer,
    cdc_function=customer_cdc,
    tra_function=customer_tra,
    del_function=customer_del,
    force_cdc=False,
    force_caching=True,
    environment="test"
)
customer_pipeline.execute()

# gold pipeline
gold_pipeline = SuperGoldPipeline(
    logger=logger,
    super_spark=super_spark,
    super_tracer=super_tracer,
    superlake_dt=superlake_dt,
    pipeline_name="customer_pipeline_gold",
    gold_function=gold_customer_agg_function,
    gold_table=gold_customer_agg,
    environment="test"
)
gold_pipeline.execute()

print("-------------------- waiting 5 seconds --------------------")
time.sleep(5)

print("------------------------ pipeline 2 ------------------------")

PIPELINE_RUN_NUMBER = 2

# set superlake_dt
superlake_dt = datetime.now()

# source > bronze > silver pipeline
customer_pipeline = SuperPipeline(
    logger=logger,
    super_spark=super_spark,
    super_tracer=super_tracer,
    superlake_dt=superlake_dt,
    pipeline_name="customer_pipeline_silver",
    bronze_table=bronze_customer,
    silver_table=silver_customer,
    cdc_function=customer_cdc,
    tra_function=customer_tra,
    del_function=customer_del,
    force_cdc=False,
    force_caching=True,
    environment="test"
)
customer_pipeline.execute()

# gold pipeline
gold_pipeline = SuperGoldPipeline(
    logger=logger,
    super_spark=super_spark,
    super_tracer=super_tracer,
    superlake_dt=superlake_dt,
    pipeline_name="customer_pipeline_gold",
    gold_function=gold_customer_agg_function,
    gold_table=gold_customer_agg,
    environment="test"
)
gold_pipeline.execute()

print("-------------------- waiting 5 seconds --------------------")
time.sleep(5)

print("------------------------ pipeline 3 ------------------------")

PIPELINE_RUN_NUMBER = 3

# same superlake_dt (simulating a rerun)
# source > bronze > silver pipeline
customer_pipeline = SuperPipeline(
    logger=logger,
    super_spark=super_spark,
    super_tracer=super_tracer,
    superlake_dt=superlake_dt,
    pipeline_name="customer_pipeline_silver",
    bronze_table=bronze_customer,
    silver_table=silver_customer,
    cdc_function=customer_cdc,
    tra_function=customer_tra,
    del_function=customer_del,
    force_cdc=True,
    environment="test",
    force_caching=True
)
customer_pipeline.execute()

# gold pipeline
gold_pipeline = SuperGoldPipeline(
    logger=logger,
    super_spark=super_spark,
    super_tracer=super_tracer,
    superlake_dt=superlake_dt,
    pipeline_name="customer_pipeline_gold",
    gold_function=gold_customer_agg_function,
    gold_table=gold_customer_agg,
    environment="test"
)
gold_pipeline.execute()

print("------------------------ optimize tables ------------------------")
bronze_customer.optimize(spark)
silver_customer.optimize(spark)
gold_customer_agg.optimize(spark)

print("------------------------ vacuum tables ------------------------")
bronze_customer.vacuum(spark)
silver_customer.vacuum(spark)
gold_customer_agg.vacuum(spark)
```

## Example Execution Log

```log
################################################################################################
------------------------ drop tables -----------------------                    
2025-05-17 11:00:33,813 - SuperLake - INFO - Dropped Delta Table spark_catalog.01_bronze.customer (managed) and removed files
2025-05-17 11:00:33,862 - SuperLake - INFO - Dropped Delta Table spark_catalog.02_silver.customer (external) and removed files
2025-05-17 11:00:33,906 - SuperLake - INFO - Dropped Delta Table spark_catalog.03_gold.customer_agg (managed) and removed files
------------------------ pipeline 1 ------------------------
2025-05-17 11:00:33,906 - SuperLake - INFO - Starting SuperPipeline customer_pipeline_silver execution.
2025-05-17 11:00:54,653 - SuperLake - INFO - Trace retrieved. bronze_u: False, silver_u: False, silver_d: False, skipped: False, del_function_defined: True, force_cdc: False
2025-05-17 11:00:54,742 - SuperLake - INFO - CDC max customer id: 0
2025-05-17 11:00:57,788 - SuperLake - INFO - Caching - CDC dataframe cached (3 rows).
2025-05-17 11:00:58,174 - SuperLake - INFO - New data from CDC.
Table save mode: append
+-----------+-------------+--------------------+-------+-----------+--------------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|
+-----------+-------------+--------------------+-------+-----------+--------------------+
|          1|     John Doe|john.doe@example.com|     US| 2022-01-15|2025-05-17 11:00:...|
|          2|   Jane Smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-17 11:00:...|
|          3|Pedro Alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-17 11:00:...|
+-----------+-------------+--------------------+-------+-----------+--------------------+

None
2025-05-17 11:00:58,905 - SuperLake - INFO - Table spark_catalog.01_bronze.customer does not exist, creating it
2025-05-17 11:00:59,619 - SuperLake - INFO - Created schema 01_bronze in catalog
2025-05-17 11:01:04,670 - SuperLake - INFO - Created Managed Delta table spark_catalog.01_bronze.customer
2025-05-17 11:01:04,848 - SuperLake - INFO - Registered managed Delta table spark_catalog.01_bronze.customer
2025-05-17 11:01:14,062 - SuperLake - INFO - Saved data to spark_catalog.01_bronze.customer (append)
2025-05-17 11:01:14,423 - SuperLake - INFO - Metric - spark_catalog.01_bronze.customer.save_row_count: 3
2025-05-17 11:01:14,423 - SuperLake - INFO - Metric - spark_catalog.01_bronze.customer.save_duration_sec: 15.52
Table save mode: merge_scd                                                      
+-----------+-------------+--------------------+-------+-----------+--------------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|
+-----------+-------------+--------------------+-------+-----------+--------------------+
|          3|pedro alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-17 11:00:...|
|          2|   jane smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-17 11:00:...|
|          1|     john doe|john.doe@example.com|     US| 2022-01-15|2025-05-17 11:00:...|
+-----------+-------------+--------------------+-------+-----------+--------------------+

None
2025-05-17 11:01:29,777 - SuperLake - INFO - Table spark_catalog.02_silver.customer does not exist, creating it
2025-05-17 11:01:29,778 - SuperLake - INFO - SCD columns ['scd_start_dt', 'scd_end_dt', 'scd_is_current'] are missing from table_schema but will be considered present for MergeSCD mode.
2025-05-17 11:01:30,189 - SuperLake - INFO - Created schema 02_silver in catalog
2025-05-17 11:01:31,471 - SuperLake - INFO - Created External Delta table spark_catalog.02_silver.customer with generated columns {}
2025-05-17 11:01:31,513 - SuperLake - INFO - Registered external Delta table spark_catalog.02_silver.customer
2025-05-17 11:01:54,129 - SuperLake - INFO - Saved data to spark_catalog.02_silver.customer (merge_scd)
2025-05-17 11:01:54,795 - SuperLake - INFO - Metric - spark_catalog.02_silver.customer.save_row_count: 3
2025-05-17 11:01:54,795 - SuperLake - INFO - Metric - spark_catalog.02_silver.customer.save_duration_sec: 25.04
2025-05-17 11:01:58,866 - SuperLake - INFO - Starting deletion of rows no longer present at the source.
2025-05-17 11:02:02,058 - SuperLake - INFO - Caching - Deletions dataframe cached (0 rows).
2025-05-17 11:02:02,345 - SuperLake - INFO - Skipped deletion for spark_catalog.02_silver.customer.
2025-05-17 11:02:02,345 - SuperLake - INFO - Metric - spark_catalog.02_silver.customer.delete_rows_deleted: 0
2025-05-17 11:02:02,356 - SuperLake - INFO - Caching - Deletions dataframe unpersisted.
2025-05-17 11:02:09,842 - SuperLake - INFO - Metric - total_duration_sec: 95.94 
2025-05-17 11:02:09,842 - SuperLake - INFO - SuperPipeline customer_pipeline_silver completed. Total duration: 95.94s

spark_catalog.01_bronze.customer:
+-----------+-------------+--------------------+-------+-----------+--------------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|
+-----------+-------------+--------------------+-------+-----------+--------------------+
|          3|Pedro Alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-17 11:00:...|
|          2|   Jane Smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-17 11:00:...|
|          1|     John Doe|john.doe@example.com|     US| 2022-01-15|2025-05-17 11:00:...|
+-----------+-------------+--------------------+-------+-----------+--------------------+


spark_catalog.02_silver.customer:
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+----------+--------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|        scd_start_dt|scd_end_dt|scd_is_current|
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+----------+--------------+
|          1|     john doe|john.doe@example.com|     US| 2022-01-15|2025-05-17 11:00:...|2025-05-17 11:00:...|      NULL|          true|
|          3|pedro alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-17 11:00:...|2025-05-17 11:00:...|      NULL|          true|
|          2|   jane smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-17 11:00:...|2025-05-17 11:00:...|      NULL|          true|
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+----------+--------------+

2025-05-17 11:02:11,626 - SuperLake - INFO - Caching - CDC dataframe unpersisted.
2025-05-17 11:02:11,627 - SuperLake - INFO - Starting SuperGoldPipeline customer_pipeline_gold execution.
2025-05-17 11:02:11,840 - SuperLake - INFO - Table spark_catalog.03_gold.customer_agg does not exist, creating it
2025-05-17 11:02:12,424 - SuperLake - INFO - Created schema 03_gold in catalog
2025-05-17 11:02:15,044 - SuperLake - INFO - Created Managed Delta table spark_catalog.03_gold.customer_agg
2025-05-17 11:02:15,138 - SuperLake - INFO - Registered managed Delta table spark_catalog.03_gold.customer_agg
2025-05-17 11:02:24,588 - SuperLake - INFO - Saved data to spark_catalog.03_gold.customer_agg (overwrite)
2025-05-17 11:02:25,915 - SuperLake - INFO - Metric - spark_catalog.03_gold.customer_agg.save_row_count: 3
2025-05-17 11:02:25,915 - SuperLake - INFO - Metric - spark_catalog.03_gold.customer_agg.save_duration_sec: 14.08
2025-05-17 11:02:30,667 - SuperLake - INFO - SuperGoldPipeline customer_pipeline_gold completed. Total duration: 19.04s

Gold table spark_catalog.03_gold.customer_agg:
+-------+--------------+--------------------+                                   
|country|customer_count|        superlake_dt|
+-------+--------------+--------------------+
|     EN|             1|2025-05-17 11:00:...|
|     US|             1|2025-05-17 11:00:...|
|     FR|             1|2025-05-17 11:00:...|
+-------+--------------+--------------------+

-------------------- waiting 5 seconds --------------------
------------------------ pipeline 2 ------------------------
2025-05-17 11:02:38,104 - SuperLake - INFO - Starting SuperPipeline customer_pipeline_silver execution.
2025-05-17 11:02:40,762 - SuperLake - INFO - Trace retrieved. bronze_u: False, silver_u: False, silver_d: False, skipped: False, del_function_defined: True, force_cdc: False
2025-05-17 11:02:43,768 - SuperLake - INFO - CDC max customer id: 0
2025-05-17 11:02:44,511 - SuperLake - INFO - Caching - CDC dataframe cached (4 rows).
2025-05-17 11:02:44,972 - SuperLake - INFO - New data from CDC.
Table save mode: append
+-----------+------------+-------------+--------------------+-------+-----------+--------------------+
|customer_id|phone_number|         name|               email|country|signup_date|        superlake_dt|
+-----------+------------+-------------+--------------------+-------+-----------+--------------------+
|          2|  0923623624| Jane changed|jane.smith@exampl...|     CH| 2022-02-20|2025-05-17 11:02:...|
|          3|  0923623625|Pedro Alvarez|pedro.alvarez@exa...|     CH| 2022-03-10|2025-05-17 11:02:...|
|          4|  0923623626|  Anna Müller|anna.mueller@exam...|     CH| 2022-04-05|2025-05-17 11:02:...|
|          5|  0923623627|       Li Wei|  li.wei@example.com|     DE| 2022-05-12|2025-05-17 11:02:...|
+-----------+------------+-------------+--------------------+-------+-----------+--------------------+

None
2025-05-17 11:02:45,325 - SuperLake - INFO - Retaining extra columns (schema_evolution_option=Merge): ['phone_number']
2025-05-17 11:02:45,335 - SuperLake - INFO - Retaining extra columns (schema_evolution_option=Merge): ['phone_number']
2025-05-17 11:02:46,689 - SuperLake - INFO - Saved data to spark_catalog.01_bronze.customer (append)
2025-05-17 11:02:46,905 - SuperLake - INFO - Metric - spark_catalog.01_bronze.customer.save_row_count: 4
2025-05-17 11:02:46,905 - SuperLake - INFO - Metric - spark_catalog.01_bronze.customer.save_duration_sec: 1.64
Table save mode: merge_scd                                                      
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|phone_number|
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+
|          3|pedro alvarez|pedro.alvarez@exa...|     CH| 2022-03-10|2025-05-17 11:02:...|  0923623625|
|          4|  anna müller|anna.mueller@exam...|     CH| 2022-04-05|2025-05-17 11:02:...|  0923623626|
|          2| jane changed|jane.smith@exampl...|     CH| 2022-02-20|2025-05-17 11:02:...|  0923623624|
|          5|       li wei|  li.wei@example.com|     DE| 2022-05-12|2025-05-17 11:02:...|  0923623627|
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+

None
2025-05-17 11:02:55,919 - SuperLake - INFO - Retaining extra columns (schema_evolution_option=Merge): ['phone_number']
2025-05-17 11:03:20,413 - SuperLake - INFO - Saved data to spark_catalog.02_silver.customer (merge_scd)
2025-05-17 11:03:21,233 - SuperLake - INFO - Metric - spark_catalog.02_silver.customer.save_row_count: 4
2025-05-17 11:03:21,234 - SuperLake - INFO - Metric - spark_catalog.02_silver.customer.save_duration_sec: 25.42
2025-05-17 11:03:26,152 - SuperLake - INFO - Starting deletion of rows no longer present at the source.
2025-05-17 11:03:29,807 - SuperLake - INFO - Caching - Deletions dataframe cached (1 rows).
2025-05-17 11:03:31,753 - SuperLake - INFO - 1 SCD rows expected to be closed in spark_catalog.02_silver.customer.
2025-05-17 11:03:42,342 - SuperLake - INFO - 1 rows deleted from spark_catalog.02_silver.customer.
2025-05-17 11:03:42,342 - SuperLake - INFO - Metric - spark_catalog.02_silver.customer.delete_rows_deleted: 1
2025-05-17 11:03:42,345 - SuperLake - INFO - Caching - Deletions dataframe unpersisted.
2025-05-17 11:03:45,867 - SuperLake - INFO - Metric - total_duration_sec: 67.76 
2025-05-17 11:03:45,867 - SuperLake - INFO - SuperPipeline customer_pipeline_silver completed. Total duration: 67.76s

spark_catalog.01_bronze.customer:
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|phone_number|
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+
|          3|Pedro Alvarez|pedro.alvarez@exa...|     CH| 2022-03-10|2025-05-17 11:02:...|  0923623625|
|          4|  Anna Müller|anna.mueller@exam...|     CH| 2022-04-05|2025-05-17 11:02:...|  0923623626|
|          2| Jane changed|jane.smith@exampl...|     CH| 2022-02-20|2025-05-17 11:02:...|  0923623624|
|          5|       Li Wei|  li.wei@example.com|     DE| 2022-05-12|2025-05-17 11:02:...|  0923623627|
|          3|Pedro Alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-17 11:00:...|        NULL|
|          2|   Jane Smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-17 11:00:...|        NULL|
|          1|     John Doe|john.doe@example.com|     US| 2022-01-15|2025-05-17 11:00:...|        NULL|
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+


spark_catalog.02_silver.customer:
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+--------------------+--------------+------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|        scd_start_dt|          scd_end_dt|scd_is_current|phone_number|
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+--------------------+--------------+------------+
|          3|pedro alvarez|pedro.alvarez@exa...|     CH| 2022-03-10|2025-05-17 11:02:...|2025-05-17 11:02:...|                NULL|          true|  0923623625|
|          4|  anna müller|anna.mueller@exam...|     CH| 2022-04-05|2025-05-17 11:02:...|2025-05-17 11:02:...|                NULL|          true|  0923623626|
|          2| jane changed|jane.smith@exampl...|     CH| 2022-02-20|2025-05-17 11:02:...|2025-05-17 11:02:...|                NULL|          true|  0923623624|
|          5|       li wei|  li.wei@example.com|     DE| 2022-05-12|2025-05-17 11:02:...|2025-05-17 11:02:...|                NULL|          true|  0923623627|
|          3|pedro alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-17 11:00:...|2025-05-17 11:00:...|2025-05-17 11:02:...|         false|        NULL|
|          2|   jane smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-17 11:00:...|2025-05-17 11:00:...|2025-05-17 11:02:...|         false|        NULL|
|          1|     john doe|john.doe@example.com|     US| 2022-01-15|2025-05-17 11:00:...|2025-05-17 11:00:...|2025-05-17 11:02:...|         false|        NULL|
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+--------------------+--------------+------------+

2025-05-17 11:03:48,603 - SuperLake - INFO - Caching - CDC dataframe unpersisted.
2025-05-17 11:03:48,604 - SuperLake - INFO - Starting SuperGoldPipeline customer_pipeline_gold execution.
2025-05-17 11:03:53,548 - SuperLake - INFO - Saved data to spark_catalog.03_gold.customer_agg (overwrite)
2025-05-17 11:03:54,969 - SuperLake - INFO - Metric - spark_catalog.03_gold.customer_agg.save_row_count: 2
2025-05-17 11:03:54,969 - SuperLake - INFO - Metric - spark_catalog.03_gold.customer_agg.save_duration_sec: 6.22
2025-05-17 11:03:58,680 - SuperLake - INFO - SuperGoldPipeline customer_pipeline_gold completed. Total duration: 10.08s

Gold table spark_catalog.03_gold.customer_agg:
+-------+--------------+--------------------+                                   
|country|customer_count|        superlake_dt|
+-------+--------------+--------------------+
|     DE|             1|2025-05-17 11:02:...|
|     CH|             3|2025-05-17 11:02:...|
+-------+--------------+--------------------+

-------------------- waiting 5 seconds --------------------
------------------------ pipeline 3 ------------------------
2025-05-17 11:04:07,064 - SuperLake - INFO - Starting SuperPipeline customer_pipeline_silver execution.
2025-05-17 11:04:10,282 - SuperLake - INFO - Trace retrieved. bronze_u: True, silver_u: True, silver_d: True, skipped: False, del_function_defined: True, force_cdc: True
2025-05-17 11:04:11,478 - SuperLake - INFO - CDC max customer id: 4
2025-05-17 11:04:11,806 - SuperLake - INFO - Caching - CDC dataframe cached (1 rows).
2025-05-17 11:04:11,900 - SuperLake - INFO - force_cdc: New data from CDC.
Table save mode: merge
+-----------+------------+------+------------------+-------+-----------+--------------------+
|customer_id|phone_number|  name|             email|country|signup_date|        superlake_dt|
+-----------+------------+------+------------------+-------+-----------+--------------------+
|          5|  0923623627|Li Wei|li.wei@example.com|     DE| 2022-05-12|2025-05-17 11:02:...|
+-----------+------------+------+------------------+-------+-----------+--------------------+

None
2025-05-17 11:04:19,528 - SuperLake - INFO - Saved data to spark_catalog.01_bronze.customer (merge)
2025-05-17 11:04:19,999 - SuperLake - INFO - Metric - spark_catalog.01_bronze.customer.save_row_count: 1
2025-05-17 11:04:19,999 - SuperLake - INFO - Metric - spark_catalog.01_bronze.customer.save_duration_sec: 8.01
Table save mode: merge_scd                                                      
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|phone_number|
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+
|          3|pedro alvarez|pedro.alvarez@exa...|     CH| 2022-03-10|2025-05-17 11:02:...|  0923623625|
|          4|  anna müller|anna.mueller@exam...|     CH| 2022-04-05|2025-05-17 11:02:...|  0923623626|
|          2| jane changed|jane.smith@exampl...|     CH| 2022-02-20|2025-05-17 11:02:...|  0923623624|
|          5|       li wei|  li.wei@example.com|     DE| 2022-05-12|2025-05-17 11:02:...|  0923623627|
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+

None
2025-05-17 11:04:31,969 - SuperLake - INFO - Saved data to spark_catalog.02_silver.customer (merge_scd)
2025-05-17 11:04:32,436 - SuperLake - INFO - Metric - spark_catalog.02_silver.customer.save_row_count: 4
2025-05-17 11:04:32,436 - SuperLake - INFO - Metric - spark_catalog.02_silver.customer.save_duration_sec: 5.59
2025-05-17 11:04:39,798 - SuperLake - INFO - Starting deletion of rows no longer present at the source.
2025-05-17 11:04:45,292 - SuperLake - INFO - Caching - Deletions dataframe cached (3 rows).
2025-05-17 11:04:47,842 - SuperLake - INFO - 3 SCD rows expected to be closed in spark_catalog.02_silver.customer.
2025-05-17 11:04:57,150 - SuperLake - INFO - 1 rows deleted from spark_catalog.02_silver.customer.
2025-05-17 11:04:57,151 - SuperLake - INFO - Metric - spark_catalog.02_silver.customer.delete_rows_deleted: 1
2025-05-17 11:04:57,155 - SuperLake - INFO - Caching - Deletions dataframe unpersisted.
2025-05-17 11:05:03,138 - SuperLake - INFO - Metric - total_duration_sec: 56.07 
2025-05-17 11:05:03,139 - SuperLake - INFO - SuperPipeline customer_pipeline_silver completed. Total duration: 56.07s

spark_catalog.01_bronze.customer:
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|phone_number|
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+
|          3|Pedro Alvarez|pedro.alvarez@exa...|     CH| 2022-03-10|2025-05-17 11:02:...|  0923623625|
|          4|  Anna Müller|anna.mueller@exam...|     CH| 2022-04-05|2025-05-17 11:02:...|  0923623626|
|          2| Jane changed|jane.smith@exampl...|     CH| 2022-02-20|2025-05-17 11:02:...|  0923623624|
|          5|       Li Wei|  li.wei@example.com|     DE| 2022-05-12|2025-05-17 11:02:...|  0923623627|
|          3|Pedro Alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-17 11:00:...|        NULL|
|          2|   Jane Smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-17 11:00:...|        NULL|
|          1|     John Doe|john.doe@example.com|     US| 2022-01-15|2025-05-17 11:00:...|        NULL|
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+


spark_catalog.02_silver.customer:
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+--------------------+--------------+------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|        scd_start_dt|          scd_end_dt|scd_is_current|phone_number|
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+--------------------+--------------+------------+
|          2| jane changed|jane.smith@exampl...|     CH| 2022-02-20|2025-05-17 11:02:...|2025-05-17 11:02:...|2025-05-17 11:02:...|         false|  0923623624|
|          3|pedro alvarez|pedro.alvarez@exa...|     CH| 2022-03-10|2025-05-17 11:02:...|2025-05-17 11:02:...|                NULL|          true|  0923623625|
|          4|  anna müller|anna.mueller@exam...|     CH| 2022-04-05|2025-05-17 11:02:...|2025-05-17 11:02:...|                NULL|          true|  0923623626|
|          5|       li wei|  li.wei@example.com|     DE| 2022-05-12|2025-05-17 11:02:...|2025-05-17 11:02:...|                NULL|          true|  0923623627|
|          3|pedro alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-17 11:00:...|2025-05-17 11:00:...|2025-05-17 11:02:...|         false|        NULL|
|          2|   jane smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-17 11:00:...|2025-05-17 11:00:...|2025-05-17 11:02:...|         false|        NULL|
|          1|     john doe|john.doe@example.com|     US| 2022-01-15|2025-05-17 11:00:...|2025-05-17 11:00:...|2025-05-17 11:02:...|         false|        NULL|
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+--------------------+--------------+------------+

2025-05-17 11:05:05,377 - SuperLake - INFO - Caching - CDC dataframe unpersisted.
2025-05-17 11:05:05,377 - SuperLake - INFO - Starting SuperGoldPipeline customer_pipeline_gold execution.
2025-05-17 11:05:08,962 - SuperLake - INFO - Saved data to spark_catalog.03_gold.customer_agg (overwrite)
2025-05-17 11:05:10,059 - SuperLake - INFO - Metric - spark_catalog.03_gold.customer_agg.save_row_count: 2
2025-05-17 11:05:10,059 - SuperLake - INFO - Metric - spark_catalog.03_gold.customer_agg.save_duration_sec: 4.62
2025-05-17 11:05:13,598 - SuperLake - INFO - SuperGoldPipeline customer_pipeline_gold completed. Total duration: 8.22s

Gold table spark_catalog.03_gold.customer_agg:
+-------+--------------+--------------------+                                   
|country|customer_count|        superlake_dt|
+-------+--------------+--------------------+
|     DE|             1|2025-05-17 11:02:...|
|     CH|             2|2025-05-17 11:02:...|
+-------+--------------+--------------------+

------------------------ optimize tables ------------------------
2025-05-17 11:05:16,131 - SuperLake - INFO - Starting optimize for table spark_catalog.01_bronze.customer.
2025-05-17 11:05:16,134 - SuperLake - INFO - optimize_table is False, skipping optimize
2025-05-17 11:05:16,134 - SuperLake - INFO - Starting optimize for table spark_catalog.02_silver.customer.
2025-05-17 11:05:23,669 - SuperLake - INFO - Optimized table spark_catalog.02_silver.customer (external)
2025-05-17 11:05:23,670 - SuperLake - INFO - Metric - optimize_table_creation_duration_sec: 0.12
2025-05-17 11:05:23,670 - SuperLake - INFO - Metric - optimize_table_optimization_duration_sec: 7.4
2025-05-17 11:05:23,670 - SuperLake - INFO - Metric - optimize_table_total_duration_sec: 7.52
2025-05-17 11:05:23,670 - SuperLake - INFO - Starting optimize for table spark_catalog.03_gold.customer_agg.
2025-05-17 11:05:28,610 - SuperLake - INFO - Optimized table spark_catalog.03_gold.customer_agg (managed)
2025-05-17 11:05:28,611 - SuperLake - INFO - Metric - optimize_table_creation_duration_sec: 0.14
2025-05-17 11:05:28,611 - SuperLake - INFO - Metric - optimize_table_optimization_duration_sec: 4.8
2025-05-17 11:05:28,611 - SuperLake - INFO - Metric - optimize_table_total_duration_sec: 4.94
------------------------ vacuum tables ------------------------
2025-05-17 11:05:28,744 - SuperLake - INFO - Registered managed Delta table spark_catalog.01_bronze.customer
Deleted 0 files and directories in a total of 3 directories.                    
2025-05-17 11:06:10,109 - SuperLake - INFO - Vacuumed table spark_catalog.01_bronze.customer with retention 168 hours
2025-05-17 11:06:10,110 - SuperLake - INFO - Metric - vacuum_table_creation_duration_sec: 0.13
2025-05-17 11:06:10,110 - SuperLake - INFO - Metric - vacuum_table_vacuum_duration_sec: 41.36
2025-05-17 11:06:10,110 - SuperLake - INFO - Metric - vacuum_table_total_duration_sec: 41.5
2025-05-17 11:06:10,155 - SuperLake - INFO - Registered external Delta table spark_catalog.02_silver.customer
Deleted 0 files and directories in a total of 3 directories.                    
2025-05-17 11:06:53,848 - SuperLake - INFO - Vacuumed table spark_catalog.02_silver.customer with retention 168 hours
2025-05-17 11:06:53,849 - SuperLake - INFO - Metric - vacuum_table_creation_duration_sec: 0.05
2025-05-17 11:06:53,849 - SuperLake - INFO - Metric - vacuum_table_vacuum_duration_sec: 43.69
2025-05-17 11:06:53,849 - SuperLake - INFO - Metric - vacuum_table_total_duration_sec: 43.74
2025-05-17 11:06:53,921 - SuperLake - INFO - Registered managed Delta table spark_catalog.03_gold.customer_agg
Deleted 0 files and directories in a total of 1 directories.                    
2025-05-17 11:07:28,568 - SuperLake - INFO - Vacuumed table spark_catalog.03_gold.customer_agg with retention 168 hours
2025-05-17 11:07:28,568 - SuperLake - INFO - Metric - vacuum_table_creation_duration_sec: 0.07
2025-05-17 11:07:28,568 - SuperLake - INFO - Metric - vacuum_table_vacuum_duration_sec: 34.65
2025-05-17 11:07:28,568 - SuperLake - INFO - Metric - vacuum_table_total_duration_sec: 34.72
```
