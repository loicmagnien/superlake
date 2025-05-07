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
from superlake.core import SuperSpark, SuperDeltaTable, TableSaveMode, SchemaEvolution, SuperPipeline, SuperGoldPipeline
from superlake.monitoring import SuperLogger

# Standard Library
import pyspark.sql.types as T
import pyspark.sql.functions as F
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
import sys
import time

# Initialize Spark
spark = SuperSpark(warehouse_dir="./data/spark-warehouse")
logger = SuperLogger()
superlake_dt = datetime.now()

# ------------------------------------------------------------------------------------------------
#                     Bronze and silver tables, cdc and transformation functions   
# ------------------------------------------------------------------------------------------------

# Bronze Customer Table
bronze_customer = SuperDeltaTable(
    table_name="01_bronze.customer",
    table_path="./data/external-table/01_bronze/customer",
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
    optimize_table=True,
    optimize_zorder_cols=[],
    optimize_target_file_size=100000000,
    compression_codec="snappy",
    schema_evolution_option=SchemaEvolution.Merge,
    logger=logger,
    managed=True  # Managed table (in spark-warehouse)
)

# Silver Customer Table
silver_customer = SuperDeltaTable(
    table_name="02_silver.customer",
    table_path="./data/external-table/02_silver/customer",
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

# Change Data Capture Function
def customer_cdc(spark):

    # ---------------------------------------------------------------------------------------
    # mockup customer source data and schema (should be a select from a table)
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
        ("4", "Anna Müller", "anna.mueller@example.com", "DE", date(2022, 4, 5)),
        ("5", "Li Wei", "li.wei@example.com", "DE", date(2022, 5, 12))
    ]
    customer_source_df = spark.createDataFrame(customer_source_data, schema=customer_source_schema)
    # ---------------------------------------------------------------------------------------

    # change data capture mechanism
    if silver_customer.table_exists(spark):
        max_customer_id = silver_customer.read(spark).select(F.max("customer_id")).collect()[0][0]
        max_customer_id = max_customer_id - 2
        # simulate a change in the source schema
        customer_source_schema = T.StructType([
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("phone_number", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("signup_date", T.DateType(), True)
        ])
        customer_source_data = [
            ("1", "0923623623","John Doe", "john.doe@changed.com", "CH", date(2022, 1, 15)),
            ("2", "0923623624","Jane changed", "jane.smith@example.com", "CH", date(2022, 2, 20)),
            ("3", "0923623625","Pedro Alvarez", "pedro.alvarez@example.com", "CH", date(2022, 3, 10)),
            ("4", "0923623626","Anna Müller", "anna.mueller@example.com", "CH", date(2022, 4, 5)),
            ("5", "0923623627","Li Wei", "li.wei@example.com", "DE", date(2022, 5, 12))
        ]
        customer_source_df = spark.createDataFrame(customer_source_data, schema=customer_source_schema)
    else:
        customer_source_df = customer_source_df.filter(F.col("customer_id") <= 3) # mockup cdc
        max_customer_id = 0 
    logger.info(f"CDC max customer id: {max_customer_id}")

    # filter out rows based on change data capture mechanism
    customer_source_df = customer_source_df.filter(F.col("customer_id") > max_customer_id)
    return customer_source_df


# Transformation Function
def customer_tra(df: SparkDataFrame):
    """Clean and transform customer data."""
    df = (
        df
        .withColumn("email", F.lower(F.col("email")))
        .withColumn("name", F.lower(F.col("name")))
        .withColumn("country", F.upper(F.col("country")))
    )
    return df


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
    table_name="03_gold.customer_agg",
    table_path="./data/external-table/03_gold/customer_agg",
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
    managed=False
)


# ------------------------------------------------------------------------------------------------
#                 Customer Data Pipeline from Source > Bronze > Silver > Gold
# ------------------------------------------------------------------------------------------------


print("################################################################################################")

print("------------------------ drop tables -----------------------")
bronze_customer.drop(spark)
silver_customer.drop(spark)
gold_customer_agg.drop(spark)
print("------------------------ pipeline 1 ------------------------")

# set superlake_dt
superlake_dt = datetime.now()

# source > bronze > silver pipeline
customer_pipeline = SuperPipeline(
    superlake_dt = superlake_dt,
    bronze_table = bronze_customer,
    silver_table = silver_customer,
    cdc_function = customer_cdc,
    tra_function = customer_tra,
    logger = logger,
    spark = spark,
    environment = "test"
)
customer_pipeline.execute()

# gold pipeline
gold_pipeline = SuperGoldPipeline(
    gold_function = gold_customer_agg_function,
    gold_table = gold_customer_agg,
    logger = logger,
    spark = spark,
    superlake_dt = superlake_dt,
    environment = "test"
)
gold_pipeline.execute()


print("-------------------- waiting 5 seconds --------------------")
time.sleep(5)

print("------------------------ pipeline 2 ------------------------")

# set superlake_dt
superlake_dt = datetime.now()

# source > bronze > silver pipeline
customer_pipeline = SuperPipeline(
    superlake_dt = superlake_dt,
    bronze_table = bronze_customer,
    silver_table = silver_customer,
    cdc_function = customer_cdc,
    tra_function = customer_tra,
    logger = logger,
    spark = spark,
    environment = "test"
)
customer_pipeline.execute()

# gold pipeline
gold_pipeline = SuperGoldPipeline(
    gold_function = gold_customer_agg_function,
    gold_table = gold_customer_agg,
    logger = logger,
    spark = spark,
    superlake_dt = superlake_dt,
    environment = "test"
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
2025-05-07 11:28:22,291 - SuperLake - INFO - Dropped Managed Delta Table 01_bronze.customer and removed data at /Users/loicmagnien/Documents/GitHub/superlake/data/spark-warehouse/01_bronze.db/customer
2025-05-07 11:28:22,351 - SuperLake - INFO - Dropped External Delta Table 02_silver.customer and removed data at ./data/external-table/02_silver/customer
2025-05-07 11:28:22,431 - SuperLake - INFO - Dropped External Delta Table 03_gold.customer_agg and removed data at ./data/external-table/03_gold/customer_agg
------------------------ pipeline 1 ------------------------
2025-05-07 11:28:22,431 - SuperLake - INFO - Starting SuperPipeline execution.
2025-05-07 11:28:23,626 - SuperLake - INFO - CDC max customer id: 0
2025-05-07 11:28:30,471 - SuperLake - INFO - CDC function completed. Rows: 3. Duration: 8.04s
2025-05-07 11:28:31,317 - SuperLake - INFO - Table 01_bronze.customer does not exist, creating it
2025-05-07 11:28:34,806 - SuperLake - INFO - Created database 01_bronze in catalog
2025-05-07 11:28:48,616 - SuperLake - INFO - Created Managed Delta table 01_bronze.customer
2025-05-07 11:28:48,959 - SuperLake - INFO - Registered managed Delta table 01_bronze.customer
2025-05-07 11:29:03,158 - SuperLake - INFO - Saved DataFrame to 01_bronze.customer in mode append
2025-05-07 11:29:03,158 - SuperLake - INFO - Saved to bronze. Duration: 32.69s
2025-05-07 11:29:09,924 - SuperLake - INFO - Filtered bronze for superlake_dt. Rows: 3. Duration: 6.77s
2025-05-07 11:29:11,525 - SuperLake - INFO - Transformation completed. Rows: 3. Duration: 1.60s
2025-05-07 11:29:11,648 - SuperLake - INFO - Table 02_silver.customer does not exist, creating it
2025-05-07 11:29:11,648 - SuperLake - INFO - SCD columns ['scd_start_dt', 'scd_end_dt', 'scd_is_current'] are missing from table_schema but will be considered present for MergeSCD mode.
2025-05-07 11:29:12,180 - SuperLake - INFO - Created database 02_silver in catalog
2025-05-07 11:29:15,543 - SuperLake - INFO - Created External Delta table 02_silver.customer at /Users/loicmagnien/Documents/GitHub/superlake/data/external-table/02_silver/customer
2025-05-07 11:29:15,718 - SuperLake - INFO - Registered external Delta table 02_silver.customer
2025-05-07 11:29:31,859 - SuperLake - INFO - Saved DataFrame to 02_silver.customer in mode merge_scd
2025-05-07 11:29:31,859 - SuperLake - INFO - Saved to silver. Duration: 20.33s

Bronze table output:
+-----------+-------------+--------------------+-------+-----------+--------------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|
+-----------+-------------+--------------------+-------+-----------+--------------------+
|          3|Pedro Alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-07 11:28:...|
|          1|     John Doe|john.doe@example.com|     US| 2022-01-15|2025-05-07 11:28:...|
|          2|   Jane Smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-07 11:28:...|
+-----------+-------------+--------------------+-------+-----------+--------------------+


Silver table output:
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+----------+--------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|        scd_start_dt|scd_end_dt|scd_is_current|
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+----------+--------------+
|          1|     john doe|john.doe@example.com|     US| 2022-01-15|2025-05-07 11:28:...|2025-05-07 11:28:...|      NULL|          true|
|          2|   jane smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-07 11:28:...|2025-05-07 11:28:...|      NULL|          true|
|          3|pedro alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-07 11:28:...|2025-05-07 11:28:...|      NULL|          true|
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+----------+--------------+

2025-05-07 11:29:36,117 - SuperLake - INFO - SuperPipeline completed. Total duration: 69.43s
2025-05-07 11:29:36,117 - SuperLake - INFO - Rows ingested in bronze: 3, rows transformed into silver: 3
2025-05-07 11:29:36,117 - SuperLake - INFO - Metric - bronze_row_count: 3
2025-05-07 11:29:36,117 - SuperLake - INFO - Metric - silver_row_count: 3
2025-05-07 11:29:36,117 - SuperLake - INFO - Metric - total_duration_sec: 69.43
2025-05-07 11:29:36,117 - SuperLake - INFO - Metric - cdc_duration_sec: 8.04
2025-05-07 11:29:36,117 - SuperLake - INFO - Metric - bronze_save_duration_sec: 32.69
2025-05-07 11:29:36,117 - SuperLake - INFO - Metric - bronze_filter_duration_sec: 6.77
2025-05-07 11:29:36,117 - SuperLake - INFO - Metric - transformation_duration_sec: 1.6
2025-05-07 11:29:36,117 - SuperLake - INFO - Metric - silver_save_duration_sec: 20.33
2025-05-07 11:29:36,117 - SuperLake - INFO - Starting SuperGoldPipeline execution.
2025-05-07 11:29:38,891 - SuperLake - INFO - Gold function completed. Rows: 3   
2025-05-07 11:29:38,924 - SuperLake - INFO - Table 03_gold.customer_agg does not exist, creating it
2025-05-07 11:29:39,568 - SuperLake - INFO - Created database 03_gold in catalog
2025-05-07 11:29:42,202 - SuperLake - INFO - Created External Delta table 03_gold.customer_agg at /Users/loicmagnien/Documents/GitHub/superlake/data/external-table/03_gold/customer_agg
2025-05-07 11:29:42,298 - SuperLake - INFO - Registered external Delta table 03_gold.customer_agg
2025-05-07 11:29:50,291 - SuperLake - INFO - Saved DataFrame to 03_gold.customer_agg in mode overwrite
2025-05-07 11:29:50,292 - SuperLake - INFO - Saved to gold table.
2025-05-07 11:29:50,292 - SuperLake - INFO - SuperGoldPipeline completed.

Gold table output:
+-------+--------------+--------------------+                                   
|country|customer_count|        superlake_dt|
+-------+--------------+--------------------+
|     EN|             1|2025-05-07 11:28:...|
|     FR|             1|2025-05-07 11:28:...|
|     US|             1|2025-05-07 11:28:...|
+-------+--------------+--------------------+

-------------------- waiting 5 seconds --------------------
------------------------ pipeline 2 ------------------------
2025-05-07 11:29:58,406 - SuperLake - INFO - Starting SuperPipeline execution.
2025-05-07 11:30:00,779 - SuperLake - INFO - CDC max customer id: 1
2025-05-07 11:30:01,106 - SuperLake - INFO - CDC function completed. Rows: 4. Duration: 2.70s
2025-05-07 11:30:01,190 - SuperLake - INFO - Retaining extra columns (schema_evolution_option=Merge): ['phone_number']
2025-05-07 11:30:03,550 - SuperLake - INFO - Saved DataFrame to 01_bronze.customer in mode append
2025-05-07 11:30:03,550 - SuperLake - INFO - Saved to bronze. Duration: 2.44s
2025-05-07 11:30:07,849 - SuperLake - INFO - Filtered bronze for superlake_dt. Rows: 4. Duration: 4.30s
2025-05-07 11:30:08,988 - SuperLake - INFO - Transformation completed. Rows: 4. Duration: 1.14s
2025-05-07 11:30:09,096 - SuperLake - INFO - Retaining extra columns (schema_evolution_option=Merge): ['phone_number']
2025-05-07 11:30:28,948 - SuperLake - INFO - Saved DataFrame to 02_silver.customer in mode merge_scd
2025-05-07 11:30:28,948 - SuperLake - INFO - Saved to silver. Duration: 19.96s

Bronze table output:
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|phone_number|
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+
|          3|Pedro Alvarez|pedro.alvarez@exa...|     CH| 2022-03-10|2025-05-07 11:29:...|  0923623625|
|          4|  Anna Müller|anna.mueller@exam...|     CH| 2022-04-05|2025-05-07 11:29:...|  0923623626|
|          2| Jane changed|jane.smith@exampl...|     CH| 2022-02-20|2025-05-07 11:29:...|  0923623624|
|          5|       Li Wei|  li.wei@example.com|     DE| 2022-05-12|2025-05-07 11:29:...|  0923623627|
|          2|   Jane Smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-07 11:28:...|        NULL|
|          3|Pedro Alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-07 11:28:...|        NULL|
|          1|     John Doe|john.doe@example.com|     US| 2022-01-15|2025-05-07 11:28:...|        NULL|
+-----------+-------------+--------------------+-------+-----------+--------------------+------------+


Silver table output:
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+--------------------+--------------+------------+
|customer_id|         name|               email|country|signup_date|        superlake_dt|        scd_start_dt|          scd_end_dt|scd_is_current|phone_number|
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+--------------------+--------------+------------+
|          1|     john doe|john.doe@example.com|     US| 2022-01-15|2025-05-07 11:28:...|2025-05-07 11:28:...|                NULL|          true|        NULL|
|          2| jane changed|jane.smith@exampl...|     CH| 2022-02-20|2025-05-07 11:29:...|2025-05-07 11:29:...|                NULL|          true|  0923623624|
|          2|   jane smith|jane.smith@exampl...|     FR| 2022-02-20|2025-05-07 11:28:...|2025-05-07 11:28:...|2025-05-07 11:29:...|         false|        NULL|
|          3|pedro alvarez|pedro.alvarez@exa...|     CH| 2022-03-10|2025-05-07 11:29:...|2025-05-07 11:29:...|                NULL|          true|  0923623625|
|          3|pedro alvarez|pedro.alvarez@exa...|     EN| 2022-03-10|2025-05-07 11:28:...|2025-05-07 11:28:...|2025-05-07 11:29:...|         false|        NULL|
|          4|  anna müller|anna.mueller@exam...|     CH| 2022-04-05|2025-05-07 11:29:...|2025-05-07 11:29:...|                NULL|          true|  0923623626|
|          5|       li wei|  li.wei@example.com|     DE| 2022-05-12|2025-05-07 11:29:...|2025-05-07 11:29:...|                NULL|          true|  0923623627|
+-----------+-------------+--------------------+-------+-----------+--------------------+--------------------+--------------------+--------------+------------+

2025-05-07 11:30:32,900 - SuperLake - INFO - SuperPipeline completed. Total duration: 30.54s
2025-05-07 11:30:32,900 - SuperLake - INFO - Rows ingested in bronze: 4, rows transformed into silver: 4
2025-05-07 11:30:32,900 - SuperLake - INFO - Metric - bronze_row_count: 4
2025-05-07 11:30:32,900 - SuperLake - INFO - Metric - silver_row_count: 4
2025-05-07 11:30:32,900 - SuperLake - INFO - Metric - total_duration_sec: 30.54
2025-05-07 11:30:32,900 - SuperLake - INFO - Metric - cdc_duration_sec: 2.7
2025-05-07 11:30:32,900 - SuperLake - INFO - Metric - bronze_save_duration_sec: 2.44
2025-05-07 11:30:32,900 - SuperLake - INFO - Metric - bronze_filter_duration_sec: 4.3
2025-05-07 11:30:32,900 - SuperLake - INFO - Metric - transformation_duration_sec: 1.14
2025-05-07 11:30:32,900 - SuperLake - INFO - Metric - silver_save_duration_sec: 19.96
2025-05-07 11:30:32,900 - SuperLake - INFO - Starting SuperGoldPipeline execution.
2025-05-07 11:30:34,466 - SuperLake - INFO - Gold function completed. Rows: 3
2025-05-07 11:30:39,471 - SuperLake - INFO - Saved DataFrame to 03_gold.customer_agg in mode overwrite
2025-05-07 11:30:39,471 - SuperLake - INFO - Saved to gold table.
2025-05-07 11:30:39,471 - SuperLake - INFO - SuperGoldPipeline completed.

Gold table output:
+-------+--------------+--------------------+                                   
|country|customer_count|        superlake_dt|
+-------+--------------+--------------------+
|     CH|             3|2025-05-07 11:29:...|
|     DE|             1|2025-05-07 11:29:...|
|     US|             1|2025-05-07 11:29:...|
+-------+--------------+--------------------+

------------------------ optimize tables ------------------------
2025-05-07 11:30:41,655 - SuperLake - INFO - Starting optimize for table 01_bronze.customer.
2025-05-07 11:30:41,756 - SuperLake - INFO - Registered managed Delta table 01_bronze.customer
2025-05-07 11:30:51,074 - SuperLake - INFO - Optimized table 01_bronze.customer (managed)
2025-05-07 11:30:51,074 - SuperLake - INFO - Metric - optimize_table_creation_duration_sec: 0.09
2025-05-07 11:30:51,074 - SuperLake - INFO - Metric - optimize_table_optimization_duration_sec: 9.32
2025-05-07 11:30:51,074 - SuperLake - INFO - Metric - optimize_table_total_duration_sec: 9.41
2025-05-07 11:30:51,075 - SuperLake - INFO - Starting optimize for table 02_silver.customer.
2025-05-07 11:30:51,148 - SuperLake - INFO - Registered external Delta table 02_silver.customer
2025-05-07 11:30:59,474 - SuperLake - INFO - Optimized table 02_silver.customer (external)
2025-05-07 11:30:59,474 - SuperLake - INFO - Metric - optimize_table_creation_duration_sec: 0.05
2025-05-07 11:30:59,474 - SuperLake - INFO - Metric - optimize_table_optimization_duration_sec: 8.33
2025-05-07 11:30:59,474 - SuperLake - INFO - Metric - optimize_table_total_duration_sec: 8.38
2025-05-07 11:30:59,474 - SuperLake - INFO - Starting optimize for table 03_gold.customer_agg.
2025-05-07 11:30:59,599 - SuperLake - INFO - Registered external Delta table 03_gold.customer_agg
2025-05-07 11:31:05,066 - SuperLake - INFO - Optimized table 03_gold.customer_agg (external)
2025-05-07 11:31:05,066 - SuperLake - INFO - Metric - optimize_table_creation_duration_sec: 0.09
2025-05-07 11:31:05,066 - SuperLake - INFO - Metric - optimize_table_optimization_duration_sec: 5.47
2025-05-07 11:31:05,066 - SuperLake - INFO - Metric - optimize_table_total_duration_sec: 5.56
------------------------ vacuum tables ------------------------
2025-05-07 11:31:05,181 - SuperLake - INFO - Registered managed Delta table 01_bronze.customer
Deleted 0 files and directories in a total of 3 directories.                    
2025-05-07 11:31:52,893 - SuperLake - INFO - Vacuumed table 01_bronze.customer with retention 168 hours
2025-05-07 11:31:52,893 - SuperLake - INFO - Metric - vacuum_table_creation_duration_sec: 0.12
2025-05-07 11:31:52,893 - SuperLake - INFO - Metric - vacuum_table_vacuum_duration_sec: 47.71
2025-05-07 11:31:52,893 - SuperLake - INFO - Metric - vacuum_table_total_duration_sec: 47.83
2025-05-07 11:31:52,999 - SuperLake - INFO - Registered external Delta table 02_silver.customer
Deleted 0 files and directories in a total of 3 directories.                    
2025-05-07 11:32:31,279 - SuperLake - INFO - Vacuumed table 02_silver.customer with retention 168 hours
2025-05-07 11:32:31,279 - SuperLake - INFO - Metric - vacuum_table_creation_duration_sec: 0.11
2025-05-07 11:32:31,279 - SuperLake - INFO - Metric - vacuum_table_vacuum_duration_sec: 38.28
2025-05-07 11:32:31,279 - SuperLake - INFO - Metric - vacuum_table_total_duration_sec: 38.39
2025-05-07 11:32:31,318 - SuperLake - INFO - Registered external Delta table 03_gold.customer_agg
Deleted 0 files and directories in a total of 1 directories.                    
2025-05-07 11:33:10,371 - SuperLake - INFO - Vacuumed table 03_gold.customer_agg with retention 168 hours
2025-05-07 11:33:10,371 - SuperLake - INFO - Metric - vacuum_table_creation_duration_sec: 0.04
2025-05-07 11:33:10,371 - SuperLake - INFO - Metric - vacuum_table_vacuum_duration_sec: 39.05
2025-05-07 11:33:10,371 - SuperLake - INFO - Metric - vacuum_table_total_duration_sec: 39.09
```
