# superlake
A modern, intuitive Python package for data lakehouse operations

## Main SuperLake Classes

- **SuperSpark**: Instantiates a SparkSession with Delta Lake support.
- **SuperDeltaTable**: Manages Delta tables (create, read, write, optimize, vacuum, SCD2, schema evolution, etc.).
- **SuperPipeline**: Orchestrates data pipelines from source to bronze and silver layers, including CDC and transformation logic.
- **SuperGoldPipeline**: Manages gold-layer aggregations and writes results to gold tables.
- **SuperDataframe**: Utility class for DataFrame cleaning, casting, and manipulation.
- **SuperLogger**: Logging and metrics for pipeline operations.

## Quick Example Usage

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
spark = SuperSpark()
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
            ("2", "0923623624","Jane changed", "jane.smith@example.com", "EN", date(2022, 2, 20)),
            ("3", "0923623625","Pedro Alvarez", "pedro.alvarez@example.com", "EN", date(2022, 3, 10)),
            ("4", "0923623626","Anna Müller", "anna.mueller@example.com", "DE", date(2022, 4, 5)),
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
