# -------------------------------------------------------------------------------------
#                                   Imports
# -------------------------------------------------------------------------------------

import sys
import os
import shutil
import pytest
import pyspark.sql.types as T
import pyspark.sql.functions as F
from datetime import datetime, date
# fix the path to include the superlake package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution, SuperSpark
from superlake.core import SuperPipeline, SuperSimplePipeline




# -------------------------------------------------------------------------------------
#                                   Fixtures
# -------------------------------------------------------------------------------------

WAREHOUSE_DIR = "./tests/data/spark-warehouse"
EXTERNAL_PATH = "./tests/data/external-table"
CATALOG_NAME = "spark_catalog"

TABLE_SCHEMA = T.StructType([
        T.StructField("id", T.IntegerType(), False),
        T.StructField("key", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
        T.StructField("superlake_dt", T.TimestampType(), True)
    ])


@pytest.fixture(scope="module")
def super_spark():
    return SuperSpark(
        session_name="SparkSession for SuperLake",
        warehouse_dir=WAREHOUSE_DIR,
        external_path=EXTERNAL_PATH,
        catalog_name=CATALOG_NAME
    )


@pytest.fixture(scope="module")
def spark(super_spark):
    return super_spark.spark


@pytest.fixture(scope="module")
def logger():
    from superlake.monitoring import SuperLogger
    return SuperLogger()


@pytest.fixture(scope="module")
def super_tracer(super_spark, logger):
    from superlake.core import SuperTracer
    return SuperTracer(
        super_spark=super_spark,
        catalog_name="spark_catalog",
        schema_name="00_superlake",
        table_name="super_trace",
        managed=False,
        logger=logger
    )


@pytest.fixture(scope="module")
def bronze_customer(super_spark, logger):
    return SuperDeltaTable(
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
        managed=True
    )


@pytest.fixture(scope="module")
def silver_customer(super_spark, logger):
    return SuperDeltaTable(
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
        managed=False
    )


@pytest.fixture(scope="module")
def gold_customer_agg(super_spark, logger):
    return SuperDeltaTable(
        super_spark=super_spark,
        catalog_name="spark_catalog",
        schema_name="03_gold",
        table_name="customer_agg",
        table_schema=T.StructType([
            T.StructField("country", T.StringType(), True),
            T.StructField("customer_count", T.LongType(), True),
            T.StructField("superlake_dt", T.TimestampType(), True)
        ]),
        table_save_mode=TableSaveMode.Overwrite,
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


# Helper to mock pipeline source data
def get_source_customer_data_for_pipeline_run(spark, pipeline_run_number):
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
        return spark.createDataFrame(customer_source_data, schema=customer_source_schema)
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
            ("2", "0923623624", "Jane changed", "jane.smith@example.com", "CH", date(2022, 2, 20)),
            ("3", "0923623625", "Pedro Alvarez", "pedro.alvarez@example.com", "CH", date(2022, 3, 10)),
            ("4", "0923623626", "Anna Müller", "anna.mueller@example.com", "CH", date(2022, 4, 5)),
            ("5", "0923623627", "Li Wei", "li.wei@example.com", "DE", date(2022, 5, 12))
        ]
        return spark.createDataFrame(customer_source_data, schema=customer_source_schema)
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
            ("3", "0923623625", "Pedro Alvarez", "pedro.alvarez@example.com", "CH", date(2022, 3, 10)),
            ("4", "0923623626", "Anna Müller", "anna.mueller@example.com", "CH", date(2022, 4, 5)),
            ("5", "0923623627", "Li Wei", "li.wei@example.com", "DE", date(2022, 5, 12))
        ]
        return spark.createDataFrame(customer_source_data, schema=customer_source_schema)


# CDC, transformation, and deletion functions
def customer_cdc(spark, silver_customer, pipeline_run_number, logger):
    if silver_customer.table_exists(spark):
        max_customer_id = silver_customer.read().select(F.max("customer_id")).collect()[0][0]
        if pipeline_run_number == 2:
            max_customer_id = 0
        if pipeline_run_number == 3:
            max_customer_id = 4
    else:
        max_customer_id = 0
    customer_source_df = get_source_customer_data_for_pipeline_run(spark, pipeline_run_number)
    customer_source_df = customer_source_df.filter(F.col("customer_id") > max_customer_id)
    logger.info(f"CDC max customer id: {max_customer_id}")
    return customer_source_df


def customer_tra(df):
    df = (
        df
        .withColumn("email", F.lower(F.col("email")))
        .withColumn("name", F.lower(F.col("name")))
        .withColumn("country", F.upper(F.col("country")))
    )
    return df


def customer_del(spark, pipeline_run_number):
    return get_source_customer_data_for_pipeline_run(spark, pipeline_run_number)


def gold_customer_agg_function(super_spark, silver_customer, superlake_dt):
    df = silver_customer.read().filter(F.col("scd_is_current").cast(T.BooleanType()))
    df = df.groupBy("country").agg(F.count("*").alias("customer_count"))
    df = df.withColumn("superlake_dt", F.lit(superlake_dt))
    return df


def df_to_set(df, columns):
    # Convert DataFrame rows to a set of tuples for comparison, selecting only the specified columns
    return set(tuple(row[c] for c in columns) for row in df.select(*columns).collect())


# Define pipeline run timestamps at module level for use in all test functions
pipeline_run_1_superlake_dt = datetime(2022, 5, 15, 16, 56, 42)  # Use a fixed value for determinism, or datetime.now() if you want dynamic
pipeline_run_2_superlake_dt = datetime(2022, 5, 15, 16, 58, 18)
pipeline_run_3_superlake_dt = pipeline_run_2_superlake_dt  # As per the log and example, run 3 reuses run 2's timestamp

# drop the tables before running the tests
tables_to_drop = [bronze_customer, silver_customer, gold_customer_agg]
for table in tables_to_drop:
    try:
        table.drop(spark)
    except Exception as e:
        print(f"Error dropping tables: {e}")

# delete all the files in the test folder
try:
    shutil.rmtree(WAREHOUSE_DIR)
except Exception as e:
    print(f"Error deleting warehouse directory: {e}")
try:
    shutil.rmtree(EXTERNAL_PATH)
except Exception as e:
    print(f"Error deleting external path: {e}")


# ------------------- Pipeline Run 1 -------------------
def test_customer_pipeline_initial_run(
    super_spark, spark, logger, super_tracer,
    bronze_customer, silver_customer, gold_customer_agg
):
    pipeline_run_number = 1

    def cdc_func(spark):
        return customer_cdc(spark, silver_customer, pipeline_run_number, logger)

    def tra_func(df):
        return customer_tra(df)

    def del_func(spark):
        return customer_del(spark, pipeline_run_number)

    customer_pipeline = SuperPipeline(
        logger=logger,
        super_spark=super_spark,
        super_tracer=super_tracer,
        superlake_dt=pipeline_run_1_superlake_dt,
        pipeline_name="customer_pipeline_silver",
        bronze_table=bronze_customer,
        silver_table=silver_customer,
        cdc_function=cdc_func,
        tra_function=tra_func,
        del_function=del_func,
        force_cdc=False,
        force_caching=True,
        environment="test"
    )
    customer_pipeline.execute()

    gold_pipeline = SuperSimplePipeline(
        logger=logger,
        super_spark=super_spark,
        super_tracer=super_tracer,
        superlake_dt=pipeline_run_1_superlake_dt,
        pipeline_name="customer_pipeline_gold",
        function=lambda super_spark, dt: gold_customer_agg_function(super_spark, silver_customer, dt),
        table=gold_customer_agg,
        environment="test"
    )
    gold_pipeline.execute()

    bronze_expected_1 = {
        ("3", "Pedro Alvarez", "pedro.alvarez@example.com", "EN", date(2022, 3, 10), pipeline_run_1_superlake_dt),
        ("2", "Jane Smith", "jane.smith@example.com", "FR", date(2022, 2, 20), pipeline_run_1_superlake_dt),
        ("1", "John Doe", "john.doe@example.com", "US", date(2022, 1, 15), pipeline_run_1_superlake_dt),
    }
    bronze_actual_1 = set(
        tuple(row[c] for c in ["customer_id", "name", "email", "country", "signup_date", "superlake_dt"])
        for row in bronze_customer.get_delta_table(spark).toDF().collect()
    )
    assert bronze_actual_1 == bronze_expected_1

    silver_expected_1 = {
        (1, "john doe", "john.doe@example.com", "US", date(2022, 1, 15),
         pipeline_run_1_superlake_dt, pipeline_run_1_superlake_dt, None, True),
        (3, "pedro alvarez", "pedro.alvarez@example.com", "EN", date(2022, 3, 10),
         pipeline_run_1_superlake_dt, pipeline_run_1_superlake_dt, None, True),
        (2, "jane smith", "jane.smith@example.com", "FR", date(2022, 2, 20),
         pipeline_run_1_superlake_dt, pipeline_run_1_superlake_dt, None, True),
    }
    silver_actual_1 = set(
        tuple(row[c] for c in [
            "customer_id", "name", "email", "country", "signup_date",
            "superlake_dt", "scd_start_dt", "scd_end_dt", "scd_is_current"
        ])
        for row in silver_customer.get_delta_table(spark).toDF().collect()
    )
    assert silver_actual_1 == silver_expected_1

    gold_expected_1 = {
        ("EN", 1, pipeline_run_1_superlake_dt),
        ("US", 1, pipeline_run_1_superlake_dt),
        ("FR", 1, pipeline_run_1_superlake_dt),
    }
    gold_actual_1 = set(
        tuple(row[c] for c in ["country", "customer_count", "superlake_dt"])
        for row in gold_customer_agg.get_delta_table(spark).toDF().collect()
    )
    assert gold_actual_1 == gold_expected_1


# ------------------- Pipeline Run 2 (merge) -------------------
def test_customer_pipeline_merge(
    super_spark, spark, logger, super_tracer,
    bronze_customer, silver_customer, gold_customer_agg
):
    pipeline_run_number = 2

    def cdc_func(spark):
        return customer_cdc(spark, silver_customer, pipeline_run_number, logger)

    def tra_func(df):
        return customer_tra(df)

    def del_func(spark):
        return customer_del(spark, pipeline_run_number)

    customer_pipeline = SuperPipeline(
        logger=logger,
        super_spark=super_spark,
        super_tracer=super_tracer,
        superlake_dt=pipeline_run_2_superlake_dt,
        pipeline_name="customer_pipeline_silver",
        bronze_table=bronze_customer,
        silver_table=silver_customer,
        cdc_function=cdc_func,
        tra_function=tra_func,
        del_function=del_func,
        force_cdc=False,
        force_caching=True,
        environment="test"
    )
    customer_pipeline.execute()

    gold_pipeline = SuperSimplePipeline(
        logger=logger,
        super_spark=super_spark,
        super_tracer=super_tracer,
        superlake_dt=pipeline_run_2_superlake_dt,
        pipeline_name="customer_pipeline_gold",
        function=lambda super_spark, dt: gold_customer_agg_function(super_spark, silver_customer, dt),
        table=gold_customer_agg,
        environment="test"
    )
    gold_pipeline.execute()

    # Accumulate all rows from run 1 (with run 1 timestamp) and new rows from run 2 (with run 2 timestamp)
    bronze_expected_2 = {
        ("3", "Pedro Alvarez", "pedro.alvarez@example.com", "CH", date(2022, 3, 10),
         pipeline_run_2_superlake_dt, "0923623625"),
        ("4", "Anna Müller", "anna.mueller@example.com", "CH", date(2022, 4, 5),
         pipeline_run_2_superlake_dt, "0923623626"),
        ("2", "Jane changed", "jane.smith@example.com", "CH", date(2022, 2, 20),
         pipeline_run_2_superlake_dt, "0923623624"),
        ("5", "Li Wei", "li.wei@example.com", "DE", date(2022, 5, 12),
         pipeline_run_2_superlake_dt, "0923623627"),
        ("3", "Pedro Alvarez", "pedro.alvarez@example.com", "EN", date(2022, 3, 10),
         pipeline_run_1_superlake_dt, None),
        ("2", "Jane Smith", "jane.smith@example.com", "FR", date(2022, 2, 20),
         pipeline_run_1_superlake_dt, None),
        ("1", "John Doe", "john.doe@example.com", "US", date(2022, 1, 15),
         pipeline_run_1_superlake_dt, None),
    }
    bronze_actual_2 = set(
        tuple(row[c] for c in [
            "customer_id", "name", "email", "country",
            "signup_date", "superlake_dt", "phone_number"
        ])
        for row in bronze_customer.get_delta_table(spark).toDF().collect()
    )
    assert bronze_actual_2 == bronze_expected_2

    # Silver SCD logic: new current rows get run 2 timestamp, closed rows get scd_end_dt set to run 2 timestamp
    silver_expected_2 = {
        (3, "pedro alvarez", "pedro.alvarez@example.com", "CH", date(2022, 3, 10),
         pipeline_run_2_superlake_dt, pipeline_run_2_superlake_dt, None, True, "0923623625"),
        (4, "anna müller", "anna.mueller@example.com", "CH", date(2022, 4, 5),
         pipeline_run_2_superlake_dt, pipeline_run_2_superlake_dt, None, True, "0923623626"),
        (2, "jane changed", "jane.smith@example.com", "CH", date(2022, 2, 20),
         pipeline_run_2_superlake_dt, pipeline_run_2_superlake_dt, None, True, "0923623624"),
        (5, "li wei", "li.wei@example.com", "DE", date(2022, 5, 12),
         pipeline_run_2_superlake_dt, pipeline_run_2_superlake_dt, None, True, "0923623627"),
        (3, "pedro alvarez", "pedro.alvarez@example.com", "EN", date(2022, 3, 10),
         pipeline_run_1_superlake_dt, pipeline_run_1_superlake_dt, pipeline_run_2_superlake_dt, False, None),
        (2, "jane smith", "jane.smith@example.com", "FR", date(2022, 2, 20),
         pipeline_run_1_superlake_dt, pipeline_run_1_superlake_dt, pipeline_run_2_superlake_dt, False, None),
        (1, "john doe", "john.doe@example.com", "US", date(2022, 1, 15),
         pipeline_run_1_superlake_dt, pipeline_run_1_superlake_dt, pipeline_run_2_superlake_dt, False, None),
    }
    silver_actual_2 = set(
        tuple(row[c] for c in [
            "customer_id", "name", "email", "country", "signup_date",
            "superlake_dt", "scd_start_dt", "scd_end_dt", "scd_is_current",
            "phone_number"
        ])
        for row in silver_customer.get_delta_table(spark).toDF().collect()
    )
    assert silver_actual_2 == silver_expected_2

    gold_expected_2 = {
        ("DE", 1, pipeline_run_2_superlake_dt),
        ("CH", 3, pipeline_run_2_superlake_dt),
    }
    gold_actual_2 = set(
        tuple(row[c] for c in ["country", "customer_count", "superlake_dt"])
        for row in gold_customer_agg.get_delta_table(spark).toDF().collect()
    )
    assert gold_actual_2 == gold_expected_2


# ------------------- Pipeline Run 3 (delete/force_cdc) -------------------
def test_customer_pipeline_delete(
        super_spark, spark, logger, super_tracer,
        bronze_customer, silver_customer, gold_customer_agg
        ):
    pipeline_run_number = 3

    def cdc_func(spark):
        return customer_cdc(spark, silver_customer, pipeline_run_number, logger)

    def tra_func(df):
        return customer_tra(df)

    def del_func(spark):
        return customer_del(spark, pipeline_run_number)

    customer_pipeline = SuperPipeline(
        logger=logger,
        super_spark=super_spark,
        super_tracer=super_tracer,
        superlake_dt=pipeline_run_3_superlake_dt,
        pipeline_name="customer_pipeline_silver",
        bronze_table=bronze_customer,
        silver_table=silver_customer,
        cdc_function=cdc_func,
        tra_function=tra_func,
        del_function=del_func,
        force_cdc=True,
        force_caching=True,
        environment="test"
    )
    customer_pipeline.execute()

    gold_pipeline = SuperSimplePipeline(
        logger=logger,
        super_spark=super_spark,
        super_tracer=super_tracer,
        superlake_dt=pipeline_run_3_superlake_dt,
        pipeline_name="customer_pipeline_gold",
        function=lambda super_spark, dt: gold_customer_agg_function(super_spark, silver_customer, dt),
        table=gold_customer_agg,
        environment="test"
    )
    gold_pipeline.execute()

    # Accumulate all rows from previous runs with their original timestamps, and new/updated rows with run 3 timestamp
    bronze_expected_3 = {
        ("3", "Pedro Alvarez", "pedro.alvarez@example.com", "CH", date(2022, 3, 10),
         pipeline_run_2_superlake_dt, "0923623625"),
        ("4", "Anna Müller", "anna.mueller@example.com", "CH", date(2022, 4, 5),
         pipeline_run_2_superlake_dt, "0923623626"),
        ("2", "Jane changed", "jane.smith@example.com", "CH", date(2022, 2, 20),
         pipeline_run_2_superlake_dt, "0923623624"),
        ("5", "Li Wei", "li.wei@example.com", "DE", date(2022, 5, 12),
         pipeline_run_2_superlake_dt, "0923623627"),
        ("3", "Pedro Alvarez", "pedro.alvarez@example.com", "EN", date(2022, 3, 10),
         pipeline_run_1_superlake_dt, None),
        ("2", "Jane Smith", "jane.smith@example.com", "FR", date(2022, 2, 20),
         pipeline_run_1_superlake_dt, None),
        ("1", "John Doe", "john.doe@example.com", "US", date(2022, 1, 15),
         pipeline_run_1_superlake_dt, None),
    }
    bronze_actual_3 = set(
        tuple(row[c] for c in [
            "customer_id", "name", "email", "country",
            "signup_date", "superlake_dt", "phone_number"
        ])
        for row in bronze_customer.get_delta_table(spark).toDF().collect()
    )
    assert bronze_actual_3 == bronze_expected_3

    # Silver SCD logic: new current rows get run 2 timestamp, closed rows get scd_end_dt set to run 2 timestamp
    silver_expected_3 = {
        (2, "jane changed", "jane.smith@example.com", "CH", date(2022, 2, 20),
         pipeline_run_2_superlake_dt, pipeline_run_2_superlake_dt, pipeline_run_2_superlake_dt, False, "0923623624"),
        (3, "pedro alvarez", "pedro.alvarez@example.com", "CH", date(2022, 3, 10),
         pipeline_run_2_superlake_dt, pipeline_run_2_superlake_dt, None, True, "0923623625"),
        (4, "anna müller", "anna.mueller@example.com", "CH", date(2022, 4, 5),
         pipeline_run_2_superlake_dt, pipeline_run_2_superlake_dt, None, True, "0923623626"),
        (5, "li wei", "li.wei@example.com", "DE", date(2022, 5, 12),
         pipeline_run_2_superlake_dt, pipeline_run_2_superlake_dt, None, True, "0923623627"),
        (3, "pedro alvarez", "pedro.alvarez@example.com", "EN", date(2022, 3, 10),
         pipeline_run_1_superlake_dt, pipeline_run_1_superlake_dt, pipeline_run_2_superlake_dt, False, None),
        (2, "jane smith", "jane.smith@example.com", "FR", date(2022, 2, 20),
         pipeline_run_1_superlake_dt, pipeline_run_1_superlake_dt, pipeline_run_2_superlake_dt, False, None),
        (1, "john doe", "john.doe@example.com", "US", date(2022, 1, 15),
         pipeline_run_1_superlake_dt, pipeline_run_1_superlake_dt, pipeline_run_2_superlake_dt, False, None),
    }
    silver_actual_3 = set(
        tuple(row[c] for c in [
            "customer_id", "name", "email", "country", "signup_date",
            "superlake_dt", "scd_start_dt", "scd_end_dt", "scd_is_current", "phone_number"
        ])
        for row in silver_customer.get_delta_table(spark).toDF().collect()
    )
    assert silver_actual_3 == silver_expected_3

    gold_expected_3 = {
        ("DE", 1, pipeline_run_2_superlake_dt),
        ("CH", 2, pipeline_run_2_superlake_dt),
    }
    gold_actual_3 = set(
        tuple(row[c] for c in ["country", "customer_count", "superlake_dt"])
        for row in gold_customer_agg.get_delta_table(spark).toDF().collect()
    )
    assert gold_actual_3 == gold_expected_3
