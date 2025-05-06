import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from unittest.mock import MagicMock
import pyspark.sql.types as T
import pyspark.sql.functions as F
from datetime import date, datetime
from pyspark.sql import SparkSession

# Import the functions and classes from the main code
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution, SuperPipeline, SuperGoldPipeline, SuperSpark
from superlake.monitoring import SuperLogger
from superlake.core.dataframe import SuperDataframe

@pytest.fixture(scope="module")
def spark():
    return SuperSpark()

@pytest.fixture
def logger():
    return SuperLogger()

@pytest.fixture
def bronze_customer(logger):
    return SuperDeltaTable(
        table_name="01_bronze.customer",
        table_path="/tmp/bronze/customer",
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
        schema_evolution_option=SchemaEvolution.Merge,
        logger=logger,
        managed=True
    )

@pytest.fixture
def silver_customer(logger):
    return SuperDeltaTable(
        table_name="02_silver.customer",
        table_path="/tmp/silver/customer",
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
        schema_evolution_option=SchemaEvolution.Merge,
        logger=logger,
        scd_change_cols=["name", "email", "country"],
        managed=False
    )

def customer_cdc(spark, silver_customer, logger):
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
        ("3", "Pedro Alvarez", "pedro.alvarez@example.com", "ES", date(2022, 3, 10)),
        ("4", "Anna Müller", "anna.mueller@example.com", "DE", date(2022, 4, 5)),
        ("5", "Li Wei", "li.wei@example.com", "CN", date(2022, 5, 12))
    ]
    customer_source_df = spark.createDataFrame(customer_source_data, schema=customer_source_schema)
    if silver_customer.table_exists(spark):
        max_customer_id = silver_customer.read(spark).select(F.max("customer_id")).collect()[0][0]
        max_customer_id = max_customer_id - 2
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
            ("3", "0923623625","Pedro Alvarez", "pedro.alvarez@example.com", "CH", date(2022, 3, 10)),
            ("4", "0923623626","Anna Müller", "anna.mueller@example.com", "DE", date(2022, 4, 5)),
            ("5", "0923623627","Li Wei", "li.wei@example.com", "CN", date(2022, 5, 12))
        ]
        customer_source_df = spark.createDataFrame(customer_source_data, schema=customer_source_schema)
    else:
        customer_source_df = customer_source_df.filter(F.col("customer_id") <= 3)
        max_customer_id = 0
    logger.info(f"CDC max customer id: {max_customer_id}")
    customer_source_df = customer_source_df.filter(F.col("customer_id") > max_customer_id)
    return customer_source_df

def customer_tra(df):
    return (
        df
        .withColumn("email", F.lower(F.col("email")))
        .withColumn("name", F.lower(F.col("name")))
        .withColumn("country", F.upper(F.col("country")))
    )

def gold_customer_agg_function(spark, silver_customer, superlake_dt):
    df = silver_customer.read(spark).filter(F.col("scd_is_current") == True)
    df = df.groupBy("country").agg(F.count("*").alias("customer_count"))
    df = df.withColumn("superlake_dt", F.lit(superlake_dt))
    return df


# ----------------------------------------------------------------------------------
#                               Testing SuperDataframe
# ----------------------------------------------------------------------------------

def test_clean_columns_names(spark):
    data = [(1, 'Alice'), (2, 'Bob')]
    columns = ['id#', 'Name (Full)']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    cleaned_df = sdf.clean_columns_names()
    # check the count of rows is the same
    assert cleaned_df.count() == 2
    # check the columns are the same
    assert cleaned_df.columns == ['id', 'Name_Full']


def test_clean_column_values(spark):
    data = [
        (1, 'value__', 'value'), 
        (2, 'value_2_!', 'value_2')
    ]
    columns = ['id', 'column_to_clean', 'column_cleaned']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    cleaned_df = sdf.clean_column_values(columns=['column_to_clean'])
    # check the count of rows is the same
    assert cleaned_df.count() == 2
    # check the values in the column_to_clean are the same as the values in the column_cleaned
    assert cleaned_df.select(F.col("column_to_clean")).collect() == df.select(F.col("column_cleaned")).collect()
    

def test_cast_columns(spark):
    data = [(1, 'Alice'), (2, 'Bob')]
    columns = ['id', 'Name']
    schema = T.StructType([
        T.StructField("id", T.IntegerType(), True),
        T.StructField("Name", T.StringType(), True)
    ])
    df = spark.createDataFrame(data, columns, schema)
    target_schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("Name", T.StringType(), True)
    ])
    sdf = SuperDataframe(df)
    casted_df = sdf.cast_columns(target_schema)
    # check the count of rows is the same
    assert casted_df.count() == 2  
    # check the schema has been casted correctly
    assert casted_df.schema == target_schema

# More tests to come...