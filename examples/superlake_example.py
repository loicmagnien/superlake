"""Example usage of SuperLake package."""

# Standard Library
import pyspark.sql.types as T
import pyspark.sql.functions as F
from datetime import date, datetime
from pyspark.sql import DataFrame
import time
import sys
import os

# fix the import path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# the superlake library
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


def main():
    """Run example pipeline."""

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


if __name__ == "__main__":
    main() 