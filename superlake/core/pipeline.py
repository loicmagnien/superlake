"""Pipeline management for SuperLake."""

import pyspark.sql.functions as F
import pyspark.sql.types as T
import time
from datetime import datetime
from typing import Optional, Callable, Any
from pyspark.sql import DataFrame, SparkSession
from superlake.core.delta import SuperDeltaTable, TableSaveMode
from superlake.core.dataframe import SuperDataframe


class SuperTracer:
    """SuperTracer for SuperLake.
    This class is used to log the trace of the SuperLake pipeline in a delta table.
    It allows to persist the trace in a delta table and to query the trace for a given superlake_dt.
    The trace is based on the key-value pairs and is very flexible.
    The trace for a pipeline could look like this to track if tables are updated:
    +---------------------+---------------------+---------------+----------------+------------+
    | superlake_dt        | trace_dt            | trace_key     | trace_value    | trace_year |
    +---------------------+---------------------+---------------+----------------+------------+
    | 2021-01-01 19:00:00 | 2021-01-01 19:05:00 | pipeline_name | bronze_updated | 2021       |
    | 2021-01-01 19:00:00 | 2021-01-01 19:05:00 | pipeline_name | silver_updated | 2021       |
    +---------------------+---------------------+---------------+----------------+------------+

    args:
        super_spark: SuperSpark
        catalog_name: catalog name
        schema_name: schema name
        table_name: table name
        managed: if the table is managed
        logger: logger
    """

    def __init__(
        self,
        super_spark: Any,
        catalog_name: Optional[str],
        schema_name: str,
        table_name: str,
        managed: bool,
        logger: Any
    ) -> None:

        # from init
        self.super_spark = super_spark
        self.spark = super_spark.spark
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.logger = logger
        self.managed = managed

        # trace table
        self.super_trace_table = SuperDeltaTable(
            super_spark=self.super_spark,
            catalog_name=self.catalog_name or self.super_spark.catalog_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
            table_schema=T.StructType([
                T.StructField("superlake_dt", T.TimestampType(), True),  # superlake_dt (pipeline run datetime)
                T.StructField("trace_dt", T.TimestampType(), True),      # trace_dt (trace datetime)
                T.StructField("trace_key", T.StringType(), True),        # trace_key (trace key)
                T.StructField("trace_value", T.StringType(), True),      # trace_value (trace value)
                T.StructField("trace_year", T.IntegerType(), True)       # trace_year (generated partition column)
            ]),
            table_save_mode=TableSaveMode.Append,
            primary_keys=["superlake_dt", "trace_dt", "trace_key", "trace_value"],
            partition_cols=["trace_year"],
            managed=self.managed
        )

    def generate_trace_table(self) -> None:
        """
        Instantiates the table if it doesn't exist.
        args:
            spark (SparkSession): The Spark session.
        returns:
            None
        """
        # create the table if it doesn't exist
        self.super_trace_table.ensure_table_exists(self.spark, log=False)

    def add_trace(self, superlake_dt: datetime, trace_key: str, trace_value: str) -> None:
        """
        Adds a trace to the trace table.
        args:
            superlake_dt (datetime): The superlake_dt.
            trace_key (str): The trace key.
            trace_value (str): The trace value.
        returns:
            None
        """
        # generate the dataframe and cast the columns
        trace_dt = datetime.now()
        data = [(superlake_dt, trace_dt, trace_key, trace_value, trace_dt.year)]
        columns = ['superlake_dt', 'trace_dt', 'trace_key', 'trace_value', 'trace_year']
        trace_df = self.spark.createDataFrame(data, columns)
        trace_df = SuperDataframe(trace_df).cast_columns(schema=self.super_trace_table.table_schema)

        # generate the table if it doesn't exist
        self.generate_trace_table()

        # insert the log dataframe into the table
        self.super_trace_table.save(
            trace_df,
            mode=str(self.super_trace_table.table_save_mode.value),
            spark=self.spark,
            log=False
        )

    def get_trace(self, superlake_dt: datetime) -> DataFrame:
        """
        Get the trace for a given superlake_dt.
        args:
            superlake_dt (datetime): The superlake_dt.
        returns:
            trace_df (DataFrame): The trace dataframe.
        """
        if self.super_trace_table.table_exists(self.spark):
            trace_df = self.super_trace_table.read().filter(F.col("superlake_dt") == superlake_dt)
            return trace_df
        else:
            self.logger.info(f"Trace table {self.super_trace_table.full_table_name()} does not exist")
            return self.spark.createDataFrame([], self.super_trace_table.table_schema)

    def has_trace(
        self,
        superlake_dt: datetime,
        trace_key: str,
        trace_value: str,
        trace_df: Optional[DataFrame] = None
    ) -> bool:
        """
        Check if the trace for a given superlake_dt, trace_key and trace_value exists.
        args:
            superlake_dt (datetime): The superlake_dt.
            trace_key (str): The trace key.
            trace_value (str): The trace value.
        returns:
            bool: True if the trace exists, False otherwise.
        """
        if trace_df is None:
            trace_df = self.get_trace(superlake_dt)
        return trace_df.filter(F.col("trace_key") == trace_key).filter(F.col("trace_value") == trace_value).count() > 0


class SuperPipeline:
    """
    Pipeline management for SuperLake.

    args:
        logger (SuperLogger): The logger.
        super_spark (SuperSpark): The super spark.
        super_tracer (SuperTracer): The super tracer.
        superlake_dt (datetime): The superlake datetime.
        pipeline_name (str): The pipeline name.
        bronze_table (SuperDeltaTable): The bronze table.
        silver_table (SuperDeltaTable): The silver table.
        cdc_function (function): The change data capture function.
        tra_function (function): The transformation function.
        del_function (function): The delete from silver function.
        force_cdc (bool): The force cdc, if true, the pipeline will run the cdc_function.
        force_caching (bool): The force caching, if true, the pipeline will cache the dataframes.
        environment (str): The environment.

    Notes on the idempotency mechanism:
    -----------------------------------

    An idempotent operation is one that can be applied multiple times without
    changing the result beyond the initial application. it is important for:
        - Failure recovery: If a job crashes midway, rerunning it should not lead to duplicated data.
        - Consistency: Ensures that repeated executions produce the same output.
        - Debugging: Easier to troubleshoot issues when pipelines behave predictably under retries.
        - Reproducibility: Essential in machine learning, reporting, and analytics pipelines.

    There are 2 different source of information to manage idempotency:
    - the superlake_dt : the current superlake_dt within the bronze and silver tables
    - the super_tracer : the trace of the previous runs operations done on the tables

    For a medalion data pipeline, the different stages are:
    - run the cdc function and append the new rows to the bronze table
    - filter the bronze table for the current superlake_dt and update the silver table with the new rows from bronze
    - delete the rows from the silver table that are no longer present in the source

    The ideal scenario is when all the operations are executed successfully. The result would be:
    - the superlake_dt is present in the bronze and silver tables
    - the super_tracer contains the key.values : pipeline.bronze_u, pipeline.silver_u, pipeline.silver_d

    In case of failure, there are different recovery behaviour depending on:
    - bronze_u: if the pipeline has already traced the bronze_updated
    - silver_u: if the pipeline has already traced the silver_updated
    - silver_d: if the pipeline has already traced the silver_deleted
    - skipped: if the pipeline has already traced the skipped
    - force_cdc: if the pipeline has a force_cdc value (True/False)
    - cdc_data: if the cdc_function retrieves data or not (empty df)
    - del_function: if the pipeline has a del_function defined or not

    the different scenarios and modes are:
    +------+----------+----------+----------+---------+-----------+----------+--------------+
    | case | bronze_u | silver_u | silver_d | skipped | force_cdc | cdc_data | del_function |
    +------+----------+----------+----------+---------+-----------+----------+--------------+
    | 01   | No       | No       | No       | No      | No        | No       | No           |
    | 02   | No       | No       | No       | No      | No        | No       | Yes          |
    | 03   | No       | No       | No       | No      | No        | Yes      | No           |
    | 04   | No       | No       | No       | No      | Yes       | Yes      | No           |
    | 05   | No       | No       | No       | No      | Yes       | No       | No           |
    | 06   | No       | No       | No       | No      | Yes       | No       | Yes          |
    | 07   | No       | No       | No       | No      | No        | Yes      | Yes          |
    | 08   | Yes      | No       | No       | No      | No        | Yes      | No           |
    | 09   | Yes      | Yes      | Yes      | No      | No        | n/a      | Yes          |
    | 10   | No       | No       | No       | Yes     | No        | n/a      | No           |
    | 11   | Yes      | No       | No       | No      | Yes       | Yes      | No           |
    | 12   | Yes      | Yes      | No       | No      | Yes       | Yes      | No           |
    | 13   | No       | No       | No       | Yes     | Yes       | No       | No           |
    | 14   | No       | No       | No       | Yes     | Yes       | Yes      | No           |
    | 15   | Yes      | Yes      | No       | No      | No/Yes    | Yes/No   | Yes          |
    +------+----------+----------+----------+---------+-----------+----------+--------------+
    +------+--------------------------------------------------------------------------------+
    | case |                                summary                                         |
    +------+--------------------------------------------------------------------------------+
    | 01   |  Run CDC (no data), trace skipped and stop.                                    |
    | 02   |  Run CDC (no data), run del_function, trace actions.                           |
    | 03   |  Run CDC, update bronze, update silver, trace actions.                         |
    | 04   |  Run CDC, update bronze, update silver, trace actions.                         |
    | 05   |  Run CDC (no data), trace skipped and stop.                                    |
    | 06   |  Run CDC (no data), run del_function, trace actions.                           |
    | 07   |  Run CDC, update bronze, update silver, run del_function, trace actions.       |
    | 08   |  Bronze already updated; update silver, trace silver_u.                        |
    | 09   |  All steps already done; nothing to do.                                        |
    | 10   |  Already skipped; nothing to do.                                               |
    | 11   |  Run CDC, merge new data into bronze, then merge into silver, trace actions.   |
    | 12   |  Run CDC, merge new data into bronze, then merge into silver, trace actions.   |
    | 13   |  Already skipped; run CDC, if no data, stop.                                   |
    | 14   |  Already skipped; run CDC, append bronze, update silver, trace actions.        |
    | 15   |  Run del_function, trace silver_d.                                             |
    +------+--------------------------------------------------------------------------------+
    """
    def __init__(
        self,
        logger: Any,
        super_spark: Any,
        super_tracer: SuperTracer,
        superlake_dt: datetime,
        pipeline_name: str,
        bronze_table: SuperDeltaTable,
        silver_table: SuperDeltaTable,
        cdc_function: Callable[[SparkSession], DataFrame],
        tra_function: Callable[[DataFrame], DataFrame],
        del_function: Optional[Callable[[SparkSession], DataFrame]] = None,
        force_cdc: bool = False,
        force_caching: bool = False,
        environment: str = "dev"
    ) -> None:
        self.logger = logger
        self.super_spark = super_spark
        self.spark = super_spark.spark
        self.super_tracer = super_tracer
        self.superlake_dt = superlake_dt
        self.pipeline_name = pipeline_name
        self.bronze_table = bronze_table
        self.silver_table = silver_table
        self.cdc_function = cdc_function
        self.tra_function = tra_function
        self.del_function = del_function
        self.force_cdc = force_cdc
        self.force_caching = force_caching
        self.environment = environment

    def delete_from_silver(self) -> None:
        """
        Deletes rows no longer present in the source from silver table based on del_function.
        The del_function returns a dataframe with all the rows (primary keys columns only) at the source.
        This function will delete the rows from the silver table that are no longer present in the source.
        args:
            None
        returns:
            None
        """
        if self.del_function:
            # get all the rows at the source
            self.logger.info("Starting deletion of rows no longer present at the source.")
            del_df = self.del_function(self.spark)
            # apply the transformation to the del_df
            if self.tra_function:
                del_df = self.tra_function(del_df)
            # build the deletion dataframe via left anti join on the primary keys
            deletions_df = (
                self.silver_table.read()
                .join(del_df, on=self.silver_table.primary_keys, how="left_anti")
            )
            if self.force_caching:
                deletions_df = deletions_df.cache()
                self.logger.info(
                    f"Caching - Deletions dataframe cached ({deletions_df.count()} rows)."
                )
            # delete the content of the deletion dataframe from silver
            self.silver_table.delete(deletions_df=deletions_df, superlake_dt=self.superlake_dt)
            if self.force_caching:
                deletions_df.unpersist()
                self.logger.info("Caching - Deletions dataframe unpersisted.")
            self.super_tracer.add_trace(self.superlake_dt, self.pipeline_name, "silver_deleted")

    def get_cdc_df(self) -> DataFrame:
        """
        Get the CDC dataframe for the current superlake_dt.
        args:
            None
        returns:
            cdc_df (DataFrame): The CDC dataframe.
        """
        return self.cdc_function(self.spark).withColumn("superlake_dt", F.lit(self.superlake_dt))

    def append_cdc_into_bronze(self, cdc_df: DataFrame) -> None:
        """
        Appends CDC data into bronze table.
        The cdc_df is the dataframe returned by the cdc_function.
        This function will append the cdc_df into the bronze table.
        args:
            cdc_df (DataFrame): The CDC dataframe.
        returns:
            None
        """
        # force the table save mode to append
        self.bronze_table.table_save_mode = TableSaveMode.Append
        if self.environment == "debug":
            print(f"Table save mode: {str(self.bronze_table.table_save_mode.value)}")
            print(cdc_df.show())
        # save the cdc_df into the bronze table
        self.bronze_table.save(cdc_df, mode=str(self.bronze_table.table_save_mode.value), spark=self.spark)
        self.super_tracer.add_trace(self.superlake_dt, self.pipeline_name, "bronze_updated")

    def merge_cdc_into_bronze(self, cdc_df: DataFrame) -> None:
        """
        Merge CDC data into bronze table.
        The cdc_df is the dataframe returned by the cdc_function.
        This function will merge the cdc_df into the bronze table.
        args:
            cdc_df (DataFrame): The CDC dataframe.
        returns:
            None
        """
        # force the table save mode to merge
        self.bronze_table.table_save_mode = TableSaveMode.Merge
        if self.environment == "debug":
            print(f"Table save mode: {str(self.bronze_table.table_save_mode.value)}")
            print(cdc_df.show())
        # save the cdc_df into the bronze table
        self.bronze_table.save(cdc_df, mode=str(self.bronze_table.table_save_mode.value), spark=self.spark)
        self.super_tracer.add_trace(self.superlake_dt, self.pipeline_name, "bronze_updated")

    def update_silver_from_bronze(self) -> None:
        """
        Update silver table from bronze table.
        This function will update the silver table from the bronze table.
        args:
            None
        returns:
            None
        """
        # read the bronze table (for current superlake_dt), apply the transformation and save the data into silver
        bronze_df = self.bronze_table.read().filter(F.col("superlake_dt") == self.superlake_dt)
        if self.tra_function:
            bronze_df = self.tra_function(bronze_df)
        try:
            self.silver_table.save(
                bronze_df,
                mode=str(self.silver_table.table_save_mode.value),
                spark=self.spark
            )
        except Exception as e:
            if "DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE" in str(e):
                self.logger.warning(
                    "Multiple source rows matched the same target row in merge. "
                    "Dropping duplicates from source DataFrame and retrying."
                )
                # Drop duplicates based on primary keys
                bronze_df_dedup = bronze_df.dropDuplicates(self.silver_table.primary_keys)
                self.silver_table.save(
                    bronze_df_dedup,
                    mode=str(self.silver_table.table_save_mode.value),
                    spark=self.spark
                )
            else:
                raise
        self.super_tracer.add_trace(self.superlake_dt, self.pipeline_name, "silver_updated")
        if self.environment == "debug":
            print(f"Table save mode: {str(self.silver_table.table_save_mode.value)}")
            print(bronze_df.show())

    def log_and_metrics_duration(self, start_time: float) -> None:
        """
        Logs and metrics the duration of the pipeline.
        args:
            start_time (float): The start time of the pipeline.
        returns:
            None
        """
        self.logger.metric("total_duration_sec", round(time.time() - start_time, 2))
        self.logger.info(
            f"SuperPipeline {self.pipeline_name} completed. "
            f"Total duration: {round(time.time() - start_time, 2)}s"
        )

    def show_tables(self) -> None:
        """
        Shows the tables.
        args:
            None
        returns:
            None
        """
        if self.environment in ["test", "debug"]:
            print(f"\n{self.bronze_table.full_table_name()}:\n")
            self.bronze_table.read().show()
            print(f"\n{self.silver_table.full_table_name()}:\n")
            self.silver_table.read().show()

    def execute(self) -> None:
        """
        Executes the ingestion, transformation and deletion logic for SuperPipeline.
        args:
            None
        returns:
            None
        """
        with self.logger.sub_name_context(self.pipeline_name):
            start_time = time.time()
            self.logger.info(f"Starting SuperPipeline {self.pipeline_name} execution.")
            super_tracer = self.super_tracer

            # 1. Get the trace for the current superlake_dt
            trace_df = super_tracer.get_trace(self.superlake_dt)
            bronze_u = super_tracer.has_trace(self.superlake_dt, self.pipeline_name, "bronze_updated", trace_df)
            silver_u = super_tracer.has_trace(self.superlake_dt, self.pipeline_name, "silver_updated", trace_df)
            silver_d = super_tracer.has_trace(self.superlake_dt, self.pipeline_name, "silver_deleted", trace_df)
            skipped = super_tracer.has_trace(
                self.superlake_dt,
                self.pipeline_name,
                "pipeline_skipped",
                trace_df
            )
            del_function_defined = self.del_function is not None
            trace_info = (
                f"Trace retrieved. bronze_u: {bronze_u}, "
                f"silver_u: {silver_u}, silver_d: {silver_d}, "
                f"skipped: {skipped}, del_function_defined: {del_function_defined}, "
                f"force_cdc: {self.force_cdc}"
            )
            self.logger.info(trace_info)

            # 2. Check force_cdc first (force_cdc always takes precedence)
            if self.force_cdc:
                cdc_df = self.get_cdc_df()
                if self.force_caching:
                    cdc_df = cdc_df.cache()
                    self.logger.info(f"Caching - CDC dataframe cached ({cdc_df.count()} rows).")
                cdc_count = cdc_df.count()
                cdc_data = cdc_count > 0
                if not cdc_data:
                    self.logger.info("force_cdc: No data from CDC.")
                    if del_function_defined:
                        self.delete_from_silver()  # (06)
                    super_tracer.add_trace(self.superlake_dt, self.pipeline_name, "pipeline_skipped")  # (05, 06)
                    self.log_and_metrics_duration(start_time)
                    self.show_tables()
                    if self.force_caching:
                        cdc_df.unpersist()
                        self.logger.info("Caching - CDC dataframe unpersisted.")
                    return
                else:
                    self.logger.info("force_cdc: New data from CDC.")
                    if bronze_u and silver_u:
                        self.merge_cdc_into_bronze(cdc_df)   # (12)
                        self.update_silver_from_bronze()     # (12)
                    elif bronze_u:
                        self.update_silver_from_bronze()     # (11)
                    else:
                        self.append_cdc_into_bronze(cdc_df)  # (04, 13, 14)
                        self.update_silver_from_bronze()     # (04, 13, 14)
                    if del_function_defined:
                        self.delete_from_silver()            # (14, 15)
                    # Log metrics after actions
                    self.log_and_metrics_duration(start_time)
                    self.show_tables()
                    if self.force_caching:
                        cdc_df.unpersist()
                        self.logger.info("Caching - CDC dataframe unpersisted.")
                    return

            # 3. If not force_cdc, check if pipeline was skipped
            if skipped:
                self.logger.info("Pipeline already skipped for this superlake_dt.")
                if del_function_defined and not silver_d:
                    self.delete_from_silver()  # (15)
                # Log metrics before exit
                self.log_and_metrics_duration(start_time)
                self.show_tables()
                return

            # 4. Standard idempotency logic
            cdc_df = self.get_cdc_df()
            if self.force_caching:
                cdc_df = cdc_df.cache()
                self.logger.info(f"Caching - CDC dataframe cached ({cdc_df.count()} rows).")
            cdc_count = cdc_df.count()
            cdc_data = cdc_count > 0
            if not cdc_data:
                self.logger.info("No new data from CDC.")
                if del_function_defined and not silver_d:
                    self.delete_from_silver()  # (02)
                super_tracer.add_trace(self.superlake_dt, self.pipeline_name, "pipeline_skipped")  # (01, 02)
                # Log metrics before exit
                self.log_and_metrics_duration(start_time)
                self.show_tables()
                if self.force_caching:
                    cdc_df.unpersist()
                    self.logger.info("Caching - CDC dataframe unpersisted.")
                return
            else:
                self.logger.info("New data from CDC.")
                if not bronze_u:
                    self.append_cdc_into_bronze(cdc_df)  # (03, 07)
                if not silver_u:
                    self.update_silver_from_bronze()     # (03, 07, 08)
                if del_function_defined:
                    self.delete_from_silver()            # (07, 15)
                self.log_and_metrics_duration(start_time)
                self.show_tables()
                if self.force_caching:
                    cdc_df.unpersist()
                    self.logger.info("Caching - CDC dataframe unpersisted.")
                return


class SuperSimplePipeline:
    """Simple pipeline for SuperLake: runs a function(spark, superlake_dt) and saves to table."""
    def __init__(
        self,
        logger: Any,
        super_spark: Any,
        super_tracer: SuperTracer,
        superlake_dt: datetime,
        pipeline_name: str,
        function: Callable[[Any, datetime], DataFrame],
        table: SuperDeltaTable,
        environment: Optional[str] = None
    ) -> None:
        self.logger = logger
        self.super_spark = super_spark
        self.spark = super_spark.spark
        self.super_tracer = super_tracer
        self.superlake_dt = superlake_dt
        self.pipeline_name = pipeline_name
        self.function = function
        self.table = table
        self.environment = environment

    def show_tables(self) -> None:
        """Shows the tables"""
        if self.environment in ["test", "debug"]:
            print(f"\nTable {self.table.full_table_name()}:\n")
            self.table.read().show()

    def execute(self) -> None:
        """Executes the function and saves to table."""
        with self.logger.sub_name_context(self.pipeline_name):
            start_time = time.time()
            self.logger.info(f"Starting SuperSimplePipeline {self.pipeline_name} execution.")
            df = self.function(self.super_spark, self.superlake_dt)
            try:
                self.table.save(
                    df,
                    mode=self.table.table_save_mode.value,
                    spark=self.super_spark.spark
                )
            except Exception as e:
                if "DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE" in str(e):
                    self.logger.warning(
                        "Multiple source rows matched the same target row in merge. "
                        "Dropping duplicates from source DataFrame and retrying."
                    )
                    # Drop duplicates based on primary keys
                    df_dedup = df.dropDuplicates(self.table.primary_keys)
                    self.table.save(
                        df_dedup,
                        mode=str(self.table.table_save_mode.value),
                        spark=self.spark
                    )
                else:
                    raise
            self.super_tracer.add_trace(
                self.superlake_dt,
                self.pipeline_name,
                "table_updated"
            )
            duration = round(time.time() - start_time, 2)
            self.logger.info(
                f"SuperSimplePipeline {self.pipeline_name} completed. "
                f"Total duration: {duration}s"
            )
            self.show_tables()
