"""Delta table management for SuperLake."""

# standard library imports
from typing import List, Optional, Dict, Any
from enum import Enum
from pyspark.sql import types as T, DataFrame
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import re
import os
import shutil
import time
import pyspark.sql.functions as F
from datetime import datetime

# custom imports
from superlake.monitoring import SuperLogger
from superlake.core import SuperSpark


# table save mode options
class TableSaveMode(Enum):
    Append = "append"
    Overwrite = "overwrite"
    Merge = "merge"
    MergeSCD = "merge_scd"


# schema evolution options
class SchemaEvolution(Enum):
    Overwrite = "overwriteSchema"
    Merge = "mergeSchema"
    Keep = "keepSchema"


# super delta table class
class SuperDeltaTable:
    def __init__(
        self,
        super_spark: SuperSpark,
        catalog_name: Optional[str],
        schema_name: str,
        table_name: str,
        table_schema: T.StructType,
        table_save_mode: TableSaveMode,
        primary_keys: List[str],
        partition_cols: Optional[List[str]] = None,
        pruning_partition_cols: bool = True,
        pruning_primary_keys: bool = False,
        optimize_table: bool = False,
        optimize_zorder_cols: Optional[List[str]] = None,
        optimize_target_file_size: Optional[int] = None,
        compression_codec: Optional[str] = None,
        schema_evolution_option: Optional[SchemaEvolution] = None,
        logger: Optional[SuperLogger] = None,
        managed: bool = False,
        scd_change_cols: Optional[List[str]] = None,
        table_path: Optional[str] = None,
        generated_columns: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Initialize a SuperDeltaTable instance.
        Args:
            super_spark (SuperSpark): The SuperSpark instance.
            catalog_name (str): Catalog name (can be None for classic Spark).
            schema_name (str): Schema name.
            table_name (str): Table name.
            table_schema (StructType): Schema of the table as Spark StructType.
            table_save_mode (TableSaveMode): Save mode for the table.
            primary_keys (List[str]): Primary keys of the table.
            partition_cols (Optional[List[str]]): Partition columns of the table.
            pruning_partition_cols (bool): Whether to prune partition columns.
            pruning_primary_keys (bool): Whether to prune primary keys.
            optimize_table (bool): Whether to optimize the table.
            optimize_zorder_cols (Optional[List[str]]):Zorder columns to optimize.
            optimize_target_file_size (Optional[int]): Target file size for optimization.
            compression_codec (Optional[str]): Compression codec to use.
            schema_evolution_option (Optional[SchemaEvolution]):Schema evolution option.
            logger (Optional[SuperLogger]): Logger to use.
            managed (bool): Whether the table is managed or external.
            scd_change_cols (Optional[list]): Columns that trigger SCD2, not including PKs.
            table_path (Optional[str]): For external tables (defaults to external_path/schema_name/table_name).
            generated_columns (Optional[Dict[str, str]]): Generated columns and their formulas,
            e.g. {"trace_year": "YEAR(trace_dt)"}
        """
        self.super_spark = super_spark
        self.spark = self.super_spark.spark
        self.warehouse_dir = self.super_spark.warehouse_dir
        self.external_path = self.super_spark.external_path
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.managed = managed
        if managed:
            self.table_path = None  # managed tables use warehouse_dir
        else:
            self.table_path = table_path or os.path.join(self.external_path, schema_name, table_name)
        self.table_schema = table_schema
        self.table_save_mode = table_save_mode
        self.primary_keys = primary_keys
        self.partition_cols = partition_cols or []
        self.pruning_partition_cols = pruning_partition_cols
        self.pruning_primary_keys = pruning_primary_keys
        self.optimize_table = optimize_table
        self.optimize_zorder_cols = optimize_zorder_cols or []
        self.optimize_target_file_size = optimize_target_file_size
        self.compression_codec = compression_codec
        self.schema_evolution_option = schema_evolution_option
        self.logger = logger or SuperLogger()
        self.scd_change_cols = scd_change_cols
        self.generated_columns = generated_columns or {}
    
    def full_table_name(self) -> str:
        """
        Returns the fully qualified table name for Spark SQL operations.
        Use only for Spark SQL, not for DeltaTable.forName.
        args:
            None
        returns:
            str: The fully qualified catalog.schema.table name.
        """
        if self.catalog_name:
            return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"
        elif hasattr(self, 'super_spark') and getattr(self.super_spark, 'catalog_name', None):
            return f"{self.super_spark.catalog_name}.{self.schema_name}.{self.table_name}"
        else:
            return f"{self.schema_name}.{self.table_name}"
        
    def forname_table_name(self) -> str:
        """
        Returns the table name in schema.table format for DeltaTable.forName.
        args:
            None
        returns:
            str: The table name in schema.table format.
        """
        return f"{self.schema_name}.{self.table_name}"

    def check_table_schema(self, check_nullability: bool = False) -> bool:
        """
        Checks if the Delta table schema matches the SuperDeltaTable schema.
        If check_nullability is False, only field names and types are compared (not nullability).
        If True, the full schema including nullability is compared.
        args:
            check_nullability (bool): Whether to check nullability.
        returns:
            bool: True if the schema matches, False otherwise.
        """
        try:
            if self.managed:
                delta_table = DeltaTable.forName(self.super_spark.spark, self.forname_table_name())
            else:
                delta_table = DeltaTable.forPath(self.super_spark.spark, self.table_path)
            delta_schema = delta_table.toDF().schema
            if check_nullability:
                match = delta_schema == self.table_schema
            else:
                # Compare only field names and types, ignore nullability
                def fields_no_null(schema: T.StructType) -> List[Any]:
                    return [(f.name, f.dataType) for f in schema.fields]
                match = fields_no_null(delta_schema) == fields_no_null(self.table_schema)
            if match:
                return True
            else:
                self.logger.warning(
                    f"Schema mismatch: delta_schema: {delta_schema} != table_schema: {self.table_schema}"
                )
                return False
        except Exception as e:
            self.logger.warning(f"Could not check schema: {e}")
            return False
        
    def get_table_path(self, spark: SparkSession) -> str:
        """
        Returns the table path for managed or external tables.
        args:
            spark (SparkSession): The Spark session.
        returns:
            str: The table path.
        """
        if self.managed:
            # get the absolute path
            table_path = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
            table_path = re.sub(r"^file:", "", table_path)
            table_path = os.path.join(table_path, f"{self.schema_name}.db", self.table_name)
        else:
            table_path = self.table_path
            table_path = os.path.abspath(table_path)
        return table_path
    
    def get_schema_path(self, spark: SparkSession) -> str:
        """
        Returns the schema path for managed or external tables.
        args:
            spark (SparkSession): The Spark session.
        returns:
            str: The schema path.
        """
        if self.managed:
            schema_path = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
            schema_path = re.sub(r"^file:", "", schema_path)
            schema_path = os.path.join(schema_path, f"{self.schema_name}.db")
        else:
            table_path = os.path.abspath(self.table_path)
            schema_path = os.path.dirname(table_path)
        return schema_path
    
    def is_delta_table_path(self, spark: SparkSession) -> bool:
        """
        Checks if the table_path is a valid Delta table.
        args:
            spark (SparkSession): The Spark session.
        returns:
            bool: True if the table_path is a valid Delta table, False otherwise.
        """
        table_path = self.get_table_path(spark)
        try:
            return DeltaTable.isDeltaTable(spark, table_path)
        except Exception:
            return False
        
    def table_exists(self, spark: SparkSession) -> bool:
        """
        Checks if the table exists in the catalog (managed) or if the path is a Delta table (external).
        args:
            spark (SparkSession): The Spark session.
        returns:
            bool: True if the table exists, False otherwise.
        """
        if self.managed:
            table_path = self.get_table_path(spark)
            return os.path.exists(table_path)
        else:
            return self.is_delta_table_path(spark)

    def schema_exists(self, spark: SparkSession) -> bool:
        """
        Checks if the schema exists in the catalog (managed) or if the path exists (external).
        args:
            spark (SparkSession): The Spark session.
        returns:
            bool: True if the schema exists, False otherwise.
        """
        schema_path = self.get_schema_path(spark)
        return os.path.exists(schema_path)

    def data_exists(self, spark: Optional[SparkSession] = None) -> bool:
        """
        Checks if the data is present in the storage for managed or external tables.
        args:
            spark (SparkSession): The Spark session.
        returns:
            bool: True if the data exists, False otherwise.
        """
        table_path = self.get_table_path(spark)
        return os.path.exists(table_path) and bool(os.listdir(table_path)) 
    
    def schema_and_table_exists(self, spark: SparkSession) -> bool:
        """
        Checks if the schema and table exists in the catalog.
        args:
            spark (SparkSession): The Spark session.
        returns:
            bool: True if the schema and table exists, False otherwise.
        """
        return self.schema_exists(spark) and self.table_exists(spark)
    
    def ensure_schema_exists(self, spark: SparkSession):
        """
        Ensures a schema exists in the catalog.
        args:
            spark (SparkSession): The Spark session.
        returns:
            None
        """
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")

    def register_table_in_catalog(self, spark: SparkSession, log=True):
        """
        Registers the table in the Spark catalog with the correct location,
        depending on whether it is managed or external.
        args:
            spark (SparkSession): The Spark session.
        returns:
            None
        """
        self.ensure_schema_exists(spark)
        table_path = self.get_table_path(spark)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.full_table_name()} USING DELTA LOCATION '{table_path}'")
        log and self.logger.info(
            f"Registered {'managed' if self.managed else 'external'} Delta table {self.full_table_name()}"
            )

    def ensure_table_exists(self, spark: SparkSession, log=True):
        """Ensures a Delta table exists at a path, creating it if needed."""
        # If MergeSCD, ensure SCD columns are present in schema (virtually)
        scd_cols = [
            ('scd_start_dt', T.TimestampType(), True),
            ('scd_end_dt', T.TimestampType(), True),
            ('scd_is_current', T.BooleanType(), True)
        ]
        effective_schema = self.table_schema
        if self.table_save_mode == TableSaveMode.MergeSCD:
            missing_scd_cols = [
                name for name, _, _ in scd_cols 
                if name not in [f.name for f in self.table_schema.fields]
            ]
            if missing_scd_cols:
                log and self.logger.info(
                    f"SCD columns {missing_scd_cols} are missing from table_schema "
                    f"but will be considered present for MergeSCD mode."
                )
                effective_schema = T.StructType(
                    self.table_schema.fields + [
                        T.StructField(name, dtype, nullable) 
                        for name, dtype, nullable in scd_cols 
                        if name in missing_scd_cols
                    ]
                )
        # Always ensure the schema exists in the catalog
        schemas_in_catalog = [row.name for row in spark.catalog.listDatabases()]
        if self.schema_name not in schemas_in_catalog:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema_name}")
            log and self.logger.info(f"Created schema {self.schema_name} in catalog")
        # Check if the table exists in the catalog
        tables_in_schema = [row.name for row in spark.catalog.listTables(self.schema_name)]
        if self.schema_name in schemas_in_catalog and self.table_name in tables_in_schema:
            log and self.logger.info(f"Table {self.full_table_name()} found in catalog")
            return
        # Dealing with the case when data directory exists and is not a Delta table
        if self.managed:
            table_path = self.get_table_path(spark)
            if os.path.exists(table_path) and not DeltaTable.isDeltaTable(spark, table_path):
                shutil.rmtree(table_path)
            # Now create the table
            empty_df = spark.createDataFrame([], effective_schema)
            (empty_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy(self.partition_cols)
                .saveAsTable(self.full_table_name()))
            log and self.logger.info(f"Created Managed Delta table {self.full_table_name()}")
        else:
            if not self.is_delta_table_path(spark):
                abs_path = os.path.abspath(self.table_path)
                empty_df = spark.createDataFrame([], effective_schema)
                # Use DeltaTable builder to support generated columns
                builder = DeltaTable.createIfNotExists(spark)
                builder = builder.tableName(f"{self.schema_name}.{self.table_name}")
                for field in effective_schema:
                    col_name = field.name
                    col_type = field.dataType
                    if col_name in self.generated_columns:
                        builder = builder.addColumn(
                            col_name, 
                            col_type, 
                            generatedAlwaysAs=self.generated_columns[col_name]
                        )
                    else:
                        builder = builder.addColumn(col_name, col_type)
                if self.table_path:
                    builder = builder.location(abs_path)
                if self.partition_cols:
                    builder = builder.partitionedBy(*self.partition_cols)
                builder = builder.property("delta.autoOptimize.optimizeWrite", "true")
                builder = builder.property("delta.autoOptimize.autoCompact", "true")
                builder.execute()
                log and self.logger.info(
                    f"Created External Delta table {self.full_table_name()} "
                    f"with generated columns {self.generated_columns}"
                )

        # Register the table in the catalog (for both managed and external)
        self.register_table_in_catalog(spark, log=log)

    def optimize(self, spark: SparkSession):
        """Runs OPTIMIZE and ZORDER on the Delta table, with optional file size tuning."""
        self.logger.info(f"Starting optimize for table {self.full_table_name()}.")
        # check if table exists
        if not self.table_exists(spark):
            self.logger.info(f"Table {self.full_table_name()} does not exist, skipping optimize")
            return
        # check if optimize_table is False
        if not self.optimize_table:
            self.logger.info("optimize_table is False, skipping optimize")
            return
        # Checking the ZORDER columns do not contain a partition
        if len(set(self.optimize_zorder_cols).intersection(self.partition_cols)) > 0:
            self.logger.warning(
                f"Table {self.full_table_name()} could not be optimized "
                f"because an optimize column is a partition column."
            )
            return
        # check if optimizeWrite and autoCompact are set to False
        ow = spark.conf.get('spark.databricks.delta.optimizeWrite.enabled', 'False')
        ac = spark.conf.get('spark.databricks.delta.autoCompact.enabled', 'False')
        # Fail safe in case of bad configuration to avoid drama and exit with False
        if not (ow == 'False' or not ow) and not (ac == 'False' or not ac):
            self.logger.warning(
                "Could not optimize as either optimizeWrite or autoCompact is not set to False. "
                f"optimizeWrite = {ow}, autoCompact = {ac}.")
            return
        # Register the table in the catalog
        t0 = time.time()
        self.register_table_in_catalog(spark, log=False)
        t1 = time.time()
        # Changing target file size
        if self.optimize_target_file_size:
            spark.conf.set("spark.databricks.delta.optimize.targetFileSize", self.optimize_target_file_size)
        # General OPTIMIZE command
        optimize_sql = f"OPTIMIZE {self.full_table_name()}"
        # ZORDER command
        if self.optimize_zorder_cols:
            optimize_zorder_cols_sanitized_str = ', '.join([f"`{col}`" for col in self.optimize_zorder_cols])
            optimize_sql += f" ZORDER BY ({optimize_zorder_cols_sanitized_str})"
        t2 = time.time()
        spark.sql(optimize_sql)
        t3 = time.time()
        self.logger.info(f"Optimized table {self.full_table_name()} ({'managed' if self.managed else 'external'})")
        self.logger.metric("optimize_table_creation_duration_sec", round(t1-t0, 2))
        self.logger.metric("optimize_table_optimization_duration_sec", round(t3-t2, 2))
        self.logger.metric("optimize_table_total_duration_sec", round(t3-t0, 2))

    def vacuum(self, spark: SparkSession, retention_hours: int = 168):
        """Runs the VACUUM command on a Delta table to clean up old files."""
        t0 = time.time()
        self.register_table_in_catalog(spark)
        t1 = time.time()
        spark.sql(f"VACUUM {self.full_table_name()} RETAIN {retention_hours} HOURS")
        t2 = time.time()
        self.logger.info(f"Vacuumed table {self.full_table_name()} with retention {retention_hours} hours")
        self.logger.metric("vacuum_table_creation_duration_sec", round(t1-t0, 2))
        self.logger.metric("vacuum_table_vacuum_duration_sec", round(t2-t1, 2))
        self.logger.metric("vacuum_table_total_duration_sec", round(t2-t0, 2))

    def read(self, spark: SparkSession) -> DataFrame:
        """Returns a Spark DataFrame for the table."""
        if self.managed:
            return spark.read.table(self.full_table_name())
        else:
            return spark.read.format("delta").load(self.table_path)

    def evolve_schema_if_needed(self, df, spark):
        """Evolve the Delta table schema to match the DataFrame if schema_evolution_option is Merge."""
        if self.schema_evolution_option == SchemaEvolution.Merge:
            # Get current table columns
            if self.table_exists(spark):
                if self.managed:
                    current_cols = set(spark.read.table(self.full_table_name()).columns)
                else:
                    current_cols = set(spark.read.format("delta").load(self.table_path).columns)
            else:
                current_cols = set()
            new_cols = set(df.columns) - current_cols
            if new_cols:
                dummy_df = spark.createDataFrame([], df.schema)
                writer = (
                    dummy_df.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                )
                if self.managed:
                    writer.saveAsTable(self.full_table_name())
                else:
                    writer.save(self.table_path)

    def align_df_to_table_schema(self, df, spark):
        """
        Align DataFrame columns to match the target table schema (cast types, add missing columns as nulls,
        drop extra columns if configured).
        args:
            df (DataFrame): The DataFrame to align.
            spark (SparkSession): The Spark session.
        returns:
            DataFrame: The aligned DataFrame.
        """
        # Get the target schema (from the table if it exists, else from self.table_schema)
        if self.table_exists(spark):
            if self.managed:
                target_schema = spark.read.table(self.full_table_name()).schema
            else:
                target_schema = spark.read.format("delta").load(self.table_path).schema
        else:
            target_schema = self.table_schema

        df_dtypes = dict(df.dtypes)
        missing_columns: List[str] = []
        for field in target_schema:
            if field.name in df.columns:
                # Compare Spark SQL type names
                if df_dtypes[field.name] != field.dataType.simpleString():
                    df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
            else:
                # Add missing columns as nulls
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
                missing_columns.append(field.name)
        extra_columns = [col for col in df.columns if col not in [f.name for f in target_schema]]
        if self.schema_evolution_option in (SchemaEvolution.Merge, SchemaEvolution.Overwrite):
            if extra_columns:
                self.logger.info(
                    f"Retaining extra columns (schema_evolution_option=Merge): {extra_columns}"
                )
            # Keep all columns: union of DataFrame and target schema
            # Ensure all target schema columns are present (already handled above)
            # No need to drop extra columns
        elif self.schema_evolution_option == SchemaEvolution.Keep:
            if extra_columns:
                self.logger.info(
                    f"Dropping extra columns (schema_evolution_option=Keep): {extra_columns}"
                )
            df = df.select([f.name for f in target_schema])
        if missing_columns:
            self.logger.info(f"Added missing columns as nulls: {missing_columns}")
        return df

    def get_delta_table(self, spark):
        """Return the correct DeltaTable object for managed or external tables."""
        if self.managed:
            return DeltaTable.forName(spark, self.forname_table_name())
        else:
            return DeltaTable.forPath(spark, self.table_path)

    def write_df(
        self,
        df: DataFrame,
        mode: str,
        merge_schema: bool = False,
        overwrite_schema: bool = False
    ) -> None:
        if merge_schema:
            df = self.align_df_to_table_schema(df, self.spark)
        writer = df.write.format("delta").mode(mode)
        if self.partition_cols:
            writer = writer.partitionBy(self.partition_cols)
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        if self.managed:
            writer.saveAsTable(self.forname_table_name())  # was full_table_name()
        else:
            writer.save(self.table_path)

    def get_merge_condition_and_updates(self, df: DataFrame, scd_change_cols: Optional[List[str]] = None):
        cond = ' AND '.join([f"target.{k}=source.{k}" for k in self.primary_keys])
        updates = {c: f"source.{c}" for c in df.columns}
        # SCD2 change detection condition
        if scd_change_cols is None:
            # Default: all non-PK, non-SCD columns
            scd_change_cols = [c for c in df.columns if c not in self.primary_keys and not c.startswith('scd_')]
        else:
            # Ensure PKs are not in scd_change_cols (should already be validated in __init__)
            scd_change_cols = [c for c in scd_change_cols if c not in self.primary_keys]
        change_cond = ' OR '.join([f"target.{c} <> source.{c}" for c in scd_change_cols]) if scd_change_cols else None
        return cond, updates, change_cond

    def merge(self, df: DataFrame, spark: SparkSession):
        self.evolve_schema_if_needed(df, spark)
        delta_table = self.get_delta_table(spark)
        cond, updates, _ = self.get_merge_condition_and_updates(df)
        delta_table.alias("target").merge(
            df.alias("source"), cond
        ).whenMatchedUpdate(set=updates).whenNotMatchedInsert(values=updates).execute()

    def merge_scd(self, df: DataFrame, spark: SparkSession):
        # Validate scd_change_cols here
        if self.scd_change_cols is not None:
            for col in self.scd_change_cols:
                if col in self.primary_keys:
                    raise ValueError(f"scd_change_cols cannot include primary key column: {col}")
        # Automatically add SCD columns if not provided by the user
        if 'scd_start_dt' not in df.columns:
            if 'superlake_dt' in df.columns:
                df = df.withColumn('scd_start_dt', F.col('superlake_dt'))
            else:
                df = df.withColumn('scd_start_dt', F.current_timestamp())
        if 'scd_end_dt' not in df.columns:
            df = df.withColumn('scd_end_dt', F.lit(None).cast('timestamp'))
        if 'scd_is_current' not in df.columns:
            df = df.withColumn('scd_is_current', F.lit(True))
        df = self.align_df_to_table_schema(df, spark)
        if not self.table_exists(spark):
            self.logger.info(f"Table {self.full_table_name()} does not exist, creating it")
            self.ensure_table_exists(spark)
        self.evolve_schema_if_needed(df, spark)
        delta_table = self.get_delta_table(spark)
        cond, updates, change_cond = self.get_merge_condition_and_updates(df, self.scd_change_cols)
        # Step 1: Update old row to set scd_is_current = false and scd_end_dt, only if change_cond is true
        update_condition = "target.scd_is_current = true"
        if change_cond:
            update_condition += f" AND ({change_cond})"
        delta_table.alias("target").merge(
            df.alias("source"), cond
        ).whenMatchedUpdate(
            condition=update_condition,
            set={"scd_is_current": "false", "scd_end_dt": "source.scd_start_dt"}
        ).execute()
        # Step 2: Append the new row(s) as current and not already in the table (for scd_is_current = true)
        filtered_df = df.join(
            delta_table.toDF().filter(F.col("scd_is_current") == True),
            on=self.primary_keys,
            how="left_anti"
        )
        current_rows = (
            filtered_df
            .withColumn("scd_is_current", F.lit(True))
            .withColumn("scd_end_dt", F.lit(None).cast("timestamp"))
        )
        self.write_df(current_rows, "append")

    def save(self, df: DataFrame, mode: str = 'append', spark: Optional[SparkSession] = None, log=True):
        """Writes a DataFrame to a Delta table, supporting append, merge, merge_scd, and overwrite modes."""
        start_time = time.time()
        spark = self.super_spark.spark
        # Always ensure table exists before any operation
        if not self.table_exists(spark):
            log and self.logger.info(f"Table {self.full_table_name()} does not exist, creating it")
            self.ensure_table_exists(spark)

        if mode == 'merge_scd':
            self.merge_scd(df, spark)
        elif mode == 'merge':
            df = self.align_df_to_table_schema(df, spark)
            self.merge(df, spark)
        elif mode == 'append':
            df = self.align_df_to_table_schema(df, spark)
            self.write_df(
                df,
                "append",
                merge_schema=(self.schema_evolution_option == SchemaEvolution.Merge)
            )
        elif mode == 'overwrite':
            df = self.align_df_to_table_schema(df, spark)
            self.write_df(
                df,
                "overwrite",
                merge_schema=(self.schema_evolution_option == SchemaEvolution.Merge),
                overwrite_schema=True
            )
        else:
            raise ValueError(f"Unknown save mode: {mode}")
        log and self.logger.info(f"Saved data to {self.full_table_name()} ({mode})")
        log and self.logger.metric(f"{self.full_table_name()}.save_row_count", df.count())
        log and self.logger.metric(f"{self.full_table_name()}.save_duration_sec", round(time.time() - start_time, 2))

    def delete(self, deletions_df: DataFrame, superlake_dt: Optional[datetime] = None):
        """Delete all rows from the table that match the deletions_df based on the primary keys.

        :param deletions_df: The DataFrame to delete from the original delta table
        :param superlake_dt: The timestamp to use for scd_end_dt
        """
        start_time = time.time()
        if superlake_dt is None:
            superlake_dt = datetime.now()
        spark = self.super_spark.spark
        if self.table_exists(spark):
            # Loading the source table and original count
            target_table = DeltaTable.forName(spark, self.forname_table_name())
            
            # counting the number of rows to delete
            to_delete_count = deletions_df.count()
            if to_delete_count > 0:
                if self.table_save_mode == TableSaveMode.MergeSCD:
                    original_count = target_table.toDF().filter(F.col("scd_is_current") == True).count()
                    # filter the deletions_df to only include rows where scd_is_current is true
                    deletions_df = deletions_df.filter(F.col("scd_is_current") == True)
                    self.logger.info(f"{to_delete_count} SCD rows expected to be closed in {self.full_table_name()}.")
                    pk_condition = " AND ".join([f"original.`{pk}` = deletion.`{pk}`" for pk in self.primary_keys])
                    pk_condition += " AND original.scd_is_current = true"
                    (
                        target_table.alias("original")
                        .merge(
                            source=deletions_df.alias("deletion"),
                            condition=pk_condition
                        )
                        .whenMatchedUpdate(
                            set={
                                "scd_end_dt": (
                                    f"timestamp'{superlake_dt}'"
                                    if isinstance(superlake_dt, datetime)
                                    else "deletion.superlake_dt"
                                ),
                                "scd_is_current": "false"
                            }
                        )
                        .execute()
                    )
                    final_count = target_table.toDF().filter(F.col("scd_is_current")).count()
                elif self.table_save_mode in (TableSaveMode.Append, TableSaveMode.Merge, TableSaveMode.Overwrite):
                    original_count = target_table.toDF().count()
                    self.logger.info(f"{to_delete_count} rows expected to be deleted from {self.full_table_name()}.")
                    # Creating a condition to check all primary keys of the table against the deletion df
                    pk_condition = " AND ".join([f"original.`{pk}` = deletion.`{pk}`" for pk in self.primary_keys])
                    # Execute the deletion
                    (
                        target_table.alias("original")
                        .merge(
                            source=deletions_df.alias("deletion"),
                            condition=pk_condition)
                        .whenMatchedDelete()
                        .execute()
                    )
                    # Counting the number of rows after the deletion
                    final_count = target_table.toDF().count()
                self.logger.info(f"{original_count - final_count} rows deleted from {self.full_table_name()}.")
                self.logger.metric(f"{self.full_table_name()}.delete_rows_deleted", original_count - final_count)
            else:
                self.logger.info(f"Skipped deletion for {self.full_table_name()}.")
                self.logger.metric(f"{self.full_table_name()}.delete_rows_deleted", 0)
        else:
            self.logger.error(f"Table {self.full_table_name()} does not exist.")
            self.logger.metric(f"{self.full_table_name()}.delete_rows_deleted", 0)
            self.logger.metric(f"{self.full_table_name()}.delete_duration_sec", round(time.time() - start_time, 2))
     
    def drop(self, spark: Optional[SparkSession] = None):
        """Drops the table from the catalog and removes the data files in storage."""
        spark = self.super_spark.spark
        spark.sql(f"DROP TABLE IF EXISTS {self.full_table_name()}")
        if self.managed:
            table_path = self.get_table_path(spark)
            if os.path.exists(table_path):
                shutil.rmtree(table_path)
            self.logger.info(f"Dropped Delta Table {self.full_table_name()} (managed) and removed files")
        else:
            shutil.rmtree(self.table_path, ignore_errors=True)
            self.logger.info(f"Dropped Delta Table {self.full_table_name()} (external) and removed files")





    

