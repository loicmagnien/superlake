"""Delta table management for SuperLake."""

# standard library imports
from typing import List, Optional, Dict, Any, Union, Tuple
from enum import Enum
from pyspark.sql import types as T
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import re
import os
import shutil
import time
import pyspark.sql.functions as F

# custom imports
from monitoring import SuperLogger


# table save mode options
class TableSaveMode(Enum):
    Append    = "append"
    Overwrite = "overwrite"
    Merge     = "merge"
    MergeSCD  = "merge_scd"

# schema evolution options
class SchemaEvolution(Enum):
    Overwrite = "overwriteSchema"
    Merge     = "mergeSchema"
    Keep      = "keepSchema"

# super delta table class
class SuperDeltaTable:
    def __init__(self,
                 table_name: str,                                           # name of the table in database_name.table_name format
                 table_path: str,                                           # path to the table in the data lake storage like .data/database_name/table_name
                 table_schema: T.StructType,                                # schema of the table as Spark StructType
                 table_save_mode: TableSaveMode,                            # save mode for the table
                 primary_keys: List[str],                                   # primary keys of the table
                 partition_cols: Optional[List[str]] = None,                # partition columns of the table
                 pruning_partition_cols: bool = True,                       # whether to prune partition columns
                 pruning_primary_keys: bool = False,                        # whether to prune primary keys
                 optimize_table: bool = False,                              # whether to optimize the table
                 optimize_zorder_cols: Optional[List[str]] = None,          # zorder columns to optimize
                 optimize_target_file_size: Optional[int] = None,           # target file size for optimization
                 compression_codec: Optional[str] = None,                   # compression codec to use
                 schema_evolution_option: Optional[SchemaEvolution] = None, # schema evolution option
                 logger: Optional[SuperLogger] = None,                      # logger to use
                 managed: bool = False,                                     # whether the table is managed or external
                 scd_change_cols: Optional[list] = None                     # columns that trigger SCD2, not including PKs
                 ):
        self.table_name = table_name
        self.table_path = table_path
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
        self.managed = managed
        self.scd_change_cols = scd_change_cols

    def check_table_name(self) -> bool:
        """Checks the table_name follows the proper database_name.table_name format."""
        pattern = r'^[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*$'
        if re.match(pattern, self.table_name):
            return True
        else:
            self.logger.warning(f"Invalid table_name format: {self.table_name}. Expected format: database_name.table_name")
            return False

    def check_table_schema(self) -> bool:
        """Checks if the Delta table schema matches the SuperDeltaTable schema."""
        try:
            delta_table = DeltaTable.forPath(self.logger.spark, self.table_path)
            delta_schema = delta_table.toDF().schema
            if delta_schema == self.table_schema:
                return True
            else:
                self.logger.warning(f"Schema mismatch: delta_schema: {delta_schema} != table_schema: {self.table_schema}")
                return False
        except Exception as e:
            self.logger.warning(f"Could not check schema: {e}")
            return False
        
    def extract_database_and_table(self) -> Tuple[str, str]:
        """Extracts the database and table from the table_name."""
        return self.table_name.split(".")
    
    def is_delta_table_path(self, spark: SparkSession) -> bool:
        """Checks if the table_path is a valid Delta table."""
        try:
            return DeltaTable.isDeltaTable(spark, self.table_path)
        except Exception:
            return False
        
    def database_exists(self, spark: SparkSession) -> bool:
        """Checks if the database_name exists in the catalog (managed) or if the path exists (external)."""
        db, _ = self.extract_database_and_table()
        if self.managed:
            # check if database directory exists in spark warehouse directory (fallback)
            warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
            warehouse_dir = re.sub(r"^file:", "", warehouse_dir)
            db_dir = os.path.join(warehouse_dir, f"{db}.db")
            return os.path.exists(db_dir)
        else:
            return os.path.exists(self.table_path)

    def table_exists(self, spark: SparkSession) -> bool:
        """Checks if the table_name exists in the catalog (managed) or if the path is a Delta table (external)."""
        db, tbl = self.extract_database_and_table()
        if self.managed:
            # check if database directory exists in spark warehouse directory
            warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
            warehouse_dir = re.sub(r"^file:", "", warehouse_dir)
            table_dir = os.path.join(warehouse_dir, f"{db}.db", tbl)
            return os.path.exists(table_dir)
        else:
            return self.is_delta_table_path(spark)

    def data_exists(self, spark: SparkSession = None) -> bool:
        """Checks if the data is present in the storage for managed or external tables."""
        if self.managed:
            warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
            warehouse_dir = re.sub(r"^file:", "", warehouse_dir)
            db, tbl = self.extract_database_and_table()
            table_dir = os.path.join(warehouse_dir, f"{db}.db", tbl)
            return os.path.exists(table_dir) and bool(os.listdir(table_dir))
        else:
            return os.path.exists(self.table_path) and bool(os.listdir(self.table_path)) 
    
    def database_and_table_exists(self, spark: SparkSession) -> bool:
        """Checks if the database and table exists in the catalog."""
        return self.database_exists(spark) and self.table_exists(spark)
    
    def register_table_in_catalog(self, spark: SparkSession):
        """
        Registers the table in the Spark catalog with the correct location,
        depending on whether it is managed or external.
        """
        db = self.table_name.split('.')[0]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db}")
        if not self.managed:
            external_path = os.path.abspath(self.table_path)
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table_name} USING DELTA LOCATION '{external_path}'")
            self.logger.info(f"Registered external Delta table {self.table_name}")
        else:
            managed_path = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
            managed_path = re.sub(r"^file:", "", managed_path)
            tbl = self.table_name.split('.')[1]
            managed_path = os.path.join(managed_path, f"{db}.db", tbl)
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table_name} USING DELTA LOCATION '{managed_path}'")
            self.logger.info(f"Registered managed Delta table {self.table_name}")

    def ensure_table_exists(self, spark: SparkSession):
        """Ensures a Delta table exists at a path, creating it if needed."""
        db, tbl = self.extract_database_and_table()
        # If MergeSCD, ensure SCD columns are present in schema (virtually)
        scd_cols = [
            ('scd_start_dt', T.TimestampType(), True),
            ('scd_end_dt', T.TimestampType(), True),
            ('scd_is_current', T.BooleanType(), True)
        ]
        effective_schema = self.table_schema
        if self.table_save_mode == TableSaveMode.MergeSCD:
            missing_scd_cols = [name for name, _, _ in scd_cols if name not in [f.name for f in self.table_schema.fields]]
            if missing_scd_cols:
                self.logger.info(f"SCD columns {missing_scd_cols} are missing from table_schema but will be considered present for MergeSCD mode.")
                # Add missing SCD columns to schema for table creation
                effective_schema = T.StructType(self.table_schema.fields + [T.StructField(name, dtype, nullable) for name, dtype, nullable in scd_cols if name in missing_scd_cols])

        # Always ensure the database exists in the catalog
        dbs_in_catalog = [row.name for row in spark.catalog.listDatabases()]
        if db not in dbs_in_catalog:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
            self.logger.info(f"Created database {db} in catalog")
        # Check if the table exists in the catalog
        if db in dbs_in_catalog and tbl in [row.name for row in spark.catalog.listTables(db)]:
            self.logger.info(f"Table {db}.{tbl} already exists in catalog")
            return

        # Dealing with the case when data directory exists and is not a Delta table
        if self.managed:
            warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
            warehouse_dir = re.sub(r"^file:", "", warehouse_dir)
            table_dir = os.path.join(warehouse_dir, f"{db}.db", tbl)
            if os.path.exists(table_dir) and not DeltaTable.isDeltaTable(spark, table_dir):
                shutil.rmtree(table_dir)
            # Now create the table
            empty_df = spark.createDataFrame([], effective_schema)
            (empty_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy(self.partition_cols)
                .saveAsTable(self.table_name))
            self.logger.info(f"Created Managed Delta table {self.table_name}")
        else:
            if not self.is_delta_table_path(spark):
                abs_path = os.path.abspath(self.table_path)
                empty_df = spark.createDataFrame([], effective_schema)
                (empty_df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .partitionBy(self.partition_cols)
                    .save(abs_path))
                self.logger.info(f"Created External Delta table {self.table_name} at {abs_path}")

        # Register the table in the catalog (for both managed and external)
        self.register_table_in_catalog(spark)

    def optimize(self, spark: SparkSession):
        """Runs OPTIMIZE and ZORDER on the Delta table, with optional file size tuning."""
        self.logger.info(f"Starting optimize for table {self.table_name}.")
        # check if table exists
        if not self.table_exists(spark):
            self.logger.info(f"Table {self.table_name} does not exist, skipping optimize")
            return
        # check if optimize_table is False
        if not self.optimize_table:
            self.logger.info(f"optimize_table is False, skipping optimize")
            return
        # Checking the ZORDER columns do not contain a partition
        if len(set(self.optimize_zorder_cols).intersection(self.partition_cols)) > 0:
            self.logger.warning(f"Table {self.table_name} could not be optimized because an optimize column is a partition column.")
            return
        # check if optimizeWrite and autoCompact are set to False
        ow = spark.conf.get('spark.databricks.delta.optimizeWrite.enabled', 'False')
        ac = spark.conf.get('spark.databricks.delta.autoCompact.enabled', 'False')
        # Fail safe in case of bad configuration to avoid drama and exit with False
        if not (ow == 'False' or not ow) and not (ac == 'False' or not ac):
            self.logger.warning(
                f"Could not optimize as either optimizeWrite or autoCompact is not set to False."
                + f" optimizeWrite = {ow}, autoCompact = {ac}.")
            return
        # Register the table in the catalog
        t0 = time.time()
        self.register_table_in_catalog(spark)
        t1 = time.time()
        # Changing target file size
        if self.optimize_target_file_size:
            spark.conf.set("spark.databricks.delta.optimize.targetFileSize", self.optimize_target_file_size)
        # General OPTIMIZE command
        optimize_sql = f"OPTIMIZE {self.table_name}"
        # ZORDER command
        if self.optimize_zorder_cols:
            optimize_zorder_cols_sanitized_str = ', '.join([f"`{col}`" for col in self.optimize_zorder_cols])
            optimize_sql += f" ZORDER BY ({optimize_zorder_cols_sanitized_str})"
        t2 = time.time()
        spark.sql(optimize_sql)
        t3 = time.time()
        self.logger.info(f"Optimized table {self.table_name} ({'managed' if self.managed else 'external'})")
        self.logger.metric("optimize_table_creation_duration_sec", round(t1-t0, 2))
        self.logger.metric("optimize_table_optimization_duration_sec", round(t3-t2, 2))
        self.logger.metric("optimize_table_total_duration_sec", round(t3-t0, 2))


    def vacuum(self, spark: SparkSession, retention_hours: int = 168):
        """Runs the VACUUM command on a Delta table to clean up old files."""
        t0 = time.time()
        self.register_table_in_catalog(spark)
        t1 = time.time()
        spark.sql(f"VACUUM {self.table_name} RETAIN {retention_hours} HOURS")
        t2 = time.time()
        self.logger.info(f"Vacuumed table {self.table_name} with retention {retention_hours} hours")
        self.logger.metric("vacuum_table_creation_duration_sec", round(t1-t0, 2))
        self.logger.metric("vacuum_table_vacuum_duration_sec", round(t2-t1, 2))
        self.logger.metric("vacuum_table_total_duration_sec", round(t2-t0, 2))

    def read(self, spark: SparkSession):
        """Returns a Spark DataFrame for the table."""
        if self.managed:
            return spark.read.table(self.table_name)
        else:
            return spark.read.format("delta").load(self.table_path)

    def _evolve_schema_if_needed(self, df, spark):
        """Evolve the Delta table schema to match the DataFrame if schema_evolution_option is Merge."""
        if self.schema_evolution_option == SchemaEvolution.Merge:
            if self.managed:
                current_cols = set(spark.read.table(self.table_name).columns)
            else:
                current_cols = set(spark.read.format("delta").load(self.table_path).columns)
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
                    writer.saveAsTable(self.table_name)
                else:
                    writer.save(self.table_path)

    def _align_df_to_table_schema(self, df, spark):
        """Align DataFrame columns to match the target table schema (cast types, add missing columns as nulls, drop extra columns if configured)."""
        # Get the target schema (from the table if it exists, else from self.table_schema)
        if self.table_exists(spark):
            if self.managed:
                target_schema = spark.read.table(self.table_name).schema
            else:
                target_schema = spark.read.format("delta").load(self.table_path).schema
        else:
            target_schema = self.table_schema

        df_dtypes = dict(df.dtypes)
        missing_columns = []
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
        if self.schema_evolution_option == SchemaEvolution.Merge or self.schema_evolution_option == SchemaEvolution.Overwrite:
            if extra_columns:
                self.logger.info(f"Retaining extra columns (schema_evolution_option=Merge): {extra_columns}")
            # Keep all columns: union of DataFrame and target schema
            # Ensure all target schema columns are present (already handled above)
            # No need to drop extra columns
        elif self.schema_evolution_option == SchemaEvolution.Keep:
            if extra_columns:
                self.logger.info(f"Dropping extra columns (schema_evolution_option=Keep): {extra_columns}")
            df = df.select([f.name for f in target_schema])
        if missing_columns:
            self.logger.info(f"Added missing columns as nulls: {missing_columns}")
        return df

    def _get_delta_table(self, spark):
        """Return the correct DeltaTable object for managed or external tables."""
        if self.managed:
            return DeltaTable.forName(spark, self.table_name)
        else:
            return DeltaTable.forPath(spark, self.table_path)

    def _write_df(self, df, mode, merge_schema=False, overwrite_schema=False):
        writer = df.write.format("delta").mode(mode)
        if self.partition_cols:
            writer = writer.partitionBy(self.partition_cols)
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        if self.managed:
            writer.saveAsTable(self.table_name)
        else:
            writer.save(self.table_path)

    def _get_merge_condition_and_updates(self, df, scd_change_cols=None):
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

    def _merge(self, df, spark):
        self._evolve_schema_if_needed(df, spark)
        delta_table = self._get_delta_table(spark)
        cond, updates, _ = self._get_merge_condition_and_updates(df)
        delta_table.alias("target").merge(
            df.alias("source"), cond
        ).whenMatchedUpdate(set=updates).whenNotMatchedInsert(values=updates).execute()

    def _merge_scd(self, df, spark):
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
        df = self._align_df_to_table_schema(df, spark)
        if not self.table_exists(spark):
            self.logger.info(f"Table {self.table_name} does not exist, creating it")
            self.ensure_table_exists(spark)
        self._evolve_schema_if_needed(df, spark)
        delta_table = self._get_delta_table(spark)
        cond, updates, change_cond = self._get_merge_condition_and_updates(df, self.scd_change_cols)
        # Step 1: Update old row to set scd_is_current = false and scd_end_dt, only if change_cond is true
        update_condition = f"target.scd_is_current = true"
        if change_cond:
            update_condition += f" AND ({change_cond})"
        delta_table.alias("target").merge(
            df.alias("source"), cond
        ).whenMatchedUpdate(
            condition=update_condition,
            set={"scd_is_current": "false", "scd_end_dt": "source.scd_start_dt"}
        ).execute()
        # Step 2: Append the new row(s) as current (only those where change_cond is true)
        current_rows = df.withColumn("scd_is_current", F.lit(True)).withColumn("scd_end_dt", F.lit(None).cast("timestamp"))
        self._write_df(current_rows, "append")

    def save(self, df, mode: str = 'append', spark: SparkSession = None):
        """Writes a DataFrame to a Delta table, supporting append, merge, merge_scd, and overwrite modes."""
        spark = spark or self.logger.spark

        # Always ensure table exists before any operation
        if not self.table_exists(spark):
            self.logger.info(f"Table {self.table_name} does not exist, creating it")
            self.ensure_table_exists(spark)
        if mode == 'merge_scd':
            self._merge_scd(df, spark)
        elif mode == 'merge':
            df = self._align_df_to_table_schema(df, spark)
            self._merge(df, spark)
        elif mode == 'append':
            df = self._align_df_to_table_schema(df, spark)
            self._write_df(
                df,
                "append",
                merge_schema=(self.schema_evolution_option == SchemaEvolution.Merge)
            )
        elif mode == 'overwrite':
            df = self._align_df_to_table_schema(df, spark)
            self._write_df(
                df,
                "overwrite",
                merge_schema=(self.schema_evolution_option == SchemaEvolution.Merge),
                overwrite_schema=True
            )
        else:
            raise ValueError(f"Unknown save mode: {mode}")
        self.logger.info(f"Saved DataFrame to {self.table_name} in mode {mode}")

    def delete(self, df, spark: SparkSession = None):
        """Deletes the content of the Spark DataFrame from the Table handling partition and primary keys pruning."""
        spark = spark or self.logger.spark
        delta_table = DeltaTable.forPath(spark, self.table_path)
        cond = ' AND '.join([f"target.{k}=source.{k}" for k in self.primary_keys])
        join_keys = self.primary_keys
        join_expr = [df[k] == delta_table.toDF()[k] for k in join_keys]
        keys_df = df.select(*join_keys).dropDuplicates()
        for row in keys_df.collect():
            where_clause = ' AND '.join([f"{k} = '{row[k]}'" for k in join_keys])
            delta_table.delete(where_clause)
        self.logger.info(f"Deleted rows from {self.table_name} matching DataFrame keys")

    def drop(self, spark: SparkSession = None):
        """Drops the table from the catalog and removes the data files in storage."""
        spark = spark or self.logger.spark
        spark.sql(f"DROP TABLE IF EXISTS {self.table_name}")
        if self.managed:
            db, tbl = self.extract_database_and_table()
            warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
            warehouse_dir = re.sub(r"^file:", "", warehouse_dir)
            table_dir = os.path.join(warehouse_dir, f"{db}.db", tbl)
            if os.path.exists(table_dir):
                shutil.rmtree(table_dir)
            self.logger.info(f"Dropped Managed Delta Table {self.table_name} and removed data at {table_dir}")
        else:
            shutil.rmtree(self.table_path, ignore_errors=True)
            self.logger.info(f"Dropped External Delta Table {self.table_name} and removed data at {self.table_path}")


        
