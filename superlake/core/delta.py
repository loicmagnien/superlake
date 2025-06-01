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
    """
    SuperDeltaTable provides unified management for Delta tables across Spark,
    Databricks and with or without Unity Catalogs environments.

    Catalog and Table Type Explanations:
    ------------------------------------
    1. hive_metastore:
        - This is the first generation of the catalog for Spark, representing the old Hive-based metastore.
        - You can still use hive_metastore on Databricks (not a Unity Catalog catalog).

    2. spark_catalog:
        - This is the second generation of the catalog for Spark, representing the new Spark SQL catalog.
        - The default Spark SQL catalog in open-source Spark (not necessarily Databricks).
        - On Databricks, spark_catalog is an alias for hive_metastore for compatibility.
        - It is still not a Unity Catalog catalog.

    3. Unity Catalog:
        - Unity Catalog is the latest generation of the catalog for Spark.
        - Unity Catalog catalogs are user-defined and created via Databricks admin tools or SQL.
        They usually look like: main, dev, prod, my_company_catalog, etc.
        - These catalogs are always distinct from hive_metastore and spark_catalog.

    4. Delta Tables:
        - Delta tables are tables that are managed by Delta Lake.
        - They are stored in the Delta table format and can be used with the Delta API.
        - When a catalog is used, they can also be used with the catalog and SQL APIs.
        - There are two types of Delta tables:

            - Managed Table:
                - Data and metadata are managed by Spark/Databricks.
                - Data is deleted on DROP TABLE.
                - In Unity Catalog, managed tables must use cloud storage
                (S3, ADLS, GCS), they do not use dbfs:/ or file:/, only urls.
                - the paths for managed tables are different for legacy and UC catalogs:
                    - legacy: spark.sql.warehouse.dir/ + schema.db/table/
                    - databricks: dbfs:/user/hive/warehouse/ + schema.db/table/
                    - UC: abfss://container@account.dfs.core.windows.net/UUID/tables/UUID/
                    also refered as the metastore_default_location + /tables/UUID/

            - External Table:
                - Only metadata is managed by Spark/Databricks.
                - Data is NOT deleted on DROP TABLE, only the metadata in the catalog is deleted.
                - In Unity Catalog, external tables must use cloud storage URIs (not dbfs:/ or file:/).
                - the paths for external tables are different for legacy and UC catalogs:
                    - legacy: /User/data/custom_path/schema/table/
                    - databricks: /mnt/custom_path/schema/table/
                    - UC : storing tables externally in UC requires creating an external location first:
                        CREATE EXTERNAL LOCATION IF NOT EXISTS `external_location`
                        URL 'abfss://container@account.dfs.core.windows.net/'
                        WITH (STORAGE CREDENTIAL `external_storage_credential`)
                    - then the external table path is:
                        abfss://container@account.dfs.core.windows.net/custom_path/schema/table/

    Note about .db Suffix in Schema/Database Paths:
    -----------------------------------------------
    - In legacy Hive and Spark SQL (hive_metastore, spark_catalog), schemas (a.k.a. databases)
      are represented as directories with a `.db` suffix in the warehouse directory.
    - For example, a table `my_schema.my_table` will be stored at `.../spark-warehouse/my_schema.db/my_table/`.
    - This convention helps Spark/Hive distinguish schema directories from other files.
    - In Unity Catalog, this `.db` convention is not used; data is managed in cloud storage with a different structure.
    - The `.db` suffix is only relevant for legacy catalogs and local Spark/Hive deployments.
    """

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
        generated_columns: Optional[Dict[str, str]] = None,
        delta_properties: Optional[Dict[str, str]] = None,
        table_description: Optional[str] = None
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
            table_properties (Optional[Dict[str, str]]): Table properties to set.
        """
        self.super_spark = super_spark
        self.spark = self.super_spark.spark
        self.warehouse_dir = self.super_spark.warehouse_dir
        self.external_path = self.super_spark.external_path
        self.catalog_name = catalog_name or self.super_spark.catalog_name
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
        self.delta_properties = delta_properties or {}
        self.table_description = table_description

    def is_unity_catalog(self):
        """
        Checks if the catalog is Unity Catalog.
        """
        # Simple check: Unity Catalog catalogs are not 'hive_metastore' or 'spark_catalog'
        return self.catalog_name and self.catalog_name not in ["hive_metastore", "spark_catalog"]

    def full_table_name(self) -> str:
        """
        Returns the fully qualified table name for Spark SQL operations.
        Use only for Spark SQL, not for DeltaTable.forName,
        for DeltaTable.forName, use forname_table_name().
        args:
            None
        returns:
            str: The fully qualified catalog.schema.table name.
        """
        # using the catalog_name of the table if it exists
        if self.catalog_name:
            return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"
        # using the catalog_name of the super_spark if it exists
        elif hasattr(self, 'super_spark') and getattr(self.super_spark, 'catalog_name', None):
            return f"{self.super_spark.catalog_name}.{self.schema_name}.{self.table_name}"
        # simply return the schema_name.table_name if no catalog_name is provided
        else:
            return f"{self.schema_name}.{self.table_name}"

    def forname_table_name(self) -> str:
        """
        Returns the table name in schema.table format for DeltaTable.forName.
        In the case of Unity Catalog, the table name is the fully qualified name.
        Use only for DeltaTable.forName, not for Spark SQL.
        args:
            None
        returns:
            str: The table name in schema.table format.
        """
        if self.is_unity_catalog():
            return self.full_table_name()
        else:
            return f"{self.schema_name}.{self.table_name}"

    def check_table_schema(self, check_nullability: bool = False) -> bool:
        """
        Checks if the Delta table schema matches the SuperDeltaTable schema.
        If check_nullability is False, only field names and types are compared (not nullability).
        If check_nullability is True, the full schema including nullability is compared.
        args:
            check_nullability (bool): Whether to check nullability.
        returns:
            bool: True if the schema matches, False otherwise.
        """
        try:
            # get the delta table schema
            if self.managed:
                delta_table = DeltaTable.forName(self.super_spark.spark, self.forname_table_name())
            else:
                delta_table = DeltaTable.forPath(self.super_spark.spark, self.table_path)
            delta_schema = delta_table.toDF().schema
            # check if the schema matches
            if check_nullability:
                # compare the full schema including nullability
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
        Returns the table path (physical location) for managed or external tables.
        For managed tables, uses Spark catalog to get the location.
        For external tables, returns the absolute path.
        """
        # managed tables
        if self.managed:
            try:
                # Use Spark catalog to get the table location (works everywhere)
                table_info = spark.catalog.getTable(self.full_table_name())
                return table_info.locationUri
            except Exception:
                # Fallback for local/classic Spark if getTable is not available
                table_path = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
                table_path = re.sub(r"^file:", "", table_path)
                table_path = os.path.join(table_path, f"{self.schema_name}.db", self.table_name)
                return table_path
        # external tables
        else:
            if self.is_unity_catalog():
                table_path = self.table_path
            else:
                table_path = os.path.abspath(self.table_path)
            return table_path

    def get_schema_path(self, spark: SparkSession) -> str:
        """
        Returns the schema/database location URI for managed tables using Spark catalog API,
        or the parent directory for external tables.
        """
        # managed tables
        if self.managed:
            try:
                # Use Spark catalog API for robust, cloud-compatible location
                db_info = spark.catalog.getDatabase(self.schema_name)
                return db_info.locationUri
            except Exception:
                # Fallback for local Spark (rarely needed)
                schema_path = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
                schema_path = re.sub(r"^file:", "", schema_path)
                schema_path = os.path.join(schema_path, f"{self.schema_name}.db")
                return schema_path
        # external tables
        else:
            table_path = os.path.abspath(self.table_path)
            return os.path.dirname(table_path)

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
        except Exception as e:
            self.logger.info(f"Table {table_path} is not a Delta table: {e}")
            return False

    def schema_exists(self, spark: SparkSession) -> bool:
        """
        Checks if the schema exists in the catalog using Spark SQL/catalog API.
        args:
            spark (SparkSession): The Spark session.
        returns:
            bool: True if the schema exists, False otherwise.
        """
        db_names = [db.name.strip('`') for db in spark.catalog.listDatabases()]
        return self.schema_name in db_names

    def table_exists(self, spark: SparkSession) -> bool:
        """
        Checks if the table exists in the catalog (managed) or if the path is a Delta table (external).
        args:
            spark (SparkSession): The Spark session.
        returns:
            bool: True if the table exists, False otherwise.
        """
        # managed tables
        if self.managed:
            if self.is_unity_catalog():
                catalog_name = self.catalog_name or self.super_spark.catalog_name
                tables_in_catalog = spark.sql(f"SHOW TABLES IN {catalog_name}.{self.schema_name}").toPandas()
                return (tables_in_catalog['tableName'] == self.table_name).any()
            else:
                # get normalised schema names by stripping backticks
                schemas_in_catalog = [db.name.strip('`') for db in spark.catalog.listDatabases()]
                if self.schema_name not in schemas_in_catalog:
                    return False
                # Now check if table exists
                table_names = [t.name for t in spark.catalog.listTables(self.schema_name)]
                return self.table_name in table_names
        # external tables
        else:
            return self.is_delta_table_path(spark)

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
        Ensures a schema exists in the catalog (supports Unity Catalog and classic Spark).
        args:
            spark (SparkSession): The Spark session.
        returns:
            None
        """
        if self.catalog_name:
            schema_qualified = f"{self.catalog_name}.{self.schema_name}"
        else:
            schema_qualified = self.schema_name
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_qualified}")

    def register_table_in_catalog(self, spark: SparkSession, log=True):
        """
        Registers the table in the Spark catalog with the correct location.
        This function is mostly relevant for external tables.
        However, it is also called for managed tables on legacy/OSS Spark.
        args:
            spark (SparkSession): The Spark session.
            log (bool): Whether to log the operation.
        returns:
            None
        """
        # Ensure schema exists with catalog support
        self.ensure_schema_exists(spark)
        # get the table path
        table_path = self.get_table_path(spark)
        # create the table in the catalog
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.full_table_name()} USING DELTA LOCATION '{table_path}'")
        log and self.logger.info(
            f"Registered {'managed' if self.managed else 'external'} Delta table {self.full_table_name()}"
        )

    def alter_catalog_table_schema(self, spark: SparkSession, log=True):
        """
        Compares and alters the schema of the catalog/metastore table to match the schema
        of the Delta table at the external location. Only supported for external tables.
        """
        # managed tables
        if self.managed:
            raise NotImplementedError("Schema sync is only supported for external tables.")
        # external tables
        else:
            # Get the schema from the Delta table at the location
            delta_table = DeltaTable.forPath(spark, self.table_path)
            delta_schema = {f.name: f.dataType.simpleString() for f in delta_table.toDF().schema.fields}
            # Get the schema from the catalog/metastore
            catalog_schema = {}
            for row in spark.sql(f"DESCRIBE TABLE {self.full_table_name()}").collect():
                col = row['col_name']
                dtype = row['data_type']
                if (col and not col.startswith('#') and col not in ('', 'partition', 'comment')):
                    catalog_schema[col] = dtype
            # Find columns in Delta table but not in catalog
            missing_cols = [(name, dtype) for name, dtype in delta_schema.items() if name not in catalog_schema]
            if not missing_cols:
                log and self.logger.info(f"No schema changes needed for {self.full_table_name()}.")
                return
            for name, dtype in missing_cols:
                log and self.logger.info(f"Altering table {self.full_table_name()}: adding column {name} {dtype}")
                spark.sql(f"ALTER TABLE {self.full_table_name()} ADD COLUMNS ({name} {dtype})")
            log and self.logger.info(
                f"Schema of {self.full_table_name()} updated to match Delta table at {self.table_path}."
            )

    def ensure_table_exists(self, spark: SparkSession, log=True):
        """
        Ensures a Delta table exists at a path, registering and creating it if needed.
        Here are the steps:
        - build the effective schema, adding SCD columns if needed for MergeSCD mode
        - ensure the schema exists in the catalog, if not, create it
        - check if the table exists in the catalog, if not:
            - in the case of UC, create the table using SQL
            - in the case of legacy/OSS metastores, use DeltaTable builder
        args:
            spark (SparkSession): The Spark session.
            log (bool): Whether to log the operation.
        returns:
            None
        """
        # build the effective schema, adding SCD columns if needed for MergeSCD mode
        effective_table_schema = self.table_schema
        if self.table_save_mode == TableSaveMode.MergeSCD:
            scd_cols = [
                ('scd_start_dt', T.TimestampType(), True),
                ('scd_end_dt', T.TimestampType(), True),
                ('scd_is_current', T.BooleanType(), True)
            ]
            missing_scd_cols = [
                name for name, _, _ in scd_cols
                if name not in [f.name for f in self.table_schema.fields]
            ]
            if missing_scd_cols:
                log and self.logger.info(
                    f"SCD columns {missing_scd_cols} are missing from table_schema "
                    f"but will be considered present for MergeSCD mode."
                )
                effective_table_schema = T.StructType(
                    self.table_schema.fields + [
                        T.StructField(name, dtype, nullable)
                        for name, dtype, nullable in scd_cols
                        if name in missing_scd_cols
                    ]
                )
        # Always ensure the schema exists in the catalog
        if not self.schema_exists(spark):
            self.ensure_schema_exists(spark)
            log and self.logger.info(f"Created schema {self.schema_name} in catalog")
        # Check if the table exists in the catalog
        if self.table_exists(spark):
            log and self.logger.info(f"Table {self.full_table_name()} found in catalog")
            return
        else:
            # --- UNITY CATALOG ---
            # SQL-first approach: create the table with full schema,
            # generated columns, partitioning, and properties
            if self.is_unity_catalog():
                # add generated columns in SQL (Databricks SQL syntax)
                generated_cols_sql = ""
                if self.generated_columns:
                    for col, expr in self.generated_columns.items():
                        generated_cols_sql += f", {col} GENERATED ALWAYS AS ({expr}) VIRTUAL"
                properties_sql = ""
                # add Delta table properties if needed
                if self.delta_properties:
                    properties_sql = " TBLPROPERTIES (" + ", ".join(
                        [f"'{k}'='{v}'" for k, v in self.delta_properties.items()]
                        ) + ")"
                # add the partition columns if they exist
                sql_query_partition = ""
                if self.partition_cols:
                    sql_query_partition = f" PARTITIONED BY ({', '.join(self.partition_cols)})"
                # add the location if the table is external
                sql_query_location = ""
                if not self.managed:
                    sql_query_location = f" LOCATION '{self.table_path}'"
                # generate the full sql query
                sql_query = f"""
                        CREATE TABLE IF NOT EXISTS {self.full_table_name()} (
                            {', '.join([f'{f.name} {f.dataType.simpleString()}' for f in effective_table_schema])}
                            {generated_cols_sql}
                        )
                        USING DELTA
                        {sql_query_partition}
                        {sql_query_location}
                        {properties_sql}
                    """
                # execute the sql query
                spark.sql(sql_query)
                log and self.logger.info(
                    f"Created External Delta table {self.full_table_name()} (Unity Catalog)"
                )
                # For external tables, data should be written to the table after creation, not before
                if not self.managed:
                    empty_df = spark.createDataFrame([], effective_table_schema)
                    mode = "overwrite" if self.table_save_mode == TableSaveMode.Overwrite else "append"
                    empty_df.write.format("delta").mode(mode).save(self.table_path)
                return
            # --- LEGACY/OSS METASTORES ---
            # Use DeltaTableBuilder for full feature support
            # for generated columns, partitioning, and properties
            else:
                # managed tables
                if self.managed:
                    # check if the table exists at location and register it if needed
                    table_path = self.get_table_path(spark)
                    if os.path.exists(table_path):
                        if self.is_delta_table_path(spark):
                            if not self.table_exists(spark):
                                self.register_table_in_catalog(spark, log=log)
                            log and self.logger.info(
                                f"Managed Delta table {self.full_table_name()} already exists at location"
                            )
                            return
                        else:
                            shutil.rmtree(table_path)
                    # save the table in the metastore
                    empty_df = spark.createDataFrame([], effective_table_schema)
                    mode = "overwrite" if self.table_save_mode == TableSaveMode.Overwrite else "append"
                    (
                        empty_df.write.format("delta")
                        .mode(mode).option("overwriteSchema", "true")
                        .partitionBy(self.partition_cols)
                        .saveAsTable(self.full_table_name())
                    )
                    log and self.logger.info(f"Created Managed Delta table {self.full_table_name()}")
                # external tables
                else:
                    if self.is_delta_table_path(spark):
                        # the table exists at location (but may not be registered in catalog)
                        pass
                    else:
                        # the table does not exist at location, create it with
                        # DeltaTable builder to support generated columns and table properties
                        abs_path = os.path.abspath(self.table_path)
                        empty_df = spark.createDataFrame([], effective_table_schema)
                        builder = DeltaTable.createIfNotExists(spark)
                        # TODO: catalog.schema.table or schema.table ?
                        table_qualified = f"{self.schema_name}.{self.table_name}"
                        builder = builder.tableName(table_qualified)
                        for field in effective_table_schema:
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
                        if self.delta_properties:
                            for property, value in self.delta_properties.items():
                                builder = builder.property(property, value)
                        builder.execute()
                        log and self.logger.info(
                            f"Created External Delta table {self.full_table_name()}."
                        )
                # Register the table in the catalog (for both managed and external)
                self.register_table_in_catalog(spark, log=log)

    def optimize(self, spark: SparkSession):
        """Runs OPTIMIZE and ZORDER on the Delta table, with optional file size tuning."""
        self.logger.info(f"Starting optimize for table {self.full_table_name()}.")
        # check if table exists
        if not self.table_exists(spark):
            self.logger.warning(f"Table {self.full_table_name()} does not exist, skipping optimize")
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
        if not self.table_exists(spark):
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
        if not self.table_exists(spark):
            self.register_table_in_catalog(spark)
        t1 = time.time()
        spark.sql(f"VACUUM {self.full_table_name()} RETAIN {retention_hours} HOURS")
        t2 = time.time()
        self.logger.info(f"Vacuumed table {self.full_table_name()} with retention {retention_hours} hours")
        self.logger.metric("vacuum_table_creation_duration_sec", round(t1-t0, 2))
        self.logger.metric("vacuum_table_vacuum_duration_sec", round(t2-t1, 2))
        self.logger.metric("vacuum_table_total_duration_sec", round(t2-t0, 2))

    def read(self) -> DataFrame:
        """Returns a Spark DataFrame for the table."""
        if self.managed:
            return self.spark.read.table(self.full_table_name())
        else:
            return self.spark.read.format("delta").load(self.table_path)

    def evolve_schema_if_needed(self, df, spark):
        """Evolve the Delta table schema to match the DataFrame if schema_evolution_option is Merge."""
        if self.schema_evolution_option == SchemaEvolution.Merge:
            # get current table columns and compare to new DataFrame columns
            if self.table_exists(spark):
                if self.managed:
                    current_cols = set(spark.read.table(self.full_table_name()).columns)
                else:
                    current_cols = set(spark.read.format("delta").load(self.table_path).columns)
            else:
                current_cols = set()
            new_cols = set(df.columns) - current_cols
            # If there are new columns, modify the delta table schema at location
            if new_cols:
                # extra step for unity catalog and external tables: use ALTER TABLE to add columns in the catalog
                if self.is_unity_catalog() and not self.managed:
                    columns_str = ', '.join([f'{col} {df.schema[col].dataType.simpleString()}' for col in new_cols])
                    spark.sql(
                        f"ALTER TABLE {self.full_table_name()} "
                        f"ADD COLUMNS ({columns_str})"
                    )
                    self.logger.info(f"Schema of {self.full_table_name()} updated to match DataFrame schema.")
                # normal case: use the writer to add columns to the delta table at location
                dummy_df = spark.createDataFrame([], df.schema)
                writer = (dummy_df.write.format("delta").mode("append").option("mergeSchema", "true"))
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
            # Managed table: use forName
            target_table = DeltaTable.forName(spark, self.forname_table_name())
        else:
            # External table: always use forPath
            target_table = DeltaTable.forPath(spark, self.table_path)
        return target_table

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
            # Ensure PKs are not in scd_change_cols
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
            df = df.withColumn('scd_end_dt', F.lit(None).cast(T.TimestampType()))
        if 'scd_is_current' not in df.columns:
            df = df.withColumn('scd_is_current', F.lit(True).cast(T.BooleanType()))
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
            delta_table.toDF().filter(F.col("scd_is_current").cast(T.BooleanType()) == True),
            on=self.primary_keys,
            how="left_anti"
        )
        current_rows = (
            filtered_df
            .withColumn("scd_is_current", F.lit(True).cast(T.BooleanType()))
            .withColumn("scd_end_dt", F.lit(None).cast(T.TimestampType()))
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

        # Only pass valid Spark save modes to DataFrameWriter
        if mode == 'merge_scd':
            # Use DeltaTable API for SCD2 merge
            self.merge_scd(df, spark)
        elif mode == 'merge':
            # Use DeltaTable API for upsert/merge
            df = self.align_df_to_table_schema(df, spark)
            self.merge(df, spark)
        elif mode == 'append':
            df = self.align_df_to_table_schema(df, spark)
            self.evolve_schema_if_needed(df, spark)
            self.write_df(
                df,
                "append",
                merge_schema=(self.schema_evolution_option == SchemaEvolution.Merge)
            )
        elif mode == 'overwrite':
            df = self.align_df_to_table_schema(df, spark)
            self.evolve_schema_if_needed(df, spark)
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
        """
        Delete all rows from the table that match the deletions_df.
        The deletions_df must have the same schema as the table.
        if the table is a SCD table, the delete rows will be closed using the superlake_dt.
        if the table is not a SCD table, the delete rows will be deleted using the primary keys.
        args:
            deletions_df (DataFrame): The DataFrame to delete from the original delta table
            superlake_dt (datetime): The timestamp to use for scd_end_dt
        returns:
            None
        """
        start_time = time.time()
        if superlake_dt is None:
            superlake_dt = datetime.now()
        spark = self.super_spark.spark
        if self.table_exists(spark):
            target_table = self.get_delta_table(spark)
            to_delete_count = deletions_df.count()
            if to_delete_count > 0:
                # if the table is a SCD table, the delete rows will be closed using the superlake_dt.
                if self.table_save_mode == TableSaveMode.MergeSCD:
                    original_count = (
                        target_table.toDF()
                        .filter(F.col("scd_is_current").cast(T.BooleanType()) == True)
                        .count()
                    )
                    # filter the deletions_df to only include rows where scd_is_current is true
                    deletions_df = deletions_df.filter(F.col("scd_is_current").cast(T.BooleanType()) == True)
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
                    final_count = (
                        target_table.toDF()
                        .filter(F.col("scd_is_current").cast(T.BooleanType()) == True)
                        .count()
                    )
                # if the table is not a SCD table, the delete rows will be deleted using the primary keys.
                elif self.table_save_mode in (TableSaveMode.Append, TableSaveMode.Merge, TableSaveMode.Overwrite):
                    original_count = target_table.toDF().count()
                    self.logger.info(f"{to_delete_count} rows expected to be deleted from {self.full_table_name()}.")
                    pk_condition = " AND ".join([f"original.`{pk}` = deletion.`{pk}`" for pk in self.primary_keys])
                    (
                        target_table.alias("original")
                        .merge(
                            source=deletions_df.alias("deletion"),
                            condition=pk_condition)
                        .whenMatchedDelete()
                        .execute()
                    )
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
        # managed tables (remove the files at the table location for legacy/OSS metastores)
        if self.managed:
            table_path = self.get_table_path(spark)
            if os.path.exists(table_path):
                shutil.rmtree(table_path)
            self.logger.info(f"Dropped Delta Table {self.full_table_name()} (managed) and removed files")
        # external tables (remove the files at the table location)
        else:
            shutil.rmtree(self.table_path, ignore_errors=True)
            self.logger.info(f"Dropped Delta Table {self.full_table_name()} (external) and removed files")

    def change_uc_columns_comments(self, spark: SparkSession, log: bool = True):
        """
        For Unity Catalog tables, set column comments based on the 'description' in the
        metadata of each StructField in self.table_schema.
        Args:
            spark (SparkSession): The Spark session.
            log (bool): Whether to log the operation.
        Returns:
            None
        """
        if not self.is_unity_catalog():
            log and self.logger.info(
                f"change_uc_columns_comments: Not a Unity Catalog table, skipping for {self.full_table_name()}.")
            return
        for field in self.table_schema.fields:
            description = None
            # PySpark >= 3.0: metadata is a dict, else may be None
            if hasattr(field, 'metadata') and field.metadata and 'description' in field.metadata:
                description = field.metadata['description']
            if description:
                # Escape single quotes in the description for SQL
                safe_description = description.replace("'", "''")
                sql = (
                    f"ALTER TABLE `{self.catalog_name}`.`{self.schema_name}`.`{self.table_name}` "
                    f"CHANGE COLUMN `{field.name}` COMMENT '{safe_description}'"
                )
                spark.sql(sql)
                log and self.logger.info(
                    f"Set comment for column `{field.name}` in {self.full_table_name()}: {description}"
                    )

    def change_uc_table_comment(self, spark: SparkSession, log: bool = True):
        """
        For Unity Catalog tables, set the table comment using self.table_description.
        Args:
            spark (SparkSession): The Spark session.
            log (bool): Whether to log the operation.
        Returns:
            None
        """
        if not self.is_unity_catalog():
            log and self.logger.info(
                f"change_uc_table_comment: Not a Unity Catalog table, skipping for {self.full_table_name()}."
            )
            return
        if not self.table_description:
            log and self.logger.info(
                f"change_uc_table_comment: No table_description set for {self.full_table_name()}, skipping."
            )
            return
        # Escape single quotes in the description for SQL
        safe_description = self.table_description.replace("'", "''")
        sql = (
            f"ALTER TABLE `{self.catalog_name}`.`{self.schema_name}`.`{self.table_name}` "
            f"SET TBLPROPERTIES ('comment' = '{safe_description}')"
        )
        spark.sql(sql)
        log and self.logger.info(
            f"Set table comment for {self.full_table_name()}: {self.table_description}"
        )

    def change_uc_table_and_columns_comments(self, spark: SparkSession, log: bool = True):
        """
        For Unity Catalog tables, set the table comment using self.table_description and
        the columns comments using the 'description' in the metadata of each StructField
        in self.table_schema.
        Args:
            spark (SparkSession): The Spark session.
            log (bool): Whether to log the operation.
        Returns:
            None
        """
        self.change_uc_table_comment(spark, log)
        self.change_uc_columns_comments(spark, log)
