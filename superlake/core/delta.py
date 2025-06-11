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
import hashlib
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

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
        table_description: Optional[str] = None,
        foreign_keys: Optional[list] = None  # List of dicts: {fk_columns, ref_table, ref_columns, fk_name}
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
            foreign_keys (Optional[list]): List of dicts, each with keys:
                - fk_columns: list of local column names
                - ref_table: fully qualified referenced table name
                - ref_columns: list of referenced column names
                - fk_name: (optional) constraint name, will be auto-generated if not provided
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
        self.foreign_keys = foreign_keys or []

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

    def get_table_path(self, spark: Optional[SparkSession] = None) -> str:
        """
        Returns the table path (physical location) for managed or external tables.
        For managed tables, uses Spark catalog to get the location.
        For external tables, returns the absolute path.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            str: The table path.
        """
        if spark is None:
            spark = self.spark
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

    def get_schema_path(self, spark: Optional[SparkSession] = None) -> str:
        """
        Returns the schema/database location URI for managed tables using Spark catalog API,
        or the parent directory for external tables.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            str: The schema/database location URI.
        """
        if spark is None:
            spark = self.spark
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

    def is_delta_table_path(self, spark: Optional[SparkSession] = None) -> bool:
        """
        Checks if the table_path is a valid Delta table.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            bool: True if the table_path is a valid Delta table, False otherwise.
        """
        if spark is None:
            spark = self.spark
        table_path = self.get_table_path(spark)
        try:
            return DeltaTable.isDeltaTable(spark, table_path)
        except Exception as e:
            self.logger.info(f"Table {table_path} is not a Delta table: {e}")
            return False

    def schema_exists(self, spark: Optional[SparkSession] = None) -> bool:
        """
        Checks if the schema exists in the catalog using Spark SQL/catalog API.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            bool: True if the schema exists, False otherwise.
        """
        if spark is None:
            spark = self.spark

        if self.is_unity_catalog():
            # Unity Catalog: check in the correct catalog
            catalog = self.catalog_name or self.super_spark.catalog_name
            # Use SQL to get schemas in the catalog
            schemas = spark.sql(f"SHOW DATABASES IN `{catalog}`").toPandas()["databaseName"].tolist()
            return self.schema_name in schemas
        else:
            # Legacy/OSS: use Spark catalog API
            db_names = [db.name.strip('`') for db in spark.catalog.listDatabases()]
            return self.schema_name in db_names

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

    def table_exists(self, spark: Optional[SparkSession] = None) -> bool:
        """
        Checks if the table exists in the catalog (managed) or if the path is a Delta table (external).
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            bool: True if the table exists, False otherwise.
        """
        if spark is None:
            spark = self.spark
        # managed tables
        if self.managed:
            if self.is_unity_catalog():
                # check if the schema exists
                catalog_name = self.catalog_name or self.super_spark.catalog_name
                schemas_in_catalog = spark.sql(
                    f"SHOW DATABASES IN {catalog_name}"
                ).toPandas()["databaseName"].tolist()
                if self.schema_name not in schemas_in_catalog:
                    return False
                else:
                    # check if the table exists
                    tables_in_catalog = spark.sql(
                        f"SHOW TABLES IN {catalog_name}.{self.schema_name}"
                    ).toPandas()["tableName"].tolist()
                    if self.table_name not in tables_in_catalog:
                        return False
                    return True
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

    def schema_and_table_exists(self, spark: Optional[SparkSession] = None) -> bool:
        """
        Checks if the schema and table exists in the catalog.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            bool: True if the schema and table exists, False otherwise.
        """
        if spark is None:
            spark = self.spark
        return self.schema_exists(spark) and self.table_exists(spark)

    def ensure_schema_exists(self, spark: Optional[SparkSession] = None):
        """
        Ensures a schema exists in the catalog (supports Unity Catalog and classic Spark).
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            None
        """
        if spark is None:
            spark = self.spark
        # adapts the schema name to the catalog name if it exists
        if self.catalog_name:
            schema_qualified = f"{self.catalog_name}.{self.schema_name}"
        else:
            schema_qualified = self.schema_name
        # create the schema in the catalog
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_qualified}")

    def register_table_in_catalog(self, spark: Optional[SparkSession] = None, log=True):
        """
        Registers the table in the Spark catalog with the correct location.
        This function is mostly relevant for external tables.
        However, it is also called for managed tables on legacy/OSS Spark.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
            log (bool): Whether to log the operation.
        Returns:
            None
        """
        if spark is None:
            spark = self.spark
        # Registering a managed table in the catalog is not supported for Unity Catalog
        if self.is_unity_catalog() and self.managed:
            log and self.logger.info("register_table_in_catalog is only supported for external tables.")
        else:
            # Ensure schema exists with catalog support
            self.ensure_schema_exists(spark)
            # get the table path
            table_path = self.get_table_path(spark)
            # create the table in the catalog
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.full_table_name()} USING DELTA LOCATION '{table_path}'")
            log and self.logger.info(
                f"Registered {'managed' if self.managed else 'external'} Delta table {self.full_table_name()}"
            )

    def alter_catalog_table_schema(self, spark: Optional[SparkSession] = None, log=True):
        """
        Compares and alters the schema of the catalog/metastore table to match the schema
        of the Delta table at the external location. Only supported for external tables.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
            log (bool): Whether to log the operation.
        Returns:
            None
        """
        if spark is None:
            spark = self.spark
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
            # Add columns to the table
            for name, dtype in missing_cols:
                log and self.logger.info(f"Altering table {self.full_table_name()}: adding column {name} {dtype}")
                spark.sql(f"ALTER TABLE {self.full_table_name()} ADD COLUMNS ({name} {dtype})")
            log and self.logger.info(
                f"Schema of {self.full_table_name()} updated to match Delta table at {self.table_path}."
            )

    def create_uc_table_foreign_keys(
        self,
        force_create: bool = False,
        foreign_keys: Optional[list] = None,
        spark: Optional[SparkSession] = None,
        log: bool = True
    ) -> DataFrame:
        """
        For Unity Catalog tables, add foreign key constraints after table creation using ALTER TABLE ... ADD CONSTRAINT ...
        Also performs quality checks and returns a DQ DataFrame of FK issues.
        Args:
            spark: SparkSession
            foreign_keys: List of dicts with keys: fk_columns, ref_table, ref_columns, fk_name
        Returns:
            DataFrame of FK issues (DQ style)
        """
        dq_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("check_key", StringType(), True),
            StructField("check_value", StringType(), True),
            StructField("check_dt", TimestampType(), True),
        ])
        if spark is None:
            spark = self.spark
        # if not Unity Catalog, return an empty dataframe
        if not self.is_unity_catalog():
            self.logger.error(f"create_uc_table_foreign_keys is only supported for Unity Catalog tables, "
                              f"not for {self.full_table_name()}.")
            return spark.createDataFrame([], dq_schema)
        else:
            if foreign_keys is None:
                foreign_keys = self.foreign_keys
            now = datetime.now()
            fk_issues = []
            valid_foreign_keys = []

            # local function to check if the referenced table exists
            def referenced_table_exists(spark, ref_table):
                try:
                    spark.table(ref_table)
                    return True
                except Exception:
                    return False

            # local function to check if the referenced columns exist in the referenced table
            def referenced_columns_exist(spark, ref_table, ref_columns):
                try:
                    df = spark.table(ref_table)
                    table_columns = set(df.columns)
                    return all(col in table_columns for col in ref_columns)
                except Exception:
                    return False

            # local function to check if the referenced columns are primary key or unique
            def referenced_columns_are_pk_or_unique(spark, ref_table, ref_columns):
                try:
                    desc = spark.sql(f"DESCRIBE TABLE EXTENDED {ref_table}").collect()
                    found = False
                    for row in desc:
                        dtype = getattr(row, 'data_type', '').strip().upper()
                        if dtype.startswith("PRIMARY KEY") or dtype.startswith("UNIQUE"):
                            match = re.search(r"\((.*?)\)", dtype)
                            if match:
                                # Remove backticks, spaces, and lowercase for comparison
                                constraint_cols = [c.replace('`', '').strip().lower() for c in match.group(1).split(',')]
                                ref_cols_norm = [c.replace('`', '').strip().lower() for c in ref_columns]
                                if set(ref_cols_norm) == set(constraint_cols):
                                    found = True
                                    break
                    return found
                except Exception as e:
                    self.logger.warning(f"Error parsing constraints: {e}")
                    return False

            # local function to check if the constraint already exists
            def constraint_exists(spark, table, constraint_name):
                desc = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
                in_constraints = False
                for row in desc:
                    if getattr(row, 'col_name', '').strip().lower() == "# constraints":
                        in_constraints = True
                        continue
                    if in_constraints:
                        if not getattr(row, 'col_name', '').strip() or getattr(row, 'col_name', '').strip().startswith("#"):
                            break
                        if getattr(row, 'col_name', '').strip() == constraint_name:
                            return True
                return False

            # iterate over the foreign keys
            for fk in foreign_keys:
                fk_columns = fk["fk_columns"]
                ref_table = fk["ref_table"]
                ref_columns = fk["ref_columns"]
                fk_name = fk.get("fk_name")
                # generate the name if not provided
                if not fk_name:
                    def clean(name):
                        return re.sub(r'[^a-zA-Z0-9_]', '_', name)
                    local_parts = self.full_table_name().split('.')
                    ref_parts = ref_table.split('.')
                    localschema = clean(local_parts[-2])
                    localtable = clean(local_parts[-1]) if len(local_parts) > 1 else clean(local_parts[-1])
                    refschema = clean(ref_parts[-2])
                    reftable = clean(ref_parts[-1]) if len(ref_parts) > 1 else clean(ref_parts[-1])
                    localcols = '__'.join([clean(col) for col in fk_columns])
                    refcols = '__'.join([clean(col) for col in ref_columns])
                    name = (
                        f"fk__{localschema}__{localtable}__{localcols}__to__{refschema}__{reftable}__{refcols}"
                    )
                    # if the name is too long, hash it to shorten it
                    if len(name) > 255:
                        hash_part = hashlib.md5(name.encode()).hexdigest()[:8]
                        name = (
                            f"fk__{localschema}__{localtable}__to__{refschema}__{reftable}__{hash_part}"
                        )
                    fk_name = name
                # check if the referenced table exists
                if not referenced_table_exists(spark, ref_table):
                    self.logger.error(
                        f"Foreign key skipped: Referenced table {ref_table} does not exist for FK columns {fk_columns}."
                    )
                    fk_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": ','.join(fk_columns),
                        "check_key": "fk_missing_table",
                        "check_value": ref_table,
                        "check_dt": now,
                    })
                    continue
                # check if the referenced columns exist in the referenced table
                if not referenced_columns_exist(spark, ref_table, ref_columns):
                    self.logger.error(
                        f"Foreign key skipped: One or more referenced columns {ref_columns} do not exist in "
                        f"{ref_table} for FK columns {fk_columns}."
                    )
                    fk_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": ','.join(fk_columns),
                        "check_key": "fk_missing_column",
                        "check_value": ','.join(ref_columns),
                        "check_dt": now,
                    })
                    continue
                # check if the referenced columns are primary key or unique
                if not referenced_columns_are_pk_or_unique(spark, ref_table, ref_columns):
                    self.logger.error(
                        f"Foreign key skipped: Referenced columns {ref_columns} in {ref_table} are not primary key or unique."
                    )
                    fk_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": ','.join(fk_columns),
                        "check_key": "fk_ref_not_pk_or_unique",
                        "check_value": ','.join(ref_columns) + " is not primary key or unique",
                        "check_dt": now,
                    })
                    continue
                # save valid FK for creation
                valid_foreign_keys.append({
                    "fk_columns": fk_columns,
                    "ref_table": ref_table,
                    "ref_columns": ref_columns,
                    "fk_name": fk_name
                })
            # actually create the valid FKs
            for fk in valid_foreign_keys:
                fk_columns = fk["fk_columns"]
                ref_table = fk["ref_table"]
                ref_columns = fk["ref_columns"]
                fk_name = fk["fk_name"]
                local_cols_str = ', '.join([f'`{col}`' for col in fk_columns])
                ref_cols_str = ', '.join([f'`{col}`' for col in ref_columns])
                sql_alter_table_fk = (
                    f"ALTER TABLE {self.full_table_name()} "
                    f"ADD CONSTRAINT {fk_name} FOREIGN KEY ({local_cols_str}) "
                    f"REFERENCES {ref_table} ({ref_cols_str})"
                )
                # check if the constraint already exists
                if constraint_exists(spark, self.full_table_name(), fk_name):
                    if force_create:
                        self.logger.warning(f"Constraint {fk_name} already exists on {self.full_table_name()} "
                                            "and force_create is True, dropping it.")
                        spark.sql(f"ALTER TABLE {self.full_table_name()} DROP CONSTRAINT {fk_name}")
                    else:
                        self.logger.warning(f"Constraint {fk_name} already exists on {self.full_table_name()} "
                                            "and force_create is False, skipping it.")
                        fk_issues.append({
                            "table_name": self.full_table_name(),
                            "column_name": ','.join(fk_columns),
                            "check_key": "fk_already_exists",
                            "check_value": fk_name,
                            "check_dt": now,
                        })
                        continue
                spark.sql(sql_alter_table_fk)
                self.logger.info(f"Added foreign key constraint {fk_name} to {self.full_table_name()}")
            # return the issues or an empty dataframe
            if fk_issues:
                fk_issues_df = spark.createDataFrame(fk_issues, schema=dq_schema)
            else:
                fk_issues_df = spark.createDataFrame([], schema=dq_schema)
            return fk_issues_df

    def create_uc_table_primary_keys(
        self,
        force_create: bool = False,
        primary_keys: Optional[list] = None,
        spark: Optional[SparkSession] = None
    ) -> DataFrame:
        """
        For Unity Catalog tables, add a primary key constraint after table creation using
        ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (...).
        Also performs quality checks and returns a DQ DataFrame of PK issues.
        Args:
            spark: SparkSession
            primary_keys: List of column names (or None to use self.primary_keys)
            force_create: If True, drop existing PK constraint before creating
        Returns:
            DataFrame of PK issues (DQ style)
        """
        dq_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("check_key", StringType(), True),
            StructField("check_value", StringType(), True),
            StructField("check_dt", TimestampType(), True),
        ])
        if spark is None:
            spark = self.spark
        if not self.is_unity_catalog():
            self.logger.error(f"create_uc_table_primary_keys is only supported for Unity Catalog tables, "
                              f"not for {self.full_table_name()}.")
            return spark.createDataFrame([], dq_schema)
        else:
            # if no primary keys are provided, use the ones from the table definition
            if primary_keys is None:
                primary_keys = self.primary_keys
            # get the current date and time and initialize the issues list
            now = datetime.now()
            pk_issues = []
            # retrieve the schema of the table to check if the columns are nullable
            try:
                schema = spark.table(self.full_table_name()).schema
            except Exception as e:
                self.logger.error(f"Could not retrieve schema for {self.full_table_name()}: {e}")
                for col in primary_keys:
                    pk_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": col,
                        "check_key": "pk_schema_unavailable",
                        "check_value": None,
                        "check_dt": now,
                    })
                pk_issues_df = spark.createDataFrame(pk_issues, schema=dq_schema)
                return pk_issues_df
            schema_fields = {field.name: field for field in schema.fields}
            for col in primary_keys:
                if col not in schema_fields:
                    self.logger.error(f"Primary key column {col} does not exist in {self.full_table_name()}")
                    pk_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": col,
                        "check_key": "pk_missing_column",
                        "check_value": None,
                        "check_dt": now,
                    })
                # check if the column is nullable and making it NOT NULL if force_create is True
                elif schema_fields[col].nullable:
                    if force_create:
                        self.logger.warning(
                            f"Primary key column {col} is nullable in {self.full_table_name()}, "
                            "force_create is True, altering to NOT NULL.")
                        try:
                            spark.sql(f"ALTER TABLE {self.full_table_name()} ALTER COLUMN {col} SET NOT NULL")
                        except Exception as e:
                            self.logger.error(f"Failed to alter column {col} to NOT NULL: {e}")
                            pk_issues.append({
                                "table_name": self.full_table_name(),
                                "column_name": col,
                                "check_key": "pk_column_nullable_alter_failed",
                                "check_value": f"The primary key column {col} is nullable",
                                "check_dt": now,
                            })
                    else:
                        self.logger.error(f"Primary key column {col} is nullable in {self.full_table_name()}")
                        pk_issues.append({
                            "table_name": self.full_table_name(),
                            "column_name": col,
                            "check_key": "pk_column_nullable",
                            "check_value": f"The primary key column {col} is nullable",
                            "check_dt": now,
                        })
            # check if the primary key already exists
            desc_ext = spark.sql(f"DESCRIBE TABLE EXTENDED {self.full_table_name()}").collect()
            pk_constraint_name = None
            pk_exists = False
            in_constraints = False
            for row in desc_ext:
                if getattr(row, 'col_name', '').strip().lower() == "# constraints":
                    in_constraints = True
                    continue
                if in_constraints:
                    if not getattr(row, 'col_name', '').strip() or getattr(row, 'col_name', '').strip().startswith("#"):
                        break
                    dtype = getattr(row, 'data_type', '').strip().upper()
                    if dtype.startswith("PRIMARY KEY"):
                        pk_constraint_name = getattr(row, 'col_name', '').strip()
                        pk_exists = True
                        break
            # if the primary key already exists, check if it should be dropped and recreated if force_create is True
            if pk_exists:
                if force_create:
                    self.logger.warning(
                        f"Primary key constraint {pk_constraint_name} already exists on {self.full_table_name()}"
                        " and force_create is True, dropping it with CASCADE."
                    )
                    try:
                        spark.sql(f"ALTER TABLE {self.full_table_name()} DROP CONSTRAINT {pk_constraint_name} CASCADE")
                    except Exception as e:
                        self.logger.error(f"Failed to drop PK constraint {pk_constraint_name} with CASCADE: {e}")
                        pk_issues.append({
                            "table_name": self.full_table_name(),
                            "column_name": ','.join(primary_keys),
                            "check_key": "pk_drop_failed",
                            "check_value": pk_constraint_name,
                            "check_dt": now,
                        })
                        pk_issues_df = spark.createDataFrame(pk_issues, schema=dq_schema)
                        return pk_issues_df
                else:
                    self.logger.warning(
                        f"Primary key constraint {pk_constraint_name} already exists on {self.full_table_name()} "
                        "and force_create is False, skipping it.")
                    pk_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": ','.join(primary_keys),
                        "check_key": "pk_already_exists",
                        "check_value": pk_constraint_name,
                        "check_dt": now,
                    })
                    pk_issues_df = spark.createDataFrame(pk_issues, schema=dq_schema)
                    return pk_issues_df
            # if no issues, add the primary key
            if not pk_issues:
                # include schema and table name in the PK constraint name for uniqueness and clarity
                pk_name = f"pk__{self.schema_name}__{self.table_name}__{'__'.join(primary_keys)}"
                pk_cols_str = ', '.join([f'`{col}`' for col in primary_keys])
                sql_alter_table_pk = (
                    f"ALTER TABLE {self.full_table_name()} "
                    f"ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols_str})"
                )
                try:
                    spark.sql(sql_alter_table_pk)
                    self.logger.info(
                        f"Added primary key constraint {pk_name} to {self.full_table_name()}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to add primary key constraint {pk_name}: {e}"
                    )
                    pk_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": ','.join(primary_keys),
                        "check_key": "pk_add_failed",
                        "check_value": pk_name,
                        "check_dt": now,
                    })
            # return the issues or an empty dataframe
            if pk_issues:
                pk_issues_df = spark.createDataFrame(pk_issues, schema=dq_schema)
            else:
                pk_issues_df = spark.createDataFrame([], schema=dq_schema)
            return pk_issues_df

    def ensure_table_exists(self, spark: Optional[SparkSession] = None, log=True):
        """
        Ensures a Delta table exists at a path, registering and creating it if needed.
        Here are the steps:
        - build the effective schema, adding SCD columns if needed for MergeSCD mode
        - ensure the schema exists in the catalog, if not, create it
        - check if the table exists in the catalog, if not:
            - in the case of UC, create the table using SQL
            - in the case of legacy/OSS metastores, use DeltaTable builder

        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
            log (bool): Whether to log the operation.

        Returns:
            None
        """
        if spark is None:
            spark = self.spark
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
            # SQL-first approach: create the table with full schema, generated columns, partitioning, and properties
            if self.is_unity_catalog():
                # Build the column definitions with backticks
                column_defs = [f'`{f.name}` {f.dataType.simpleString()}' for f in effective_table_schema]
                # Add generated columns
                if self.generated_columns:
                    for col, expr in self.generated_columns.items():
                        column_defs.append(f"`{col}` GENERATED ALWAYS AS ({expr}) VIRTUAL")
                # Add the partition columns if they exist
                sql_query_partition = ""
                if self.partition_cols:
                    sql_query_partition = f" PARTITIONED BY ({', '.join([f'`{col}`' for col in self.partition_cols])})"
                # managed vs external: add the location if the table is external
                sql_query_location = ""
                if not self.managed:
                    sql_query_location = f" LOCATION '{self.table_path}'"
                # Add properties if needed
                properties_sql = ""
                if self.delta_properties:
                    properties_sql = " TBLPROPERTIES (" + ", ".join(
                        [f"'{k}'='{v}'" for k, v in self.delta_properties.items()]
                    ) + ")"
                # Generate the full sql query
                sql_query = f"""
                        CREATE TABLE IF NOT EXISTS {self.full_table_name()} (
                            {', '.join(column_defs)}
                        )
                        USING DELTA
                        {sql_query_partition}
                        {sql_query_location}
                        {properties_sql}
                    """
                # Execute the sql query
                spark.sql(sql_query)
                log and self.logger.info(
                    f"Created {'managed' if self.managed else 'external'} Delta table {self.full_table_name()} (Unity Catalog)"
                )
                # For external tables, data should be written to the table after creation, not before
                if not self.managed:
                    empty_df = spark.createDataFrame([], effective_table_schema)
                    mode = "overwrite" if self.table_save_mode == TableSaveMode.Overwrite else "append"
                    empty_df.write.format("delta").mode(mode).save(self.table_path)
            # --- LEGACY/OSS METASTORES ---
            # Use DeltaTableBuilder for full feature support for generated columns, partitioning, and properties
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
                    # DEBUG: Ensure the path is initialized as a Delta table
                    empty_df = spark.createDataFrame([], effective_table_schema)
                    mode = "overwrite" if self.table_save_mode == TableSaveMode.Overwrite else "append"
                    empty_df.write.format("delta").mode(mode).save(self.table_path)

    def optimize(self, spark: Optional[SparkSession] = None):
        """
        Runs OPTIMIZE and ZORDER on the Delta table, with optional file size tuning.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.

        Returns:
            None
        """
        if spark is None:
            spark = self.spark
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
        self.logger.metric("optimize_table_creation_duration_sec", round(t1 - t0, 2))
        self.logger.metric("optimize_table_optimization_duration_sec", round(t3 - t2, 2))
        self.logger.metric("optimize_table_total_duration_sec", round(t3 - t0, 2))

    def vacuum(self, spark: Optional[SparkSession] = None, retention_hours: int = 168):
        """
        Runs the VACUUM command on a Delta table to clean up old files.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
            retention_hours (int): Number of hours to retain data.

        Returns:
            None
        """
        if spark is None:
            spark = self.spark
        t0 = time.time()
        if not self.table_exists(spark):
            self.register_table_in_catalog(spark)
        t1 = time.time()
        spark.sql(f"VACUUM {self.full_table_name()} RETAIN {retention_hours} HOURS")
        t2 = time.time()
        self.logger.info(f"Vacuumed table {self.full_table_name()} with retention {retention_hours} hours")
        self.logger.metric("vacuum_table_creation_duration_sec", round(t1 - t0, 2))
        self.logger.metric("vacuum_table_vacuum_duration_sec", round(t2 - t1, 2))
        self.logger.metric("vacuum_table_total_duration_sec", round(t2 - t0, 2))

    def read(self, spark: Optional[SparkSession] = None) -> DataFrame:
        """
        Returns a Spark DataFrame for the table.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            DataFrame: The DataFrame.
        """
        if spark is None:
            spark = self.spark
        if self.managed:
            return spark.read.table(self.full_table_name())
        else:
            if not self.is_delta_table_path(spark):
                # This case could happen if the table has been created via ensure_table_exists
                return spark.createDataFrame([], self.table_schema)
            return spark.read.format("delta").load(self.table_path)

    def evolve_schema_if_needed(self, df, spark: Optional[SparkSession] = None):
        """Evolve the Delta table schema to match the DataFrame if schema_evolution_option is Merge.
        Args:
            df (DataFrame): The DataFrame to evolve the schema of.
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            None
        """
        if spark is None:
            spark = self.spark
        if self.schema_evolution_option == SchemaEvolution.Merge:

            def normalize(col):
                return col.replace('`', '').strip().lower()

            if not self.table_exists(spark):
                self.logger.info(f"Table {self.full_table_name()} does not exist yet, skipping schema evolution.")
                return
            else:
                if self.managed:
                    table_fields = spark.read.table(self.full_table_name()).schema.fields
                else:
                    table_fields = spark.read.format("delta").load(self.table_path).schema.fields
                table_field_names = set(normalize(f.name) for f in table_fields)
            # Only add columns that are not already present (normalized)
            new_fields = [f for f in df.schema.fields if normalize(f.name) not in table_field_names]
            if new_fields:
                if self.is_unity_catalog() and not self.managed:
                    columns_str = ', '.join([f'`{f.name}` {f.dataType.simpleString()}' for f in new_fields])
                    self.logger.info(f"Attempting to add columns: {[f.name for f in new_fields]}")
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

    def align_df_to_table_schema(self, df, spark: Optional[SparkSession] = None):
        """
        Align DataFrame columns to match the target table schema (cast types, add missing columns as nulls,
        drop extra columns if configured).
        args:
            df (DataFrame): The DataFrame to align.
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        returns:
            DataFrame: The aligned DataFrame.
        """
        if spark is None:
            spark = self.spark
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

    def get_delta_table(self, spark: Optional[SparkSession] = None):
        """
        Return the correct DeltaTable object for managed or external tables.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            DeltaTable: The DeltaTable object.
        """
        if spark is None:
            spark = self.spark
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
        overwrite_schema: bool = False,
        spark: Optional[SparkSession] = None
    ) -> None:
        """
        Write a DataFrame to a Delta table.
        Args:
            df (DataFrame): The DataFrame to write.
            mode (str): The mode to use for the write.
            merge_schema (bool): Whether to merge the schema of the DataFrame with the table schema.
            overwrite_schema (bool): Whether to overwrite the schema of the table.
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            None
        """
        if spark is None:
            spark = self.spark
        # align the schema of the dataframe to the table schema
        if merge_schema:
            df = self.align_df_to_table_schema(df, spark)
        # create the writer
        writer = df.write.format("delta").mode(mode)
        # add the partition columns if they exist
        if self.partition_cols:
            writer = writer.partitionBy(self.partition_cols)
        # add the merge or overwrite schema option if needed
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        # save the dataframe
        if self.managed:
            writer.saveAsTable(self.forname_table_name())  # note: this used to be full_table_name()
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

    def merge(self, df: DataFrame, spark: Optional[SparkSession] = None):
        """
        Performs a merge (upsert) operation on the Delta table.
        Args:
            df (DataFrame): The DataFrame to merge.
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.

        Returns:
            None
        """
        if spark is None:
            spark = self.spark
        # evolve the schema of the table to match the dataframe
        self.evolve_schema_if_needed(df, spark)
        # retrieve the conditions for merge and execute the merge
        delta_table = self.get_delta_table(spark)
        cond, updates, _ = self.get_merge_condition_and_updates(df)
        delta_table.alias("target").merge(
            df.alias("source"), cond
        ).whenMatchedUpdate(set=updates).whenNotMatchedInsert(values=updates).execute()

    def merge_scd(self, df: DataFrame, spark: Optional[SparkSession] = None):
        """
        Performs a Slowly Changing Dimension (SCD) merge operation on the Delta table.
        Args:
            df (DataFrame): The DataFrame to merge.
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.

        Returns:
            None
        """
        if spark is None:
            spark = self.spark
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
        if spark is None:
            spark = self.spark
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
            # append the dataframe to the table
            df = self.align_df_to_table_schema(df, spark)
            self.evolve_schema_if_needed(df, spark)
            self.write_df(
                df,
                "append",
                merge_schema=(self.schema_evolution_option == SchemaEvolution.Merge)
            )
        elif mode == 'overwrite':
            # overwrite the table with the dataframe
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

    def delete(self, deletions_df: DataFrame, superlake_dt: Optional[datetime] = None, spark: Optional[SparkSession] = None):
        """
        Delete all rows from the table that match the deletions_df.
        The deletions_df must have the same schema as the table.
        if the table is a SCD table, the delete rows will be closed using the superlake_dt.
        if the table is not a SCD table, the delete rows will be deleted using the primary keys.
        Args:
            deletions_df (DataFrame): The DataFrame to delete from the original delta table
            superlake_dt (datetime): The timestamp to use for scd_end_dt
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.

        Returns:
            None
        """
        if spark is None:
            spark = self.spark
        start_time = time.time()
        if superlake_dt is None:
            superlake_dt = datetime.now()
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
        """
        Drops the table from the catalog and removes the data files in storage.
        Args:
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.

        Returns:
            None
        """
        if spark is None:
            spark = self.spark
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

    def change_uc_columns_comments(self, log: bool = True, spark: Optional[SparkSession] = None):
        """
        For Unity Catalog tables, set column comments based on the 'description' in the
        metadata of each StructField in self.table_schema. Only update if different.
        Log a warning if the current comment differs from the StructType description.
        Args:
            log (bool): Whether to log the operation.
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            None
        """
        dq_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("check_key", StringType(), True),
            StructField("check_value", StringType(), True),
            StructField("check_dt", TimestampType(), True),
        ])
        if spark is None:
            spark = self.spark
        if not self.is_unity_catalog():
            self.logger.error(f"change_uc_columns_comments is only supported for Unity Catalog tables, "
                              f"not for {self.full_table_name()}.")
            return spark.createDataFrame([], dq_schema)
        else:
            # Define the schema for the DQ issues DataFrame
            dq_issues = []
            now = datetime.now()
            # Fetch current column comments from the catalog
            columns_info = spark.sql(
                f"DESCRIBE TABLE `{self.catalog_name}`.`{self.schema_name}`.`{self.table_name}`"
            ).toPandas()
            # Build a dict: column_name -> current_comment
            current_comments = {}
            for _, row in columns_info.iterrows():
                col_name = row['col_name']
                comment = row['comment'] if 'comment' in row and row['comment'] is not None else None
                if col_name and not col_name.startswith('#'):
                    current_comments[col_name] = comment

            # Prepare the list of fields to iterate over, including SCD columns if needed
            fields = list(self.table_schema.fields)
            if self.table_save_mode == TableSaveMode.MergeSCD:
                scd_fields = {
                    'scd_start_dt': (T.TimestampType(), 'SCD2 Record validity start date'),
                    'scd_end_dt': (T.TimestampType(), 'SCD2 Record validity end date'),
                    'scd_is_current': (T.BooleanType(), 'SCD2 Flag for current record version'),
                }
                existing_field_names = {f.name for f in fields}
                for name, (dtype, desc) in scd_fields.items():
                    if name not in existing_field_names:
                        fields.append(T.StructField(name, dtype, True, {'description': desc}))

            # Log warning for columns in the real table but not in the StructType
            structtype_field_names = {f.name for f in fields}
            for col_name in current_comments:
                # Flag issues for columns in the table but not in the StructType
                comment_val = current_comments[col_name]
                if col_name not in structtype_field_names:
                    dq_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": col_name,
                        "check_key": "column_missing_from_structtype",
                        "check_value": comment_val,
                        "check_dt": now,
                    })
                    log and self.logger.warning(
                        f"Column `{col_name}` exists in the table {self.full_table_name()} "
                        "but not in the StructType and has a non-empty comment.")

            # Update comments for columns in the schema (including SCD columns if needed)
            for field in fields:
                description = None
                if hasattr(field, 'metadata') and field.metadata and 'description' in field.metadata:
                    description = field.metadata['description']
                if description:
                    current_comment = current_comments.get(field.name)
                    if current_comment != description:
                        # Only flag as an issue if the current comment is not empty (None or empty string)
                        if current_comment not in (None, ""):
                            dq_issues.append({
                                "table_name": self.full_table_name(),
                                "column_name": field.name,
                                "check_key": "column_comment_mismatch",
                                "check_value": f"The column {field.name} has comment [{current_comment}] "
                                f"which differs from StructType description [{description}]",
                                "check_dt": now,
                            })
                        log and self.logger.warning(
                            f"Column `{field.name}` in {self.full_table_name()} has comment '{current_comment}' "
                            f"which differs from StructType description '{description}'. Updating.")
                        safe_comment = description.replace('"', '\\"')
                        sql = (
                            f"ALTER TABLE `{self.catalog_name}`.`{self.schema_name}`.`{self.table_name}` "
                            + 'CHANGE COLUMN `' + field.name + '` COMMENT "' + safe_comment + '"'
                        )
                        spark.sql(sql)
                    else:
                        log and self.logger.info(
                            f"Column `{field.name}` in {self.full_table_name()} already has the correct comment "
                            f"[{current_comment}]")

            # Return a DataFrame of DQ issues if any, else None
            if dq_issues:
                dq_df = spark.createDataFrame(dq_issues, schema=dq_schema)
                return dq_df
            else:
                return spark.createDataFrame([], schema=dq_schema)

    def change_uc_table_comment(self, log: bool = True, spark: Optional[SparkSession] = None):
        """
        For Unity Catalog tables, set the table comment using self.table_description.
        Only update if the current comment is different. Log a warning if the current comment differs.
        Args:
            log (bool): Whether to log the operation.
            spark (Optional[SparkSession]): The Spark session to use. If None, uses self.spark.
        Returns:
            None
        """
        # Define the schema for the DQ issues DataFrame
        dq_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("check_key", StringType(), True),
            StructField("check_value", StringType(), True),
            StructField("check_dt", TimestampType(), True),
        ])
        if spark is None:
            spark = self.spark
        if not self.is_unity_catalog():
            self.logger.error(f"change_uc_table_comment is only supported for Unity Catalog tables, "
                              f"not for {self.full_table_name()}.")
            return spark.createDataFrame([], dq_schema)
        else:
            # initialize the issues list and the current date and time
            dq_issues = []
            now = datetime.now()
            # Fetch current table comment from the catalog
            table_info = spark.sql(
                f"DESCRIBE TABLE EXTENDED `{self.catalog_name}`.`{self.schema_name}`.`{self.table_name}`"
            ).toPandas()
            # Find the row with col_name == 'Comment'
            current_comment = None
            for _, row in table_info.iterrows():
                if str(row['col_name']).strip().lower() == 'comment':
                    current_comment = row['data_type'] if 'data_type' in row else row.get('comment', None)
                    break
            # if there is no table description, check the current table comment and log if there is a mismatch
            if not self.table_description:
                if current_comment is not None:
                    dq_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": None,
                        "check_key": "table_comment_missing_from_structtype",
                        "check_value": current_comment,
                        "check_dt": now,
                    })
                    log and self.logger.warning(
                        f"Table description is missing for {self.full_table_name()}, skipping."
                    )
                    return spark.createDataFrame(dq_issues, schema=dq_schema)
                else:
                    log and self.logger.info(
                        f"Table {self.full_table_name()} already has the correct comment.")
                    return spark.createDataFrame([], schema=dq_schema)
            else:
                # Compare and update if needed
                if current_comment != self.table_description:
                    dq_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": None,
                        "check_key": "table_comment_mismatch",
                        "check_value": f"The table comment is [{current_comment}] "
                        f"which differs from StructType description [{self.table_description}]",
                        "check_dt": now,
                    })
                    if current_comment is not None:
                        log and self.logger.warning(
                            f"Table comment for {self.full_table_name()} is '{current_comment}' "
                            f"but StructType description is '{self.table_description}'. Updating.")
                    else:
                        log and self.logger.info(
                            f"Setting table comment for {self.full_table_name()} to '{self.table_description}'")
                    # Escape single quotes in the description for SQL
                    safe_description = self.table_description.replace('"', '\\"')
                    sql = (
                        f"ALTER TABLE `{self.catalog_name}`.`{self.schema_name}`.`{self.table_name}` "
                        + 'SET TBLPROPERTIES ("comment" = "' + safe_description + '")'
                    )
                    spark.sql(sql)
                    log and self.logger.info(
                        f"Set table comment for {self.full_table_name()}: {self.table_description}"
                    )
                    return spark.createDataFrame([], schema=dq_schema)
                else:
                    log and self.logger.info(
                        f"Table {self.full_table_name()} already has the correct comment.")
                dq_df = spark.createDataFrame(dq_issues, schema=dq_schema)
                return dq_df

    def drop_uc_table_primary_keys(self, spark: Optional[SparkSession] = None):
        """
        Drops all primary key constraints from the table (Unity Catalog only), without CASCADE.
        Returns a DQ DataFrame of PK drop actions/issues.
        Args:
            spark: SparkSession
        Returns:
            DataFrame of PK drop issues (DQ style)
        """
        dq_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("check_key", StringType(), True),
            StructField("check_value", StringType(), True),
            StructField("check_dt", TimestampType(), True),
        ])
        if spark is None:
            spark = self.spark
        if not self.is_unity_catalog():
            self.logger.error(f"drop_uc_table_primary_keys is only supported for Unity Catalog tables, "
                              f"not for {self.full_table_name()}.")
            return spark.createDataFrame([], dq_schema)
        else:
            now = datetime.now()
            pk_issues = []
            # Find all PK constraints in the constraints section
            desc_ext = spark.sql(f"DESCRIBE TABLE EXTENDED {self.full_table_name()}").collect()
            in_constraints = False
            pk_constraints = []
            for row in desc_ext:
                if getattr(row, 'col_name', '').strip().lower() == "# constraints":
                    in_constraints = True
                    continue
                if in_constraints:
                    if not getattr(row, 'col_name', '').strip() or getattr(row, 'col_name', '').strip().startswith("#"):
                        break
                    dtype = getattr(row, 'data_type', '').strip().upper()
                    if dtype.startswith("PRIMARY KEY"):
                        pk_constraints.append(getattr(row, 'col_name', '').strip())
            # Drop each PK constraint (without CASCADE)
            for pk_name in pk_constraints:
                try:
                    spark.sql(f"ALTER TABLE {self.full_table_name()} DROP CONSTRAINT {pk_name}")
                    self.logger.info(f"Dropped primary key constraint {pk_name} from {self.full_table_name()}")
                except Exception as e:
                    self.logger.warning(f"Failed to drop primary key constraint {pk_name} from {self.full_table_name()}: {e}")
                    pk_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": None,
                        "check_key": "pk_drop_failed",
                        "check_value": pk_name,
                        "check_dt": now,
                    })
            pk_issues_df = spark.createDataFrame(pk_issues, schema=dq_schema)
            return pk_issues_df

    def drop_uc_table_foreign_keys(self, spark: Optional[SparkSession] = None):
        """
        Drops all foreign key constraints from the table (Unity Catalog only), without CASCADE.
        Returns a DQ DataFrame of FK drop actions/issues.
        Args:
            spark: SparkSession
        Returns:
            DataFrame of FK drop issues (DQ style)
        """
        dq_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("check_key", StringType(), True),
            StructField("check_value", StringType(), True),
            StructField("check_dt", TimestampType(), True),
        ])
        if spark is None:
            spark = self.spark
        if not self.is_unity_catalog():
            self.logger.error(f"drop_uc_table_foreign_keys is only supported for Unity Catalog tables, "
                              f"not for {self.full_table_name()}.")
            return spark.createDataFrame([], dq_schema)
        else:
            now = datetime.now()
            fk_issues = []
            # Find all FK constraints in the constraints section
            desc_ext = spark.sql(f"DESCRIBE TABLE EXTENDED {self.full_table_name()}").collect()
            in_constraints = False
            fk_constraints = []
            for row in desc_ext:
                if getattr(row, 'col_name', '').strip().lower() == "# constraints":
                    in_constraints = True
                    continue
                if in_constraints:
                    if not getattr(row, 'col_name', '').strip() or getattr(row, 'col_name', '').strip().startswith("#"):
                        break
                    dtype = getattr(row, 'data_type', '').strip().upper()
                    if dtype.startswith("FOREIGN KEY"):
                        fk_constraints.append(getattr(row, 'col_name', '').strip())
            # Drop each FK constraint (without CASCADE)
            for fk_name in fk_constraints:
                try:
                    spark.sql(f"ALTER TABLE {self.full_table_name()} DROP CONSTRAINT {fk_name}")
                    self.logger.info(f"Dropped foreign key constraint {fk_name} from {self.full_table_name()}")
                except Exception as e:
                    self.logger.warning(f"Failed to drop foreign key constraint {fk_name} from {self.full_table_name()}: {e}")
                    fk_issues.append({
                        "table_name": self.full_table_name(),
                        "column_name": None,
                        "check_key": "fk_drop_failed",
                        "check_value": fk_name,
                        "check_dt": now,
                    })
            fk_issues_df = spark.createDataFrame(fk_issues, schema=dq_schema)
            return fk_issues_df
