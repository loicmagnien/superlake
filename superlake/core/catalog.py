import os
import importlib.util
import inspect
from pathlib import Path
import pyspark.sql.types as T
from superlake.core import SuperDeltaTable, SuperDataframe, TableSaveMode


class SuperCatalogQualityTable:
    """
    Utility class to persist data quality issues
    in a Delta table, similar to SuperTracer.
    """
    def __init__(self, super_spark, catalog_name, schema_name, table_name, managed, logger):
        self.super_spark = super_spark
        self.spark = super_spark.spark
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.logger = logger
        self.managed = managed
        self.dq_schema = T.StructType([
            T.StructField("table_name", T.StringType(), True),
            T.StructField("column_name", T.StringType(), True),
            T.StructField("check_key", T.StringType(), True),
            T.StructField("check_value", T.StringType(), True),
            T.StructField("check_dt", T.TimestampType(), False),
        ])
        self.dq_table = SuperDeltaTable(
            super_spark=self.super_spark,
            catalog_name=self.catalog_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
            table_schema=self.dq_schema,
            table_save_mode=TableSaveMode.Append,
            primary_keys=["table_name", "column_name", "check_key", "check_dt"],
            managed=self.managed
        )

    def ensure_table_exists(self):
        self.dq_table.ensure_table_exists(self.spark, log=False)

    def save_dq_df(self, dq_df):
        self.ensure_table_exists()
        if dq_df is not None and dq_df.count() > 0:
            self.dq_table.save(dq_df, mode="append", spark=self.spark, log=False)
            self.logger.info(f"Persisted {dq_df.count()} DQ issues to {self.dq_table.full_table_name()}")
        else:
            self.logger.info("No DQ issues to persist.")


class SuperCataloguer:
    """
    Utility class to discover and register all model and ingestion tables in a SuperLake lakehouse project.
    """
    def __init__(
        self,
        project_root: str,
        modelisation_folder: str = "modelisation",
        ingestion_folder: str = "ingestion"
    ):
        self.project_root = project_root
        self.modelisation_folder = modelisation_folder
        self.ingestion_folder = ingestion_folder
        self.modelisation_dir = os.path.join(self.project_root, self.modelisation_folder)
        self.ingestion_dir = os.path.join(self.project_root, self.ingestion_folder)

    def find_table_generators(self, base_dir: str, generator_prefix: str) -> list:
        """
        Discover all generator functions in Python files under base_dir whose names start with generator_prefix.
        """
        generators = []
        base_dir = str(base_dir)
        for root, _, files in os.walk(base_dir):
            for file in files:
                if file.endswith('.py') and not file.startswith('__'):
                    path = os.path.join(root, file)
                    module_name = Path(path).with_suffix('').as_posix().replace('/', '.')
                    spec = importlib.util.spec_from_file_location(module_name, path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    for name, obj in inspect.getmembers(module):
                        if name.startswith(generator_prefix) and inspect.isfunction(obj):
                            generators.append(obj)
        return generators

    def collect_all_tables(
        self, super_spark, catalog_name, logger, managed, superlake_dt,
        check_table_exists=True, target_tables=None
    ):
        """
        Collect all SuperDeltaTable objects from both modelisation and ingestion folders that exist.
        Optionally register them in the catalog. If target_tables is provided, only process tables whose fully qualified name matches.
        """
        all_tables = []
        for base_dir, generator_prefix, extract_tables_fn in [
            (self.modelisation_dir, 'get_model_', lambda result: [result[0]]),
            (self.ingestion_dir, 'get_pipeline_objects_', lambda result: [result[0], result[1]])
        ]:
            generators = self.find_table_generators(base_dir, generator_prefix)
            for generator in generators:
                try:
                    result = generator(super_spark, catalog_name, logger, managed, superlake_dt)
                    for table in extract_tables_fn(result):
                        # Only process if in target_tables (if specified)
                        if target_tables is not None and hasattr(table, 'full_table_name'):
                            if table.full_table_name() not in target_tables:
                                continue
                        if isinstance(table, SuperDeltaTable) and table.table_exists():
                            all_tables.append(table)
                        else:
                            if check_table_exists is False:
                                all_tables.append(table)
                            else:
                                table_name = getattr(table, 'full_table_name', lambda: str(table))()
                                warn_msg = (
                                    f"Table {table_name} does not exist. Skipping."
                                )
                                logger.warning(warn_msg)
                except Exception as e:
                    logger.error(f"Error processing table from {generator.__name__}: {e}")
        return all_tables

    def _process_tables(
        self, super_spark, catalog_name, logger, managed, superlake_dt,
        table_op, check_table_exists,
        persist_catalog_quality=False, super_catalog_quality_table=None,
        target_tables=None
    ):
        """
        Helper to process all tables with a single operation, handle DQ persistence and logging.
        If target_tables is provided, only process those tables.
        """
        all_tables = self.collect_all_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            check_table_exists=check_table_exists,
            target_tables=target_tables
        )
        dq_dfs = []
        for table in all_tables:
            dq_df = table_op(table)
            if persist_catalog_quality and super_catalog_quality_table:
                has_dq = False
                if dq_df is not None and hasattr(dq_df, 'count'):
                    try:
                        has_dq = dq_df.count() > 0
                    except Exception as e:
                        logger.error(f"Error calling count() on dq_df for table {getattr(table, 'full_table_name', lambda: str(table))()}: {e}")
                        has_dq = False
                else:
                    if dq_df is None:
                        logger.warning(f"dq_df is None for table {getattr(table, 'full_table_name', lambda: str(table))()}")
                    else:
                        logger.warning(f"dq_df is not a DataFrame for table {getattr(table, 'full_table_name', lambda: str(table))()}: {type(dq_df)}")
                if has_dq:
                    dq_dfs.append(dq_df)
        if persist_catalog_quality and super_catalog_quality_table and dq_dfs:
            combined_dq_df = SuperDataframe.super_union_by_name(dq_dfs)
            super_catalog_quality_table.save_dq_df(combined_dq_df)

    def register_tables(self, super_spark, catalog_name, logger, managed, superlake_dt, target_tables=None):
        logger.info("Registering tables in catalog...")
        self._process_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            table_op=lambda t: t.register_table_in_catalog(),
            check_table_exists=True,
            target_tables=target_tables
        )

    def ensure_tables_exist(self, super_spark, catalog_name, logger, managed, superlake_dt, target_tables=None):
        logger.info("Ensuring tables exist...")
        self._process_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            table_op=lambda t: t.ensure_table_exists(),
            check_table_exists=False,
            target_tables=target_tables
        )

    def apply_table_comment(
        self, super_spark, catalog_name, logger, managed, superlake_dt,
        persist_catalog_quality=False, super_catalog_quality_table=None,
        target_tables=None
    ):
        logger.info("Applying table comments to tables...")
        self._process_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            table_op=lambda t: t.change_uc_table_comment(),
            check_table_exists=True,
            persist_catalog_quality=persist_catalog_quality,
            super_catalog_quality_table=super_catalog_quality_table,
            target_tables=target_tables
        )

    def apply_columns_comments(
        self, super_spark, catalog_name, logger, managed, superlake_dt,
        persist_catalog_quality=False, super_catalog_quality_table=None,
        target_tables=None
    ):
        logger.info("Applying columns comments to tables...")
        self._process_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            table_op=lambda t: t.change_uc_columns_comments(),
            check_table_exists=True,
            persist_catalog_quality=persist_catalog_quality,
            super_catalog_quality_table=super_catalog_quality_table,
            target_tables=target_tables
        )

    def drop_primary_keys(
        self, super_spark, catalog_name, logger, managed, superlake_dt,
        persist_catalog_quality=False, super_catalog_quality_table=None,
        target_tables=None
    ):
        logger.info("Dropping primary keys for tables...")
        self._process_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            table_op=lambda t: t.drop_uc_table_primary_keys(spark=t.spark),
            check_table_exists=True,
            persist_catalog_quality=persist_catalog_quality,
            super_catalog_quality_table=super_catalog_quality_table,
            target_tables=target_tables
        )

    def drop_foreign_keys(
        self, super_spark, catalog_name, logger, managed, superlake_dt,
        persist_catalog_quality=False, super_catalog_quality_table=None,
        target_tables=None
    ):
        logger.info("Dropping foreign keys for tables...")
        self._process_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            table_op=lambda t: t.drop_uc_table_foreign_keys(spark=t.spark),
            check_table_exists=True,
            persist_catalog_quality=persist_catalog_quality,
            super_catalog_quality_table=super_catalog_quality_table,
            target_tables=target_tables
        )

    def create_primary_keys(
        self, super_spark, catalog_name, logger, managed, superlake_dt,
        force_create_primary_keys=False,
        persist_catalog_quality=False, super_catalog_quality_table=None,
        target_tables=None
    ):
        logger.info("Creating primary keys for tables...")
        self._process_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            table_op=lambda t: t.create_uc_table_primary_keys(spark=t.spark, force_create=force_create_primary_keys),
            check_table_exists=True,
            persist_catalog_quality=persist_catalog_quality,
            super_catalog_quality_table=super_catalog_quality_table,
            target_tables=target_tables
        )

    def create_foreign_keys(
        self, super_spark, catalog_name, logger, managed, superlake_dt,
        force_create_foreign_keys=False,
        persist_catalog_quality=False, super_catalog_quality_table=None,
        target_tables=None
    ):
        logger.info("Starting creation of foreign keys for tables...")
        self._process_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            table_op=lambda t: t.create_uc_table_foreign_keys(spark=t.spark, force_create=force_create_foreign_keys),
            check_table_exists=True,
            persist_catalog_quality=persist_catalog_quality,
            super_catalog_quality_table=super_catalog_quality_table,
            target_tables=target_tables
        )
