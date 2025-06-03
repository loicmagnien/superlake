import os
import importlib.util
import inspect
from pathlib import Path
from superlake.core import SuperDeltaTable
import pyspark.sql.types as T
from superlake.core.delta import TableSaveMode


class SuperCatalogQualityTable:
    """
    Utility class to persist data quality issues (from change_uc_table_comment and change_uc_columns_comments)
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
            T.StructField("issue_type", T.StringType(), True),
            T.StructField("current_value", T.StringType(), True),
            T.StructField("expected_value", T.StringType(), True),
            T.StructField("checked_at", T.TimestampType(), False),
        ])
        self.dq_table = SuperDeltaTable(
            super_spark=self.super_spark,
            catalog_name=self.catalog_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
            table_schema=self.dq_schema,
            table_save_mode=TableSaveMode.Append,
            primary_keys=["table_name", "column_name", "issue_type", "checked_at"],
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

    def process_table_and_columns_comments(
            self,
            table,
            change_table_comments,
            change_column_comments,
            persist_catalog_quality,
            super_catalog_quality_table
            ):
        """
        Helper to process table/column comments and persist DQ issues if requested.
        Returns (table_dq_df, columns_dq_df)
        """
        # create empty dataframes to store dq issues
        spark = super_catalog_quality_table.spark
        table_dq_df = spark.createDataFrame([], schema=super_catalog_quality_table.dq_schema)
        columns_dq_df = spark.createDataFrame([], schema=super_catalog_quality_table.dq_schema)
        # change table and columns comments
        if change_table_comments:
            table_dq_df = table.change_uc_table_comment()
        if change_column_comments:
            columns_dq_df = table.change_uc_columns_comments()
        # persist dq issues
        if persist_catalog_quality:
            if table_dq_df is not None and table_dq_df.count() > 0:
                super_catalog_quality_table.save_dq_df(table_dq_df)
            if columns_dq_df is not None and columns_dq_df.count() > 0:
                super_catalog_quality_table.save_dq_df(columns_dq_df)

    def register_model_tables(
            self, super_spark, catalog_name: str, logger, managed: bool, superlake_dt,
            register_tables: bool = True,
            change_table_comments: bool = True,
            change_column_comments: bool = True,
            persist_catalog_quality: bool = False,
            super_catalog_quality_table: 'SuperCatalogQualityTable' = None
            ):
        """
        Register all model tables found in the modelisation directory.
        """
        generators = self.find_table_generators(self.modelisation_dir, 'get_model_')
        for generator in generators:
            try:
                table, _ = generator(super_spark, catalog_name, logger, managed, superlake_dt)
                if isinstance(table, SuperDeltaTable):
                    if not table.table_exists():
                        logger.warning(f"Table {table.full_table_name()} does not exist. Skipping registration and comment update.")
                        continue
                    if register_tables:
                        table.register_table_in_catalog()
                    # process and persist DQ issues
                    self.process_table_and_columns_comments(
                        table, change_table_comments, change_column_comments,
                        persist_catalog_quality, super_catalog_quality_table)
                    logger.info(f"Processed model table: {table.full_table_name()}")
            except Exception as e:
                logger.error(f"Error processing model table from {generator.__name__}: {e}")

    def register_ingestion_tables(
            self, super_spark, catalog_name: str, logger, managed: bool, superlake_dt,
            register_tables: bool = True,
            change_table_comments: bool = True,
            change_column_comments: bool = True,
            persist_catalog_quality: bool = False,
            super_catalog_quality_table: 'SuperCatalogQualityTable' = None
            ):
        """
        Register all ingestion tables found in the ingestion directory.
        """
        generators = self.find_table_generators(self.ingestion_dir, 'get_pipeline_objects_')
        for generator in generators:
            try:
                bronze, silver, *_ = generator(super_spark, catalog_name, logger, managed, superlake_dt)
                for table in (bronze, silver):
                    if isinstance(table, SuperDeltaTable):
                        if not table.table_exists():
                            logger.warning(f"Table {table.full_table_name()} does not exist. Skipping registration and comment update.")
                            continue
                        if register_tables:
                            table.register_table_in_catalog()
                        # process and persist DQ issues
                        self.process_table_and_columns_comments(
                            table, change_table_comments, change_column_comments,
                            persist_catalog_quality, super_catalog_quality_table)
                        logger.info(f"Processed ingestion table: {table.full_table_name()}")
            except Exception as e:
                logger.error(f"Error processing ingestion table from {generator.__name__}: {e}")

    def execute(self, super_spark, catalog_name: str, logger, managed: bool, superlake_dt,
                register_tables: bool = True,
                change_table_comments: bool = True,
                change_column_comments: bool = True,
                persist_catalog_quality: bool = False,
                super_catalog_quality_table: 'SuperCatalogQualityTable' = None
                ):
        """
        Process all model and ingestion tables in the lakehouse project.
        """
        logger.info('Processing ingestion tables...')
        self.register_ingestion_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            register_tables, change_table_comments, change_column_comments, 
            persist_catalog_quality, super_catalog_quality_table
        )
        logger.info('Processing model tables...')
        self.register_model_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            register_tables, change_table_comments, change_column_comments,
            persist_catalog_quality, super_catalog_quality_table
        )
