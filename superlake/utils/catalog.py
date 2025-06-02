import os
import importlib.util
import inspect
from pathlib import Path
from superlake.core import SuperDeltaTable


class SuperCataloguer:
    """
    Utility class to discover and register all model and ingestion tables in a SuperLake lakehouse project.
    """
    def __init__(self, project_root: str, modelisation_folder: str = "modelisation", ingestion_folder: str = "ingestion"):
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

    def register_model_tables(self, super_spark, catalog_name: str, logger, managed: bool, superlake_dt,
                              register_tables: bool = True,
                              change_table_and_columns_comments: bool = True,
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
                    if change_table_and_columns_comments:
                        table.change_uc_table_and_columns_comments()
                    logger.info(f"Processed model table: {table.full_table_name()}")
            except Exception as e:
                logger.error(f"Error processing model table from {generator.__name__}: {e}")

    def register_ingestion_tables(self, super_spark, catalog_name: str, logger, managed: bool, superlake_dt,
                                  register_tables: bool = True,
                                  change_table_and_columns_comments: bool = True,
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
                        if change_table_and_columns_comments:
                            table.change_uc_table_and_columns_comments()
                        logger.info(f"Processed ingestion table: {table.full_table_name()}")
            except Exception as e:
                logger.error(f"Error processing ingestion table from {generator.__name__}: {e}")

    def execute(self, super_spark, catalog_name: str, logger, managed: bool, superlake_dt,
                register_tables: bool = True,
                change_table_and_columns_comments: bool = True,
                ):
        """
        Process all model and ingestion tables in the lakehouse project.
        """
        logger.info('Processing ingestion tables...')
        self.register_ingestion_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            register_tables, change_table_and_columns_comments
        )
        logger.info('Processing model tables...')
        self.register_model_tables(
            super_spark, catalog_name, logger, managed, superlake_dt,
            register_tables, change_table_and_columns_comments
        )
