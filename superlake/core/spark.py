"""Spark session management for SuperLake."""
 
import sys
import os
import yaml
from pyspark.sql import SparkSession

# Helper to load config.yaml
CONFIG_PATH = os.environ.get("SUPERLAKE_CONFIG", "config.yaml")


# Load the config.yaml file
def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f)
    return {}


class SuperSpark:
    def __init__(self, 
                 session_name: str,
                 warehouse_dir: str = None, 
                 external_path: str = None, 
                 catalog_name: str = None
                 ):
        """
        Initialize a Spark session with Delta Lake support and store configuration.

        Args:
            warehouse_dir (str): Path to the Spark SQL warehouse directory. Defaults to './data/spark-warehouse'.
            external_path (str): Root path for external tables. Defaults to './data/external-table/'.
            catalog_name (str): Name of the catalog to use (for Unity Catalog). Defaults to None.
        """
        config = load_config()
        self.session_name = session_name
        self.warehouse_dir = warehouse_dir or config.get("warehouse_dir", "./data/spark-warehouse")
        self.external_path = external_path or config.get("external_path", "./data/external-table")
        self.catalog_name = catalog_name or config.get("catalog_name")
        
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """
        Create a Spark session with Delta Lake support.
        Args:
            None
        Returns:
            SparkSession: The Spark session.
        """

        builder = (
            SparkSession.builder
            .appName(self.session_name)
            # add delta lake support
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # set the warehouse directory
            .config("spark.sql.warehouse.dir", self.warehouse_dir)
            # set the python path so driver and executor use the same python interpreter
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
        )
        spark = builder.getOrCreate()

        # set the catalog name
        if self.catalog_name:
            spark.conf.set("spark.sql.catalog", self.catalog_name)

        # set the verbosity of the spark session
        spark.conf.set("spark.sql.debug.maxToStringFields", 2000)
        spark.sparkContext.setLogLevel("ERROR")

        return spark
