"""Spark session management for SuperLake."""
import sys
from pyspark.sql import SparkSession


class SuperSpark:
    def __init__(self,
                 session_name: str,
                 warehouse_dir: str = None,
                 external_path: str = None,
                 catalog_name: str = None
                 ):
        """
        Initialize a Spark session with Delta Lake support and metastore/catalog configuration.

        Args:
            session_name (str): Name of the Spark session.
            Usually different for each pipeline and environment.
            The session name helps you trace which job produced which logs or metrics.

            warehouse_dir (str): Path to the Spark SQL warehouse directory.
            This is the directory where managed Delta Lake tables will be stored.
            example:
                - legacy: spark.sql.warehouse.dir + /schema.db/table/
                - databricks: dbfs:/user/hive/warehouse + /schema.db/table/
                - Unity Catalog : looks like abfss://container@account.dfs.core.windows.net/UUID/
                metastore_default_location = (
                    spark.sql("SHOW EXTERNAL LOCATIONS")
                    .filter("name = 'metastore_default_location'")
                    .select("url").collect()[0][0]
                )
                this is the metastore_default_location made of a storage url and a UUID,
                the tables are in metastore_default_location + /tables/UUID/

            external_path (str): Root path for external tables.
            This is the directory where external tables will be stored.
            SuperLake uses external tables to store data in a storage account / location.
            example:
                - legacy: /User/data/custom_path/ + schema/table/
                - databricks: dbfs:/ or /mnt/ + custom_path/ + schema/table/
                - Unity Catalog : abfss://container@account.dfs.core.windows.net/ + custom_path/ + schema/table/
                where abfss://container@account.dfs.core.windows.net is an external_location.
                existing external locations can be found with spark.sql("SHOW EXTERNAL LOCATIONS")

            catalog_name (str): Name of the catalog to use (for Unity Catalog).
            example:
                - legacy: hive_metastore
                - databricks: spark_catalog (alias for hive_metastore)
                - Unity Catalog : main, dev, prod, my_company_catalog
        """
        self.session_name = session_name
        self.warehouse_dir = warehouse_dir
        self.external_path = external_path
        self.catalog_name = catalog_name
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
            # Purpose: Sets the name of the Spark application (as seen in Spark UI, logs, etc.).
            # Why: Helps to identify the job in Spark’s monitoring tools.
            .appName(self.session_name)
            # Purpose: Attaches the Delta Lake library to the Spark session.
            # Why: Required for Delta Lake support (ACID tables, time travel, etc.).
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
            # Purpose: Enables Delta Lake's SQL extensions.
            # Why: Allows to use Delta Lake's SQL commands (e.g., CREATE TABLE AS SELECT).
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            # Purpose: Configures the Delta Lake catalog.
            # Why: Allows to use Delta Lake's catalog commands (e.g., CREATE TABLE AS SELECT).
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Purpose: Sets the warehouse directory for Delta Lake.
            # Why: Specifies where Delta Lake tables will be stored.
            .config("spark.sql.warehouse.dir", self.warehouse_dir)
            # Purpose: Ensures the driver and executor use the same Python interpreter.
            # Why: Maintains consistency across the Spark cluster.
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
        )

        if self.catalog_name:
            # Purpose: Sets the default catalog for the Spark session.
            # Why: Specifies the catalog to use when no other catalog is specified.
            builder = builder.config("spark.sql.defaultCatalog", self.catalog_name)
            # Purpose: Sets the catalog for the Spark session.
            # Why: Specifies the catalog to use when no other catalog is specified.
            builder = builder.config("spark.sql.catalog", self.catalog_name)

        # create the spark session
        spark = builder.getOrCreate()

        # set the verbosity of the spark session
        spark.conf.set("spark.sql.debug.maxToStringFields", 2000)
        spark.sparkContext.setLogLevel("ERROR")

        return spark
