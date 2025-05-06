"""Spark session management for SuperLake."""
 
import sys
from pyspark.sql import SparkSession

def SuperSpark() -> SparkSession:
    """Create Spark session with Delta Lake support."""
    # build a spark session
    spark = (
        SparkSession.builder
        .appName("PySparkLakeCraft Example")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "./data/spark-warehouse")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    # manage verbosity 
    spark.conf.set("spark.sql.debug.maxToStringFields", 2000)
    spark.sparkContext.setLogLevel("ERROR")

    return spark
