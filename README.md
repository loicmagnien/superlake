# superlake
A modern, intuitive Python package for data lakehouse operations

## Main SuperLake Classes

- **SuperSpark**: Instantiates a SparkSession with Delta Lake support.
- **SuperDeltaTable**: Manages Delta tables (create, read, write, optimize, vacuum, SCD2, schema evolution, etc.).
- **SuperPipeline**: Orchestrates data pipelines from source to bronze and silver layers, including CDC and transformation logic.
- **SuperGoldPipeline**: Manages gold-layer aggregations and writes results to gold tables.
- **SuperDataframe**: Utility class for DataFrame cleaning, casting, and manipulation.
- **SuperLogger**: Logging and metrics for pipeline operations.

## Quick Example Usage

```python
from superlake.core import SuperSpark, SuperDeltaTable, TableSaveMode, SchemaEvolution, SuperPipeline, SuperGoldPipeline
from superlake.monitoring import SuperLogger
import pyspark.sql.types as T
from datetime import datetime

# Initialize Spark and logger
spark = SuperSpark()
logger = SuperLogger()
superlake_dt = datetime.now()

# Define a Delta table
bronze_customer = SuperDeltaTable(
    table_name="01_bronze.customer",
    table_path="/path/to/bronze/customer",
    table_schema=T.StructType([
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("signup_date", T.DateType(), True),
        T.StructField("superlake_dt", T.TimestampType(), True)
    ]),
    table_save_mode=TableSaveMode.Append,
    primary_keys=["customer_id"],
    partition_cols=["superlake_dt"],
    schema_evolution_option=SchemaEvolution.Merge,
    logger=logger,
    managed=True
)

# Define CDC and transformation functions
def customer_cdc(spark):
    # Return a DataFrame with new/changed customer data
    ...

def customer_tra(df):
    # Clean and transform customer data
    return df

# Create and run a pipeline
customer_pipeline = SuperPipeline(
    superlake_dt=superlake_dt,
    bronze_table=bronze_customer,
    silver_table=...,  # another SuperDeltaTable
    cdc_function=customer_cdc,
    tra_function=customer_tra,
    logger=logger,
    spark=spark,
    environment="test"
)
customer_pipeline.execute()
```

See `example/superlake_example.py` for a full pipeline example, including gold table aggregation.
