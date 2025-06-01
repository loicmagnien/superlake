import pyspark.sql.functions as F
from superlake.core import TableSaveMode, SuperDataframe

# ------------------------------------------------------------------------------------------------
# TODO:
# - add a function to check the rows in the fact without rows in the dimensions
#   (rename store_key with dim_store_key for programmatic use)
# - add a function to fill the dimension with unmatched surrogate keys from the fact table
# ------------------------------------------------------------------------------------------------


class SuperModeler:
    @staticmethod
    def generate_dimension(super_spark, superlake_dt, source_table, source_columns, source_keys, sink_table):
        """
        Generate a dimension table from a source table.
        the function will rename the superlake_dt column to source_superlake_dt
        and add a new column source_superlake_dt with the superlake_dt value.
        the function will also generate a surrogate key for the source table.
        the sink table is used to get the primary key of the dimension table.
        Args:
            super_spark (SuperSpark): The SuperSpark object.
            superlake_dt (str): The superlake timestamp.
            source_table (SuperDataframe): The source table.
            source_columns (list): The source columns.
            source_keys (list): The source keys.
            sink_table (SuperDataframe): The sink table.
        """
        spark = super_spark.spark
        mode = sink_table.table_save_mode
        if mode == TableSaveMode.Overwrite:
            df = source_table.read(spark)
        elif mode in [TableSaveMode.MergeSCD, TableSaveMode.Merge, TableSaveMode.Append]:
            if sink_table.table_exists(spark):
                max_superlake_dt = sink_table.read(spark).select(F.max("superlake_dt")).collect()[0][0]
                df = source_table.read(spark).filter(F.col("superlake_dt") > max_superlake_dt)
            else:
                df = source_table.read(spark)
        else:
            raise ValueError(f"Invalid mode: {mode}")
        not_null_filters = [F.col(col).isNotNull() for col in source_keys]
        combined_filter = not_null_filters[0]
        for f in not_null_filters[1:]:
            combined_filter = combined_filter & f
        df = df.select(*source_columns + ["superlake_dt"]).dropDuplicates().filter(combined_filter)
        df = df.withColumnRenamed("superlake_dt", "source_superlake_dt")
        surrogate_key_name = sink_table.primary_keys[0]
        final_columns = [surrogate_key_name] + source_columns + ["source_superlake_dt"]
        df = SuperDataframe(df).generate_surrogate_key(
            field_list=source_keys,
            key_column_name=surrogate_key_name
        ).select(*final_columns)
        df = df.withColumn("superlake_dt", F.lit(superlake_dt))
        return df
